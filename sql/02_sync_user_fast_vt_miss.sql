-- 全量阶段 2：宽表中无 mobile_token 的用户，运行时 UDF 调 VT /v2t
-- 前置: 阶段 1（02_sync_user_fast.sql）已完成；user_sync_staging 已重建
-- 并行建议: FLINK_PARALLELISM_VT_MISS=2（避免打满 VT 接口）
-- 执行: ./scripts/run-user-fast-vt-miss.sh

CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_user_staging_miss (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    reg_time BIGINT,
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    mobile_norm STRING,
    mobile_token STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_sync_staging',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_user (
    user_id BIGINT,
    app_id INT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag TINYINT,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_term STRING,
    campaign_id STRING,
    ad_group_id STRING,
    advertiser_id STRING,
    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user
SELECT
    e.user_id,
    e.app_id,
    e.group_user_id,
    e.info_user_id,
    e.mobile_token,
    e.closed_time,
    e.reg_device_uuid,
    e.reg_time,
    e.test_flag,
    e.utm_source,
    e.utm_medium,
    e.utm_campaign,
    e.utm_content,
    e.utm_term,
    e.campaign_id,
    e.ad_group_id,
    e.advertiser_id
FROM (
    SELECT
        s.id + 100000000 AS user_id,
        CAST(s.app_code AS INT) AS app_id,
        s.id + 100000000 AS group_user_id,
        s.id + 100000000 AS info_user_id,
        vt_tokenize(s.mobile_norm) AS mobile_token,
        CAST(0 AS BIGINT) AS closed_time,
        COALESCE(s.device_id, '') AS reg_device_uuid,
        COALESCE(s.reg_time, CAST(0 AS BIGINT)) AS reg_time,
        CAST(0 AS TINYINT) AS test_flag,
        CASE
            WHEN COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), '')) IS NULL
                OR TRIM(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) = ''
                THEN CAST(NULL AS STRING)
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%unattributed%'
                THEN CAST(NULL AS STRING)
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%organic%'
                THEN 'organic'
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%google%'
                THEN 'google'
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%tiktok%'
                THEN 'tiktok'
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%facebook%'
                OR LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%instagram%'
                OR LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%messenger%'
                THEN 'facebook'
            WHEN LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%kuai%'
                OR LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%kwai%'
                OR LOWER(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))) LIKE '%kuaishou%'
                THEN 'kwai'
            ELSE LOWER(TRIM(COALESCE(NULLIF(TRIM(s.network_name), ''), NULLIF(TRIM(s.tracker_name), ''))))
        END AS utm_source,
        s.campaign_tracker AS utm_medium,
        s.campaign_name AS utm_campaign,
        s.creative_name AS utm_content,
        s.adgroup_tracker AS utm_term,
        s.creative_tracker AS campaign_id,
        s.campaign_tracker AS ad_group_id,
        s.adgroup_tracker AS advertiser_id
    FROM src_user_staging_miss s
    WHERE (s.mobile_token IS NULL OR TRIM(s.mobile_token) = '')
      AND s.mobile_norm IS NOT NULL
      AND TRIM(s.mobile_norm) <> ''
) e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> '';
