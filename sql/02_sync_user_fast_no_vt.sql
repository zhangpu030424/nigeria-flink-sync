-- 全量高速（无 VT）：CDC user_sync_staging → 目标库，mobile 明文直传
-- 用于测速 / 对比 VT 瓶颈；正式环境再换 02_sync_user_fast.sql
--
-- 执行: ./scripts/run-sql.sh sql/02_sync_user_fast_no_vt.sql
-- 自动全量+增量: ./scripts/sync-user-auto.sh --no-vt

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_user_staging (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    create_time TIMESTAMP(3),
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_sync_staging',
    'server-time-zone' = 'Africa/Lagos',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
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
    id + 100000000,
    CAST(app_code AS INT),
    id + 100000000,
    id + 100000000,
    mobile,
    CAST(0 AS BIGINT),
    COALESCE(device_id, ''),
    UNIX_TIMESTAMP(DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000,
    CAST(0 AS TINYINT),
    CASE
        WHEN COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), '')) IS NULL
            OR TRIM(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) = ''
            THEN CAST(NULL AS STRING)
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%unattributed%'
            THEN CAST(NULL AS STRING)
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%organic%'
            THEN 'organic'
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%google%'
            THEN 'google'
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%tiktok%'
            THEN 'tiktok'
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%facebook%'
            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%instagram%'
            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%messenger%'
            THEN 'facebook'
        WHEN LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%kuai%'
            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%kwai%'
            OR LOWER(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))) LIKE '%kuaishou%'
            THEN 'kwai'
        ELSE LOWER(TRIM(COALESCE(NULLIF(TRIM(network_name), ''), NULLIF(TRIM(tracker_name), ''))))
    END,
    campaign_tracker,
    campaign_name,
    creative_name,
    adgroup_tracker,
    creative_tracker,
    campaign_tracker,
    adgroup_tracker
FROM src_user_staging;
