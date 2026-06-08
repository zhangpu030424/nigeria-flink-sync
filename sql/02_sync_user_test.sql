-- 阶段 B：user 表同步（全量 + 增量）
-- 字段映射见 docs/FIELD_MAPPING.md §3.1
-- 前置：源库执行 sql/ddl/source_views_adjust.sql
-- 执行: ./scripts/run-sql.sh sql/02_sync_user_test.sql

CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    gps_adid STRING,
    idfa STRING,
    idfv STRING,
    status INT,
    create_time TIMESTAMP(3),
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_adjust_by_gps (
    gps_adid STRING,
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (gps_adid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'v_adjust_latest_by_gps_adid',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '100000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_adjust_by_idfa (
    idfa STRING,
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (idfa) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'v_adjust_latest_by_idfa',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '100000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_adjust_by_idfv (
    idfv STRING,
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (idfv) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'v_adjust_latest_by_idfv',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '100000',
    'lookup.cache.ttl' = '2h'
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
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user
SELECT
    t.user_id,
    t.app_id,
    t.group_user_id,
    t.info_user_id,
    vt_tokenize(t.mobile_norm) AS mobile,
    t.closed_time,
    t.reg_device_uuid,
    t.reg_time,
    t.test_flag,
    CASE
        WHEN t.channel_raw IS NULL OR TRIM(t.channel_raw) = '' THEN CAST(NULL AS STRING)
        WHEN LOWER(t.channel_raw) LIKE '%unattributed%' THEN CAST(NULL AS STRING)
        WHEN LOWER(t.channel_raw) LIKE '%organic%' THEN 'organic'
        WHEN LOWER(t.channel_raw) LIKE '%google%' THEN 'google'
        WHEN LOWER(t.channel_raw) LIKE '%tiktok%' THEN 'tiktok'
        WHEN LOWER(t.channel_raw) LIKE '%facebook%'
            OR LOWER(t.channel_raw) LIKE '%instagram%'
            OR LOWER(t.channel_raw) LIKE '%messenger%' THEN 'facebook'
        WHEN LOWER(t.channel_raw) LIKE '%kuai%'
            OR LOWER(t.channel_raw) LIKE '%kwai%'
            OR LOWER(t.channel_raw) LIKE '%kuaishou%' THEN 'kwai'
        ELSE LOWER(TRIM(t.channel_raw))
    END AS utm_source,
    t.adj_campaign_tracker AS utm_medium,
    t.adj_campaign_name AS utm_campaign,
    t.adj_creative_name AS utm_content,
    t.adj_adgroup_tracker AS utm_term,
    t.adj_creative_tracker AS campaign_id,
    t.adj_campaign_tracker AS ad_group_id,
    t.adj_adgroup_tracker AS advertiser_id
FROM (
    SELECT
        u.id + 100000000 AS user_id,
        CAST(u.app_code AS INT) AS app_id,
        u.id + 100000000 AS group_user_id,
        u.id + 100000000 AS info_user_id,
        CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN u.mobile
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END AS mobile_norm,
        CAST(0 AS BIGINT) AS closed_time,
        COALESCE(u.device_id, '') AS reg_device_uuid,
        UNIX_TIMESTAMP(DATE_FORMAT(u.create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000 AS reg_time,
        CAST(0 AS TINYINT) AS test_flag,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.network_name
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.network_name
            WHEN v.create_time IS NOT NULL THEN v.network_name
            ELSE CAST(NULL AS STRING)
        END AS adj_network_name,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.tracker_name
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.tracker_name
            WHEN v.create_time IS NOT NULL THEN v.tracker_name
            ELSE CAST(NULL AS STRING)
        END AS adj_tracker_name,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.campaign_tracker
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.campaign_tracker
            WHEN v.create_time IS NOT NULL THEN v.campaign_tracker
            ELSE CAST(NULL AS STRING)
        END AS adj_campaign_tracker,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.campaign_name
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.campaign_name
            WHEN v.create_time IS NOT NULL THEN v.campaign_name
            ELSE CAST(NULL AS STRING)
        END AS adj_campaign_name,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.creative_name
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.creative_name
            WHEN v.create_time IS NOT NULL THEN v.creative_name
            ELSE CAST(NULL AS STRING)
        END AS adj_creative_name,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.adgroup_tracker
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.adgroup_tracker
            WHEN v.create_time IS NOT NULL THEN v.adgroup_tracker
            ELSE CAST(NULL AS STRING)
        END AS adj_adgroup_tracker,
        CASE
            WHEN g.create_time IS NOT NULL
                AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.creative_tracker
            WHEN i.create_time IS NOT NULL
                AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.creative_tracker
            WHEN v.create_time IS NOT NULL THEN v.creative_tracker
            ELSE CAST(NULL AS STRING)
        END AS adj_creative_tracker,
        COALESCE(
            NULLIF(TRIM(CASE
                WHEN g.create_time IS NOT NULL
                    AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                    AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.network_name
                WHEN i.create_time IS NOT NULL
                    AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.network_name
                WHEN v.create_time IS NOT NULL THEN v.network_name
                ELSE CAST(NULL AS STRING)
            END), ''),
            NULLIF(TRIM(CASE
                WHEN g.create_time IS NOT NULL
                    AND g.create_time >= COALESCE(i.create_time, TIMESTAMP '1970-01-01 00:00:00')
                    AND g.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN g.tracker_name
                WHEN i.create_time IS NOT NULL
                    AND i.create_time >= COALESCE(v.create_time, TIMESTAMP '1970-01-01 00:00:00') THEN i.tracker_name
                WHEN v.create_time IS NOT NULL THEN v.tracker_name
                ELSE CAST(NULL AS STRING)
            END), '')
        ) AS channel_raw
    FROM src_user AS u
    LEFT JOIN dim_adjust_by_gps FOR SYSTEM_TIME AS OF u.proc_time AS g
        ON u.gps_adid IS NOT NULL AND u.gps_adid <> '' AND g.gps_adid = u.gps_adid
    LEFT JOIN dim_adjust_by_idfa FOR SYSTEM_TIME AS OF u.proc_time AS i
        ON u.idfa IS NOT NULL AND u.idfa <> '' AND i.idfa = u.idfa
    LEFT JOIN dim_adjust_by_idfv FOR SYSTEM_TIME AS OF u.proc_time AS v
        ON u.idfv IS NOT NULL AND u.idfv <> '' AND v.idfv = u.idfv
) AS t;
