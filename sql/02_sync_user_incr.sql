-- 增量：CDC user + adjust Lookup（adid）+ 逐条 VT（变更量少）
-- 全量请用 ./scripts/run-user-fast-vt.sh（批量 10 万条/次）
--
-- CDC_STARTUP_MODE（run-sql / sync-user-auto 注入）:
--   timestamp        — 从 BULK_START_MS 起补 binlog（推荐，覆盖全量期间变更）
--   latest-offset    — 仅从提交 Job 时刻起（会漏全量窗口内变更）
--
-- 执行: ./scripts/run-sql.sh sql/02_sync_user_incr.sql

CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    adid STRING,
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
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'false',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_adjust (
    adid STRING,
    network_name STRING,
    tracker_name STRING,
    campaign_tracker STRING,
    campaign_name STRING,
    creative_name STRING,
    adgroup_tracker STRING,
    creative_tracker STRING,
    adgroup_name STRING,
    PRIMARY KEY (adid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'adjust_latest_by_adid',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
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
    u.id + 100000000,
    CAST(u.app_code AS INT),
    u.id + 100000000,
    u.id + 100000000,
    vt_tokenize(
        CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN u.mobile
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END
    ),
    CAST(0 AS BIGINT),
    COALESCE(u.device_id, ''),
    UNIX_TIMESTAMP(DATE_FORMAT(u.create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000,
    CAST(0 AS TINYINT),
    CASE
        WHEN COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), '')) IS NULL
            OR TRIM(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) = ''
            THEN CAST(NULL AS STRING)
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%unattributed%'
            THEN CAST(NULL AS STRING)
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%organic%'
            THEN 'organic'
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%google%'
            THEN 'google'
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%tiktok%'
            THEN 'tiktok'
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%facebook%'
            OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%instagram%'
            OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%messenger%'
            THEN 'facebook'
        WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%kuai%'
            OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%kwai%'
            OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%kuaishou%'
            THEN 'kwai'
        ELSE LOWER(TRIM(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))))
    END,
    adj.campaign_tracker,
    adj.campaign_name,
    adj.creative_name,
    adj.adgroup_tracker,
    adj.creative_tracker,
    adj.campaign_tracker,
    adj.adgroup_tracker
FROM src_user AS u
LEFT JOIN dim_user_adjust FOR SYSTEM_TIME AS OF u.proc_time AS adj
    ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid;
