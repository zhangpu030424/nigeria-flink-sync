-- 增量 user：多源 CDC 触发 + Lookup 组装
-- CDC: user, adjust_callback_record（UTM 变更）
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';

CREATE TABLE IF NOT EXISTS cdc_user (
    id BIGINT,
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
    'server-id' = '${CDC_SERVER_ID_USER_MAIN}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_adjust_callback (
    id BIGINT,
    adid STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'adjust_callback_record',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_USER_ADJUST}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_row (
    id BIGINT,
    app_code BIGINT,
    mobile STRING,
    device_id STRING,
    adid STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_incr_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '5s'
);

CREATE TABLE IF NOT EXISTS dim_users_by_adid (
    adid STRING,
    user_id BIGINT,
    PRIMARY KEY (adid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'users_by_adid_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '5s'
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
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'adjust_latest_by_adid',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '5s'
);

CREATE TEMPORARY VIEW v_user_triggers AS
SELECT id AS user_id, proc_time FROM cdc_user WHERE id IS NOT NULL
UNION ALL
SELECT ua.user_id, adj.proc_time
FROM cdc_adjust_callback AS adj
INNER JOIN dim_users_by_adid FOR SYSTEM_TIME AS OF adj.proc_time AS ua
    ON ua.adid = adj.adid
WHERE adj.adid IS NOT NULL AND TRIM(adj.adid) <> '' AND ua.user_id IS NOT NULL;

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
        u.id + 100000000 AS user_id,
        CAST(u.app_code AS INT) AS app_id,
        u.id + 100000000 AS group_user_id,
        u.id + 100000000 AS info_user_id,
        vt_tokenize(
            CASE
                WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN CAST(NULL AS STRING)
                WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                ELSE CONCAT('+234', TRIM(u.mobile))
            END
        ) AS mobile_token,
        CAST(0 AS BIGINT) AS closed_time,
        COALESCE(u.device_id, '') AS reg_device_uuid,
        CAST(UNIX_TIMESTAMP(CAST(u.create_time AS STRING)) * 1000 AS BIGINT) AS reg_time,
        CAST(0 AS TINYINT) AS test_flag,
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
        END AS utm_source,
        adj.campaign_tracker AS utm_medium,
        adj.campaign_name AS utm_campaign,
        adj.creative_name AS utm_content,
        adj.adgroup_tracker AS utm_term,
        adj.campaign_tracker AS campaign_id,
        adj.adgroup_tracker AS ad_group_id,
        adj.campaign_tracker AS advertiser_id
    FROM v_user_triggers AS t
    INNER JOIN dim_user_row FOR SYSTEM_TIME AS OF t.proc_time AS u ON u.id = t.user_id
    LEFT JOIN dim_user_adjust FOR SYSTEM_TIME AS OF t.proc_time AS adj
        ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid
) AS e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> '';
