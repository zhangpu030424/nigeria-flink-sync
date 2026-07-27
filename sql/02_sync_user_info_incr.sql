-- 增量 user_info：多源 CDC 触发 + 单次 bundle Lookup（对齐 application）
-- CDC: user, user_personal_info, user_work_related, user_emergency_contact,
--      risk_user_credit_callback, vt_token_cache(BVN), adjust_callback_record
-- 触发侧只出 user_id；组装侧 Lookup user_info_incr_bundle_lookup
-- Flink：id_number VT 兜底 + emergency_contacts 明文手机 VT 兜底
-- 前置: ./scripts/deploy-source-ddl.sh
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';
CREATE TEMPORARY FUNCTION vt_tokenize_emergency_contacts AS 'com.nigeria.flink.udf.VtTokenizeEmergencyContactsFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '60s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';

CREATE TABLE IF NOT EXISTS cdc_ui_user (
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
    'server-id' = '${CDC_SERVER_ID_UI_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_personal (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_personal_info',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_PERSONAL}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'debezium.event.deserialization.failure.handling.mode' = 'warn',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_work (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_work_related',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_WORK}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_emergency (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_emergency_contact',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_EMERGENCY}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_credit (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'risk_user_credit_callback',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_CREDIT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_vt (
    id BIGINT,
    vt_type INT,
    raw_value STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'vt_token_cache',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_VT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_ui_adjust (
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
    'server-id' = '${CDC_SERVER_ID_UI_ADJUST}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_users_by_bvn (
    bvn STRING,
    user_id BIGINT,
    PRIMARY KEY (bvn) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'users_by_bvn_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
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
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_user_info_bundle (
    user_id BIGINT,
    bvn STRING,
    first_name STRING,
    sur_name STRING,
    vt_token STRING,
    vt_status BIGINT,
    info_json STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_incr_bundle_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

-- 全部触发收敛为 user_id，无 CDC 双流 Join
CREATE TEMPORARY VIEW v_user_info_triggers AS
SELECT id AS user_id, proc_time FROM cdc_ui_user WHERE id IS NOT NULL
UNION ALL
SELECT user_id, proc_time FROM cdc_ui_personal WHERE user_id IS NOT NULL
UNION ALL
SELECT user_id, proc_time FROM cdc_ui_work WHERE user_id IS NOT NULL
UNION ALL
SELECT user_id, proc_time FROM cdc_ui_emergency WHERE user_id IS NOT NULL
UNION ALL
SELECT user_id, proc_time FROM cdc_ui_credit WHERE user_id IS NOT NULL
UNION ALL
SELECT ub.user_id, vt.proc_time
FROM cdc_ui_vt AS vt
INNER JOIN dim_users_by_bvn FOR SYSTEM_TIME AS OF vt.proc_time AS ub
    ON ub.bvn = vt.raw_value
WHERE vt.vt_type = 4
  AND vt.raw_value IS NOT NULL AND TRIM(vt.raw_value) <> ''
  AND ub.user_id IS NOT NULL
UNION ALL
SELECT ua.user_id, adj.proc_time
FROM cdc_ui_adjust AS adj
INNER JOIN dim_users_by_adid FOR SYSTEM_TIME AS OF adj.proc_time AS ua
    ON ua.adid = adj.adid
WHERE adj.adid IS NOT NULL AND TRIM(adj.adid) <> ''
  AND ua.user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '200ms',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}',
    'connection.max-retry-timeout' = '${FLINK_JDBC_RETRY_TIMEOUT}'
);

INSERT INTO sink_user_info
SELECT
    e.user_id,
    e.id_number,
    e.full_name,
    CAST('' AS STRING),
    CAST('' AS STRING),
    CAST('' AS STRING),
    e.info_json
FROM (
    SELECT
        t.user_id + 100000000 AS user_id,
        b.bvn AS bvn_raw,
        COALESCE(
            CASE
                WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN CAST('' AS STRING)
                WHEN b.vt_status = 1 AND b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN b.vt_token
                ELSE vt_tokenize(TRIM(b.bvn))
            END,
            ''
        ) AS id_number,
        COALESCE(TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))), '') AS full_name,
        vt_tokenize_emergency_contacts(COALESCE(b.info_json, '{}')) AS info_json
    FROM v_user_info_triggers AS t
    INNER JOIN dim_user_info_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
        ON b.user_id = t.user_id
) AS e
WHERE (e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = '')
   OR (e.id_number IS NOT NULL AND TRIM(e.id_number) <> '');
