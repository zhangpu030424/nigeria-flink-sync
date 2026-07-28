-- 增量 id_mapping：多源 CDC 触发 + 轻量 pair Lookup + ARRAY/UNNEST 双向展开
-- CDC: user, user_order, user_bank_info, user_personal_info, device_ids
-- 前置: ./scripts/deploy-source-ddl.sh（含 id_mapping_pair_by_*）
-- 设计要点：
--   1) 不再 CDC 宽表 id_mapping_sync_staging
--   2) 每个触发源只 Lookup 一次，再用 ARRAY+UNNEST 展开边（避免 UNION 重复打维表）
--   3) 点查键裸列；VT miss 用 vt_tokenize；目标 UPSERT 幂等
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'table.exec.async-lookup.buffer-capacity' = '200';
SET 'table.exec.async-lookup.timeout' = '60s';

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
    'server-id' = '${CDC_SERVER_ID_IDMAP_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_order (
    id DECIMAL(20, 0),
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_order',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_IDMAP_ORDER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_bank_info (
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
    'table-name' = 'user_bank_info',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_IDMAP_BANK}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_personal_info (
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
    'server-id' = '${CDC_SERVER_ID_IDMAP_PERSONAL}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'debezium.event.deserialization.failure.handling.mode' = 'warn',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_device_ids (
    id BIGINT,
    device_uuid STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'device_ids',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_IDMAP_DEVICE}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_idmap_by_user (
    user_id BIGINT,
    app_code BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    device_uuid STRING,
    bvn_raw STRING,
    id_number_token STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    event_time BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_pair_by_user',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_idmap_by_order (
    order_id DECIMAL(20, 0),
    app_code BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    device_uuid STRING,
    bvn_raw STRING,
    id_number_token STRING,
    gaid_idfa_raw STRING,
    gaid_idfa_token STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    event_time BIGINT,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_pair_by_order',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_idmap_by_bank (
    user_id BIGINT,
    app_code BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    event_time BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_pair_by_bank',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_idmap_by_id_number (
    user_id BIGINT,
    app_code BIGINT,
    mobile_norm STRING,
    mobile_token STRING,
    bvn_raw STRING,
    id_number_token STRING,
    event_time BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_pair_by_id_number',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_idmap_by_device (
    device_uuid STRING,
    app_code BIGINT,
    gaid_idfa_raw STRING,
    gaid_idfa_token STRING,
    event_time BIGINT,
    PRIMARY KEY (device_uuid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'id_mapping_pair_by_device',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS sink_id_mapping (
    id STRING,
    app_id INT,
    mapping_id STRING,
    type STRING,
    event_time BIGINT,
    PRIMARY KEY (id, app_id, mapping_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'id_mapping',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '200ms',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}',
    'connection.max-retry-timeout' = '${FLINK_JDBC_RETRY_TIMEOUT}'
);

INSERT INTO sink_id_mapping
SELECT e.id, x.app_id, e.mapping_id, e.type, x.event_time
FROM (
    SELECT
        CAST(p.app_code AS INT) AS app_id,
        p.event_time AS event_time,
        ARRAY[
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CAST('device_uuid' AS STRING)
            ),
            ROW(
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CAST('id_number' AS STRING)
            ),
            ROW(
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CAST('bank_account' AS STRING)
            ),
            ROW(
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            )
        ] AS edges
    FROM cdc_user AS t
    INNER JOIN dim_idmap_by_user FOR SYSTEM_TIME AS OF t.proc_time AS p
        ON p.user_id = t.id
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, type)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> ''
  AND e.id <> e.mapping_id

UNION ALL

SELECT e.id, x.app_id, e.mapping_id, e.type, x.event_time
FROM (
    SELECT
        CAST(p.app_code AS INT) AS app_id,
        p.event_time AS event_time,
        ARRAY[
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CAST('gaid_idfa' AS STRING)
            ),
            ROW(
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CAST('bank_account' AS STRING)
            ),
            ROW(
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CAST('id_number' AS STRING)
            ),
            ROW(
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CAST('device_uuid' AS STRING)
            ),
            ROW(
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            ),
            ROW(
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CAST('gaid_idfa' AS STRING)
            ),
            ROW(
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CASE WHEN p.device_uuid IS NULL OR TRIM(p.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(p.device_uuid) END,
                CAST('device_uuid' AS STRING)
            )
        ] AS edges
    FROM cdc_user_order AS t
    INNER JOIN dim_idmap_by_order FOR SYSTEM_TIME AS OF t.proc_time AS p
        ON p.order_id = t.id
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, type)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> ''
  AND e.id <> e.mapping_id

UNION ALL

SELECT e.id, x.app_id, e.mapping_id, e.type, x.event_time
FROM (
    SELECT
        CAST(p.app_code AS INT) AS app_id,
        p.event_time AS event_time,
        ARRAY[
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CAST('bank_account' AS STRING)
            ),
            ROW(
                CASE WHEN p.bank_account_raw IS NULL OR TRIM(p.bank_account_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.bank_account_token IS NOT NULL AND TRIM(p.bank_account_token) <> '' THEN p.bank_account_token
                     ELSE vt_tokenize(TRIM(p.bank_account_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            )
        ] AS edges
    FROM cdc_user_bank_info AS t
    INNER JOIN dim_idmap_by_bank FOR SYSTEM_TIME AS OF t.proc_time AS p
        ON p.user_id = t.user_id
    WHERE t.user_id IS NOT NULL
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, type)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> ''
  AND e.id <> e.mapping_id

UNION ALL

SELECT e.id, x.app_id, e.mapping_id, e.type, x.event_time
FROM (
    SELECT
        CAST(p.app_code AS INT) AS app_id,
        p.event_time AS event_time,
        ARRAY[
            ROW(
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CAST('id_number' AS STRING)
            ),
            ROW(
                CASE WHEN p.bvn_raw IS NULL OR TRIM(p.bvn_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.id_number_token IS NOT NULL AND TRIM(p.id_number_token) <> '' THEN p.id_number_token
                     ELSE vt_tokenize(TRIM(p.bvn_raw)) END,
                CASE WHEN p.mobile_token IS NOT NULL AND TRIM(p.mobile_token) <> '' THEN p.mobile_token
                     WHEN p.mobile_norm IS NULL OR TRIM(p.mobile_norm) = '' THEN CAST(NULL AS STRING)
                     ELSE vt_tokenize(TRIM(p.mobile_norm)) END,
                CAST('mobile' AS STRING)
            )
        ] AS edges
    FROM cdc_user_personal_info AS t
    INNER JOIN dim_idmap_by_id_number FOR SYSTEM_TIME AS OF t.proc_time AS p
        ON p.user_id = t.user_id
    WHERE t.user_id IS NOT NULL
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, type)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> ''
  AND e.id <> e.mapping_id

UNION ALL

SELECT e.id, x.app_id, e.mapping_id, e.type, x.event_time
FROM (
    SELECT
        CAST(p.app_code AS INT) AS app_id,
        p.event_time AS event_time,
        ARRAY[
            ROW(
                CASE WHEN t.device_uuid IS NULL OR TRIM(t.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(t.device_uuid) END,
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CAST('gaid_idfa' AS STRING)
            ),
            ROW(
                CASE WHEN p.gaid_idfa_raw IS NULL OR TRIM(p.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
                     WHEN p.gaid_idfa_token IS NOT NULL AND TRIM(p.gaid_idfa_token) <> '' THEN p.gaid_idfa_token
                     ELSE vt_tokenize(TRIM(p.gaid_idfa_raw)) END,
                CASE WHEN t.device_uuid IS NULL OR TRIM(t.device_uuid) = '' THEN CAST(NULL AS STRING) ELSE TRIM(t.device_uuid) END,
                CAST('device_uuid' AS STRING)
            )
        ] AS edges
    FROM cdc_device_ids AS t
    INNER JOIN dim_idmap_by_device FOR SYSTEM_TIME AS OF t.proc_time AS p
        ON p.device_uuid = t.device_uuid
    WHERE t.device_uuid IS NOT NULL AND TRIM(t.device_uuid) <> ''
) AS x
CROSS JOIN UNNEST(x.edges) AS e(id, mapping_id, type)
WHERE e.id IS NOT NULL AND TRIM(e.id) <> ''
  AND e.mapping_id IS NOT NULL AND TRIM(e.mapping_id) <> ''
  AND e.id <> e.mapping_id;
