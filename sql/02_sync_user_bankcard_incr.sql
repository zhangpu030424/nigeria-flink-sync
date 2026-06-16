-- 增量 user_bankcard：多源 CDC 触发 + Lookup 组装
-- CDC: user_bank_info, vt_token_cache(bank_account)
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

CREATE TABLE IF NOT EXISTS cdc_user_bank_info (
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
    'table-name' = 'user_bank_info',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_BANKCARD_INFO}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_vt_token_cache (
    vt_type TINYINT,
    raw_value STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (vt_type, raw_value) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'vt_token_cache',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_BANKCARD_VT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_bankcard_by_account (
    bank_account STRING,
    bank_id BIGINT,
    PRIMARY KEY (bank_account) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_bankcard_id_by_account_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_user_bankcard (
    id BIGINT,
    user_id BIGINT,
    bank_code STRING,
    bank_account STRING,
    is_default BIGINT,
    deleted BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_bankcard_incr_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TEMPORARY VIEW v_bankcard_triggers AS
SELECT id AS bank_id, proc_time FROM cdc_user_bank_info WHERE id IS NOT NULL
UNION ALL
SELECT ba.bank_id, vt.proc_time
FROM cdc_vt_token_cache AS vt
INNER JOIN dim_bankcard_by_account FOR SYSTEM_TIME AS OF vt.proc_time AS ba
    ON ba.bank_account = TRIM(vt.raw_value)
WHERE vt.vt_type = 3
  AND vt.raw_value IS NOT NULL AND TRIM(vt.raw_value) <> ''
  AND ba.bank_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sink_user_bankcard (
    id BIGINT,
    group_user_id BIGINT,
    bank_code STRING,
    bank_account_number STRING,
    is_default TINYINT,
    PRIMARY KEY (group_user_id, bank_account_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_bankcard',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_bankcard
SELECT
    e.id,
    e.group_user_id,
    e.bank_code,
    e.bank_account_number,
    e.is_default
FROM (
    SELECT
        CAST(0 AS BIGINT) AS id,
        b.user_id + 100000000 AS group_user_id,
        COALESCE(b.bank_code, '') AS bank_code,
        vt_tokenize(TRIM(b.bank_account)) AS bank_account_number,
        CAST(COALESCE(b.is_default, 0) AS TINYINT) AS is_default
    FROM v_bankcard_triggers AS t
    INNER JOIN dim_user_bankcard FOR SYSTEM_TIME AS OF t.proc_time AS b ON b.id = t.bank_id
    WHERE b.deleted = 0
      AND b.bank_account IS NOT NULL
      AND TRIM(b.bank_account) <> ''
) AS e
WHERE e.bank_account_number IS NOT NULL AND TRIM(e.bank_account_number) <> '';
