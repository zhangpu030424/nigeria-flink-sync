-- 增量（预 VT + 兜底）：CDC user_bank_info + vt_token_cache Lookup → 目标 user_bankcard
-- 全量: ./scripts/run-user-bankcard-fast.sh
-- 过滤: deleted=0；Lookup miss 时 vt_tokenize() 调 /v2t
--
-- 执行: ./scripts/run-sql.sh sql/02_sync_user_bankcard_incr.sql

CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user_bank_info (
    id BIGINT,
    user_id BIGINT,
    bank_code STRING,
    bank_account STRING,
    is_default TINYINT,
    deleted INT,
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
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_vt_bank_account (
    vt_type STRING,
    raw_value STRING,
    token STRING,
    status INT,
    PRIMARY KEY (vt_type, raw_value) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'vt_token_cache',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '2h'
);

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
        b.id + 100000000 AS id,
        b.user_id + 100000000 AS group_user_id,
        COALESCE(b.bank_code, '') AS bank_code,
        COALESCE(
            NULLIF(TRIM(vt.token), ''),
            vt_tokenize(TRIM(b.bank_account))
        ) AS bank_account_number,
        CAST(COALESCE(b.is_default, 0) AS TINYINT) AS is_default
    FROM src_user_bank_info AS b
    LEFT JOIN dim_vt_bank_account FOR SYSTEM_TIME AS OF b.proc_time AS vt
        ON vt.vt_type = 'bank_account'
        AND vt.status = 1
        AND vt.raw_value = TRIM(b.bank_account)
    WHERE b.deleted = 0
      AND b.bank_account IS NOT NULL
      AND TRIM(b.bank_account) <> ''
) AS e
WHERE e.bank_account_number IS NOT NULL AND TRIM(e.bank_account_number) <> '';
