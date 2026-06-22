-- 全量阶段 2：银行卡无 bank_account_token，运行时 UDF 调 VT /v2t
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';
CREATE TEMPORARY FUNCTION snowflake_id AS 'com.nigeria.flink.udf.SnowflakeIdFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_bankcard_staging_miss (
    id BIGINT,
    user_id BIGINT,
    bank_code STRING,
    bank_account_raw STRING,
    bank_account_token STRING,
    is_default TINYINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_bankcard_sync_staging',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
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
    'sink.buffer-flush.interval' = '500ms',
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
        snowflake_id() AS id,
        s.user_id + 100000000 AS group_user_id,
        COALESCE(s.bank_code, '') AS bank_code,
        vt_tokenize(s.bank_account_raw) AS bank_account_number,
        CAST(COALESCE(s.is_default, 0) AS TINYINT) AS is_default
    FROM src_bankcard_staging_miss s
    WHERE (s.bank_account_token IS NULL OR TRIM(s.bank_account_token) = '')
      AND s.bank_account_raw IS NOT NULL
      AND TRIM(s.bank_account_raw) <> ''
) e
WHERE e.bank_account_number IS NOT NULL AND TRIM(e.bank_account_number) <> '';
