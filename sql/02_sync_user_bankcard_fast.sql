-- 全量阶段 1：已有 bank_account_token；无 token 见 02_sync_user_bankcard_fast_vt_miss.sql
-- 映射: id 暂传 0；group_user_id=user_id+1亿, bank_account_number=VT token
--
-- 执行: ./scripts/run-user-bankcard-fast.sh

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_bankcard_staging (
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
    CAST(0 AS BIGINT),
    user_id + 100000000,
    COALESCE(bank_code, ''),
    bank_account_token,
    CAST(COALESCE(is_default, 0) AS TINYINT)
FROM src_bankcard_staging
WHERE bank_account_token IS NOT NULL AND TRIM(bank_account_token) <> '';
