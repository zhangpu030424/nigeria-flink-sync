-- 贷超 DK/LD 宽表 → 目标 ng.loan（独立 Job，不走 sync-migrate-auto）
-- 源: LM_MYSQL_* / loan_dk_ld_sync_staging（须先有 sync_shard 列，见 scripts/run-lm-loan-dk-ld-sync.sh）
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

-- 源表 BIGINT UNSIGNED 须声明 DECIMAL，否则 JDBC 读成 BigInteger → ClassCastException
CREATE TABLE IF NOT EXISTS src_lm_loan_dk_ld_staging (
    loan_no STRING,
    application_no STRING,
    `period` INT,
    roll_sequence INT,
    start_date DATE,
    due_date DATE,
    due_date_final DATE,
    principal DECIMAL(20, 0),
    interest DECIMAL(20, 0),
    admin_fee DECIMAL(20, 0),
    roll_fee DECIMAL(20, 0),
    penalty_amount DECIMAL(20, 0),
    reduction_amount DECIMAL(20, 0),
    total_amount DECIMAL(20, 0),
    paid_amount DECIMAL(20, 0),
    roll_paid_amount DECIMAL(20, 0),
    paid_time DECIMAL(20, 0),
    paid_off_date DATE,
    created_time DECIMAL(20, 0),
    status INT,
    sync_shard INT,
    PRIMARY KEY (application_no, `period`, roll_sequence) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'loan_dk_ld_sync_staging',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'sync_shard',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '0',
    'scan.partition.upper-bound' = '${LM_LOAN_SYNC_SHARD_MAX}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_lm_loan_dk_ld (
    loan_no STRING,
    application_no STRING,
    `period` TINYINT,
    roll_sequence TINYINT,
    start_date DATE,
    due_date DATE,
    due_date_final DATE,
    principal BIGINT,
    interest BIGINT,
    admin_fee BIGINT,
    roll_fee BIGINT,
    penalty_amount BIGINT,
    reduction_amount BIGINT,
    total_amount BIGINT,
    paid_amount BIGINT,
    roll_paid_amount BIGINT,
    paid_time BIGINT,
    paid_off_date DATE,
    created_time BIGINT,
    status TINYINT,
    PRIMARY KEY (application_no, `period`, roll_sequence) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'loan',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_lm_loan_dk_ld
SELECT
    loan_no,
    application_no,
    CAST(`period` AS TINYINT),
    CAST(roll_sequence AS TINYINT),
    start_date,
    due_date,
    due_date_final,
    CAST(principal AS BIGINT),
    CAST(interest AS BIGINT),
    CAST(admin_fee AS BIGINT),
    CAST(roll_fee AS BIGINT),
    CAST(penalty_amount AS BIGINT),
    CAST(reduction_amount AS BIGINT),
    CAST(total_amount AS BIGINT),
    CAST(paid_amount AS BIGINT),
    CAST(roll_paid_amount AS BIGINT),
    CAST(paid_time AS BIGINT),
    paid_off_date,
    CAST(created_time AS BIGINT),
    CAST(status AS TINYINT)
FROM src_lm_loan_dk_ld_staging;
