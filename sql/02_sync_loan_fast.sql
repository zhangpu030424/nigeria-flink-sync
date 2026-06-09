-- 全量 loan（预聚合宽表）
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_loan_staging (
    id BIGINT, loan_no STRING, application_no STRING, `period` TINYINT, roll_sequence TINYINT,
    start_date DATE, due_date DATE, due_date_final DATE,
    principal_minor BIGINT, interest_minor BIGINT, admin_fee_minor BIGINT, roll_fee_minor BIGINT,
    penalty_amount_minor BIGINT, reduction_amount_minor BIGINT, total_amount_minor BIGINT,
    paid_amount_minor BIGINT, roll_paid_amount_minor BIGINT, paid_off_date DATE, risk_status INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'loan_sync_staging',
    'server-time-zone' = 'Africa/Lagos',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_loan (
    loan_no STRING, application_no STRING, `period` TINYINT, roll_sequence TINYINT,
    start_date DATE, due_date DATE, due_date_final DATE,
    principal BIGINT, interest BIGINT, admin_fee BIGINT,
    penalty_amount BIGINT, reduction_amount BIGINT, total_amount BIGINT,
    paid_amount BIGINT, roll_paid_amount BIGINT, paid_time BIGINT, paid_off_date DATE,
    created_time BIGINT, status TINYINT,
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

INSERT INTO sink_loan
SELECT
    loan_no, application_no, `period`, roll_sequence,
    start_date, due_date, due_date_final,
    principal_minor, interest_minor, admin_fee_minor,
    penalty_amount_minor, reduction_amount_minor, total_amount_minor,
    paid_amount_minor, roll_paid_amount_minor,
    CAST(NULL AS BIGINT), paid_off_date,
    CAST(UNIX_TIMESTAMP(CAST(start_date AS STRING)) * 1000 AS BIGINT),
    CAST(risk_status AS TINYINT)
FROM src_loan_staging;
