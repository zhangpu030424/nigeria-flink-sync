-- 增量 loan：CDC user_order_installment + 源表 Lookup（user_order / user_repay）
-- 前置: mysql ... < sql/ddl/source_lookup_views.sql
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user_order_installment (
    id BIGINT,
    user_order_id BIGINT,
    installment_order_no STRING,
    current_period INT,
    received STRING,
    interests STRING,
    poundage_fees STRING,
    penalty_amount STRING,
    amt_due STRING,
    repaid_amount STRING,
    repayment_time TIMESTAMP(3),
    is_overdue TINYINT,
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
    'table-name' = 'user_order_installment',
    'server-time-zone' = 'Africa/Lagos',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_order (
    id BIGINT,
    order_no STRING,
    order_time TIMESTAMP(3),
    disburse_time TIMESTAMP(3),
    settled_time TIMESTAMP(3),
    risk_order_status INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_order_loan_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_repay_period (
    order_no STRING,
    current_period INT,
    callback_time TIMESTAMP(3),
    PRIMARY KEY (order_no, current_period) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_repay_paid_by_order_period',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS sink_loan (
    loan_no STRING, application_no STRING, `period` TINYINT, roll_sequence TINYINT,
    start_date DATE, due_date DATE, due_date_final DATE,
    principal BIGINT, interest BIGINT, admin_fee BIGINT,
    penalty_amount BIGINT, reduction_amount BIGINT, total_amount BIGINT,
    paid_amount BIGINT, paid_time BIGINT, paid_off_date DATE,
    created_time BIGINT, status TINYINT,
    PRIMARY KEY (application_no, `period`, roll_sequence) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'loan',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_loan
SELECT
    i.installment_order_no,
    o.order_no,
    CAST(COALESCE(i.current_period, 1) AS TINYINT),
    CAST(0 AS TINYINT),
    CAST(COALESCE(CAST(o.disburse_time AS DATE), CAST(o.order_time AS DATE), CAST(i.create_time AS DATE)) AS DATE),
    CASE WHEN i.repayment_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(i.repayment_time AS DATE) END,
    CASE WHEN i.repayment_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(i.repayment_time AS DATE) END,
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.interests), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.poundage_fees), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.penalty_amount), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(0 AS BIGINT),
    CAST(COALESCE(ROUND((CAST(NULLIF(TRIM(i.amt_due), '') AS DECIMAL(20, 2))
        + CAST(NULLIF(TRIM(i.penalty_amount), '') AS DECIMAL(20, 2))), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.repaid_amount), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CASE
        WHEN rp.callback_time IS NOT NULL AND UNIX_TIMESTAMP(CAST(rp.callback_time AS STRING)) > 0
            THEN CAST(UNIX_TIMESTAMP(CAST(rp.callback_time AS STRING)) * 1000 AS BIGINT)
        ELSE CAST(NULL AS BIGINT)
    END,
    CASE WHEN o.settled_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(o.settled_time AS DATE) END,
    GREATEST(
        CAST(COALESCE(UNIX_TIMESTAMP(CAST(COALESCE(CAST(o.disburse_time AS DATE), CAST(o.order_time AS DATE), CAST(i.create_time AS DATE)) AS STRING)), 0) * 1000 AS BIGINT),
        CAST(0 AS BIGINT)
    ),
    CAST(
        CASE
            WHEN CAST(o.risk_order_status AS INT) = 10 AND COALESCE(CAST(i.is_overdue AS INT), 0) = 1 THEN 23
            WHEN CAST(o.risk_order_status AS INT) = 10
                AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) = 0 THEN 20
            WHEN CAST(o.risk_order_status AS INT) = 10
                AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) <> 0 THEN 24
            WHEN CAST(o.risk_order_status AS INT) = 11 THEN 23
            WHEN CAST(o.risk_order_status AS INT) = 40 THEN 25
            WHEN CAST(o.risk_order_status AS INT) IN (20, 30, 50) THEN 27
            ELSE 20
        END AS TINYINT
    )
FROM src_user_order_installment AS i
INNER JOIN dim_user_order FOR SYSTEM_TIME AS OF i.proc_time AS o ON CAST(o.id AS BIGINT) = i.user_order_id
LEFT JOIN dim_repay_period FOR SYSTEM_TIME AS OF i.proc_time AS rp
    ON rp.order_no = o.order_no AND CAST(rp.current_period AS INT) = CAST(i.current_period AS INT)
WHERE i.installment_order_no IS NOT NULL AND TRIM(i.installment_order_no) <> ''
  AND o.risk_order_status IS NOT NULL
  AND CAST(o.risk_order_status AS INT) NOT IN (0, 2, 4, 6, 8);
