-- 增量 loan：多源 CDC 触发 + 单次 bundle Lookup
-- CDC: user_order_installment, user_order, user_repay
-- 前置: ./scripts/deploy-source-ddl.sh（需含 loan_incr_bundle_lookup）
-- 优化要点：
--   1) 主路径 3 段 LookupJoin 合并为 1 次 bundle 点查（installment PK）
--   2) 点查键用裸 id + Flink DECIMAL，避免 CAST(id) 导致全表扫
--   3) repay 回调在 bundle 内相关子查询，勿 GROUP BY 物化整表
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

CREATE TABLE IF NOT EXISTS cdc_user_order_installment (
    id DECIMAL(20, 0),
    user_order_id DECIMAL(20, 0),
    current_period INT,
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
    'server-id' = '${CDC_SERVER_ID_LOAN_INSTALLMENT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_order (
    id DECIMAL(20, 0),
    order_no STRING,
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
    'server-id' = '${CDC_SERVER_ID_LOAN_ORDER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_repay (
    id DECIMAL(20, 0),
    order_no STRING,
    current_period INT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_repay',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_LOAN_REPAY}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

-- order CDC → 分期 id（通常每单一期；裸 installment_id 走索引）
-- user_order_id 源列是 signed bigint → JDBC Long，Flink 必须 BIGINT（勿 DECIMAL）
CREATE TABLE IF NOT EXISTS dim_installment_by_order (
    user_order_id BIGINT,
    installment_id DECIMAL(20, 0),
    PRIMARY KEY (user_order_id, installment_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'loan_installment_ids_by_user_order_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_installment_by_order_period (
    order_no STRING,
    current_period BIGINT,
    installment_id DECIMAL(20, 0),
    PRIMARY KEY (order_no, current_period) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'loan_installment_id_by_order_no_period_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

-- 主维表：一次 Lookup 替代 installment + order + repay 三段串行
CREATE TABLE IF NOT EXISTS dim_loan_bundle (
    installment_id DECIMAL(20, 0),
    user_order_id BIGINT,
    current_period BIGINT,
    received STRING,
    interests STRING,
    poundage_fees STRING,
    penalty_amount STRING,
    amt_due STRING,
    repaid_amount STRING,
    repayment_time TIMESTAMP(3),
    is_overdue BIGINT,
    create_time TIMESTAMP(3),
    order_no STRING,
    app_code BIGINT,
    order_time TIMESTAMP(3),
    disburse_time TIMESTAMP(3),
    settled_time TIMESTAMP(3),
    risk_order_status BIGINT,
    callback_time TIMESTAMP(3),
    PRIMARY KEY (installment_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'loan_incr_bundle_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TEMPORARY VIEW v_loan_triggers AS
SELECT id AS installment_id, proc_time FROM cdc_user_order_installment WHERE id IS NOT NULL
UNION ALL
SELECT di.installment_id, o.proc_time
FROM cdc_user_order AS o
INNER JOIN dim_installment_by_order FOR SYSTEM_TIME AS OF o.proc_time AS di
    ON di.user_order_id = CAST(o.id AS BIGINT)
WHERE di.installment_id IS NOT NULL
UNION ALL
SELECT dip.installment_id, ur.proc_time
FROM cdc_user_repay AS ur
INNER JOIN dim_installment_by_order_period FOR SYSTEM_TIME AS OF ur.proc_time AS dip
    ON dip.order_no = ur.order_no AND dip.current_period = CAST(ur.current_period AS BIGINT)
WHERE ur.order_no IS NOT NULL AND TRIM(ur.order_no) <> '' AND dip.installment_id IS NOT NULL;

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
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'loan',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '200ms',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}',
    'connection.max-retry-timeout' = '${FLINK_JDBC_RETRY_TIMEOUT}'
);

INSERT INTO sink_loan
SELECT
    CONCAT(
        'ng-', b.order_no, '-',
        LPAD(CAST(COALESCE(b.current_period, 1) AS STRING), 2, '0'),
        LPAD(CAST(CAST(0 AS TINYINT) AS STRING), 3, '0')
    ),
    CONCAT('ng0', CAST(b.app_code AS STRING), '-', b.order_no),
    CAST(COALESCE(b.current_period, 1) AS TINYINT),
    CAST(0 AS TINYINT),
    CAST(COALESCE(CAST(b.disburse_time AS DATE), CAST(b.order_time AS DATE), CAST(b.create_time AS DATE)) AS DATE),
    CASE WHEN b.repayment_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(b.repayment_time AS DATE) END,
    CASE WHEN b.repayment_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(b.repayment_time AS DATE) END,
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(b.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(b.interests), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(b.poundage_fees), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(b.penalty_amount), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(0 AS BIGINT),
    CAST(COALESCE(ROUND((CAST(NULLIF(TRIM(b.amt_due), '') AS DECIMAL(20, 2))
        + CAST(NULLIF(TRIM(b.penalty_amount), '') AS DECIMAL(20, 2))), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(b.repaid_amount), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CASE
        WHEN b.callback_time IS NOT NULL AND UNIX_TIMESTAMP(CAST(b.callback_time AS STRING)) > 0
            THEN CAST(UNIX_TIMESTAMP(CAST(b.callback_time AS STRING)) * 1000 AS BIGINT)
        WHEN CAST(b.risk_order_status AS INT) IN (20, 30, 50)
            AND b.settled_time IS NOT NULL
            AND UNIX_TIMESTAMP(CAST(b.settled_time AS STRING)) > 0
            THEN CAST(UNIX_TIMESTAMP(CAST(b.settled_time AS STRING)) * 1000 AS BIGINT)
        ELSE CAST(NULL AS BIGINT)
    END,
    CASE WHEN b.settled_time IS NULL THEN CAST(NULL AS DATE) ELSE CAST(b.settled_time AS DATE) END,
    GREATEST(
        CAST(COALESCE(
            UNIX_TIMESTAMP(CAST(b.disburse_time AS STRING)),
            UNIX_TIMESTAMP(CAST(b.order_time AS STRING)),
            UNIX_TIMESTAMP(CAST(b.create_time AS STRING)),
            0
        ) * 1000 AS BIGINT),
        CAST(0 AS BIGINT)
    ),
    CAST(
        CASE
            WHEN CAST(b.risk_order_status AS INT) = 10 AND COALESCE(CAST(b.is_overdue AS INT), 0) = 1 THEN 23
            WHEN CAST(b.risk_order_status AS INT) = 10
                AND CAST(COALESCE(NULLIF(TRIM(b.repaid_amount), ''), '0') AS DECIMAL(20, 2)) = 0 THEN 20
            WHEN CAST(b.risk_order_status AS INT) = 10
                AND CAST(COALESCE(NULLIF(TRIM(b.repaid_amount), ''), '0') AS DECIMAL(20, 2)) <> 0 THEN 24
            WHEN CAST(b.risk_order_status AS INT) = 11 THEN 23
            WHEN CAST(b.risk_order_status AS INT) = 40 THEN 25
            WHEN CAST(b.risk_order_status AS INT) IN (20, 30, 50) THEN 27
            ELSE 20
        END AS TINYINT
    )
FROM v_loan_triggers AS t
INNER JOIN dim_loan_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
    ON b.installment_id = t.installment_id
WHERE b.order_no IS NOT NULL AND TRIM(b.order_no) <> ''
  AND CAST(b.app_code AS INT) IN (567, 568, 571, 572, 573)
  AND b.risk_order_status IS NOT NULL
  AND CAST(b.risk_order_status AS INT) NOT IN (0, 2, 4, 6, 8);
