-- 增量 user_product：CDC user_order，按 user+product 取最新一单金额
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user_order_product (
    id BIGINT,
    user_id BIGINT,
    product_id STRING,
    amount_max STRING,
    order_time TIMESTAMP(3),
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
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS sink_user_product (
    group_user_id BIGINT, product_id STRING, schemes STRING, is_open TINYINT,
    credit_amount BIGINT, unpaid_amount BIGINT, locked_amount BIGINT, available_amount BIGINT,
    PRIMARY KEY (group_user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_product',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_product
SELECT
    user_id + 100000000,
    product_id,
    '{"repayment_method":1,"interest_start":"next_day","term":7,"periods":1,"periods_days":[7],"param_tpl":{"aha":0.5,"interest_rate":0,"penalty_rate":0.05,"post_paid_rate":0,"reduction_rate":0,"roll_allowed":0,"roll_due_method":1,"rollover_rate":0,"service_fee_rate":0,"tax_fee_rate":0,"upfront_rate":0.35,"value_date":0}}',
    CAST(1 AS TINYINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(0 AS BIGINT),
    CAST(0 AS BIGINT)
FROM src_user_order_product;
