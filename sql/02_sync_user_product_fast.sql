-- 全量 user_product（预聚合宽表）
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_user_product_staging (
    user_id BIGINT,
    product_id STRING,
    credit_amount_minor BIGINT,
    unpaid_amount_minor BIGINT,
    PRIMARY KEY (user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_product_sync_staging',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_user_product (
    group_user_id BIGINT,
    product_id STRING,
    schemes STRING,
    is_open TINYINT,
    credit_amount BIGINT,
    unpaid_amount BIGINT,
    locked_amount BIGINT,
    available_amount BIGINT,
    PRIMARY KEY (group_user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_product',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_product
SELECT
    user_id + 100000000,
    product_id,
    '{"repayment_method":1,"interest_start":"next_day","term":7,"periods":1,"periods_days":[7],"param_tpl":{"aha":0.5,"interest_rate":0,"penalty_rate":0.05,"post_paid_rate":0,"reduction_rate":0,"roll_allowed":0,"roll_due_method":1,"rollover_rate":0,"service_fee_rate":0,"tax_fee_rate":0,"upfront_rate":0.35,"value_date":0}}',
    CAST(1 AS TINYINT),
    credit_amount_minor,
    unpaid_amount_minor,
    CAST(0 AS BIGINT),
    CAST(0 AS BIGINT)
FROM src_user_product_staging;
