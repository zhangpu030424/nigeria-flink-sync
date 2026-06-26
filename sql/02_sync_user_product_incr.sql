-- 增量 user_product：CDC user_order 触发 + Lookup 取 user+product 最新一单
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '3s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';

CREATE TABLE IF NOT EXISTS cdc_user_order (
    id BIGINT,
    user_id BIGINT,
    product_id STRING,
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
    'server-id' = '${CDC_SERVER_ID_USER_PRODUCT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_product_latest (
    user_id BIGINT,
    product_id STRING,
    amount_max STRING,
    PRIMARY KEY (user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_product_latest_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TEMPORARY VIEW v_user_product_triggers AS
SELECT user_id, product_id, proc_time
FROM cdc_user_order
WHERE user_id IS NOT NULL
  AND product_id IS NOT NULL
  AND TRIM(product_id) <> '';

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
    t.user_id + 100000000,
    t.product_id,
    CONCAT(
        '[{"schemeId":"PROD-001-D7","amountRange":[',
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(p.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS STRING),
        ']}]'
    ),
    CAST(1 AS TINYINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(p.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(p.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
    CAST(0 AS BIGINT),
    CAST(0 AS BIGINT)
FROM v_user_product_triggers AS t
INNER JOIN dim_user_product_latest FOR SYSTEM_TIME AS OF t.proc_time AS p
    ON p.user_id = t.user_id AND p.product_id = t.product_id;
