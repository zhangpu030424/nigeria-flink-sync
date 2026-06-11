-- 增量 id_mapping：CDC id_mapping_sync_staging
-- 新关系需先刷新宽表（重跑 source_all_sync_staging.sql 第 7 段或全量重建）再追 binlog
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_id_mapping_staging (
    row_id BIGINT,
    id STRING,
    app_id BIGINT,
    mapping_id STRING,
    type STRING,
    event_time BIGINT,
    PRIMARY KEY (row_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'id_mapping_sync_staging',
    'server-time-zone' = 'Africa/Lagos',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS sink_id_mapping (
    id STRING,
    app_id INT,
    mapping_id STRING,
    type STRING,
    event_time BIGINT,
    PRIMARY KEY (id, app_id, mapping_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'id_mapping',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_id_mapping
SELECT id, CAST(app_id AS INT), mapping_id, type, event_time
FROM src_id_mapping_staging;
