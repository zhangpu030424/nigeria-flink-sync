-- 增量 user_info：CDC user_info_sync_staging（宽表需先重建；info_json 在宽表内已组装完整）
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user_info_staging (
    user_id BIGINT,
    bvn_raw STRING,
    id_number_token STRING,
    full_name STRING,
    info_json STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_info_sync_staging',
    'server-time-zone' = 'Africa/Lagos',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only'
);

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    e.user_id,
    e.id_number,
    e.full_name,
    CAST('' AS STRING),
    CAST('' AS STRING),
    CAST('' AS STRING),
    e.info_json
FROM (
    SELECT
        s.user_id + 100000000 AS user_id,
        CASE
            WHEN s.bvn_raw IS NULL OR TRIM(s.bvn_raw) = '' THEN CAST('' AS STRING)
            WHEN s.id_number_token IS NOT NULL AND TRIM(s.id_number_token) <> '' THEN s.id_number_token
            ELSE vt_tokenize(TRIM(s.bvn_raw))
        END AS id_number,
        COALESCE(s.full_name, '') AS full_name,
        s.info_json
    FROM src_user_info_staging s
) e
WHERE e.id_number IS NOT NULL;
