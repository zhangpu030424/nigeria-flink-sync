-- 全量阶段 1 user_info：宽表已有 id_number_token；无 token 见 02_sync_user_info_fast_vt_miss.sql
-- 原始 bvn 为空 → id_number 写 NULL（无需 VT）；有 bvn 无 token → 阶段 2 补
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';

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
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT,
    id_number STRING,
    full_name STRING,
    password STRING,
    live_image STRING,
    id_card STRING,
    info STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    user_id + 100000000,
    CASE
        WHEN bvn_raw IS NULL OR TRIM(bvn_raw) = '' THEN CAST(NULL AS STRING)
        ELSE id_number_token
    END,
    COALESCE(full_name, ''),
    CAST('' AS STRING),
    CAST('' AS STRING),
    CAST('' AS STRING),
    info_json
FROM src_user_info_staging
WHERE (bvn_raw IS NULL OR TRIM(bvn_raw) = '')
   OR (id_number_token IS NOT NULL AND TRIM(id_number_token) <> '');
