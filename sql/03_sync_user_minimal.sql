-- 最小链路：CDC 读 user → 写目标库（无 Lookup、无 VT）
-- 用于排查「目标库一直 0」：若此 Job 仍 0 条，问题在 CDC/网络/权限；若有数，问题在 Lookup 或 VT
-- 执行: ./scripts/run-sql.sh sql/03_sync_user_minimal.sql

SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE IF NOT EXISTS src_user_min (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_user_min (
    user_id BIGINT,
    app_id INT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag TINYINT,
    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_min
SELECT
    id + 100000000,
    CAST(app_code AS INT),
    id + 100000000,
    id + 100000000,
    mobile,
    CAST(0 AS BIGINT),
    COALESCE(device_id, ''),
    UNIX_TIMESTAMP(DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000,
    CAST(0 AS TINYINT)
FROM src_user_min;
