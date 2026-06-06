-- 性能诊断：CDC + 写目标库，但不 Join app_config（隔离「写」瓶颈）
-- 执行: ./scripts/run-sql.sh sql/04_bench_sink_no_join.sql
-- 若比 02 快很多 → Lookup Join 是瓶颈；若仍慢 → JDBC/目标库是瓶颈

SET 'parallelism.default' = '4';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';

CREATE TABLE IF NOT EXISTS bench_src_user (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
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
    'scan.incremental.snapshot.chunk.size' = '50000',
    'scan.snapshot.fetch.size' = '5000'
);

CREATE TABLE IF NOT EXISTS bench_sink_user (
    user_id BIGINT,
    app_id BIGINT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag TINYINT,
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '5000',
    'sink.buffer-flush.interval' = '2s'
);

INSERT INTO bench_sink_user
SELECT
    u.id + 100000000,
    CAST(0 AS BIGINT),
    u.id + 100000000,
    u.id + 100000000,
    u.mobile,
    CAST(0 AS BIGINT),
    COALESCE(u.device_id, ''),
    UNIX_TIMESTAMP(DATE_FORMAT(u.create_time, 'yyyy-MM-dd HH:mm:ss')) * 1000,
    CAST(0 AS TINYINT),
    u.create_time,
    u.update_time
FROM bench_src_user AS u;
