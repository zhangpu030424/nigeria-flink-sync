-- 性能诊断：只测 CDC 读源库速度（不写目标库）
-- 执行: ./scripts/run-sql.sh sql/03_bench_cdc_read.sql
-- 看 TaskManager 日志 + Web UI Source 算子 numRecordsOutPerSecond

SET 'parallelism.default' = '4';

CREATE TABLE IF NOT EXISTS bench_src_user (
    id BIGINT,
    app_code STRING,
    mobile STRING,
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

CREATE TABLE IF NOT EXISTS bench_print (
    id BIGINT,
    app_code STRING,
    mobile STRING
) WITH ('connector' = 'print');

INSERT INTO bench_print SELECT id, app_code, mobile FROM bench_src_user;
