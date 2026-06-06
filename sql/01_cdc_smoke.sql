-- 阶段 A：CDC 连通性冒烟（只读源库 user 表，打印到控制台）
-- 执行前将 SOURCE_* 替换为 .env 中的实际值

CREATE TABLE IF NOT EXISTS src_user_cdc (
    id BIGINT,
    app_code STRING,
    mobile STRING,
    device_id STRING,
    status INT,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'SOURCE_MYSQL_HOST',
    'port' = '3306',
    'username' = 'SOURCE_MYSQL_USER',
    'password' = 'SOURCE_MYSQL_PASSWORD',
    'database-name' = 'nigeria_backend',
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'scan.incremental.snapshot.chunk.size' = '8096'
);

CREATE TABLE IF NOT EXISTS print_user_cdc (
    id BIGINT,
    app_code STRING,
    mobile STRING
) WITH ('connector' = 'print');

-- 提交 Job（持续运行，TaskManager 日志可见 +I 行）
-- INSERT INTO print_user_cdc SELECT id, app_code, mobile FROM src_user_cdc;
