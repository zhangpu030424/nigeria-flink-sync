-- 阶段 B：user 表最小同步测试（源 → 目标，不含 VT/UTM）
-- 执行前：目标库已执行 docs/schema/Target.sql；替换下方连接参数

CREATE TABLE IF NOT EXISTS src_user (
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
    'server-time-zone' = 'Africa/Lagos'
);

CREATE TABLE IF NOT EXISTS dim_app_config (
    id INT,
    app_code STRING,
    PRIMARY KEY (app_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://SOURCE_MYSQL_HOST:3306/nigeria_backend?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'app_config',
    'username' = 'SOURCE_MYSQL_USER',
    'password' = 'SOURCE_MYSQL_PASSWORD',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '10min'
);

CREATE TABLE IF NOT EXISTS sink_user (
    user_id BIGINT,
    app_id INT,
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
    'url' = 'jdbc:mysql://TARGET_MYSQL_HOST:3306/platform_db?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'user',
    'username' = 'TARGET_MYSQL_USER',
    'password' = 'TARGET_MYSQL_PASSWORD'
);

INSERT INTO sink_user
SELECT
    u.id + 100000000 AS user_id,
    COALESCE(a.id, 0) AS app_id,
    u.id AS group_user_id,
    u.id + 100000000 AS info_user_id,
    u.mobile,
    CAST(0 AS BIGINT) AS closed_time,
    COALESCE(u.device_id, '') AS reg_device_uuid,
    UNIX_TIMESTAMP(u.create_time) * 1000 AS reg_time,
    CAST(0 AS TINYINT) AS test_flag,
    u.create_time AS created_at,
    u.update_time AS updated_at
FROM src_user AS u
LEFT JOIN dim_app_config FOR SYSTEM_TIME AS OF u.proc_time AS a
    ON u.app_code = a.app_code;
