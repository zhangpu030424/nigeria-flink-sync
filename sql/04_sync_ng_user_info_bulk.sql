-- 作业 1/5：user_info（01_user_info v2 + BigInteger 修复）
-- 执行: bash scripts/run-ng-user-info-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=10000 bash scripts/run-ng-user-info-bulk.sh
--
-- BigInteger 根因：src_mkt_user 上 DISTINCT/GROUP BY deviceId 与 JDBC Source 算子链融合 → getLong 失败
-- 修复：去掉 v_user_devices/v_user_keys 预聚合；id/userId 用 INT；deviceId 用 STRING

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

CREATE TABLE src_mkt_user (
    id INT,
    `appId` INT,
    mobile STRING,
    `deviceId` STRING,
    created TIMESTAMP(0),
    updated TIMESTAMP(0),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${LM_MYSQL_USER_PARTITION_NUM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '${LM_MYSQL_USER_ID_MAX}',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_user_data (
    id INT,
    `userId` INT,
    bvn STRING,
    `firstName` STRING,
    `middleName` STRING,
    `lastName` STRING,
    email STRING,
    gender TINYINT,
    birthday STRING,
    marital TINYINT,
    profession STRING,
    education TINYINT,
    salary INT,
    `addressState` STRING,
    `addressDistrict` STRING,
    address STRING,
    `emergencyContact` STRING,
    `numberOfChildren` TINYINT,
    `payCycle` TINYINT,
    company STRING,
    `salaryDay` TINYINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_data',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${LM_MYSQL_USER_DATA_PARTITION_NUM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '${LM_MYSQL_USER_DATA_ID_MAX}',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_log_user_password (
    id INT,
    `appId` INT,
    mobile STRING,
    password STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'log_user_password',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${LM_MYSQL_LOG_PASSWORD_PARTITION_NUM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '${LM_MYSQL_LOG_PASSWORD_ID_MAX}',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_device_ad_channel (
    id INT,
    `deviceId` STRING,
    channel STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'device_ad_channel',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${LM_MYSQL_DEVICE_CHANNEL_PARTITION_NUM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '${LM_MYSQL_DEVICE_CHANNEL_ID_MAX}',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TEMPORARY VIEW v_user_batch AS
SELECT id, `appId`, mobile, `deviceId`, created, updated
FROM src_mkt_user
WHERE id <= ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT `userId`, bvn, `firstName`, `middleName`, `lastName`, email, gender, birthday,
       marital, profession, education, salary, `addressState`, `addressDistrict`, address,
       `emergencyContact`, `numberOfChildren`, `payCycle`, company, `salaryDay`
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `userId` ORDER BY id DESC) AS rn
    FROM src_mkt_user_data
) t WHERE rn = 1;

-- 不再经 v_user_keys 过滤（避免 user 源表 HashAggregate）；JOIN 时按 appId+mobile 匹配
CREATE TEMPORARY VIEW v_lup_latest AS
SELECT `appId`, mobile, password
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `appId`, mobile ORDER BY id DESC) AS rn
    FROM src_mkt_log_user_password
) t WHERE rn = 1;

-- 不再经 v_user_devices 过滤；JOIN 时按 deviceId 字符串匹配
CREATE TEMPORARY VIEW v_dac_latest AS
SELECT `deviceId`, channel
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `deviceId` ORDER BY id DESC) AS rn
    FROM src_mkt_device_ad_channel
    WHERE `deviceId` IS NOT NULL AND TRIM(`deviceId`) <> '' AND `deviceId` <> '0'
) t WHERE rn = 1;

CREATE TABLE sink_user_info (
    user_id BIGINT,
    id_number STRING,
    full_name STRING,
    password STRING,
    live_image STRING,
    id_card STRING,
    info STRING,
    created_at TIMESTAMP(0),
    updated_at TIMESTAMP(0),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    CAST(u.id AS BIGINT),
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    lup.password,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    JSON_STRING(JSON_OBJECT(
        'full_name' VALUE TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)),
        'email' VALUE ud.email,
        'birthday' VALUE ud.birthday,
        'gender' VALUE ud.gender,
        'address' VALUE JSON_OBJECT(
            'province' VALUE ud.`addressState`,
            'city' VALUE ud.`addressDistrict`,
            'detail' VALUE ud.address
        ),
        'company' VALUE ud.company,
        'education' VALUE ud.education,
        'marital' VALUE ud.marital,
        'profession' VALUE ud.profession,
        'salary' VALUE ud.salary,
        'emergency_contacts' VALUE ud.`emergencyContact`,
        'children_num' VALUE ud.`numberOfChildren`,
        'pay_cycle' VALUE ud.`payCycle`,
        'salary_day' VALUE ud.`salaryDay`,
        'app' VALUE JSON_OBJECT('app_id' VALUE CAST(u.`appId` AS STRING)),
        'install_source' VALUE dac.channel
    )),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM v_user_batch u
LEFT JOIN v_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_dac_latest dac
    ON u.`deviceId` IS NOT NULL
   AND TRIM(u.`deviceId`) <> ''
   AND u.`deviceId` <> '0'
   AND dac.`deviceId` = u.`deviceId`;
