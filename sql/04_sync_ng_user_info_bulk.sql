-- 作业 1/5：user_info（01_user_info v2 + BigInteger 修复）
-- 执行: bash scripts/run-ng-user-info-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=10000 bash scripts/run-ng-user-info-bulk.sh
--
-- 前置: 老库已建 VIEW v_flink_mkt_*（run-ng-user-info-bulk.sh 会自动创建）
-- 根因: JDBC 读 MySQL BIGINT → BigInteger，与 HashAggregate getLong 冲突
-- 修复: MySQL VIEW 侧 CAST 为 CHAR，Flink 全 STRING 读，下游再 CAST

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';
SET 'table.exec.resource.default-shuffle-mode' = 'ALL';

CREATE TABLE src_mkt_user (
    id STRING,
    `appId` STRING,
    mobile STRING,
    `deviceId` STRING,
    created TIMESTAMP(0),
    updated TIMESTAMP(0),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_mkt_user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_user_data (
    id STRING,
    `userId` STRING,
    bvn STRING,
    `firstName` STRING,
    `middleName` STRING,
    `lastName` STRING,
    email STRING,
    gender STRING,
    birthday STRING,
    marital STRING,
    profession STRING,
    education STRING,
    salary STRING,
    `addressState` STRING,
    `addressDistrict` STRING,
    address STRING,
    `emergencyContact` STRING,
    `numberOfChildren` STRING,
    `payCycle` STRING,
    company STRING,
    `salaryDay` STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_mkt_user_data',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_log_user_password (
    id STRING,
    `appId` STRING,
    mobile STRING,
    password STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_mkt_log_user_password',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_device_ad_channel (
    id STRING,
    `deviceId` STRING,
    channel STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_mkt_device_ad_channel',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'driver' = 'com.mysql.cj.jdbc.Driver',
    'scan.fetch-size' = '${LM_JDBC_FETCH_SIZE}'
);

CREATE TEMPORARY VIEW v_user_batch AS
SELECT id, `appId`, mobile, `deviceId`, created, updated
FROM src_mkt_user
WHERE CAST(id AS BIGINT) <= ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT
    ud.`userId`, ud.bvn, ud.`firstName`, ud.`middleName`, ud.`lastName`, ud.email,
    CAST(COALESCE(NULLIF(TRIM(ud.gender), ''), '0') AS TINYINT) AS gender,
    ud.birthday,
    CAST(COALESCE(NULLIF(TRIM(ud.marital), ''), '0') AS TINYINT) AS marital,
    ud.profession,
    CAST(COALESCE(NULLIF(TRIM(ud.education), ''), '0') AS TINYINT) AS education,
    CAST(COALESCE(NULLIF(TRIM(ud.salary), ''), '0') AS INT) AS salary,
    ud.`addressState`, ud.`addressDistrict`, ud.address, ud.`emergencyContact`,
    CAST(COALESCE(NULLIF(TRIM(ud.`numberOfChildren`), ''), '0') AS TINYINT) AS `numberOfChildren`,
    CAST(COALESCE(NULLIF(TRIM(ud.`payCycle`), ''), '0') AS TINYINT) AS `payCycle`,
    ud.company,
    CAST(COALESCE(NULLIF(TRIM(ud.`salaryDay`), ''), '0') AS TINYINT) AS `salaryDay`
FROM src_mkt_user_data ud
INNER JOIN (
    SELECT `userId`, MAX(CAST(id AS BIGINT)) AS max_id
    FROM src_mkt_user_data
    GROUP BY `userId`
) m ON ud.`userId` = m.`userId` AND CAST(ud.id AS BIGINT) = m.max_id;

CREATE TEMPORARY VIEW v_lup_latest AS
SELECT l1.`appId`, l1.mobile, l1.password
FROM src_mkt_log_user_password l1
INNER JOIN (
    SELECT `appId`, mobile, MAX(CAST(id AS BIGINT)) AS max_id
    FROM src_mkt_log_user_password
    GROUP BY `appId`, mobile
) l2 ON l2.`appId` = l1.`appId` AND l2.mobile = l1.mobile AND CAST(l1.id AS BIGINT) = l2.max_id;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT dac1.`deviceId`, dac1.channel
FROM src_mkt_device_ad_channel dac1
INNER JOIN (
    SELECT `deviceId`, MAX(CAST(id AS BIGINT)) AS max_id
    FROM src_mkt_device_ad_channel
    WHERE `deviceId` IS NOT NULL AND TRIM(`deviceId`) <> '' AND `deviceId` <> '0'
    GROUP BY `deviceId`
) m ON dac1.`deviceId` = m.`deviceId` AND CAST(dac1.id AS BIGINT) = m.max_id;

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
        'app' VALUE JSON_OBJECT('app_id' VALUE u.`appId`),
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
