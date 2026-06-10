-- 作业 1/5：user_info（来自 Downloads/01_user_info(2).sql）
-- 执行: bash scripts/run-ng-user-info-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=10000 bash scripts/run-ng-user-info-bulk.sh
--
-- 慢因：JDBC 默认单线程全表扫；log_user_password 千万级且无 (appId,mobile) 索引最慢
-- 优化：4 表 id 分区并行读；密码/渠道先按 user 键过滤再 ROW_NUMBER；无 ALL_EXCHANGES_BLOCKING
-- 仍慢时在 ng_loan_market 执行:
--   ALTER TABLE log_user_password ADD INDEX idx_app_mobile (`appId`, mobile);
--
-- JDBC 读 MySQL BIGINT 常为 BigInteger，HashAggregate/DISTINCT 会 ClassCastException → 下游视图统一 CAST

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_mkt_user (
    id BIGINT, `appId` INT, mobile STRING, `deviceId` BIGINT,
    created TIMESTAMP(0), updated TIMESTAMP(0),
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
    id INT, `userId` BIGINT, bvn STRING,
    `firstName` STRING, `middleName` STRING, `lastName` STRING,
    email STRING, gender TINYINT, birthday STRING, marital TINYINT,
    profession STRING, education TINYINT, salary INT,
    `addressState` STRING, `addressDistrict` STRING, address STRING,
    `emergencyContact` STRING, `numberOfChildren` TINYINT,
    `payCycle` TINYINT, company STRING, `salaryDay` TINYINT,
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
    id INT, `appId` INT, mobile STRING, password STRING,
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
    id INT, `deviceId` BIGINT, channel STRING,
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

CREATE TEMPORARY VIEW v_src_mkt_user AS
SELECT
    CAST(id AS BIGINT) AS id,
    CAST(`appId` AS INT) AS `appId`,
    mobile,
    CAST(`deviceId` AS BIGINT) AS `deviceId`,
    created,
    updated
FROM src_mkt_user;

CREATE TEMPORARY VIEW v_src_mkt_user_data AS
SELECT
    CAST(id AS INT) AS id,
    CAST(`userId` AS BIGINT) AS `userId`,
    bvn, `firstName`, `middleName`, `lastName`, email,
    CAST(gender AS TINYINT) AS gender,
    birthday,
    CAST(marital AS TINYINT) AS marital,
    profession,
    CAST(education AS TINYINT) AS education,
    CAST(salary AS INT) AS salary,
    `addressState`, `addressDistrict`, address,
    `emergencyContact`,
    CAST(`numberOfChildren` AS TINYINT) AS `numberOfChildren`,
    CAST(`payCycle` AS TINYINT) AS `payCycle`,
    company,
    CAST(`salaryDay` AS TINYINT) AS `salaryDay`
FROM src_mkt_user_data;

CREATE TEMPORARY VIEW v_src_mkt_log_user_password AS
SELECT
    CAST(id AS INT) AS id,
    CAST(`appId` AS INT) AS `appId`,
    mobile,
    password
FROM src_mkt_log_user_password;

CREATE TEMPORARY VIEW v_src_mkt_device_ad_channel AS
SELECT
    CAST(id AS INT) AS id,
    CAST(`deviceId` AS BIGINT) AS `deviceId`,
    channel
FROM src_mkt_device_ad_channel;

CREATE TEMPORARY VIEW v_user_batch AS
SELECT * FROM v_src_mkt_user
WHERE id <= ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_user_keys AS
SELECT DISTINCT `appId`, mobile FROM v_user_batch;

CREATE TEMPORARY VIEW v_user_devices AS
SELECT DISTINCT `deviceId` FROM v_user_batch WHERE `deviceId` > 0;

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT `userId`, bvn, `firstName`, `middleName`, `lastName`, email, gender, birthday,
       marital, profession, education, salary, `addressState`, `addressDistrict`, address,
       `emergencyContact`, `numberOfChildren`, `payCycle`, company, `salaryDay`
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `userId` ORDER BY id DESC) AS rn
    FROM v_src_mkt_user_data
) t WHERE rn = 1;

CREATE TEMPORARY VIEW v_lup_latest AS
SELECT `appId`, mobile, password
FROM (
    SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.`appId`, l.mobile ORDER BY l.id DESC) AS rn
    FROM v_src_mkt_log_user_password l
    INNER JOIN v_user_keys k ON l.`appId` = k.`appId` AND l.mobile = k.mobile
) t WHERE rn = 1;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT `deviceId`, channel
FROM (
    SELECT d.*, ROW_NUMBER() OVER (PARTITION BY d.`deviceId` ORDER BY d.id DESC) AS rn
    FROM v_src_mkt_device_ad_channel d
    INNER JOIN v_user_devices ud ON d.`deviceId` = ud.`deviceId`
    WHERE d.`deviceId` IS NOT NULL AND d.`deviceId` > 0
) t WHERE rn = 1;

CREATE TABLE sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    created_at TIMESTAMP(0), updated_at TIMESTAMP(0),
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
    u.id,
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    lup.password,
    CAST(NULL AS STRING), CAST(NULL AS STRING),
    JSON_STRING(JSON_OBJECT(
        'full_name' VALUE TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)),
        'email' VALUE ud.email, 'birthday' VALUE ud.birthday, 'gender' VALUE ud.gender,
        'address' VALUE JSON_OBJECT(
            'province' VALUE ud.`addressState`, 'city' VALUE ud.`addressDistrict`, 'detail' VALUE ud.address
        ),
        'company' VALUE ud.company, 'education' VALUE ud.education, 'marital' VALUE ud.marital,
        'profession' VALUE ud.profession, 'salary' VALUE ud.salary,
        'emergency_contacts' VALUE ud.`emergencyContact`,
        'children_num' VALUE ud.`numberOfChildren`, 'pay_cycle' VALUE ud.`payCycle`, 'salary_day' VALUE ud.`salaryDay`,
        'app' VALUE JSON_OBJECT('app_id' VALUE CAST(u.`appId` AS STRING)),
        'install_source' VALUE dac.channel
    )),
    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
FROM v_user_batch u
LEFT JOIN v_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_dac_latest dac ON dac.`deviceId` = u.`deviceId` AND u.`deviceId` > 0;
