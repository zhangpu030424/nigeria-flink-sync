-- 作业 1/5：user_info（来自 01_user_info.sql）
-- 执行: bash scripts/run-ng-user-info-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=20

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.resource.default-parallelism' = '${FLINK_PARALLELISM}';

CREATE TABLE src_mkt_user (
    id BIGINT, `appId` INT, mobile STRING, `deviceId` BIGINT,
    created TIMESTAMP(0), updated TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user'
);

CREATE TABLE src_mkt_user_data (
    id INT, `userId` BIGINT, bvn STRING,
    `firstName` STRING, `middleName` STRING, `lastName` STRING,
    email STRING, gender TINYINT, birthday STRING, marital TINYINT,
    profession STRING, education TINYINT, salary INT,
    `addressState` STRING, `addressDistrict` STRING, address STRING,
    `emergencyContact` STRING, `numberOfChildren` TINYINT,
    `payCycle` TINYINT, company STRING, `salaryDay` TINYINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user_data'
);

CREATE TABLE src_mkt_log_user_password (
    id INT, `appId` INT, mobile STRING, password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'log_user_password'
);

CREATE TABLE src_mkt_device_ad_channel (
    id INT, `deviceId` BIGINT, channel STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'device_ad_channel'
);

CREATE TABLE sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    created_at TIMESTAMP(0), updated_at TIMESTAMP(0),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3',
    'table-name' = 'user_info'
);

CREATE TEMPORARY VIEW v_user_batch AS
SELECT * FROM src_mkt_user ORDER BY id LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT ud.* FROM src_mkt_user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id FROM src_mkt_user_data GROUP BY `userId`
) x ON x.max_id = ud.id;

CREATE TEMPORARY VIEW v_lup_latest AS
SELECT l1.`appId`, l1.mobile, l1.password FROM src_mkt_log_user_password l1
INNER JOIN (
    SELECT `appId`, mobile, MAX(id) AS max_id FROM src_mkt_log_user_password GROUP BY `appId`, mobile
) x ON x.max_id = l1.id;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT d1.* FROM src_mkt_device_ad_channel d1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id FROM src_mkt_device_ad_channel GROUP BY `deviceId`
) x ON x.max_id = d1.id;

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
LEFT JOIN v_dac_latest dac ON dac.`deviceId` = u.`deviceId`
ORDER BY u.id
LIMIT ${LM_MIGRATION_LIMIT};
