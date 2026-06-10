-- 老库 user_info 单表试跑（索引优化版 SQL 逻辑）
-- 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-opt.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_mkt_user (
    id              BIGINT,
    `appId`         INT,
    mobile          STRING,
    `deviceId`      BIGINT,
    created         TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user'
);

CREATE TABLE src_mkt_app (
    id   INT,
    name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app'
);

CREATE TABLE src_mkt_user_data (
    id                  INT,
    `userId`            BIGINT,
    bvn                 STRING,
    `firstName`         STRING,
    `middleName`        STRING,
    `lastName`          STRING,
    email               STRING,
    gender              TINYINT,
    birthday            STRING,
    marital             TINYINT,
    profession          STRING,
    education           TINYINT,
    salary              INT,
    `addressState`      STRING,
    `addressDistrict`   STRING,
    address             STRING,
    `emergencyContact`  STRING,
    `numberOfChildren`  TINYINT,
    `payCycle`          TINYINT,
    company             STRING,
    `salaryDay`         TINYINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user_data'
);

CREATE TABLE src_mkt_device_ad_channel (
    id                                    INT,
    `deviceId`                            BIGINT,
    channel                               STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'device_ad_channel'
);

CREATE TABLE src_mkt_log_user_password (
    id       INT,
    `appId`  INT,
    mobile   STRING,
    password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'log_user_password'
);

-- 老库常见表名 user_registration_ip；若是 user_reg_ip 请改 table-name
CREATE TABLE src_mkt_user_reg_ip (
    id       INT,
    `userId` BIGINT,
    ip       STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user_registration_ip'
);

CREATE TABLE sink_user_info (
    user_id     BIGINT,
    id_number   STRING,
    full_name   STRING,
    password    STRING,
    live_image  STRING,
    id_card     STRING,
    info        STRING,
    created_at  TIMESTAMP(0),
    updated_at  TIMESTAMP(0),
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

CREATE TEMPORARY VIEW v_user_lim AS
SELECT * FROM src_mkt_user
ORDER BY id DESC
LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT ud.*
FROM src_mkt_user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM src_mkt_user_data
    GROUP BY `userId`
) ud_max ON ud_max.max_id = ud.id;

CREATE TEMPORARY VIEW v_lup_latest AS
SELECT l1.`appId`, l1.mobile, l1.password
FROM src_mkt_log_user_password l1
INNER JOIN (
    SELECT l.`appId`, l.mobile, MAX(l.id) AS max_id
    FROM src_mkt_log_user_password l
    INNER JOIN v_user_lim uk ON uk.`appId` = l.`appId` AND uk.mobile = l.mobile
    GROUP BY l.`appId`, l.mobile
) lm ON lm.max_id = l1.id;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT dac1.`deviceId`, dac1.channel
FROM src_mkt_device_ad_channel dac1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id
    FROM src_mkt_device_ad_channel
    WHERE `deviceId` IS NOT NULL AND `deviceId` > 0
    GROUP BY `deviceId`
) dac_max ON dac_max.max_id = dac1.id;

CREATE TEMPORARY VIEW v_uri_latest AS
SELECT uri1.`userId`, uri1.ip
FROM src_mkt_user_reg_ip uri1
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM src_mkt_user_reg_ip
    GROUP BY `userId`
) uri_max ON uri_max.max_id = uri1.id;

INSERT INTO sink_user_info
SELECT
    u.id,
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    lup.password,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    JSON_STRING(
        JSON_OBJECT(
            'email' VALUE ud.email,
            'birthday' VALUE ud.birthday,
            'gender' VALUE ud.gender,
            'address' VALUE JSON_OBJECT(
                'province' VALUE CAST(NULL AS STRING),
                'city' VALUE ud.`addressState`,
                'district' VALUE ud.`addressDistrict`,
                'detail' VALUE ud.address
            ),
            'company' VALUE ud.company,
            'education' VALUE ud.education,
            'marital' VALUE ud.marital,
            'profession' VALUE ud.profession,
            'salary' VALUE ud.salary,
            'emergency_contacts' VALUE ud.`emergencyContact`,
            'registration_ip' VALUE uri.ip,
            'registration_time' VALUE CAST(UNIX_TIMESTAMP(CAST(u.created AS STRING)) AS BIGINT) * 1000,
            'children_num' VALUE ud.`numberOfChildren`,
            'pay_cycle' VALUE ud.`payCycle`,
            'salary_day' VALUE ud.`salaryDay`,
            'app' VALUE JSON_OBJECT(
                'name' VALUE ap.name,
                'app_id' VALUE CAST(u.`appId` AS STRING),
                'version' VALUE CAST(NULL AS STRING)
            ),
            'install_source' VALUE dac.channel
        )
    ),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM v_user_lim u
LEFT JOIN v_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_uri_latest uri ON uri.`userId` = u.id
LEFT JOIN v_dac_latest dac ON dac.`deviceId` = u.`deviceId` AND u.`deviceId` IS NOT NULL AND u.`deviceId` > 0
LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`;
