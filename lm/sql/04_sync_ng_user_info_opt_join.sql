-- user_info Flink 多表 Join（减轻版）：读 MySQL VIEW，过滤与 MAX(id) 在库内完成
-- 前置: bash lm/scripts/refresh-flink-migration-pick.sh
-- 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-opt-join.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_pick_user (
    id              DECIMAL(20, 0),
    `appId`         DECIMAL(20, 0),
    mobile          STRING,
    `deviceId`      DECIMAL(20, 0),
    created         TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'v_flink_pick_user',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_pick_ud (
    id                  DECIMAL(20, 0),
    `userId`            DECIMAL(20, 0),
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
    salary              DECIMAL(20, 0),
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
    'table-name' = 'v_flink_pick_ud_latest',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_pick_lup (
    `appId`  DECIMAL(20, 0),
    mobile   STRING,
    password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'v_flink_pick_lup_latest',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_pick_dac (
    `deviceId` DECIMAL(20, 0),
    channel    STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'v_flink_pick_dac_latest',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_mkt_app (
    id   DECIMAL(20, 0),
    name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

-- registration_ip 源：refresh 脚本创建 v_flink_pick_uri_latest；无表时 run 脚本会去掉
CREATE TABLE src_pick_uri (
    `userId` DECIMAL(20, 0),
    ip       STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'v_flink_pick_uri_latest',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
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

INSERT INTO sink_user_info
SELECT
    CAST(u.id AS BIGINT),
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    lup.password,
    CAST(NULL AS STRING),
    CAST(NULL AS STRING),
    JSON_STRING(
        JSON_OBJECT(
            'email' VALUE ud.email,
            'birthday' VALUE ud.birthday,
            'gender' VALUE CAST(ud.gender AS INT),
            'address' VALUE JSON_OBJECT(
                'province' VALUE CAST(NULL AS STRING),
                'city' VALUE ud.`addressState`,
                'district' VALUE ud.`addressDistrict`,
                'detail' VALUE ud.address
            ),
            'company' VALUE ud.company,
            'education' VALUE CAST(ud.education AS INT),
            'marital' VALUE CAST(ud.marital AS INT),
            'profession' VALUE ud.profession,
            'salary' VALUE CAST(ud.salary AS BIGINT),
            'emergency_contacts' VALUE ud.`emergencyContact`,
            'registration_ip' VALUE uri.ip,
            'registration_time' VALUE CAST(UNIX_TIMESTAMP(CAST(u.created AS STRING)) AS BIGINT) * 1000,
            'children_num' VALUE CAST(ud.`numberOfChildren` AS INT),
            'pay_cycle' VALUE CAST(ud.`payCycle` AS INT),
            'salary_day' VALUE CAST(ud.`salaryDay` AS INT),
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
FROM src_pick_user u
LEFT JOIN src_pick_ud ud ON ud.`userId` = u.id
LEFT JOIN src_pick_lup lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN src_pick_uri uri ON uri.`userId` = u.id
LEFT JOIN src_pick_dac dac ON dac.`deviceId` = u.`deviceId` AND u.`deviceId` IS NOT NULL AND u.`deviceId` > 0
LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`;
