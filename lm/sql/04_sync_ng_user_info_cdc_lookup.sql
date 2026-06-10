-- user_info：MySQL CDC 分片快照 + Temporal Lookup（无库内 VIEW）
-- 驱动表 user 走 CDC chunk；小子表 app 走 JDBC Lookup；大表 CDC 读入后在 Flink ROW_NUMBER 取最新
-- 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-cdc-lookup.sh
-- 全量: LM_MIGRATION_LIMIT=2147483647 FLINK_PARALLELISM=8 bash lm/scripts/run-ng-user-info-cdc-lookup.sh
-- 前置: LM_MYSQL_USER 需 REPLICATION SLAVE/CLIENT 权限（CDC binlog 快照）

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE cdc_mkt_user (
    id              DECIMAL(20, 0),
    `appId`         DECIMAL(20, 0),
    mobile          STRING,
    `deviceId`      DECIMAL(20, 0),
    created         TIMESTAMP(0),
    proc_time       AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${LM_MYSQL_HOST}',
    'port' = '${LM_MYSQL_PORT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'database-name' = '${LM_MYSQL_DATABASE}',
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${LM_CDC_SERVER_ID_USER}',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
    __CDC_USER_SNAPSHOT_OVERRIDE__
);

CREATE TABLE cdc_mkt_user_data (
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
    `salaryDay`         TINYINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${LM_MYSQL_HOST}',
    'port' = '${LM_MYSQL_PORT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'database-name' = '${LM_MYSQL_DATABASE}',
    'table-name' = 'user_data',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${LM_CDC_SERVER_ID_USER_DATA}',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE cdc_mkt_log_user_password (
    id       DECIMAL(20, 0),
    `appId`  DECIMAL(20, 0),
    mobile   STRING,
    password STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${LM_MYSQL_HOST}',
    'port' = '${LM_MYSQL_PORT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'database-name' = '${LM_MYSQL_DATABASE}',
    'table-name' = 'log_user_password',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${LM_CDC_SERVER_ID_LUP}',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE cdc_mkt_device_ad_channel (
    id         DECIMAL(20, 0),
    `deviceId` DECIMAL(20, 0),
    channel    STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${LM_MYSQL_HOST}',
    'port' = '${LM_MYSQL_PORT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'database-name' = '${LM_MYSQL_DATABASE}',
    'table-name' = 'device_ad_channel',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${LM_CDC_SERVER_ID_DAC}',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

-- registration_ip：run 脚本按老库表名注入；无表时去掉
CREATE TABLE cdc_mkt_user_reg_ip (
    id       DECIMAL(20, 0),
    `userId` DECIMAL(20, 0),
    ip       STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${LM_MYSQL_HOST}',
    'port' = '${LM_MYSQL_PORT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'database-name' = '${LM_MYSQL_DATABASE}',
    'table-name' = '${LM_USER_REG_IP_TABLE}',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${LM_CDC_SERVER_ID_URI}',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

-- 小子表 app：Temporal Lookup（适合维表点查）
CREATE TABLE dim_mkt_app (
    id   DECIMAL(20, 0),
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app',
    'lookup.cache.max-rows' = '5000',
    'lookup.cache.ttl' = '2h'
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
SELECT id, `appId`, mobile, `deviceId`, created, proc_time
FROM cdc_mkt_user
ORDER BY id DESC
LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT id, `userId`, bvn, `firstName`, `middleName`, `lastName`, email, gender, birthday,
       marital, profession, education, salary, `addressState`, `addressDistrict`, address,
       `emergencyContact`, `numberOfChildren`, `payCycle`, company, `salaryDay`
FROM (
    SELECT ud.*, ROW_NUMBER() OVER (PARTITION BY ud.`userId` ORDER BY ud.id DESC) AS rn
    FROM cdc_mkt_user_data ud
    INNER JOIN v_user_lim u ON u.id = ud.`userId`
) t
WHERE rn = 1;

CREATE TEMPORARY VIEW v_lup_latest AS
SELECT `appId`, mobile, password
FROM (
    SELECT l.`appId`, l.mobile, l.password,
           ROW_NUMBER() OVER (PARTITION BY l.`appId`, l.mobile ORDER BY l.id DESC) AS rn
    FROM cdc_mkt_log_user_password l
    INNER JOIN v_user_lim u ON u.`appId` = l.`appId` AND u.mobile = l.mobile
) t
WHERE rn = 1;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT `deviceId`, channel
FROM (
    SELECT dac.`deviceId`, dac.channel,
           ROW_NUMBER() OVER (PARTITION BY dac.`deviceId` ORDER BY dac.id DESC) AS rn
    FROM cdc_mkt_device_ad_channel dac
    INNER JOIN v_user_lim u
        ON u.`deviceId` = dac.`deviceId`
       AND u.`deviceId` IS NOT NULL
       AND u.`deviceId` > 0
) t
WHERE rn = 1;

CREATE TEMPORARY VIEW v_uri_latest AS
SELECT `userId`, ip
FROM (
    SELECT uri.`userId`, uri.ip,
           ROW_NUMBER() OVER (PARTITION BY uri.`userId` ORDER BY uri.id DESC) AS rn
    FROM cdc_mkt_user_reg_ip uri
    INNER JOIN v_user_lim u ON u.id = uri.`userId`
) t
WHERE rn = 1;

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
FROM v_user_lim u
LEFT JOIN v_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_uri_latest uri ON uri.`userId` = u.id
LEFT JOIN dim_mkt_app FOR SYSTEM_TIME AS OF u.proc_time AS ap
    ON ap.id = u.`appId`
LEFT JOIN v_dac_latest dac ON dac.`deviceId` = u.`deviceId`;
