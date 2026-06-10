-- 老库 ng_loan_market → DWD 中间表（Flink Batch，ROW_NUMBER 在 Flink 内聚合）
-- 来源: sql.md §1-2  user_info 子集
-- 执行: bash scripts/run-ng-user-info-gpt-dwd.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

-- ========== 老库源表 ==========
CREATE TABLE m_user (
    id DECIMAL(20, 0),
    `appId` INT,
    mobile STRING,
    `deviceId` DECIMAL(20, 0),
    created TIMESTAMP(0),
    updated TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_user_data (
    id DECIMAL(20, 0),
    `userId` DECIMAL(20, 0),
    bvn STRING,
    `firstName` STRING,
    `middleName` STRING,
    `lastName` STRING,
    email STRING,
    birthday STRING,
    gender STRING,
    `addressState` STRING,
    `addressDistrict` STRING,
    address STRING,
    company STRING,
    education STRING,
    marital STRING,
    profession STRING,
    salary STRING,
    `numberOfChildren` STRING,
    `payCycle` STRING,
    `salaryDay` STRING,
    `emergencyContact` STRING,
    created TIMESTAMP(0),
    updated TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_data',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_log_user_password (
    id DECIMAL(20, 0),
    `appId` INT,
    mobile STRING,
    password STRING,
    created TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'log_user_password',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_device_ad_channel (
    id DECIMAL(20, 0),
    `deviceId` DECIMAL(20, 0),
    channel STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'device_ad_channel',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_user_reg_ip (
    id DECIMAL(20, 0),
    `userId` DECIMAL(20, 0),
    ip STRING,
    created TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_URI_BASE}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

-- ========== DWD Sink（目标/DWD 库）==========
CREATE TABLE dwd_user_base (
    user_id BIGINT,
    app_id INT,
    mobile STRING,
    device_id BIGINT,
    is_cancel TINYINT,
    created TIMESTAMP(0),
    updated TIMESTAMP(0),
    closed_time BIGINT,
    reg_time BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_user_base',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

CREATE TABLE dwd_latest_user_data (
    userId BIGINT,
    id BIGINT,
    bvn STRING,
    firstName STRING,
    middleName STRING,
    lastName STRING,
    email STRING,
    birthday STRING,
    gender STRING,
    addressState STRING,
    addressDistrict STRING,
    address STRING,
    company STRING,
    education STRING,
    marital STRING,
    profession STRING,
    salary STRING,
    numberOfChildren STRING,
    payCycle STRING,
    salaryDay STRING,
    emergencyContact STRING,
    created TIMESTAMP(0),
    updated TIMESTAMP(0),
    PRIMARY KEY (userId) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_data',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

CREATE TABLE dwd_latest_user_password (
    appId INT,
    mobile STRING,
    id BIGINT,
    password STRING,
    created TIMESTAMP(0),
    PRIMARY KEY (appId, mobile) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_password',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

CREATE TABLE dwd_latest_device_channel (
    deviceId BIGINT,
    id BIGINT,
    channel STRING,
    PRIMARY KEY (deviceId) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_device_channel',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

CREATE TABLE dwd_latest_user_reg_ip (
    userId BIGINT,
    id BIGINT,
    ip STRING,
    created TIMESTAMP(0),
    PRIMARY KEY (userId) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_reg_ip',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

-- ========== DWD INSERT（同 sql.md §2 子集）==========
INSERT INTO dwd_user_base
SELECT
    CAST(id AS BIGINT) AS user_id,
    CAST(`appId` AS INT) AS app_id,
    mobile,
    CAST(COALESCE(`deviceId`, 0) AS BIGINT) AS device_id,
    CAST(0 AS TINYINT) AS is_cancel,
    created,
    updated,
    CAST(0 AS BIGINT) AS closed_time,
    CAST(UNIX_TIMESTAMP(CAST(created AS STRING)) AS BIGINT) * 1000 AS reg_time
FROM m_user
WHERE mobile IS NOT NULL AND TRIM(mobile) <> ''
  AND id IS NOT NULL
${LM_MIGRATION_LIMIT_CLAUSE_DWD_USER};

INSERT INTO dwd_latest_user_data
SELECT
    CAST(`userId` AS BIGINT) AS userId,
    CAST(id AS BIGINT) AS id,
    COALESCE(bvn, '') AS bvn,
    COALESCE(`firstName`, '') AS firstName,
    COALESCE(`middleName`, '') AS middleName,
    COALESCE(`lastName`, '') AS lastName,
    COALESCE(email, '') AS email,
    COALESCE(birthday, '') AS birthday,
    COALESCE(CAST(gender AS STRING), '') AS gender,
    COALESCE(`addressState`, '') AS addressState,
    COALESCE(`addressDistrict`, '') AS addressDistrict,
    COALESCE(address, '') AS address,
    COALESCE(company, '') AS company,
    COALESCE(CAST(education AS STRING), '') AS education,
    COALESCE(CAST(marital AS STRING), '') AS marital,
    COALESCE(CAST(profession AS STRING), '') AS profession,
    COALESCE(CAST(salary AS STRING), '') AS salary,
    COALESCE(CAST(`numberOfChildren` AS STRING), '') AS numberOfChildren,
    COALESCE(CAST(`payCycle` AS STRING), '') AS payCycle,
    COALESCE(CAST(`salaryDay` AS STRING), '') AS salaryDay,
    COALESCE(`emergencyContact`, '') AS emergencyContact,
    created,
    updated
FROM (
    SELECT
        ud.*,
        ROW_NUMBER() OVER (PARTITION BY ud.`userId` ORDER BY ud.id DESC) AS rn
    FROM m_user_data ud
)
WHERE rn = 1;

INSERT INTO dwd_latest_user_password
SELECT
    CAST(`appId` AS INT) AS appId,
    mobile,
    CAST(id AS BIGINT) AS id,
    COALESCE(password, '') AS password,
    created
FROM (
    SELECT
        lup.*,
        ROW_NUMBER() OVER (PARTITION BY lup.`appId`, lup.mobile ORDER BY lup.id DESC) AS rn
    FROM m_log_user_password lup
    WHERE lup.mobile IS NOT NULL AND TRIM(lup.mobile) <> ''
)
WHERE rn = 1;

INSERT INTO dwd_latest_device_channel
SELECT
    CAST(`deviceId` AS BIGINT) AS deviceId,
    CAST(id AS BIGINT) AS id,
    COALESCE(channel, '') AS channel
FROM (
    SELECT
        dac.*,
        ROW_NUMBER() OVER (PARTITION BY dac.`deviceId` ORDER BY dac.id DESC) AS rn
    FROM m_device_ad_channel dac
    WHERE dac.`deviceId` IS NOT NULL AND dac.`deviceId` <> 0
)
WHERE rn = 1;
