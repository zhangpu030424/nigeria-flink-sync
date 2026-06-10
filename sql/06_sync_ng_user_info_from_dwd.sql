-- DWD 中间表 → 目标 user_info（Batch 普通 JOIN，DWD 已物化）
-- 前置: bash scripts/run-ng-user-info-gpt-dwd.sh Step1 已灌满 dwd_*
-- 执行: bash scripts/run-sql.sh sql/06_sync_ng_user_info_from_dwd.sql

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

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
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_user_base',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
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
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_data',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
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
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_password',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_device_channel (
    deviceId BIGINT,
    id BIGINT,
    channel STRING,
    PRIMARY KEY (deviceId) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_device_channel',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_user_reg_ip (
    userId BIGINT,
    id BIGINT,
    ip STRING,
    created TIMESTAMP(0),
    PRIMARY KEY (userId) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'dwd_latest_user_reg_ip',
    'username' = '${DWD_MYSQL_USER}',
    'password' = '${DWD_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_app (
    id INT,
    name STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_APP_BASE}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '1000'
);

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
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    u.user_id AS user_id,
    COALESCE(ud.bvn, '') AS id_number,
    COALESCE(TRIM(CONCAT_WS(' ', ud.firstName, ud.middleName, ud.lastName)), '') AS full_name,
    COALESCE(lup.password, '') AS password,
    '' AS live_image,
    '' AS id_card,
    JSON_STRING(
        JSON_OBJECT(
            'email' VALUE COALESCE(ud.email, ''),
            'birthday' VALUE COALESCE(ud.birthday, ''),
            'gender' VALUE COALESCE(ud.gender, ''),
            'id_card' VALUE CAST(NULL AS STRING),
            'live_image' VALUE CAST(NULL AS STRING),
            'face_similarity' VALUE CAST(NULL AS STRING),
            'address' VALUE JSON_OBJECT(
                'province' VALUE CAST(NULL AS STRING),
                'city' VALUE COALESCE(ud.addressState, ''),
                'district' VALUE COALESCE(ud.addressDistrict, ''),
                'village' VALUE CAST(NULL AS STRING),
                'detail' VALUE COALESCE(ud.address, '')
            ),
            'company' VALUE COALESCE(ud.company, ''),
            'education' VALUE COALESCE(ud.education, ''),
            'loan_purpose' VALUE CAST(NULL AS STRING),
            'marital' VALUE COALESCE(ud.marital, ''),
            'job_type' VALUE CAST(NULL AS STRING),
            'profession' VALUE COALESCE(ud.profession, ''),
            'religion' VALUE CAST(NULL AS STRING),
            'salary' VALUE COALESCE(ud.salary, ''),
            'registration_ip' VALUE COALESCE(uri.ip, ''),
            'registration_time' VALUE u.reg_time,
            'children_num' VALUE COALESCE(ud.numberOfChildren, ''),
            'pay_cycle' VALUE COALESCE(ud.payCycle, ''),
            'salary_day' VALUE COALESCE(ud.salaryDay, ''),
            'survey' VALUE JSON_OBJECT(
                'survey_loan_cnt' VALUE CAST(NULL AS STRING),
                'survey_outstanding_cnt' VALUE CAST(NULL AS STRING),
                'survey_overdue_max_days' VALUE CAST(NULL AS STRING),
                'survey_overdue_6m' VALUE CAST(NULL AS STRING),
                'survey_loan_amt_total' VALUE CAST(NULL AS STRING)
            ),
            'app' VALUE JSON_OBJECT(
                'name' VALUE COALESCE(app.name, ''),
                'app_id' VALUE CAST(u.app_id AS STRING),
                'version' VALUE CAST(NULL AS STRING)
            ),
            'emergency_contacts' VALUE COALESCE(ud.emergencyContact, ''),
            'install_source' VALUE COALESCE(ldc.channel, ''),
            'credit_limit' VALUE CAST(NULL AS STRING)
        )
    ) AS info,
    CURRENT_TIMESTAMP AS created_at,
    CURRENT_TIMESTAMP AS updated_at
FROM dwd_user_base AS u
LEFT JOIN dwd_latest_user_data AS ud
    ON ud.userId = u.user_id
LEFT JOIN dwd_latest_user_password AS lup
    ON lup.appId = u.app_id AND lup.mobile = u.mobile
LEFT JOIN dwd_latest_user_reg_ip AS uri
    ON uri.userId = u.user_id
LEFT JOIN m_app AS app
    ON app.id = u.app_id
LEFT JOIN dwd_latest_device_channel AS ldc
    ON ldc.deviceId = u.device_id
WHERE u.mobile IS NOT NULL AND TRIM(u.mobile) <> ''
${LM_USER_ID_RANGE_CLAUSE}
${LM_MIGRATION_LIMIT_CLAUSE};
