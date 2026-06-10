-- GPT 版 user_info：Flink 直连老库 VIEW，无 JDBC 分区、无 flink_stg_* 物化表
-- 前置: v_flink_* / v_flink_uri_latest / v_flink_mkt_app（主库建 VIEW 后等同步到从库）
-- 执行: bash scripts/run-ng-user-info-gpt-direct.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

CREATE TABLE m_user (
    id_part DECIMAL(20, 0),
    id STRING,
    `appId` STRING,
    mobile STRING,
    `deviceId` STRING,
    created TIMESTAMP(0),
    updated TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_MKT}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_user_data (
    user_id_part DECIMAL(20, 0),
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
    `salaryDay` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_UD}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_user_password (
    id_part DECIMAL(20, 0),
    `appId` STRING,
    mobile STRING,
    password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_LUP}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_device_channel (
    id_part DECIMAL(20, 0),
    `deviceId` STRING,
    channel STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_DAC}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE dwd_latest_user_reg_ip (
    user_id_part DECIMAL(20, 0),
    `userId` STRING,
    ip STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_URI}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE m_app (
    id STRING,
    name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_APP}',
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
    user_id,
    id_number,
    full_name,
    password,
    live_image,
    id_card,
    info,
    created_at,
    updated_at
FROM (
    SELECT
        CAST(u.id AS BIGINT) AS user_id,
        COALESCE(ud.bvn, '') AS id_number,
        COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), '') AS full_name,
        COALESCE(lup.password, '') AS password,
        '' AS live_image,
        '' AS id_card,
        JSON_STRING(
            JSON_OBJECT(
                'email' VALUE ud.email,
                'birthday' VALUE ud.birthday,
                'gender' VALUE CAST(NULLIF(TRIM(ud.gender), '') AS INT),
                'id_card' VALUE CAST(NULL AS STRING),
                'live_image' VALUE CAST(NULL AS STRING),
                'face_similarity' VALUE CAST(NULL AS STRING),
                'address' VALUE JSON_OBJECT(
                    'province' VALUE CAST(NULL AS STRING),
                    'city' VALUE ud.`addressState`,
                    'district' VALUE ud.`addressDistrict`,
                    'village' VALUE CAST(NULL AS STRING),
                    'detail' VALUE ud.address
                ),
                'company' VALUE ud.company,
                'education' VALUE CAST(NULLIF(TRIM(ud.education), '') AS INT),
                'loan_purpose' VALUE CAST(NULL AS STRING),
                'marital' VALUE CAST(NULLIF(TRIM(ud.marital), '') AS INT),
                'job_type' VALUE CAST(NULL AS STRING),
                'profession' VALUE ud.profession,
                'religion' VALUE CAST(NULL AS STRING),
                'salary' VALUE CAST(NULLIF(TRIM(ud.salary), '') AS INT),
                'registration_ip' VALUE uri.ip,
                'registration_time' VALUE CAST(UNIX_TIMESTAMP(CAST(u.created AS STRING)) AS BIGINT) * 1000,
                'children_num' VALUE CAST(NULLIF(TRIM(ud.`numberOfChildren`), '') AS INT),
                'pay_cycle' VALUE CAST(NULLIF(TRIM(ud.`payCycle`), '') AS INT),
                'salary_day' VALUE CAST(NULLIF(TRIM(ud.`salaryDay`), '') AS INT),
                'survey' VALUE JSON_OBJECT(
                    'survey_loan_cnt' VALUE CAST(NULL AS STRING),
                    'survey_outstanding_cnt' VALUE CAST(NULL AS STRING),
                    'survey_overdue_max_days' VALUE CAST(NULL AS STRING),
                    'survey_overdue_6m' VALUE CAST(NULL AS STRING),
                    'survey_loan_amt_total' VALUE CAST(NULL AS STRING)
                ),
                'app' VALUE JSON_OBJECT(
                    'name' VALUE app.name,
                    'app_id' VALUE u.`appId`,
                    'version' VALUE CAST(NULL AS STRING)
                ),
                'emergency_contacts' VALUE ud.`emergencyContact`,
                'install_source' VALUE ldc.channel,
                'credit_limit' VALUE CAST(NULL AS STRING)
            )
        ) AS info,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM m_user u
    LEFT JOIN dwd_latest_user_data ud ON ud.`userId` = u.id
    LEFT JOIN dwd_latest_user_password lup
        ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
    LEFT JOIN dwd_latest_user_reg_ip uri ON uri.`userId` = u.id
    LEFT JOIN m_app app ON app.id = u.`appId`
    LEFT JOIN dwd_latest_device_channel ldc ON ldc.`deviceId` = u.`deviceId`
    WHERE u.id IS NOT NULL AND TRIM(u.id) <> ''
) AS t
${LM_MIGRATION_LIMIT_CLAUSE};
