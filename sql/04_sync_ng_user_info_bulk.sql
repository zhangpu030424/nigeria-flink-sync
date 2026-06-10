-- 作业：user_info（多表拼 JSON，老库 VIEW 预聚合 + 16 路分区读 user）
-- 前置: v_flink_mkt_user / v_flink_ud_latest / v_flink_lup_latest / v_flink_dac_latest
-- 执行: bash scripts/run-ng-user-info-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=20 bash scripts/run-ng-user-info-bulk.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_mkt_user (
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
    'table-name' = 'v_flink_mkt_user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'id_part',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '2000000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_ud_latest (
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
    'table-name' = 'v_flink_ud_latest',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id_part',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '2000000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_lup_latest (
    `appId` STRING,
    mobile STRING,
    password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_lup_latest',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE src_dac_latest (
    `deviceId` STRING,
    channel STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'v_flink_dac_latest',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
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
        lup.password AS password,
        CAST(NULL AS STRING) AS live_image,
        CAST(NULL AS STRING) AS id_card,
        JSON_STRING(
            JSON_OBJECT(
                'full_name' VALUE TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)),
                'email' VALUE ud.email,
                'birthday' VALUE ud.birthday,
                'gender' VALUE CAST(NULLIF(TRIM(ud.gender), '') AS INT),
                'address' VALUE JSON_OBJECT(
                    'province' VALUE ud.`addressState`,
                    'city' VALUE ud.`addressDistrict`,
                    'detail' VALUE ud.address
                ),
                'company' VALUE ud.company,
                'education' VALUE CAST(NULLIF(TRIM(ud.education), '') AS INT),
                'marital' VALUE CAST(NULLIF(TRIM(ud.marital), '') AS INT),
                'profession' VALUE ud.profession,
                'salary' VALUE CAST(NULLIF(TRIM(ud.salary), '') AS INT),
                'emergency_contacts' VALUE ud.`emergencyContact`,
                'children_num' VALUE CAST(NULLIF(TRIM(ud.`numberOfChildren`), '') AS INT),
                'pay_cycle' VALUE CAST(NULLIF(TRIM(ud.`payCycle`), '') AS INT),
                'salary_day' VALUE CAST(NULLIF(TRIM(ud.`salaryDay`), '') AS INT),
                'app' VALUE JSON_OBJECT('app_id' VALUE u.`appId`),
                'install_source' VALUE dac.channel
            )
        ) AS info,
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM src_mkt_user u
    LEFT JOIN src_ud_latest ud ON ud.`userId` = u.id
    LEFT JOIN src_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
    LEFT JOIN src_dac_latest dac ON dac.`deviceId` = u.`deviceId`
    WHERE u.id IS NOT NULL
      AND TRIM(u.id) <> ''
      AND u.mobile IS NOT NULL
      AND TRIM(u.mobile) <> ''
) AS t
${LM_MIGRATION_LIMIT_CLAUSE};
