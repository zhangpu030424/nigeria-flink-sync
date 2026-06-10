-- 尼日利亚老库 → 目标库 Flink Batch（来自 ng_migration_flink.sql）
-- 覆盖: user_info / user_bankcard / user_product / application / loan（不含 user / id_mapping）
-- 执行: bash lm/scripts/run-ng-migration-bulk.sh
-- 试跑: .env 设 LM_MIGRATION_LIMIT=20

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

-- ===================== 连接参数（按需修改） =====================
-- 源库 market
-- jdbc:mysql://127.0.0.1:3306/ng_loan_market?useUnicode=true&characterEncoding=utf8&useSSL=false
-- 源库 core
-- jdbc:mysql://127.0.0.1:3306/ng_loan_core?useUnicode=true&characterEncoding=utf8&useSSL=false
-- 目标库 id
-- jdbc:mysql://127.0.0.1:3306/id?useUnicode=true&characterEncoding=utf8&useSSL=false

-- ===================== 源表：ng_loan_market =====================

CREATE TABLE src_mkt_user (
    id              BIGINT,
    `appId`         BIGINT,
    mobile          STRING,
    `deviceId`      BIGINT,
    `credentialNo`  STRING,
    `isCancel`      TINYINT,
    created         TIMESTAMP(0),
    updated         TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user'
);

CREATE TABLE src_mkt_app_config (
    id      BIGINT,
    `appId` BIGINT,
    `key`   STRING,
    `value` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app_config'
);

CREATE TABLE src_mkt_app (
    id   BIGINT,
    name STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app'
);

CREATE TABLE src_mkt_application (
    id                  BIGINT,
    `applicationNo`     STRING,
    `appId`             BIGINT,
    `userId`            BIGINT,
    `deviceId`          BIGINT,
    mobile              STRING,
    `productId`         BIGINT,
    amount              BIGINT,
    repayment           BIGINT,
    `shouldLoanAmount`  BIGINT,
    `disburseAmount`    BIGINT,
    `bankCode`          STRING,
    `bankAccount`       STRING,
    term                INT,
    `repeatLoan`        TINYINT,
    `applyDate`         BIGINT,
    `dueDate`           BIGINT,
    `disburseTime`      BIGINT,
    `paidTime`          BIGINT,
    `status`            TINYINT,
    gaid                STRING,
    created             TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'application'
);

CREATE TABLE src_mkt_user_data (
    id                  BIGINT,
    `userId`            BIGINT,
    bvn                 STRING,
    `firstName`         STRING,
    `middleName`        STRING,
    `lastName`          STRING,
    `bankCode`          STRING,
    `bankAccount`       STRING,
    email               STRING,
    gender              TINYINT,
    birthday            STRING,
    marital             TINYINT,
    profession          STRING,
    education           TINYINT,
    salary              BIGINT,
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

CREATE TABLE src_mkt_device (
    id           BIGINT,
    `deviceUUID` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'device'
);

CREATE TABLE src_mkt_device_ad_channel (
    id                                    BIGINT,
    `deviceId`                            BIGINT,
    channel                               STRING,
    google_ads_campaign_id                STRING,
    google_ads_adgroup_id                 STRING,
    fb_install_referrer_campaign_id       STRING,
    fb_install_referrer_campaign_group_id STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'device_ad_channel'
);

CREATE TABLE src_mkt_log_user_password (
    id       BIGINT,
    `appId`  BIGINT,
    mobile   STRING,
    password STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'log_user_password'
);

-- ===================== 源表：ng_loan_core =====================

CREATE TABLE src_core_application (
    sn         STRING,
    ext_sn     STRING,
    apply_time BIGINT,
    audit_time BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_CORE_MYSQL_HOST}:${LM_CORE_MYSQL_PORT}/${LM_CORE_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_CORE_MYSQL_USER}',
    'password' = '${LM_CORE_MYSQL_PASSWORD}',
    'table-name' = 'application'
);

CREATE TABLE src_core_repay_plan (
    plan_sn          STRING,
    sn               STRING,
    `status`         TINYINT,
    start_date       BIGINT,
    due_date         BIGINT,
    settle_time      BIGINT,
    prin_amt         BIGINT,
    interest         BIGINT,
    orig_fee         BIGINT,
    penalty          BIGINT,
    amt              BIGINT,
    repaid_amt       BIGINT,
    repay_last_time  BIGINT,
    created_at       TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_CORE_MYSQL_HOST}:${LM_CORE_MYSQL_PORT}/${LM_CORE_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_CORE_MYSQL_USER}',
    'password' = '${LM_CORE_MYSQL_PASSWORD}',
    'table-name' = 'repay_plan'
);

CREATE TABLE src_core_repay_record (
    sn          STRING,
    repay_time  BIGINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_CORE_MYSQL_HOST}:${LM_CORE_MYSQL_PORT}/${LM_CORE_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_CORE_MYSQL_USER}',
    'password' = '${LM_CORE_MYSQL_PASSWORD}',
    'table-name' = 'repay_record'
);

-- ===================== 目标表：id 库（无 dt_ 前缀） =====================

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

CREATE TABLE sink_user_bankcard (
    group_user_id       BIGINT,
    bank_code           STRING,
    bank_account_number STRING,
    is_default          TINYINT,
    PRIMARY KEY (group_user_id, bank_account_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3',
    'table-name' = 'user_bankcard'
);

CREATE TABLE sink_user_product (
    group_user_id    BIGINT,
    product_id       STRING,
    schemes          STRING,
    is_open          TINYINT,
    credit_amount    BIGINT,
    unpaid_amount    BIGINT,
    locked_amount    BIGINT,
    available_amount BIGINT,
    PRIMARY KEY (group_user_id, product_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3',
    'table-name' = 'user_product'
);

CREATE TABLE sink_application (
    application_no               STRING,
    mobile                       STRING,
    bid                          STRING,
    app_id                       INT,
    app_version                  STRING,
    user_id                      BIGINT,
    group_user_id                BIGINT,
    sn                           STRING,
    is_test                      TINYINT,
    is_first_apply               TINYINT,
    is_auto_apply                TINYINT,
    id_number                    STRING,
    gaid_idfa                    STRING,
    device_uuid                  STRING,
    session_id                   STRING,
    bank_code                    STRING,
    bank_account_name            STRING,
    bank_account_number          STRING,
    product_id                   STRING,
    product_scheme_id            STRING,
    product_calculator_version   STRING,
    product_scheme_param         STRING,
    term                         INT,
    periods                      INT,
    repayment_method             TINYINT,
    repayment_plan               STRING,
    credit_limit                 BIGINT,
    loan_amount                  BIGINT,
    principal                    BIGINT,
    total_amount                 BIGINT,
    disbursed_amount             BIGINT,
    created_time                 BIGINT,
    submited_time                BIGINT,
    reviewed_time                BIGINT,
    disbursed_time               BIGINT,
    last_paid_time               BIGINT,
    paid_off_time                BIGINT,
    lock_expire_time             BIGINT,
    due_date                     DATE,
    due_date_final               DATE,
    `status`                     TINYINT,
    PRIMARY KEY (application_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3',
    'table-name' = 'application'
);

CREATE TABLE sink_loan (
    loan_no           STRING,
    application_no    STRING,
    period            TINYINT,
    roll_sequence     TINYINT,
    start_date        DATE,
    due_date          DATE,
    due_date_final    DATE,
    principal         BIGINT,
    interest          BIGINT,
    admin_fee         BIGINT,
    penalty_amount    BIGINT,
    reduction_amount  BIGINT,
    total_amount      BIGINT,
    paid_amount       BIGINT,
    paid_time         BIGINT,
    paid_off_date     DATE,
    created_time      BIGINT,
    `status`          TINYINT,
    PRIMARY KEY (loan_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3',
    'table-name' = 'loan'
);

-- ===================== 公共视图 =====================

-- 试跑：LM_MIGRATION_LIMIT 控制用户数参与 group_user_id 计算
CREATE TEMPORARY VIEW v_user_lim AS
SELECT * FROM src_mkt_user
ORDER BY id
LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_cam AS
SELECT CAST(ac.`value` AS BIGINT) AS sub_app_id, ac.`appId` AS main_app_id
FROM src_mkt_app_config ac
INNER JOIN (
    SELECT CAST(`value` AS BIGINT) AS sub_app_id, MAX(id) AS max_id
    FROM src_mkt_app_config
    WHERE `key` = 'coreAppId'
    GROUP BY CAST(`value` AS BIGINT)
) pick ON pick.max_id = ac.id;

CREATE TEMPORARY VIEW v_user_eff AS
SELECT
    u.id,
    u.`appId`,
    u.mobile,
    u.created,
    COALESCE(cam.main_app_id, u.`appId`) AS eff_app_id
FROM src_mkt_user u
LEFT JOIN v_cam cam ON cam.sub_app_id = u.`appId`;

CREATE TEMPORARY VIEW v_group_user_id AS
SELECT user_id, group_user_id
FROM (
    SELECT
        c.id AS user_id,
        p.id AS group_user_id,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY p.created ASC, p.id ASC) AS rn
    FROM v_user_eff c
    INNER JOIN v_user_eff p
        ON p.mobile = c.mobile
        AND p.eff_app_id = c.eff_app_id
        AND p.created <= c.created
) t
WHERE rn = 1;

CREATE TEMPORARY VIEW tmp_user_group AS
SELECT
    u.id AS user_id,
    u.`appId` AS appId,
    u.mobile,
    u.created,
    COALESCE(g.group_user_id, u.id) AS group_user_id
FROM v_user_lim u
LEFT JOIN v_group_user_id g ON g.user_id = u.id;

-- user_data 每用户最新一条
CREATE TEMPORARY VIEW v_ud_latest AS
SELECT ud.*
FROM src_mkt_user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM src_mkt_user_data
    GROUP BY `userId`
) ud_max ON ud_max.max_id = ud.id;

-- log_user_password 每 appId+mobile 最新一条
CREATE TEMPORARY VIEW v_lup_latest AS
SELECT l1.`appId`, l1.mobile, l1.password
FROM src_mkt_log_user_password l1
INNER JOIN (
    SELECT `appId`, mobile, MAX(id) AS max_id
    FROM src_mkt_log_user_password
    GROUP BY `appId`, mobile
) l2 ON l2.max_id = l1.id;

-- device_ad_channel 每 device 最新一条
CREATE TEMPORARY VIEW v_dac_latest AS
SELECT dac1.*
FROM src_mkt_device_ad_channel dac1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id
    FROM src_mkt_device_ad_channel
    GROUP BY `deviceId`
) dac_max ON dac_max.max_id = dac1.id;

-- core 最后一次还款时间
CREATE TEMPORARY VIEW v_last_paid_time AS
SELECT ca.ext_sn, MAX(rr.repay_time) AS last_paid_time
FROM src_core_application ca
INNER JOIN src_core_repay_record rr ON rr.sn = ca.sn
GROUP BY ca.ext_sn;

-- ===================== Step 1: user_info（LIMIT ${LM_MIGRATION_LIMIT}） =====================

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
            'full_name' VALUE TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)),
            'email' VALUE ud.email,
            'birthday' VALUE ud.birthday,
            'gender' VALUE CAST(ud.gender AS INT),
            'address' VALUE JSON_OBJECT(
                'province' VALUE ud.`addressState`,
                'city' VALUE ud.`addressDistrict`,
                'detail' VALUE ud.address
            ),
            'company' VALUE ud.company,
            'education' VALUE CAST(ud.education AS INT),
            'marital' VALUE CAST(ud.marital AS INT),
            'profession' VALUE ud.profession,
            'salary' VALUE CAST(ud.salary AS BIGINT),
            'emergency_contacts' VALUE ud.`emergencyContact`,
            'children_num' VALUE CAST(ud.`numberOfChildren` AS INT),
            'pay_cycle' VALUE CAST(ud.`payCycle` AS INT),
            'salary_day' VALUE CAST(ud.`salaryDay` AS INT),
            'app' VALUE JSON_OBJECT(
                'name' VALUE ap.name,
                'app_id' VALUE CAST(u.`appId` AS STRING)
            ),
            'install_source' VALUE dac.channel
        )
    ),
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM v_user_lim u
LEFT JOIN v_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_dac_latest dac ON dac.`deviceId` = u.`deviceId`
LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`
ORDER BY u.id
LIMIT ${LM_MIGRATION_LIMIT};

-- ===================== Step 2: user_bankcard（LIMIT ${LM_MIGRATION_LIMIT}） =====================

INSERT INTO sink_user_bankcard
SELECT
    ug.group_user_id,
    ud.`bankCode`,
    ud.`bankAccount`,
    CAST(1 AS TINYINT)
FROM tmp_user_group ug
INNER JOIN v_ud_latest ud ON ud.`userId` = ug.group_user_id
WHERE ud.`bankCode` <> '' AND ud.`bankAccount` <> ''
ORDER BY ug.group_user_id
LIMIT ${LM_MIGRATION_LIMIT};

-- ===================== Step 3: user_product（LIMIT ${LM_MIGRATION_LIMIT}） =====================

INSERT INTO sink_user_product
SELECT
    t.group_user_id,
    CAST(t.`productId` AS STRING),
    JSON_STRING(
        JSON_OBJECT(
            'repayment_method' VALUE 1,
            'interest_start' VALUE 'next_day',
            'term' VALUE 7,
            'periods' VALUE 1,
            'periods_days' VALUE JSON_ARRAY(7),
            'param_tpl' VALUE JSON_OBJECT(
                'aha' VALUE 0.5,
                'interest_rate' VALUE 0,
                'penalty_rate' VALUE 0.05,
                'upfront_rate' VALUE 0.35
            )
        )
    ),
    CAST(1 AS TINYINT),
    CAST(t.credit_amount AS BIGINT),
    CAST(t.credit_amount AS BIGINT),
    CAST(NULL AS BIGINT),
    CAST(NULL AS BIGINT)
FROM (
    SELECT
        pick.group_user_id,
        pick.`productId`,
        a.amount AS credit_amount
    FROM (
        SELECT ug.group_user_id, a2.`productId`, MAX(a2.id) AS max_app_id
        FROM tmp_user_group ug
        INNER JOIN src_mkt_application a2 ON a2.`userId` = ug.user_id
        GROUP BY ug.group_user_id, a2.`productId`
    ) pick
    INNER JOIN src_mkt_application a ON a.id = pick.max_app_id
) t
ORDER BY t.group_user_id, t.`productId`
LIMIT ${LM_MIGRATION_LIMIT};

-- ===================== Step 4: application（LIMIT ${LM_MIGRATION_LIMIT}） =====================

INSERT INTO sink_application
SELECT
    a.`applicationNo`,
    CASE
        WHEN a.mobile LIKE '+234%' THEN a.mobile
        WHEN a.mobile LIKE '234%'  THEN CONCAT('+', a.mobile)
        WHEN a.mobile LIKE '0%'    THEN CONCAT('+234', SUBSTRING(a.mobile, 2))
        ELSE CONCAT('+234', a.mobile)
    END,
    'ng01',
    CAST(a.`appId` AS INT),
    '1.0.0',
    a.`userId`,
    COALESCE(ug.group_user_id, a.`userId`),
    a.`applicationNo`,
    CAST(0 AS TINYINT),
    CASE WHEN a.`repeatLoan` = 0 THEN CAST(1 AS TINYINT) ELSE CAST(0 AS TINYINT) END,
    CAST(0 AS TINYINT),
    COALESCE(ud.bvn, ''),
    COALESCE(a.gaid, ''),
    COALESCE(d.`deviceUUID`, ''),
    CAST(NULL AS STRING),
    COALESCE(a.`bankCode`, ''),
    CAST(NULL AS STRING),
    COALESCE(a.`bankAccount`, ''),
    CAST(a.`productId` AS STRING),
    'PROD-002-D7',
    '1',
    JSON_STRING(JSON_OBJECT(
        'penalty_rate' VALUE 0.05,
        'upfront_rate' VALUE 0.35,
        'interest_rate' VALUE 0,
        'post_paid_rate' VALUE 0.05
    )),
    a.term,
    1,
    CAST(1 AS TINYINT),
    JSON_STRING(JSON_OBJECT(
        'roll_sequence' VALUE 0,
        'period' VALUE 1,
        'principal' VALUE CAST(a.`shouldLoanAmount` AS BIGINT),
        'disbursed_amount' VALUE CAST(a.`disburseAmount` AS BIGINT),
        'interest' VALUE 0,
        'admin_fee' VALUE CAST(GREATEST(a.amount - a.`shouldLoanAmount`, 0) AS BIGINT),
        'total_amount' VALUE CAST(a.repayment AS BIGINT),
        'term' VALUE CAST(a.term AS INT),
        'start_date' VALUE CAST(CAST(FROM_UNIXTIME(a.`applyDate`) AS DATE) AS STRING),
        'due_date' VALUE CAST(CAST(FROM_UNIXTIME(a.`dueDate`) AS DATE) AS STRING),
        'roll_allowed' VALUE 0
    )),
    CAST(a.amount AS BIGINT),
    CAST(a.amount AS BIGINT),
    CAST(a.`shouldLoanAmount` AS BIGINT),
    CAST(a.repayment AS BIGINT),
    CAST(a.`disburseAmount` AS BIGINT),
    a.`applyDate` * 1000,
    COALESCE(ca.apply_time, 0) * 1000,
    COALESCE(ca.audit_time, 0) * 1000,
    a.`disburseTime` * 1000,
    COALESCE(lpt.last_paid_time, 0) * 1000,
    a.`paidTime` * 1000,
    (a.`applyDate` + 7 * 86400) * 1000,
    CAST(FROM_UNIXTIME(a.`dueDate`) AS DATE),
    CAST(FROM_UNIXTIME(a.`dueDate`) AS DATE),
    CAST(
        CASE a.`status`
            WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1
            WHEN 5 THEN 3 WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7
            WHEN 7 THEN 11 WHEN 9 THEN 13 WHEN 12 THEN 15
            WHEN 13 THEN 20 WHEN 14 THEN 20 WHEN 15 THEN 23
            WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
            ELSE a.`status`
        END AS TINYINT
    )
FROM src_mkt_application a
LEFT JOIN tmp_user_group ug ON ug.user_id = a.`userId`
LEFT JOIN v_ud_latest ud ON ud.`userId` = a.`userId`
LEFT JOIN src_mkt_device d ON d.id = a.`deviceId`
LEFT JOIN src_core_application ca ON ca.ext_sn = a.`applicationNo`
LEFT JOIN v_last_paid_time lpt ON lpt.ext_sn = a.`applicationNo`
WHERE a.`applicationNo` <> ''
ORDER BY a.id
LIMIT ${LM_MIGRATION_LIMIT};

-- ===================== Step 5: loan（LIMIT ${LM_MIGRATION_LIMIT}） =====================

INSERT INTO sink_loan
SELECT
    CONCAT('NG-', rp.plan_sn),
    ma.`applicationNo`,
    CAST(1 AS TINYINT),
    CAST(0 AS TINYINT),
    CAST(FROM_UNIXTIME(rp.start_date) AS DATE),
    CAST(FROM_UNIXTIME(rp.due_date) AS DATE),
    CAST(FROM_UNIXTIME(rp.due_date) AS DATE),
    CAST(rp.prin_amt AS BIGINT),
    CAST(rp.interest AS BIGINT),
    CAST(rp.orig_fee AS BIGINT),
    CAST(rp.penalty AS BIGINT),
    CAST(0 AS BIGINT),
    CAST(rp.amt AS BIGINT),
    CASE WHEN rp.`status` IN (2, 4) THEN CAST(rp.repaid_amt AS BIGINT) ELSE CAST(0 AS BIGINT) END,
    rp.repay_last_time * 1000,
    CASE WHEN rp.settle_time > 0 THEN CAST(FROM_UNIXTIME(rp.settle_time) AS DATE) ELSE CAST(NULL AS DATE) END,
    CAST(UNIX_TIMESTAMP(CAST(rp.created_at AS STRING)) AS BIGINT) * 1000,
    CAST(
        CASE
            WHEN rp.`status` = 1 AND rp.repaid_amt = 0 THEN 20
            WHEN rp.`status` = 1 AND rp.repaid_amt <> 0 THEN 24
            WHEN rp.`status` = 3 THEN 23
            WHEN rp.`status` = 4 THEN 25
            WHEN rp.`status` = 2 THEN 27
            ELSE rp.`status`
        END AS TINYINT
    )
FROM src_mkt_application ma
INNER JOIN src_core_application ca ON ca.ext_sn = ma.`applicationNo`
INNER JOIN src_core_repay_plan rp ON rp.sn = ca.sn
WHERE ma.`applicationNo` <> ''
ORDER BY ma.id, rp.plan_sn
LIMIT ${LM_MIGRATION_LIMIT};

-- ===================== 验证查询（各 LIMIT ${LM_MIGRATION_LIMIT}） =====================

-- SELECT 'user_info' AS tbl, COUNT(*) AS cnt FROM sink_user_info;
-- SELECT * FROM sink_user_info LIMIT ${LM_MIGRATION_LIMIT};
-- SELECT * FROM sink_user_bankcard LIMIT ${LM_MIGRATION_LIMIT};
-- SELECT * FROM sink_user_product LIMIT ${LM_MIGRATION_LIMIT};
-- SELECT * FROM sink_application LIMIT ${LM_MIGRATION_LIMIT};
-- SELECT * FROM sink_loan LIMIT ${LM_MIGRATION_LIMIT};
