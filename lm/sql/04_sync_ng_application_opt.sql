-- 老库 application 单表试跑（索引优化版 SQL 逻辑）
-- 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-application-opt.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_mkt_user (
    id       BIGINT,
    `appId`  INT,
    mobile   STRING,
    created  TIMESTAMP(0)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'user'
);

CREATE TABLE src_mkt_app_config (
    id      INT,
    `appId` INT,
    `key`   STRING,
    `value` STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'app_config'
);

CREATE TABLE src_mkt_application (
    id                  BIGINT,
    `applicationNo`     STRING,
    `appId`             INT,
    `userId`            BIGINT,
    `deviceId`          BIGINT,
    mobile              STRING,
    `productId`         INT,
    amount              INT,
    repayment           INT,
    `shouldLoanAmount`  INT,
    `disburseAmount`    INT,
    `bankCode`          STRING,
    `bankAccount`       STRING,
    term                INT,
    `repeatLoan`        TINYINT,
    `applyDate`         BIGINT,
    `dueDate`           BIGINT,
    `disburseTime`      BIGINT,
    `paidTime`          BIGINT,
    `status`            TINYINT,
    gaid                STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'table-name' = 'application'
);

CREATE TABLE src_mkt_user_data (
    id       INT,
    `userId` BIGINT,
    bvn      STRING
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

CREATE TEMPORARY VIEW v_app_lim AS
SELECT *
FROM src_mkt_application
WHERE `applicationNo` IS NOT NULL AND `applicationNo` <> ''
ORDER BY id DESC
LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_cam AS
SELECT CAST(ac.`value` AS INT) AS sub_app_id, ac.`appId` AS main_app_id
FROM src_mkt_app_config ac
INNER JOIN (
    SELECT CAST(`value` AS INT) AS sub_app_id, MAX(id) AS max_id
    FROM src_mkt_app_config
    WHERE `key` = 'coreAppId'
    GROUP BY CAST(`value` AS INT)
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

CREATE TEMPORARY VIEW v_ud_latest AS
SELECT ud.*
FROM src_mkt_user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM src_mkt_user_data
    GROUP BY `userId`
) ud_max ON ud_max.max_id = ud.id;

CREATE TEMPORARY VIEW v_last_paid_time AS
SELECT ca.ext_sn, MAX(rr.repay_time) AS last_paid_time
FROM src_core_application ca
INNER JOIN src_core_repay_record rr ON rr.sn = ca.sn
INNER JOIN v_app_lim a ON a.`applicationNo` = ca.ext_sn
GROUP BY ca.ext_sn;

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
    a.`appId`,
    '1.0.0',
    a.`userId`,
    COALESCE(g.group_user_id, a.`userId`),
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
        'principal' VALUE a.`shouldLoanAmount`,
        'disbursed_amount' VALUE a.`disburseAmount`,
        'interest' VALUE 0,
        'admin_fee' VALUE GREATEST(a.amount - a.`shouldLoanAmount`, 0),
        'service_fee' VALUE 0,
        'tax_fee' VALUE 0,
        'reduction_amount' VALUE 0,
        'total_amount' VALUE a.repayment,
        'term' VALUE a.term,
        'start_date' VALUE CAST(FROM_UNIXTIME(a.`applyDate`) AS DATE),
        'due_date' VALUE CAST(FROM_UNIXTIME(a.`dueDate`) AS DATE),
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
            WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 3
            WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7 WHEN 7 THEN 11 WHEN 9 THEN 13
            WHEN 12 THEN 15 WHEN 13 THEN 20 WHEN 14 THEN 20
            WHEN 15 THEN 23 WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
            ELSE a.`status`
        END AS TINYINT
    )
FROM v_app_lim a
INNER JOIN src_mkt_user u ON u.id = a.`userId`
LEFT JOIN v_group_user_id g ON g.user_id = u.id
LEFT JOIN v_ud_latest ud ON ud.`userId` = a.`userId`
LEFT JOIN src_mkt_device d ON d.id = a.`deviceId`
LEFT JOIN src_core_application ca ON ca.ext_sn = a.`applicationNo`
LEFT JOIN v_last_paid_time lpt ON lpt.ext_sn = a.`applicationNo`;
