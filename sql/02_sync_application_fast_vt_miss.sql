-- 全量阶段 2：application 宽表缺 VT token 的字段，运行时 UDF 调 /v2t
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_application_staging_miss (
    id BIGINT,
    application_no STRING,
    sn STRING,
    user_id BIGINT,
    app_code STRING,
    device_uuid STRING,
    session_id STRING,
    mobile_norm STRING,
    bvn_raw STRING,
    bank_account_raw STRING,
    gaid_idfa_raw STRING,
    mobile_token STRING,
    id_number_token STRING,
    gaid_idfa_token STRING,
    bank_code STRING,
    bank_account_name STRING,
    bank_account_token STRING,
    product_id STRING,
    period_days INT,
    period_count INT,
    re_loan INT,
    order_time TIMESTAMP(3),
    reviewed_time TIMESTAMP(3),
    disburse_time TIMESTAMP(3),
    settled_time TIMESTAMP(3),
    last_paid_time TIMESTAMP(3),
    last_repayment_time TIMESTAMP(3),
    credit_limit_minor BIGINT,
    loan_amount_minor BIGINT,
    principal_minor BIGINT,
    total_amount_minor BIGINT,
    disbursed_amount_minor BIGINT,
    risk_status INT,
    repayment_plan_json STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'application_sync_staging',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_application (
    application_no STRING, mobile STRING, bid STRING, app_id INT, app_version STRING,
    user_id BIGINT, group_user_id BIGINT, sn STRING, is_test TINYINT, is_first_apply TINYINT, is_auto_apply TINYINT,
    id_number STRING, gaid_idfa STRING, device_uuid STRING, session_id STRING,
    bank_code STRING, bank_account_name STRING, bank_account_number STRING,
    product_id STRING, product_scheme_id STRING, product_calculator_version STRING, product_scheme_param STRING,
    term INT, periods INT, repayment_method TINYINT, repayment_plan STRING,
    credit_limit BIGINT, loan_amount BIGINT, principal BIGINT, total_amount BIGINT, disbursed_amount BIGINT,
    created_time BIGINT, submited_time BIGINT, reviewed_time BIGINT, disbursed_time BIGINT,
    last_paid_time BIGINT, paid_off_time BIGINT, lock_expire_time BIGINT,
    due_date DATE, due_date_final DATE, status TINYINT,
    PRIMARY KEY (mobile, group_user_id, sn) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'application',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_application
SELECT
    e.application_no, e.mobile_token,
     'ng01',
    CAST(e.app_code AS INT),
     '1',
    e.user_id, e.group_user_id, e.sn,
    CAST(0 AS TINYINT),
    CAST(e.re_loan AS TINYINT),
    CAST(0 AS TINYINT),
    e.id_number_token, e.gaid_idfa_token,
    COALESCE(e.device_uuid, ''), e.session_id,
    COALESCE(e.bank_code, ''), COALESCE(e.bank_account_name, ''), e.bank_account_token,
    e.product_id, 'PROD-002-D7', '1.0',
    '{"penalty_rate":0.05,"upfront_rate":0.35,"interest_rate":0,"post_paid_rate":0.05}',
    COALESCE(e.period_days, 7), COALESCE(e.period_count, 1), CAST(1 AS TINYINT),
    e.repayment_plan_json,
    e.credit_limit_minor, e.loan_amount_minor, e.principal_minor, e.total_amount_minor, e.disbursed_amount_minor,
    UNIX_TIMESTAMP(CAST(e.order_time AS STRING)) * 1000,
    UNIX_TIMESTAMP(CAST(e.order_time AS STRING)) * 1000,
    CASE WHEN e.reviewed_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(e.reviewed_time AS STRING)) * 1000 END,
    CASE WHEN e.disburse_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(e.disburse_time AS STRING)) * 1000 END,
    CASE WHEN e.last_paid_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(e.last_paid_time AS STRING)) * 1000 END,
    CASE WHEN e.settled_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(e.settled_time AS STRING)) * 1000 END,
    (UNIX_TIMESTAMP(CAST(e.order_time AS STRING)) + 7 * 86400) * 1000,
    CAST(e.last_repayment_time AS DATE),
    CAST(e.last_repayment_time AS DATE),
    CAST(e.risk_status AS TINYINT)
FROM (
    SELECT
        s.application_no, s.sn, s.user_id + 100000000 AS user_id, s.user_id + 100000000 AS group_user_id,
        s.app_code, s.device_uuid, s.session_id, s.re_loan, s.product_id,
        s.period_days, s.period_count, s.order_time, s.reviewed_time, s.disburse_time, s.settled_time,
        s.last_paid_time, s.last_repayment_time, s.credit_limit_minor, s.loan_amount_minor, s.principal_minor,
        s.total_amount_minor, s.disbursed_amount_minor, s.risk_status, s.repayment_plan_json,
        s.bank_code, s.bank_account_name,
        COALESCE(NULLIF(TRIM(s.mobile_token), ''), vt_tokenize(s.mobile_norm)) AS mobile_token,
        COALESCE(NULLIF(TRIM(s.id_number_token), ''), vt_tokenize(s.bvn_raw)) AS id_number_token,
        COALESCE(
            NULLIF(TRIM(s.bank_account_token), ''),
            vt_tokenize(s.bank_account_raw)
        ) AS bank_account_token,
        CASE
            WHEN s.gaid_idfa_raw IS NOT NULL AND TRIM(s.gaid_idfa_raw) <> ''
            THEN COALESCE(NULLIF(TRIM(s.gaid_idfa_token), ''), vt_tokenize(s.gaid_idfa_raw))
            ELSE s.gaid_idfa_token
        END AS gaid_idfa_token
    FROM src_application_staging_miss s
    WHERE (
        ((s.mobile_token IS NULL OR TRIM(s.mobile_token) = '')
            AND s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> '')
        OR ((s.id_number_token IS NULL OR TRIM(s.id_number_token) = '')
            AND s.bvn_raw IS NOT NULL AND TRIM(s.bvn_raw) <> '')
        OR ((s.bank_account_token IS NULL OR TRIM(s.bank_account_token) = '')
            AND s.bank_account_raw IS NOT NULL AND TRIM(s.bank_account_raw) <> '')
    )
) e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> ''
  AND e.bank_account_token IS NOT NULL AND TRIM(e.bank_account_token) <> ''
  AND (
      e.id_number_token IS NOT NULL AND TRIM(e.id_number_token) <> ''
  );
