-- 全量阶段 1 application：mobile/id_number/bank 均有 token；缺 token 见 02_sync_application_fast_vt_miss.sql
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_application_staging (
    id BIGINT,
    application_no STRING,
    sn STRING,
    user_id BIGINT,
    app_code STRING,
    device_uuid STRING,
    session_id STRING,
    bvn_raw STRING,
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
    application_no, mobile_token, 'ng01', CAST(app_code AS INT), '',
    user_id + 100000000, user_id + 100000000, sn,
    CAST(0 AS TINYINT),
    CAST(re_loan AS TINYINT),
    CAST(0 AS TINYINT),
    CASE
        WHEN bvn_raw IS NULL OR TRIM(bvn_raw) = '' THEN CAST('' AS STRING)
        ELSE id_number_token
    END,
    CASE
        WHEN gaid_idfa_raw IS NULL OR TRIM(gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
        ELSE gaid_idfa_token
    END,
    COALESCE(device_uuid, ''), session_id,
    COALESCE(bank_code, ''), COALESCE(bank_account_name, ''), bank_account_token,
    product_id, 'PROD-002-D7', '1.0',
    '{"penalty_rate":0.05,"upfront_rate":0.35,"interest_rate":0,"post_paid_rate":0.05}',
    COALESCE(period_days, 7), COALESCE(period_count, 1), CAST(1 AS TINYINT),
    repayment_plan_json,
    credit_limit_minor, loan_amount_minor, principal_minor, total_amount_minor, disbursed_amount_minor,
    UNIX_TIMESTAMP(CAST(order_time AS STRING)) * 1000,
    UNIX_TIMESTAMP(CAST(order_time AS STRING)) * 1000,
    CASE WHEN reviewed_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(reviewed_time AS STRING)) * 1000 END,
    CASE WHEN disburse_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(disburse_time AS STRING)) * 1000 END,
    CASE WHEN last_paid_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(last_paid_time AS STRING)) * 1000 END,
    CASE WHEN settled_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(settled_time AS STRING)) * 1000 END,
    (UNIX_TIMESTAMP(CAST(order_time AS STRING)) + 7 * 86400) * 1000,
    CAST(last_repayment_time AS DATE),
    CAST(last_repayment_time AS DATE),
    CAST(risk_status AS TINYINT)
FROM src_application_staging
WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
  AND bank_account_token IS NOT NULL AND TRIM(bank_account_token) <> ''
  AND (
      bvn_raw IS NULL OR TRIM(bvn_raw) = ''
      OR (id_number_token IS NOT NULL AND TRIM(id_number_token) <> '')
  );
