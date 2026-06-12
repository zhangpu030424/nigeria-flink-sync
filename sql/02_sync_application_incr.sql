-- 增量 application：多源 CDC 触发 + Lookup 组装
-- CDC: user_order, user, user_bank_info, user_personal_info, device_ids,
--       user_repay, risk_user_approval_callback, user_order_installment
-- 前置: ./scripts/deploy-source-ddl.sh
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS cdc_user_order (
    id BIGINT,
    user_id BIGINT,
    order_no STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_order',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_ORDER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user (
    id BIGINT,
    device_id STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_USER}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_bank_info (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_bank_info',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_BANK}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_personal_info (
    id BIGINT,
    user_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_personal_info',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_PERSONAL}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'debezium.event.deserialization.failure.handling.mode' = 'warn',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_device_ids (
    id BIGINT,
    device_uuid STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'device_ids',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_DEVICE}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_repay (
    id BIGINT,
    order_no STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_repay',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_REPAY}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_risk_user_approval (
    id BIGINT,
    order_no STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'risk_user_approval_callback',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_RISK}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS cdc_user_order_installment (
    id BIGINT,
    user_order_id BIGINT,
    proc_time AS PROCTIME(),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_order_installment',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_APP_INSTALLMENT}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_application_order (
    id BIGINT,
    order_no STRING,
    user_id BIGINT,
    app_code BIGINT,
    product_id STRING,
    period_days BIGINT,
    period_count BIGINT,
    re_loan BIGINT,
    amount_max STRING,
    received STRING,
    repayment STRING,
    poundage STRING,
    order_time TIMESTAMP(3),
    disburse_time TIMESTAMP(3),
    settled_time TIMESTAMP(3),
    last_repayment_time TIMESTAMP(3),
    risk_order_status BIGINT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_order_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_user (
    id BIGINT,
    mobile STRING,
    device_id STRING,
    gps_adid STRING,
    idfa STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_user_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_user_bank_default (
    user_id BIGINT,
    bank_code STRING,
    bank_holder STRING,
    bank_account STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_bank_default_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_user_bvn (
    user_id BIGINT,
    bvn STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_bvn_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_device_ids (
    device_uuid STRING,
    session_uuid STRING,
    aaid STRING,
    idfa STRING,
    PRIMARY KEY (device_uuid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'device_ids_latest_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_risk_approval (
    order_no STRING,
    callback_time TIMESTAMP(3),
    PRIMARY KEY (order_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'risk_approval_latest_by_order',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_user_repay_paid (
    order_no STRING,
    callback_time TIMESTAMP(3),
    PRIMARY KEY (order_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_repay_paid_latest_by_order',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TABLE IF NOT EXISTS dim_installment_overdue (
    user_order_id BIGINT,
    is_overdue BIGINT,
    PRIMARY KEY (user_order_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_order_installment_overdue',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '30m'
);

CREATE TEMPORARY VIEW v_application_triggers AS
SELECT id AS order_id, proc_time FROM cdc_user_order WHERE id IS NOT NULL
UNION ALL
SELECT o.id AS order_id, u.proc_time
FROM cdc_user AS u
INNER JOIN cdc_user_order AS o ON o.user_id = u.id
UNION ALL
SELECT o.id AS order_id, b.proc_time
FROM cdc_user_bank_info AS b
INNER JOIN cdc_user_order AS o ON o.user_id = b.user_id
UNION ALL
SELECT o.id AS order_id, p.proc_time
FROM cdc_user_personal_info AS p
INNER JOIN cdc_user_order AS o ON o.user_id = p.user_id
UNION ALL
SELECT o.id AS order_id, di.proc_time
FROM cdc_device_ids AS di
INNER JOIN cdc_user AS u ON u.device_id = di.device_uuid
INNER JOIN cdc_user_order AS o ON o.user_id = u.id
WHERE di.device_uuid IS NOT NULL AND TRIM(di.device_uuid) <> ''
UNION ALL
SELECT o.id AS order_id, ur.proc_time
FROM cdc_user_repay AS ur
INNER JOIN cdc_user_order AS o ON o.order_no = ur.order_no
WHERE ur.order_no IS NOT NULL AND TRIM(ur.order_no) <> ''
UNION ALL
SELECT o.id AS order_id, ra.proc_time
FROM cdc_risk_user_approval AS ra
INNER JOIN cdc_user_order AS o ON o.order_no = ra.order_no
WHERE ra.order_no IS NOT NULL AND TRIM(ra.order_no) <> ''
UNION ALL
SELECT i.user_order_id AS order_id, i.proc_time
FROM cdc_user_order_installment AS i
WHERE i.user_order_id IS NOT NULL;

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
    'sink.buffer-flush.interval' = '1s',
    'sink.max-retries' = '3'
);

INSERT INTO sink_application
SELECT
    e.application_no,
    e.mobile_token,
    'ng01',
    e.app_id,
    '1',
    e.user_id,
    e.group_user_id,
    e.sn,
    CAST(0 AS TINYINT),
    e.re_loan,
    CAST(0 AS TINYINT),
    e.id_number_token,
    e.gaid_idfa_token,
    e.device_uuid,
    e.session_id,
    e.bank_code,
    e.bank_account_name,
    e.bank_account_token,
    e.product_id,
    'PROD-002-D7',
    '1.0',
    '{"penalty_rate":0.05,"upfront_rate":0.35,"interest_rate":0,"post_paid_rate":0.05}',
    e.period_days,
    e.period_count,
    CAST(1 AS TINYINT),
    e.repayment_plan_json,
    e.credit_limit_minor,
    e.loan_amount_minor,
    e.principal_minor,
    e.total_amount_minor,
    e.disbursed_amount_minor,
    e.created_time_ms,
    e.created_time_ms,
    e.reviewed_time_ms,
    e.disbursed_time_ms,
    e.last_paid_time_ms,
    e.paid_off_time_ms,
    e.lock_expire_ms,
    e.due_date,
    e.due_date_final,
    e.risk_status
FROM (
    SELECT
        o.order_no AS application_no,
        o.order_no AS sn,
        o.user_id + 100000000 AS user_id,
        o.user_id + 100000000 AS group_user_id,
        CAST(o.app_code AS INT) AS app_id,
        vt_tokenize(
            CASE
                WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN CAST(NULL AS STRING)
                WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                ELSE CONCAT('+234', TRIM(u.mobile))
            END
        ) AS mobile_token,
        CASE
            WHEN bvn.bvn IS NULL OR TRIM(bvn.bvn) = '' THEN CAST('' AS STRING)
            ELSE vt_tokenize(TRIM(bvn.bvn))
        END AS id_number_token,
        CASE
            WHEN TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''), NULLIF(TRIM(u.idfa), ''), NULLIF(TRIM(di.aaid), ''))) IS NULL
                OR TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''), NULLIF(TRIM(u.idfa), ''), NULLIF(TRIM(di.aaid), ''))) = ''
                THEN CAST(NULL AS STRING)
            ELSE vt_tokenize(TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''), NULLIF(TRIM(u.idfa), ''), NULLIF(TRIM(di.aaid), ''))))
        END AS gaid_idfa_token,
        COALESCE(u.device_id, '') AS device_uuid,
        di.session_uuid AS session_id,
        COALESCE(ub.bank_code, '') AS bank_code,
        COALESCE(ub.bank_holder, '') AS bank_account_name,
        vt_tokenize(TRIM(ub.bank_account)) AS bank_account_token,
        o.product_id,
        CAST(COALESCE(o.period_days, 7) AS INT) AS period_days,
        CAST(COALESCE(o.period_count, 1) AS INT) AS period_count,
        CAST(COALESCE(o.re_loan, 0) AS TINYINT) AS re_loan,
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT) AS credit_limit_minor,
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT) AS loan_amount_minor,
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT) AS principal_minor,
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.repayment), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT) AS total_amount_minor,
        CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT) AS disbursed_amount_minor,
        CAST(UNIX_TIMESTAMP(CAST(o.order_time AS STRING)) * 1000 AS BIGINT) AS created_time_ms,
        CASE WHEN ra.callback_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(ra.callback_time AS STRING)) * 1000 END AS reviewed_time_ms,
        CASE WHEN o.disburse_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(o.disburse_time AS STRING)) * 1000 END AS disbursed_time_ms,
        CASE WHEN ur.callback_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(ur.callback_time AS STRING)) * 1000 END AS last_paid_time_ms,
        CASE WHEN o.settled_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(o.settled_time AS STRING)) * 1000 END AS paid_off_time_ms,
        (UNIX_TIMESTAMP(CAST(o.order_time AS STRING)) + 7 * 86400) * 1000 AS lock_expire_ms,
        CAST(o.last_repayment_time AS DATE) AS due_date,
        CAST(o.last_repayment_time AS DATE) AS due_date_final,
        CAST(
            CASE CAST(o.risk_order_status AS INT)
                WHEN 2 THEN 3
                WHEN 4 THEN 5
                WHEN 6 THEN 13
                WHEN 8 THEN 15
                WHEN 10 THEN CASE WHEN COALESCE(ov.is_overdue, 0) = 1 THEN 23 ELSE 20 END
                WHEN 11 THEN 23
                WHEN 40 THEN 25
                WHEN 20 THEN 27
                WHEN 30 THEN 27
                WHEN 50 THEN 27
                ELSE 1
            END AS TINYINT
        ) AS risk_status,
        JSON_STRING(JSON_OBJECT(
            KEY 'roll_sequence' VALUE CAST(0 AS INT),
            KEY 'period' VALUE CAST(1 AS INT),
            KEY 'principal' VALUE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
            KEY 'disbursed_amount' VALUE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
            KEY 'interest' VALUE CAST(0 AS BIGINT),
            KEY 'admin_fee' VALUE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.poundage), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
            KEY 'service_fee' VALUE CAST(0 AS BIGINT),
            KEY 'tax_fee' VALUE CAST(0 AS BIGINT),
            KEY 'reduction_amount' VALUE CAST(0 AS BIGINT),
            KEY 'total_amount' VALUE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.repayment), '') AS DECIMAL(20, 2)), 0), 0) AS BIGINT),
            KEY 'term' VALUE CAST(COALESCE(o.period_days, 7) AS INT),
            KEY 'start_date' VALUE DATE_FORMAT(o.order_time, 'yyyy-MM-dd'),
            KEY 'due_date' VALUE DATE_FORMAT(o.last_repayment_time, 'yyyy-MM-dd'),
            KEY 'roll_allowed' VALUE CAST(0 AS INT)
        )) AS repayment_plan_json,
        bvn.bvn AS bvn_raw
    FROM v_application_triggers AS t
    INNER JOIN dim_application_order FOR SYSTEM_TIME AS OF t.proc_time AS o ON o.id = t.order_id
    INNER JOIN dim_user FOR SYSTEM_TIME AS OF t.proc_time AS u ON CAST(u.id AS BIGINT) = o.user_id
    LEFT JOIN dim_user_bank_default FOR SYSTEM_TIME AS OF t.proc_time AS ub ON CAST(ub.user_id AS BIGINT) = o.user_id
    LEFT JOIN dim_user_bvn FOR SYSTEM_TIME AS OF t.proc_time AS bvn ON CAST(bvn.user_id AS BIGINT) = o.user_id
    LEFT JOIN dim_device_ids FOR SYSTEM_TIME AS OF t.proc_time AS di
        ON u.device_id IS NOT NULL AND TRIM(u.device_id) <> '' AND di.device_uuid = u.device_id
    LEFT JOIN dim_risk_approval FOR SYSTEM_TIME AS OF t.proc_time AS ra ON ra.order_no = o.order_no
    LEFT JOIN dim_user_repay_paid FOR SYSTEM_TIME AS OF t.proc_time AS ur ON ur.order_no = o.order_no
    LEFT JOIN dim_installment_overdue FOR SYSTEM_TIME AS OF t.proc_time AS ov ON CAST(ov.user_order_id AS BIGINT) = o.id
    WHERE o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
) AS e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> ''
  AND e.bank_account_token IS NOT NULL AND TRIM(e.bank_account_token) <> ''
  AND (
      e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = ''
      OR (e.id_number_token IS NOT NULL AND TRIM(e.id_number_token) <> '')
  );
