-- 增量 application（目标：源变更 ≤30s 可见）
-- 优化：
--   1) 触发侧：CDC 只出 order_id，禁止 cdc_user ⋈ cdc_user_order 双流 Join
--   2) 组装侧：单次 Lookup application_incr_bundle_lookup（源库内 JOIN+VT cache）
--   3) VT miss 再调 vt_tokenize；lookup.cache.ttl=${LOOKUP_CACHE_TTL}（默认 30s）
-- CDC: user_order, user, user_bank_info, user_personal_info, device_ids,
--      user_repay, risk_user_approval_callback, user_order_installment
-- 前置: ./scripts/deploy-source-ddl.sh（含 application_incr_bundle_lookup 等）
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '200ms';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '60s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'table.exec.state.ttl' = '2 h';

CREATE TABLE IF NOT EXISTS cdc_user_order (
    id BIGINT,
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

CREATE TABLE IF NOT EXISTS dim_order_id_by_order_no (
    order_no STRING,
    order_id BIGINT,
    PRIMARY KEY (order_no) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_order_id_by_order_no_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_latest_order_by_user (
    user_id BIGINT,
    order_id BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_latest_order_by_user_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_latest_order_by_device (
    device_uuid STRING,
    order_id BIGINT,
    PRIMARY KEY (device_uuid) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_latest_order_by_device_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

CREATE TABLE IF NOT EXISTS dim_application_bundle (
    -- user_order.id 为 bigint unsigned；Flink BIGINT(signed) 会 ClassCast，故用 DECIMAL
    -- 视图主键列保持裸 o.id（勿 CAST），否则 WHERE id=? 无法走 PRIMARY
    id DECIMAL(20, 0),
    application_no STRING,
    sn STRING,
    user_id BIGINT,
    app_code BIGINT,
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
    period_days BIGINT,
    period_count BIGINT,
    re_loan BIGINT,
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
    risk_status BIGINT,
    repayment_plan_json STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'application_incr_bundle_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '${LOOKUP_CACHE_TTL}'
);

-- 全部触发收敛为 order_id，无 CDC 双流 Join
CREATE TEMPORARY VIEW v_application_triggers AS
SELECT id AS order_id, proc_time FROM cdc_user_order WHERE id IS NOT NULL
UNION ALL
SELECT lo.order_id, u.proc_time
FROM cdc_user AS u
INNER JOIN dim_latest_order_by_user FOR SYSTEM_TIME AS OF u.proc_time AS lo
    ON CAST(lo.user_id AS BIGINT) = u.id
WHERE lo.order_id IS NOT NULL
UNION ALL
SELECT lo.order_id, b.proc_time
FROM cdc_user_bank_info AS b
INNER JOIN dim_latest_order_by_user FOR SYSTEM_TIME AS OF b.proc_time AS lo
    ON CAST(lo.user_id AS BIGINT) = b.user_id
WHERE b.user_id IS NOT NULL AND lo.order_id IS NOT NULL
UNION ALL
SELECT lo.order_id, p.proc_time
FROM cdc_user_personal_info AS p
INNER JOIN dim_latest_order_by_user FOR SYSTEM_TIME AS OF p.proc_time AS lo
    ON CAST(lo.user_id AS BIGINT) = p.user_id
WHERE p.user_id IS NOT NULL AND lo.order_id IS NOT NULL
UNION ALL
SELECT ld.order_id, di.proc_time
FROM cdc_device_ids AS di
INNER JOIN dim_latest_order_by_device FOR SYSTEM_TIME AS OF di.proc_time AS ld
    ON ld.device_uuid = di.device_uuid
WHERE di.device_uuid IS NOT NULL AND TRIM(di.device_uuid) <> '' AND ld.order_id IS NOT NULL
UNION ALL
SELECT oid.order_id, ur.proc_time
FROM cdc_user_repay AS ur
INNER JOIN dim_order_id_by_order_no FOR SYSTEM_TIME AS OF ur.proc_time AS oid
    ON oid.order_no = ur.order_no
WHERE ur.order_no IS NOT NULL AND TRIM(ur.order_no) <> '' AND oid.order_id IS NOT NULL
UNION ALL
SELECT oid.order_id, ra.proc_time
FROM cdc_risk_user_approval AS ra
INNER JOIN dim_order_id_by_order_no FOR SYSTEM_TIME AS OF ra.proc_time AS oid
    ON oid.order_no = ra.order_no
WHERE ra.order_no IS NOT NULL AND TRIM(ra.order_no) <> '' AND oid.order_id IS NOT NULL
UNION ALL
SELECT i.user_order_id AS order_id, i.proc_time
FROM cdc_user_order_installment AS i
WHERE i.user_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS sink_application (
    application_no STRING, mobile STRING, bid STRING, app_id INT, app_version STRING,
    user_id BIGINT, group_user_id BIGINT, sn STRING, is_test TINYINT, is_first_apply TINYINT, is_auto_apply TINYINT,
    id_number STRING, gaid_idfa STRING, device_uuid STRING, session_id STRING,
    bank_code STRING, bank_account_name STRING, bank_account_number STRING,
    product_id STRING, product_scheme_id STRING,
    product_calculator_version STRING, repay_calculator_version STRING, rollover_calculator_version STRING,
    product_scheme_param STRING,
    term INT, periods INT, repayment_method TINYINT, repayment_plan STRING,
    credit_limit BIGINT, loan_amount BIGINT, principal BIGINT, total_amount BIGINT, disbursed_amount BIGINT,
    created_time BIGINT, submited_time BIGINT, reviewed_time BIGINT, disbursed_time BIGINT,
    last_paid_time BIGINT, paid_off_time BIGINT, lock_expire_time BIGINT,
    coupon_code STRING,
    status TINYINT,
    PRIMARY KEY (mobile, group_user_id, sn) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?${TARGET_JDBC_PARAMS}',
    'table-name' = 'application',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '200ms',
    'sink.max-retries' = '${FLINK_SINK_MAX_RETRIES}',
    'connection.max-retry-timeout' = '${FLINK_JDBC_RETRY_TIMEOUT}'
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
    '48',
    '50',
    '49',
    '{}',
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
    CAST('' AS STRING),
    e.risk_status
FROM (
    SELECT
        b.application_no,
        b.sn,
        b.user_id + 100000000 AS user_id,
        b.user_id + 100000000 AS group_user_id,
        CAST(b.app_code AS INT) AS app_id,
        CASE
            WHEN b.mobile_token IS NOT NULL AND TRIM(b.mobile_token) <> '' THEN b.mobile_token
            WHEN b.mobile_norm IS NULL OR TRIM(b.mobile_norm) = '' THEN CAST(NULL AS STRING)
            ELSE vt_tokenize(TRIM(b.mobile_norm))
        END AS mobile_token,
        CASE
            WHEN b.bvn_raw IS NULL OR TRIM(b.bvn_raw) = '' THEN CAST('' AS STRING)
            WHEN b.id_number_token IS NOT NULL AND TRIM(b.id_number_token) <> '' THEN b.id_number_token
            ELSE vt_tokenize(TRIM(b.bvn_raw))
        END AS id_number_token,
        CASE
            WHEN b.gaid_idfa_raw IS NULL OR TRIM(b.gaid_idfa_raw) = '' THEN CAST(NULL AS STRING)
            WHEN b.gaid_idfa_token IS NOT NULL AND TRIM(b.gaid_idfa_token) <> '' THEN b.gaid_idfa_token
            ELSE vt_tokenize(TRIM(b.gaid_idfa_raw))
        END AS gaid_idfa_token,
        COALESCE(b.device_uuid, '') AS device_uuid,
        b.session_id,
        COALESCE(b.bank_code, '') AS bank_code,
        COALESCE(b.bank_account_name, '') AS bank_account_name,
        CASE
            WHEN b.bank_account_token IS NOT NULL AND TRIM(b.bank_account_token) <> '' THEN b.bank_account_token
            WHEN b.bank_account_raw IS NULL OR TRIM(b.bank_account_raw) = '' THEN CAST(NULL AS STRING)
            ELSE vt_tokenize(TRIM(b.bank_account_raw))
        END AS bank_account_token,
        b.product_id,
        CAST(b.period_days AS INT) AS period_days,
        CAST(b.period_count AS INT) AS period_count,
        CAST(b.re_loan AS TINYINT) AS re_loan,
        b.credit_limit_minor,
        b.loan_amount_minor,
        b.principal_minor,
        b.total_amount_minor,
        b.disbursed_amount_minor,
        CAST(UNIX_TIMESTAMP(CAST(b.order_time AS STRING)) * 1000 AS BIGINT) AS created_time_ms,
        CASE WHEN b.reviewed_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(b.reviewed_time AS STRING)) * 1000 END AS reviewed_time_ms,
        CASE WHEN b.disburse_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(b.disburse_time AS STRING)) * 1000 END AS disbursed_time_ms,
        CASE WHEN b.last_paid_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(b.last_paid_time AS STRING)) * 1000 END AS last_paid_time_ms,
        CASE WHEN b.settled_time IS NULL THEN CAST(NULL AS BIGINT) ELSE UNIX_TIMESTAMP(CAST(b.settled_time AS STRING)) * 1000 END AS paid_off_time_ms,
        (UNIX_TIMESTAMP(CAST(b.order_time AS STRING)) + 7 * 86400) * 1000 AS lock_expire_ms,
        CAST(b.risk_status AS TINYINT) AS risk_status,
        b.repayment_plan_json,
        b.bvn_raw
    FROM v_application_triggers AS t
    INNER JOIN dim_application_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b
        ON b.id = t.order_id
    WHERE b.sn IS NOT NULL AND TRIM(b.sn) <> ''
) AS e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> ''
  AND e.bank_account_token IS NOT NULL AND TRIM(e.bank_account_token) <> ''
  AND e.app_id IN (567, 568, 571, 572, 573)
  AND (
      e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = ''
      OR (e.id_number_token IS NOT NULL AND TRIM(e.id_number_token) <> '')
  );
