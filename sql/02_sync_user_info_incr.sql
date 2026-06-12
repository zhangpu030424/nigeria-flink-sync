-- 增量 user_info：单路 CDC user_info_dirty + 单行 bundle Lookup（源库一次 JOIN）
-- 前置: ./scripts/deploy-source-ddl.sh
-- 并行: FLINK_PARALLELISM_INCR（建议 4）；实时验证: CDC_STARTUP_MODE=latest-offset
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'execution.checkpointing.interval' = '${FLINK_CHECKPOINT_INTERVAL}';
SET 'execution.checkpointing.timeout' = '${FLINK_CHECKPOINT_TIMEOUT}';
SET 'execution.checkpointing.min-pause' = '120s';
SET 'execution.checkpointing.tolerable-failed-checkpoints' = '10';
SET 'execution.checkpointing.unaligned' = 'true';

CREATE TABLE IF NOT EXISTS cdc_user_info_dirty (
    user_id BIGINT,
    updated_at TIMESTAMP(3),
    proc_time AS PROCTIME(),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'hostname' = '${SOURCE_MYSQL_HOST}',
    'port' = '${SOURCE_MYSQL_PORT}',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'database-name' = '${SOURCE_MYSQL_DATABASE}',
    'table-name' = 'user_info_dirty',
    'server-time-zone' = 'Africa/Lagos',
    'server-id' = '${CDC_SERVER_ID_UI_DIRTY}',
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'false',
    'debezium.snapshot.mode' = 'schema_only',
    -- 云 RDS 的 flink_cdc 常无 RELOAD/FLUSH_TABLES；避免 FLUSH TABLES WITH READ LOCK
    'debezium.snapshot.locking.mode' = 'none',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_info_bundle (
    user_id BIGINT,
    app_code BIGINT,
    create_time TIMESTAMP(3),
    bvn STRING,
    first_name STRING,
    sur_name STRING,
    date_of_birth DATE,
    education_level BIGINT,
    gender BIGINT,
    living_address_state STRING,
    living_address_city STRING,
    living_address_first_line STRING,
    living_address_second_line STRING,
    number_of_children BIGINT,
    marriage BIGINT,
    vt_token STRING,
    vt_status BIGINT,
    work_type STRING,
    occupation STRING,
    company_name STRING,
    monthly_income STRING,
    app_name STRING,
    app_version STRING,
    credit_limit STRING,
    reg_ip STRING,
    emergency_contacts STRING,
    install_source STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_incr_bundle_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '200000',
    'lookup.cache.ttl' = '30s'
);

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT, id_number STRING, full_name STRING, password STRING,
    live_image STRING, id_card STRING, info STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    e.user_id,
    e.id_number,
    e.full_name,
    CAST('' AS STRING),
    CAST('' AS STRING),
    CAST('' AS STRING),
    e.info_json
FROM (
    SELECT
        t.user_id + 100000000 AS user_id,
        b.bvn AS bvn_raw,
        COALESCE(
            CASE
                WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN CAST('' AS STRING)
                WHEN b.vt_status = 1 AND b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN b.vt_token
                ELSE vt_tokenize(TRIM(b.bvn))
            END,
            ''
        ) AS id_number,
        COALESCE(TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))), '') AS full_name,
        JSON_STRING(JSON_OBJECT(
            KEY 'birthday' VALUE CASE
                WHEN b.date_of_birth IS NULL THEN CAST(NULL AS STRING)
                ELSE DATE_FORMAT(CAST(b.date_of_birth AS TIMESTAMP(3)), 'yyyy-MM-dd')
            END,
            KEY 'job_type' VALUE CAST(b.work_type AS STRING),
            KEY 'education' VALUE CAST(b.education_level AS BIGINT),
            KEY 'gender' VALUE CAST(b.gender AS BIGINT),
            KEY 'registration_ip' VALUE CAST(b.reg_ip AS STRING),
            KEY 'salary' VALUE CASE
                WHEN b.monthly_income IS NULL OR TRIM(b.monthly_income) = '' THEN CAST(NULL AS BIGINT)
                WHEN CHAR_LENGTH(REPLACE(TRIM(b.monthly_income), ',', '')) BETWEEN 1 AND 19
                    AND REGEXP(REPLACE(TRIM(b.monthly_income), ',', ''), '^[0-9]+$')
                    THEN CAST(REPLACE(TRIM(b.monthly_income), ',', '') AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END,
            KEY 'loan_purpose' VALUE CAST(NULL AS STRING),
            KEY 'face_similarity' VALUE CAST(NULL AS STRING),
            KEY 'pay_cycle' VALUE CAST(NULL AS STRING),
            KEY 'salary_yearly' VALUE CAST(NULL AS STRING),
            KEY 'credit_limit' VALUE CASE
                WHEN b.credit_limit IS NULL OR TRIM(b.credit_limit) = '' THEN CAST(NULL AS BIGINT)
                WHEN REGEXP(TRIM(b.credit_limit), '^[0-9]{1,19}$') THEN CAST(b.credit_limit AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END,
            KEY 'company' VALUE CASE
                WHEN b.company_name IS NULL OR TRIM(b.company_name) = '' THEN CAST(NULL AS STRING)
                ELSE TRIM(b.company_name)
            END,
            KEY 'install_source' VALUE CAST(b.install_source AS STRING),
            KEY 'registration_time' VALUE CASE
                WHEN b.create_time IS NULL THEN CAST(NULL AS BIGINT)
                ELSE CAST(UNIX_TIMESTAMP(CAST(b.create_time AS STRING)) AS BIGINT)
            END,
            KEY 'email' VALUE CAST(NULL AS STRING),
            KEY 'ocr' VALUE CAST(NULL AS STRING),
            KEY 'profession' VALUE CAST(b.occupation AS STRING),
            KEY 'app' VALUE JSON_OBJECT(
                KEY 'name' VALUE CAST(b.app_name AS STRING),
                KEY 'version' VALUE CAST(b.app_version AS STRING),
                KEY 'app_id' VALUE CAST(b.app_code AS BIGINT)
                NULL ON NULL
            ),
            KEY 'emergency_contacts' VALUE COALESCE(b.emergency_contacts, '[]'),
            KEY 'salary_day' VALUE CAST(NULL AS STRING),
            KEY 'address' VALUE JSON_OBJECT(
                KEY 'province' VALUE CAST(b.living_address_state AS STRING),
                KEY 'city' VALUE CAST(b.living_address_city AS STRING),
                KEY 'district' VALUE CAST(NULL AS STRING),
                KEY 'detail' VALUE CASE
                    WHEN TRIM(CONCAT(COALESCE(b.living_address_first_line, ''), ' ', COALESCE(b.living_address_second_line, ''))) = ''
                        THEN CAST(NULL AS STRING)
                    ELSE TRIM(CONCAT(COALESCE(b.living_address_first_line, ''), ' ', COALESCE(b.living_address_second_line, '')))
                END,
                KEY 'village' VALUE CAST(NULL AS STRING)
                NULL ON NULL
            ),
            KEY 'salary_fortnightly' VALUE CAST(NULL AS STRING),
            KEY 'salary_daily' VALUE CAST(NULL AS STRING),
            KEY 'salary_monthly' VALUE CAST(1 AS BIGINT),
            KEY 'children_num' VALUE CAST(b.number_of_children AS BIGINT),
            KEY 'religion' VALUE CAST(NULL AS STRING),
            KEY 'marital' VALUE CAST(b.marriage AS BIGINT),
            KEY 'full_name' VALUE CASE
                WHEN TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))) = ''
                    THEN CAST(NULL AS STRING)
                ELSE TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, '')))
            END,
            KEY 'salary_weekly' VALUE CAST(NULL AS STRING),
            KEY 'survey' VALUE CAST(NULL AS STRING),
            KEY 'salary_type' VALUE CAST(NULL AS STRING)
            NULL ON NULL
        )) AS info_json
    FROM cdc_user_info_dirty AS t
    INNER JOIN dim_user_info_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b ON b.user_id = t.user_id
) AS e
WHERE (e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = '')
   OR (e.id_number IS NOT NULL AND TRIM(e.id_number) <> '');
