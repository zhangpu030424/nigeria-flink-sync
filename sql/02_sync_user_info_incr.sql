-- 增量 user_info：CDC user_personal_info + JDBC Lookup（须 CAST 视图，勿直查源表）
-- 前置: ./scripts/deploy-source-ddl.sh
-- 验证: bash scripts/verify-user-info-incr.sh [源库 user_id，如 211038]
-- 注意: 只监听 user_personal_info（不是 user）；目标 user_id = 源 user_id + 100000000
--       info JSON 键集合与 user_info_sync_staging 宽表固定一致
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_user_personal_info (
    id BIGINT,
    user_id BIGINT,
    bvn STRING,
    first_name STRING,
    sur_name STRING,
    date_of_birth DATE,
    education_level INT,
    gender INT,
    living_address_state STRING,
    living_address_city STRING,
    living_address_first_line STRING,
    living_address_second_line STRING,
    number_of_children INT,
    marriage INT,
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
    'scan.startup.mode' = '${CDC_STARTUP_MODE}',
    'scan.startup.timestamp-millis' = '${CDC_STARTUP_TIMESTAMP_MILLIS}',
    'scan.incremental.snapshot.enabled' = 'true',
    'debezium.snapshot.mode' = 'schema_only',
    'scan.incremental.snapshot.chunk.size' = '${FLINK_CDC_CHUNK_SIZE}',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_vt_cache (
    vt_type STRING,
    raw_value STRING,
    token STRING,
    status BIGINT,
    PRIMARY KEY (vt_type, raw_value) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'vt_token_cache_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_user (
    id BIGINT,
    app_code BIGINT,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_user_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_user_work (
    user_id BIGINT,
    work_type STRING,
    occupation STRING,
    company_name STRING,
    monthly_income STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_work_latest_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_app_config (
    app_code BIGINT,
    app_name STRING,
    version STRING,
    PRIMARY KEY (app_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'app_config_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl' = '24h'
);

CREATE TABLE IF NOT EXISTS dim_user_credit (
    user_id BIGINT,
    credit_limit STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_credit_latest_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_user_reg_ip (
    user_id BIGINT,
    ip STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_reg_ip_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_user_emergency_contacts (
    user_id BIGINT,
    emergency_contacts STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_emergency_contacts_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_user_install_source (
    user_id BIGINT,
    install_source STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_install_source_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '2h'
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
    'sink.buffer-flush.interval' = '1s',
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
        p.user_id + 100000000 AS user_id,
        p.bvn AS bvn_raw,
        COALESCE(
            CASE
                WHEN p.bvn IS NULL OR TRIM(p.bvn) = '' THEN CAST('' AS STRING)
                WHEN vt.status = 1 AND vt.token IS NOT NULL AND TRIM(vt.token) <> '' THEN vt.token
                ELSE vt_tokenize(TRIM(p.bvn))
            END,
            ''
        ) AS id_number,
        COALESCE(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))), '') AS full_name,
        JSON_STRING(JSON_OBJECT(
            NULL ON NULL NULL,
            KEY 'birthday' VALUE CASE
                WHEN p.date_of_birth IS NULL THEN CAST(NULL AS STRING)
                ELSE DATE_FORMAT(CAST(p.date_of_birth AS TIMESTAMP(3)), 'yyyy-MM-dd')
            END,
            KEY 'job_type' VALUE CAST(wr.work_type AS STRING),
            KEY 'education' VALUE CAST(p.education_level AS BIGINT),
            KEY 'gender' VALUE CAST(p.gender AS BIGINT),
            KEY 'registration_ip' VALUE CAST(reg_ip.ip AS STRING),
            KEY 'salary' VALUE CASE
                WHEN wr.monthly_income IS NULL OR TRIM(wr.monthly_income) = '' THEN CAST(NULL AS BIGINT)
                WHEN CHAR_LENGTH(REPLACE(TRIM(wr.monthly_income), ',', '')) BETWEEN 1 AND 19
                    AND REGEXP(REPLACE(TRIM(wr.monthly_income), ',', ''), '^[0-9]+$')
                    THEN CAST(REPLACE(TRIM(wr.monthly_income), ',', '') AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END,
            KEY 'loan_purpose' VALUE CAST(NULL AS STRING),
            KEY 'face_similarity' VALUE CAST(NULL AS STRING),
            KEY 'pay_cycle' VALUE CAST(NULL AS STRING),
            KEY 'salary_yearly' VALUE CAST(NULL AS STRING),
            KEY 'credit_limit' VALUE CASE
                WHEN cc.credit_limit IS NULL OR TRIM(cc.credit_limit) = '' THEN CAST(NULL AS BIGINT)
                WHEN REGEXP(TRIM(cc.credit_limit), '^[0-9]{1,19}$') THEN CAST(cc.credit_limit AS BIGINT)
                ELSE CAST(NULL AS BIGINT)
            END,
            KEY 'company' VALUE CASE
                WHEN wr.company_name IS NULL OR TRIM(wr.company_name) = '' THEN CAST(NULL AS STRING)
                ELSE TRIM(wr.company_name)
            END,
            KEY 'install_source' VALUE CAST(isrc.install_source AS STRING),
            KEY 'registration_time' VALUE CASE
                WHEN u.create_time IS NULL THEN CAST(NULL AS BIGINT)
                ELSE CAST(UNIX_TIMESTAMP(CAST(u.create_time AS STRING)) AS BIGINT)
            END,
            KEY 'email' VALUE CAST(NULL AS STRING),
            KEY 'ocr' VALUE CAST(NULL AS STRING),
            KEY 'profession' VALUE CAST(wr.occupation AS STRING),
            KEY 'app' VALUE JSON_OBJECT(
                NULL ON NULL NULL,
                KEY 'name' VALUE CAST(ac.app_name AS STRING),
                KEY 'version' VALUE CAST(ac.version AS STRING),
                KEY 'app_id' VALUE CAST(u.app_code AS BIGINT)
            ),
            KEY 'emergency_contacts' VALUE COALESCE(ec.emergency_contacts, '[]'),
            KEY 'salary_day' VALUE CAST(NULL AS STRING),
            KEY 'address' VALUE JSON_OBJECT(
                NULL ON NULL NULL,
                KEY 'province' VALUE CAST(p.living_address_state AS STRING),
                KEY 'city' VALUE CAST(p.living_address_city AS STRING),
                KEY 'district' VALUE CAST(NULL AS STRING),
                KEY 'detail' VALUE CASE
                    WHEN TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ', COALESCE(p.living_address_second_line, ''))) = ''
                        THEN CAST(NULL AS STRING)
                    ELSE TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ', COALESCE(p.living_address_second_line, '')))
                END,
                KEY 'village' VALUE CAST(NULL AS STRING)
            ),
            KEY 'salary_fortnightly' VALUE CAST(NULL AS STRING),
            KEY 'salary_daily' VALUE CAST(NULL AS STRING),
            KEY 'salary_monthly' VALUE CAST(1 AS BIGINT),
            KEY 'children_num' VALUE CAST(p.number_of_children AS BIGINT),
            KEY 'religion' VALUE CAST(NULL AS STRING),
            KEY 'marital' VALUE CAST(p.marriage AS BIGINT),
            KEY 'full_name' VALUE CASE
                WHEN TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))) = ''
                    THEN CAST(NULL AS STRING)
                ELSE TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, '')))
            END,
            KEY 'salary_weekly' VALUE CAST(NULL AS STRING),
            KEY 'survey' VALUE CAST(NULL AS STRING),
            KEY 'salary_type' VALUE CAST(NULL AS STRING)
        )) AS info_json
    FROM src_user_personal_info AS p
    INNER JOIN dim_user FOR SYSTEM_TIME AS OF p.proc_time AS u ON CAST(u.id AS BIGINT) = p.user_id
    LEFT JOIN dim_vt_cache FOR SYSTEM_TIME AS OF p.proc_time AS vt
        ON vt.vt_type = 'id_number'
        AND p.bvn IS NOT NULL AND TRIM(p.bvn) <> ''
        AND vt.raw_value = TRIM(p.bvn)
    LEFT JOIN dim_user_work FOR SYSTEM_TIME AS OF p.proc_time AS wr ON CAST(wr.user_id AS BIGINT) = p.user_id
    LEFT JOIN dim_app_config FOR SYSTEM_TIME AS OF p.proc_time AS ac ON ac.app_code = u.app_code
    LEFT JOIN dim_user_credit FOR SYSTEM_TIME AS OF p.proc_time AS cc ON cc.user_id = p.user_id
    LEFT JOIN dim_user_reg_ip FOR SYSTEM_TIME AS OF p.proc_time AS reg_ip ON reg_ip.user_id = p.user_id
    LEFT JOIN dim_user_emergency_contacts FOR SYSTEM_TIME AS OF p.proc_time AS ec ON ec.user_id = p.user_id
    LEFT JOIN dim_user_install_source FOR SYSTEM_TIME AS OF p.proc_time AS isrc ON isrc.user_id = p.user_id
) AS e
WHERE (e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = '')
   OR (e.id_number IS NOT NULL AND TRIM(e.id_number) <> '');
