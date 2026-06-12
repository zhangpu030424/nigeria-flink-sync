-- 增量 user_info：CDC user_personal_info + 源表 Lookup；BVN 直接 vt_tokenize(/v2t)
-- 前置: mysql ... < sql/ddl/source_lookup_views.sql（user_info_user_lookup / user_work_latest_lookup）
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
    date_of_birth TIMESTAMP(3),
    education_level STRING,
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

CREATE TABLE IF NOT EXISTS dim_user (
    id BIGINT,
    app_code STRING,
    create_time TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
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
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'user_work_latest_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
    'lookup.cache.ttl' = '2h'
);

CREATE TABLE IF NOT EXISTS dim_app_config (
    app_code STRING,
    app_name STRING,
    version STRING,
    PRIMARY KEY (app_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'app_config',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '1000',
    'lookup.cache.ttl' = '24h'
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
        COALESCE(
            CASE
                WHEN p.bvn IS NULL OR TRIM(p.bvn) = '' THEN CAST('' AS STRING)
                ELSE vt_tokenize(TRIM(p.bvn))
            END,
            ''
        ) AS id_number,
        COALESCE(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))), '') AS full_name,
        JSON_STRING(JSON_OBJECT(
            'birthday' VALUE DATE_FORMAT(p.date_of_birth, 'yyyy-MM-dd'),
            'job_type' VALUE wr.work_type,
            'education' VALUE p.education_level,
            'gender' VALUE CAST(p.gender AS STRING),
            'salary' VALUE CASE
                WHEN wr.monthly_income IS NULL OR TRIM(wr.monthly_income) = '' THEN CAST(NULL AS STRING)
                ELSE wr.monthly_income
            END,
            'company' VALUE wr.company_name,
            'profession' VALUE wr.occupation,
            'registration_time' VALUE CAST(UNIX_TIMESTAMP(CAST(u.create_time AS STRING)) AS BIGINT),
            'marital' VALUE CAST(p.marriage AS STRING),
            'children_num' VALUE p.number_of_children,
            'full_name' VALUE TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))),
            'app' VALUE JSON_OBJECT(
                'name' VALUE ac.app_name,
                'version' VALUE ac.version,
                'app_id' VALUE CAST(u.app_code AS STRING)
            ),
            'address' VALUE JSON_OBJECT(
                'province' VALUE p.living_address_state,
                'city' VALUE p.living_address_city,
                'detail' VALUE TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ', COALESCE(p.living_address_second_line, '')))
            )
        )) AS info_json
    FROM src_user_personal_info AS p
    INNER JOIN dim_user FOR SYSTEM_TIME AS OF p.proc_time AS u ON CAST(u.id AS BIGINT) = p.user_id
    LEFT JOIN dim_user_work FOR SYSTEM_TIME AS OF p.proc_time AS wr ON CAST(wr.user_id AS BIGINT) = p.user_id
    LEFT JOIN dim_app_config FOR SYSTEM_TIME AS OF p.proc_time AS ac ON ac.app_code = u.app_code
) AS e
WHERE (e.id_number IS NOT NULL AND TRIM(e.id_number) <> '')
   OR e.full_name IS NOT NULL;
