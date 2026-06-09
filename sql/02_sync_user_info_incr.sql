-- 增量 user_info：Lookup id_number VT，miss 则 vt_tokenize
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '${FLINK_MINI_BATCH_SIZE}';

CREATE TABLE IF NOT EXISTS src_personal (
    user_id BIGINT,
    bvn STRING,
    first_name STRING,
    sur_name STRING,
    date_of_birth DATE,
    gender INT,
    education_level INT,
    marriage INT,
    number_of_children INT,
    living_address_state STRING,
    living_address_city STRING,
    living_address_first_line STRING,
    living_address_second_line STRING,
    proc_time AS PROCTIME(),
    PRIMARY KEY (user_id) NOT ENFORCED
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
    'scan.incremental.snapshot.enabled' = 'false'
);

CREATE TABLE IF NOT EXISTS dim_vt_id_number (
    vt_type STRING, raw_value STRING, token STRING, status INT,
    PRIMARY KEY (vt_type, raw_value) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'vt_token_cache',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '300000',
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
SELECT e.user_id, e.id_number, e.full_name, '', '', '', e.info_json
FROM (
    SELECT
        p.user_id + 100000000 AS user_id,
        COALESCE(NULLIF(TRIM(vt.token), ''), vt_tokenize(TRIM(p.bvn))) AS id_number,
        TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))) AS full_name,
        CONCAT('{"birthday":"', CAST(p.date_of_birth AS STRING), '","gender":', CAST(COALESCE(p.gender, 0) AS STRING), '}') AS info_json
    FROM src_personal p
    LEFT JOIN dim_vt_id_number FOR SYSTEM_TIME AS OF p.proc_time vt
        ON vt.vt_type = 'id_number' AND vt.status = 1 AND vt.raw_value = TRIM(p.bvn)
    WHERE p.bvn IS NOT NULL AND TRIM(p.bvn) <> ''
) e
WHERE e.id_number IS NOT NULL AND TRIM(e.id_number) <> '';
