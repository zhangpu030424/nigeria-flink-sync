-- 全量阶段 1 user_info：宽表 id_number_token + info_json；紧急联系人 mobile 无 token 时 UDF 调 VT
CREATE TEMPORARY FUNCTION vt_tokenize_emergency_contacts AS 'com.nigeria.flink.udf.VtTokenizeEmergencyContactsFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE IF NOT EXISTS src_user_info_staging (
    user_id BIGINT,
    bvn_raw STRING,
    id_number_token STRING,
    full_name STRING,
    info_json STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user_info_sync_staging',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS sink_user_info (
    user_id BIGINT,
    id_number STRING,
    full_name STRING,
    password STRING,
    live_image STRING,
    id_card STRING,
    info STRING,
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
    user_id + 100000000,
    CASE
        WHEN bvn_raw IS NULL OR TRIM(bvn_raw) = '' THEN CAST('' AS STRING)
        ELSE COALESCE(id_number_token, '')
    END,
    COALESCE(full_name, ''),
    CAST('' AS STRING),
    CAST('' AS STRING),
    CAST('' AS STRING),
    COALESCE(vt_tokenize_emergency_contacts(COALESCE(CAST(info_json AS STRING), '{}')), '{}')
FROM src_user_info_staging
WHERE (bvn_raw IS NULL OR TRIM(bvn_raw) = '')
   OR (id_number_token IS NOT NULL AND TRIM(id_number_token) <> '');
