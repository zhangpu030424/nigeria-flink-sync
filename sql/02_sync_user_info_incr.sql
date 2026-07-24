-- 增量 user_info：单路 CDC user_info_dirty + bundle Lookup
-- dirty 表 PK=user_id，TRIGGER debounce 后 CDC 多为 UPDATE，不能用 TUMBLE 窗口（仅支持 append-only）
-- 去重：源库 sp_user_info_dirty_enqueue* debounce + 本 Job mini-batch
-- info_json 在 MySQL user_info_incr_bundle_lookup 组装（与宽表 JSON 结构一致，固定全部 key）
-- Flink 仅：id_number token/UDF 兜底 + emergency_contacts 内明文手机号 VT 兜底
-- 前置: ./scripts/deploy-source-ddl.sh
CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';
CREATE TEMPORARY FUNCTION vt_tokenize_emergency_contacts AS 'com.nigeria.flink.udf.VtTokenizeEmergencyContactsFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '${USER_INFO_DIRTY_COALESCE_SEC}s';
SET 'table.exec.mini-batch.size' = '5000';
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
    'debezium.snapshot.locking.mode' = 'none',
    'scan.snapshot.fetch.size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_user_info_bundle (
    user_id BIGINT,
    bvn STRING,
    first_name STRING,
    sur_name STRING,
    vt_token STRING,
    vt_status BIGINT,
    info_json STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info_incr_bundle_lookup',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '5s'
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
        vt_tokenize_emergency_contacts(COALESCE(b.info_json, '{}')) AS info_json
    FROM cdc_user_info_dirty AS t
    INNER JOIN dim_user_info_bundle FOR SYSTEM_TIME AS OF t.proc_time AS b ON b.user_id = t.user_id
) AS e
WHERE (e.bvn_raw IS NULL OR TRIM(e.bvn_raw) = '')
   OR (e.id_number IS NOT NULL AND TRIM(e.id_number) <> '');
