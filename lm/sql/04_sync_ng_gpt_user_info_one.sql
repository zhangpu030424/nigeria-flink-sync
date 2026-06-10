-- GPT user_info：单表 JDBC 读 MySQL VIEW（无 Flink 多表 JOIN、无 JDBC 分区）
-- 源: v_flink_gpt_user_info_sink 或 flink_stg_user_info_ready（LM_SRC_TABLE_READY）
-- 执行: bash lm/scripts/run-ng-user-info-gpt-direct.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

CREATE TABLE src_user_info_ready (
    user_id STRING,
    id_number STRING,
    full_name STRING,
    password STRING,
    live_image STRING,
    id_card STRING,
    info STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '${LM_SRC_TABLE_READY}',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE sink_user_info (
    user_id BIGINT,
    id_number STRING,
    full_name STRING,
    password STRING,
    live_image STRING,
    id_card STRING,
    info STRING,
    created_at TIMESTAMP(0),
    updated_at TIMESTAMP(0),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos&tinyInt1isBit=false',
    'table-name' = 'user_info',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user_info
SELECT
    CAST(user_id AS BIGINT),
    COALESCE(id_number, ''),
    COALESCE(full_name, ''),
    COALESCE(password, ''),
    COALESCE(live_image, ''),
    COALESCE(id_card, ''),
    info,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM src_user_info_ready
WHERE user_id IS NOT NULL AND TRIM(user_id) <> ''
${LM_USER_ID_RANGE_CLAUSE}
${LM_MIGRATION_LIMIT_CLAUSE};
