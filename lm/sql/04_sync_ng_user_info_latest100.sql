-- flink_stg_user_info_ready → 目标 user_info（单表 JDBC，试跑/全量均可）
-- 前置: lm/scripts/refresh-lm-user-info-gpt-full.sh 或 lm/scripts/refresh-lm-user-info-latest100.sh
-- 全量: bash lm/scripts/run-ng-user-info-gpt-bulk-max.sh

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'pipeline.operator-chaining' = 'false';

CREATE TABLE src_user_info_ready (
    user_id_part DECIMAL(20, 0),
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
    'table-name' = 'flink_stg_user_info_ready',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id_part',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '2000000000',
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
${LM_MIGRATION_LIMIT_CLAUSE};
