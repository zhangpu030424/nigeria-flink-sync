-- 老库宽表 id_add_user → 目标 user（直传，无 VT、无 JOIN）
-- 执行: bash scripts/run-id-add-user-bulk.sh
-- 试跑: LM_MIGRATION_LIMIT=20 bash scripts/run-id-add-user-bulk.sh
-- 全量: bash scripts/run-id-add-user-bulk.sh（默认不加 LIMIT，避免 SortLimit OOM）

SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'parallelism.default' = '${FLINK_PARALLELISM}';

CREATE TABLE src_id_add_user (
    user_id BIGINT,
    app_id INT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag TINYINT,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_term STRING,
    campaign_id STRING,
    ad_group_id STRING,
    advertiser_id STRING
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'id_add_user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE sink_user (
    user_id BIGINT,
    app_id INT,
    group_user_id BIGINT,
    info_user_id BIGINT,
    mobile STRING,
    closed_time BIGINT,
    reg_device_uuid STRING,
    reg_time BIGINT,
    test_flag TINYINT,
    utm_source STRING,
    utm_medium STRING,
    utm_campaign STRING,
    utm_content STRING,
    utm_term STRING,
    campaign_id STRING,
    ad_group_id STRING,
    advertiser_id STRING,
    PRIMARY KEY (mobile, app_id, closed_time) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useUnicode=true&characterEncoding=utf8&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

INSERT INTO sink_user
SELECT
    user_id,
    app_id,
    COALESCE(group_user_id, user_id),
    COALESCE(info_user_id, user_id),
    TRIM(mobile),
    COALESCE(closed_time, CAST(0 AS BIGINT)),
    COALESCE(reg_device_uuid, ''),
    COALESCE(reg_time, CAST(0 AS BIGINT)),
    COALESCE(test_flag, CAST(0 AS TINYINT)),
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    campaign_id,
    ad_group_id,
    advertiser_id
FROM src_id_add_user
${LM_MIGRATION_LIMIT_CLAUSE};
