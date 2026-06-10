-- 老库 user 全量（索引优化版 SQL 逻辑 · Flink Batch）
-- 相对 03_sync_user_lm_bulk.sql：coreAppId 映射、group_user_id(created<=)、dac 取最新 MAX(id)
-- 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-user-lm-bulk-opt.sh
-- 全量: LM_MIGRATION_LIMIT=2147483647（默认）

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE IF NOT EXISTS src_lm_user_raw (
    id BIGINT,
    `appId` BIGINT,
    mobile STRING,
    `isCancel` TINYINT,
    updated TIMESTAMP(3),
    `deviceId` BIGINT,
    created TIMESTAMP(3),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'user',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS src_lm_dac_raw (
    id BIGINT,
    `deviceId` BIGINT,
    channel STRING,
    google_ads_campaign_id STRING,
    fb_install_referrer_campaign_id STRING,
    google_ads_adgroup_id STRING,
    fb_install_referrer_campaign_group_id STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'device_ad_channel',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS src_lm_app_config (
    id BIGINT,
    `appId` BIGINT,
    `key` STRING,
    `value` STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = 'app_config',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.fetch-size' = '1000'
);

CREATE TABLE IF NOT EXISTS sink_user (
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
    'url' = 'jdbc:mysql://${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true',
    'table-name' = 'user',
    'username' = '${TARGET_MYSQL_USER}',
    'password' = '${TARGET_MYSQL_PASSWORD}',
    'sink.buffer-flush.max-rows' = '${FLINK_SINK_BUFFER_ROWS}',
    'sink.buffer-flush.interval' = '500ms',
    'sink.max-retries' = '3'
);

CREATE TEMPORARY VIEW v_user_lim AS
SELECT id, `appId`, mobile, `isCancel`, updated, `deviceId`, created
FROM src_lm_user_raw
ORDER BY id DESC
LIMIT ${LM_MIGRATION_LIMIT};

CREATE TEMPORARY VIEW v_cam AS
SELECT CAST(ac.`value` AS BIGINT) AS sub_app_id, ac.`appId` AS main_app_id
FROM src_lm_app_config ac
INNER JOIN (
    SELECT CAST(`value` AS BIGINT) AS sub_app_id, MAX(id) AS max_id
    FROM src_lm_app_config
    WHERE `key` = 'coreAppId'
    GROUP BY CAST(`value` AS BIGINT)
) pick ON pick.max_id = ac.id;

CREATE TEMPORARY VIEW v_dac_latest AS
SELECT dac1.`deviceId`, dac1.channel, dac1.google_ads_campaign_id,
       dac1.fb_install_referrer_campaign_id, dac1.google_ads_adgroup_id,
       dac1.fb_install_referrer_campaign_group_id
FROM src_lm_dac_raw dac1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id
    FROM src_lm_dac_raw
    WHERE `deviceId` IS NOT NULL AND `deviceId` <> 0
    GROUP BY `deviceId`
) dac_max ON dac_max.max_id = dac1.id;

CREATE TEMPORARY VIEW v_user_eff AS
SELECT
    u.id,
    u.`appId`,
    u.mobile,
    u.created,
    COALESCE(cam.main_app_id, u.`appId`) AS eff_app_id
FROM src_lm_user_raw u
LEFT JOIN v_cam cam ON cam.sub_app_id = u.`appId`;

CREATE TEMPORARY VIEW v_group_user_id AS
SELECT user_id, group_user_id
FROM (
    SELECT
        c.id AS user_id,
        p.id AS group_user_id,
        ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY p.created ASC, p.id ASC) AS rn
    FROM v_user_eff c
    INNER JOIN v_user_eff p
        ON p.mobile = c.mobile
        AND p.eff_app_id = c.eff_app_id
        AND p.created <= c.created
) t
WHERE rn = 1;

INSERT INTO sink_user
SELECT
    u.id AS user_id,
    CAST(u.`appId` AS INT) AS app_id,
    COALESCE(g.group_user_id, u.id) AS group_user_id,
    u.id AS info_user_id,
    TRIM(u.mobile) AS mobile,
    CASE
        WHEN u.`isCancel` = 1 OR CAST(u.`isCancel` AS STRING) = '1' THEN
            CAST(UNIX_TIMESTAMP(CAST(u.updated AS STRING)) * 1000 AS BIGINT)
        ELSE CAST(0 AS BIGINT)
    END AS closed_time,
    CAST(u.`deviceId` AS STRING) AS reg_device_uuid,
    CAST(UNIX_TIMESTAMP(CAST(u.created AS STRING)) * 1000 AS BIGINT) AS reg_time,
    CAST(0 AS TINYINT) AS test_flag,
    CASE UPPER(dac.channel)
        WHEN 'ORGANIC' THEN 'organic'
        WHEN 'FB' THEN 'facebook'
        WHEN 'TT' THEN 'tiktok'
        WHEN 'GG' THEN 'google'
        ELSE CAST(NULL AS STRING)
    END AS utm_source,
    CAST(NULL AS STRING) AS utm_medium,
    CAST(NULL AS STRING) AS utm_campaign,
    CAST(NULL AS STRING) AS utm_content,
    CAST(NULL AS STRING) AS utm_term,
    CASE dac.channel
        WHEN 'GG' THEN CAST(dac.google_ads_campaign_id AS STRING)
        WHEN 'FB' THEN CAST(dac.fb_install_referrer_campaign_id AS STRING)
        ELSE CAST(NULL AS STRING)
    END AS campaign_id,
    CASE dac.channel
        WHEN 'GG' THEN CAST(dac.google_ads_adgroup_id AS STRING)
        WHEN 'FB' THEN CAST(dac.fb_install_referrer_campaign_group_id AS STRING)
        ELSE CAST(NULL AS STRING)
    END AS ad_group_id,
    CAST(NULL AS STRING) AS advertiser_id
FROM v_user_lim u
LEFT JOIN v_group_user_id g ON g.user_id = u.id
LEFT JOIN v_dac_latest dac
    ON dac.`deviceId` = u.`deviceId`
   AND u.`deviceId` IS NOT NULL AND u.`deviceId` <> 0
WHERE u.mobile IS NOT NULL AND TRIM(u.mobile) <> '';
