-- 老库 ng_loan_market 一次性全量 → 目标 user（不建 VIEW，无 VT，mobile 明文直传）
-- 执行: bash lm/scripts/run-user-lm-bulk.sh

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE IF NOT EXISTS src_lm_user_raw (
    id BIGINT,
    appid BIGINT,
    mobile STRING,
    isCancel TINYINT,
    updated TIMESTAMP(3),
    deviceId BIGINT,
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
    deviceId BIGINT,
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
    value STRING,
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

INSERT INTO sink_user
SELECT
    base.user_id,
    base.app_id,
    base.group_user_id,
    base.info_user_id,
    base.mobile,
    base.closed_time,
    base.reg_device_uuid,
    base.reg_time,
    base.test_flag,
    base.utm_source,
    base.utm_medium,
    base.utm_campaign,
    base.utm_content,
    base.utm_term,
    base.campaign_id,
    base.ad_group_id,
    base.advertiser_id
FROM (
    SELECT
        u.id AS user_id,
        CAST(u.appid AS INT) AS app_id,
        CASE
            WHEN app_hit.user_id IS NOT NULL THEN COALESCE(grp.first_user_id, u.id)
            ELSE u.id
        END AS group_user_id,
        u.id AS info_user_id,
        TRIM(u.mobile) AS mobile,
        CASE
            WHEN u.isCancel = 1 OR CAST(u.isCancel AS STRING) = '1' THEN
                CAST(UNIX_TIMESTAMP(CAST(u.updated AS STRING)) * 1000 AS BIGINT)
            ELSE CAST(0 AS BIGINT)
        END AS closed_time,
        COALESCE(CAST(u.deviceId AS STRING), '') AS reg_device_uuid,
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
    FROM src_lm_user_raw AS u
    LEFT JOIN (
        SELECT deviceId, channel, google_ads_campaign_id, fb_install_referrer_campaign_id,
               google_ads_adgroup_id, fb_install_referrer_campaign_group_id
        FROM (
            SELECT d.deviceId, d.channel, d.google_ads_campaign_id, d.fb_install_referrer_campaign_id,
                   d.google_ads_adgroup_id, d.fb_install_referrer_campaign_group_id,
                   ROW_NUMBER() OVER (PARTITION BY d.deviceId ORDER BY d.id) AS rn
            FROM src_lm_dac_raw AS d
            WHERE d.deviceId IS NOT NULL AND d.deviceId <> 0
        ) t
        WHERE t.rn = 1
    ) AS dac ON u.deviceId = dac.deviceId
    LEFT JOIN (
        SELECT DISTINCT u2.id AS user_id
        FROM src_lm_user_raw AS u2
        INNER JOIN src_lm_app_config AS ac
            ON ac.value IS NOT NULL
            AND (
                ac.value = CAST(u2.appid AS STRING)
                OR POSITION(CAST(u2.appid AS STRING) IN ac.value) > 0
            )
    ) AS app_hit ON app_hit.user_id = u.id
    LEFT JOIN (
        SELECT t.user_id, t.first_user_id
        FROM (
            SELECT c.id AS user_id, p.id AS first_user_id,
                   ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY p.created, p.id) AS rn
            FROM src_lm_user_raw AS c
            INNER JOIN src_lm_user_raw AS p
                ON c.mobile = p.mobile AND p.created < c.created
        ) t
        WHERE t.rn = 1
    ) AS grp ON grp.user_id = u.id
) AS base
WHERE base.mobile IS NOT NULL AND TRIM(base.mobile) <> '';
