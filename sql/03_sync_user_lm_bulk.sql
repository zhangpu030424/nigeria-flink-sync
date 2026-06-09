-- 老库 ng_loan_market 一次性全量 → 目标 user（不建 VIEW，JDBC 内嵌 generate_user.py 等价 SELECT）
-- 前置: ./scripts/lm-vt-seed-mobile.sh && vt-preload（或 vt_tokenize 兜底）
-- 执行: ./scripts/run-user-lm-bulk.sh

CREATE TEMPORARY FUNCTION vt_tokenize AS 'com.nigeria.flink.udf.VtTokenizeFunction';

SET 'parallelism.default' = '${FLINK_PARALLELISM}';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE IF NOT EXISTS src_lm_user (
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
    proc_time AS PROCTIME(),
    PRIMARY KEY (user_id, app_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Africa/Lagos',
    'table-name' = '(SELECT u.`id` AS `user_id`, u.`appid` AS `app_id`, CASE WHEN EXISTS (SELECT 1 FROM `ng_loan_market`.`app_config` ac WHERE ac.`value` IS NOT NULL AND (ac.`value` = CAST(u.`appid` AS CHAR) OR INSTR(ac.`value`, CAST(u.`appid` AS CHAR)) > 0)) THEN COALESCE((SELECT u2.`id` FROM `ng_loan_market`.`user` u2 WHERE u2.`mobile` = u.`mobile` AND u2.`created` < u.`created` ORDER BY u2.`created` ASC, u2.`id` ASC LIMIT 1), u.`id`) ELSE u.`id` END AS `group_user_id`, u.`id` AS `info_user_id`, u.`mobile`, CASE WHEN u.`isCancel` IN (1, ''1'') THEN UNIX_TIMESTAMP(u.`updated`) * 1000 ELSE 0 END AS `closed_time`, CAST(u.`deviceId` AS CHAR) AS `reg_device_uuid`, UNIX_TIMESTAMP(u.`created`) * 1000 AS `reg_time`, 0 AS `test_flag`, CASE UPPER(dac.`channel`) WHEN ''ORGANIC'' THEN ''organic'' WHEN ''FB'' THEN ''facebook'' WHEN ''TT'' THEN ''tiktok'' WHEN ''GG'' THEN ''google'' ELSE NULL END AS `utm_source`, NULL AS `utm_medium`, NULL AS `utm_campaign`, NULL AS `utm_content`, NULL AS `utm_term`, CASE dac.`channel` WHEN ''GG'' THEN dac.`google_ads_campaign_id` WHEN ''FB'' THEN dac.`fb_install_referrer_campaign_id` ELSE NULL END AS `campaign_id`, CASE dac.`channel` WHEN ''GG'' THEN dac.`google_ads_adgroup_id` WHEN ''FB'' THEN dac.`fb_install_referrer_campaign_group_id` ELSE NULL END AS `ad_group_id`, NULL AS `advertiser_id` FROM `ng_loan_market`.`user` u LEFT JOIN (SELECT d.`deviceId`, d.`channel`, d.`google_ads_campaign_id`, d.`fb_install_referrer_campaign_id`, d.`google_ads_adgroup_id`, d.`fb_install_referrer_campaign_group_id` FROM `ng_loan_market`.`device_ad_channel` d INNER JOIN (SELECT `deviceId`, MIN(`id`) AS `min_id` FROM `ng_loan_market`.`device_ad_channel` WHERE `deviceId` IS NOT NULL AND `deviceId` != 0 GROUP BY `deviceId`) dm ON d.`deviceId` = dm.`deviceId` AND d.`id` = dm.`min_id`) dac ON u.`deviceId` = dac.`deviceId`) AS lm_user_src',
    'username' = '${LM_MYSQL_USER}',
    'password' = '${LM_MYSQL_PASSWORD}',
    'scan.partition.column' = 'user_id',
    'scan.partition.num' = '${FLINK_PARALLELISM}',
    'scan.partition.lower-bound' = '1',
    'scan.partition.upper-bound' = '500000000',
    'scan.fetch-size' = '${FLINK_CDC_FETCH_SIZE}'
);

CREATE TABLE IF NOT EXISTS dim_vt_mobile (
    vt_type STRING,
    raw_value STRING,
    token STRING,
    status INT,
    PRIMARY KEY (vt_type, raw_value) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}?useSSL=false&allowPublicKeyRetrieval=true',
    'table-name' = 'vt_token_cache',
    'username' = '${SOURCE_MYSQL_USER}',
    'password' = '${SOURCE_MYSQL_PASSWORD}',
    'lookup.cache.max-rows' = '500000',
    'lookup.cache.ttl' = '24h'
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
    e.user_id,
    e.app_id,
    e.group_user_id,
    e.info_user_id,
    e.mobile_token,
    e.closed_time,
    e.reg_device_uuid,
    e.reg_time,
    e.test_flag,
    e.utm_source,
    e.utm_medium,
    e.utm_campaign,
    e.utm_content,
    e.utm_term,
    e.campaign_id,
    e.ad_group_id,
    e.advertiser_id
FROM (
    SELECT
        u.user_id,
        u.app_id,
        u.group_user_id,
        u.info_user_id,
        COALESCE(
            NULLIF(TRIM(vt.token), ''),
            vt_tokenize(
                CASE
                    WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN CAST(NULL AS STRING)
                    WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                    WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                    WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                    ELSE CONCAT('+234', TRIM(u.mobile))
                END
            )
        ) AS mobile_token,
        u.closed_time,
        COALESCE(u.reg_device_uuid, '') AS reg_device_uuid,
        u.reg_time,
        CAST(u.test_flag AS TINYINT) AS test_flag,
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        u.utm_content,
        u.utm_term,
        CAST(u.campaign_id AS STRING) AS campaign_id,
        CAST(u.ad_group_id AS STRING) AS ad_group_id,
        CAST(u.advertiser_id AS STRING) AS advertiser_id
    FROM src_lm_user AS u
    LEFT JOIN dim_vt_mobile FOR SYSTEM_TIME AS OF u.proc_time AS vt
        ON vt.vt_type = 'mobile'
        AND vt.status = 1
        AND vt.raw_value = CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN CAST(NULL AS STRING)
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END
) AS e
WHERE e.mobile_token IS NOT NULL AND TRIM(e.mobile_token) <> '';
