-- 索引优化版 user 校验（MySQL 直跑，对照 Flink lm/sql/03_sync_user_lm_bulk_opt.sql）
-- 由 lm/scripts/run-lm-verify-mysql.sh envsubst 后执行

SELECT
    u.`id` AS `user_id`,
    u.`appId` AS `app_id`,
    COALESCE(
        (
            SELECT u2.`id`
            FROM `${LM_MYSQL_DATABASE}`.`user` u2
            WHERE u2.`mobile` = u.`mobile`
              AND u2.`created` <= u.`created`
              AND u2.`appId` = COALESCE(cam.`main_app_id`, u.`appId`)
            ORDER BY u2.`created` ASC, u2.`id` ASC
            LIMIT 1
        ),
        u.`id`
    ) AS `group_user_id`,
    u.`id` AS `info_user_id`,
    u.`mobile`,
    CASE WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.`updated`) * 1000 ELSE 0 END AS `closed_time`,
    CAST(u.`deviceId` AS CHAR) AS `reg_device_uuid`,
    UNIX_TIMESTAMP(u.`created`) * 1000 AS `reg_time`,
    0 AS `test_flag`,
    CASE UPPER(dac.`channel`)
        WHEN 'ORGANIC' THEN 'organic' WHEN 'FB' THEN 'facebook'
        WHEN 'TT' THEN 'tiktok' WHEN 'GG' THEN 'google' ELSE NULL
    END AS `utm_source`,
    NULL AS `utm_medium`, NULL AS `utm_campaign`, NULL AS `utm_content`, NULL AS `utm_term`,
    CASE dac.`channel`
        WHEN 'GG' THEN dac.`google_ads_campaign_id`
        WHEN 'FB' THEN dac.`fb_install_referrer_campaign_id` ELSE NULL
    END AS `campaign_id`,
    CASE dac.`channel`
        WHEN 'GG' THEN dac.`google_ads_adgroup_id`
        WHEN 'FB' THEN dac.`fb_install_referrer_campaign_group_id` ELSE NULL
    END AS `ad_group_id`,
    NULL AS `advertiser_id`
FROM (
    SELECT `id`, `appId`, `mobile`, `deviceId`, `isCancel`, `updated`, `created`
    FROM `${LM_MYSQL_DATABASE}`.`user`
    ORDER BY `id` DESC
    LIMIT ${LM_MIGRATION_LIMIT}
) u
LEFT JOIN (
    SELECT CAST(ac.`value` AS UNSIGNED) AS `sub_app_id`, ac.`appId` AS `main_app_id`
    FROM `${LM_MYSQL_DATABASE}`.`app_config` ac
    INNER JOIN (
        SELECT CAST(`value` AS UNSIGNED) AS `sub_app_id`, MAX(`id`) AS `max_id`
        FROM `${LM_MYSQL_DATABASE}`.`app_config`
        WHERE `key` = 'coreAppId'
        GROUP BY CAST(`value` AS UNSIGNED)
    ) pick ON pick.`max_id` = ac.`id`
) cam ON cam.`sub_app_id` = u.`appId`
LEFT JOIN `${LM_MYSQL_DATABASE}`.`device_ad_channel` dac
    ON dac.`deviceId` = u.`deviceId`
   AND u.`deviceId` IS NOT NULL AND u.`deviceId` > 0
   AND dac.`id` = (
       SELECT MAX(d2.`id`)
       FROM `${LM_MYSQL_DATABASE}`.`device_ad_channel` d2
       WHERE d2.`deviceId` = u.`deviceId`
   )
ORDER BY u.`id` DESC;
