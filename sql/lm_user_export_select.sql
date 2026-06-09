-- generate_user.py 等价查询（仅 SELECT，手动验证用）
-- mysql -h<LM_HOST> -P17033 -u<user> -p ng_loan_market < sql/lm_user_export_select.sql
-- 验证时可加: LIMIT 10  或  WHERE u.id = 123456

SELECT
    u.`id` AS `user_id`,
    u.`appid` AS `app_id`,

    CASE
        WHEN EXISTS (
            SELECT 1
            FROM `ng_loan_market`.`app_config` ac
            WHERE ac.`value` IS NOT NULL
              AND (
                  ac.`value` = CAST(u.`appid` AS CHAR)
                  OR INSTR(ac.`value`, CAST(u.`appid` AS CHAR)) > 0
              )
        ) THEN COALESCE(
            (
                SELECT u2.`id`
                FROM `ng_loan_market`.`user` u2
                WHERE u2.`mobile` = u.`mobile`
                  AND u2.`created` < u.`created`
                ORDER BY u2.`created` ASC, u2.`id` ASC
                LIMIT 1
            ),
            u.`id`
        )
        ELSE u.`id`
    END AS `group_user_id`,

    u.`id` AS `info_user_id`,
    u.`mobile`,
    CASE
        WHEN u.`isCancel` IN (1, '1') THEN UNIX_TIMESTAMP(u.`updated`) * 1000
        ELSE 0
    END AS `closed_time`,
    CAST(u.`deviceId` AS CHAR) AS `reg_device_uuid`,
    UNIX_TIMESTAMP(u.`created`) * 1000 AS `reg_time`,
    0 AS `test_flag`,

    CASE UPPER(dac.`channel`)
        WHEN 'ORGANIC' THEN 'organic'
        WHEN 'FB'      THEN 'facebook'
        WHEN 'TT'      THEN 'tiktok'
        WHEN 'GG'      THEN 'google'
        ELSE NULL
    END AS `utm_source`,

    NULL AS `utm_medium`,
    NULL AS `utm_campaign`,
    NULL AS `utm_content`,
    NULL AS `utm_term`,

    CASE dac.`channel`
        WHEN 'GG' THEN dac.`google_ads_campaign_id`
        WHEN 'FB' THEN dac.`fb_install_referrer_campaign_id`
        ELSE NULL
    END AS `campaign_id`,

    CASE dac.`channel`
        WHEN 'GG' THEN dac.`google_ads_adgroup_id`
        WHEN 'FB' THEN dac.`fb_install_referrer_campaign_group_id`
        ELSE NULL
    END AS `ad_group_id`,

    NULL AS `advertiser_id`

FROM `ng_loan_market`.`user` u

LEFT JOIN (
    SELECT
        d.`deviceId`,
        d.`channel`,
        d.`google_ads_campaign_id`,
        d.`fb_install_referrer_campaign_id`,
        d.`google_ads_adgroup_id`,
        d.`fb_install_referrer_campaign_group_id`
    FROM `ng_loan_market`.`device_ad_channel` d
    INNER JOIN (
        SELECT `deviceId`, MIN(`id`) AS `min_id`
        FROM `ng_loan_market`.`device_ad_channel`
        WHERE `deviceId` IS NOT NULL AND `deviceId` != 0
        GROUP BY `deviceId`
    ) dm ON d.`deviceId` = dm.`deviceId` AND d.`id` = dm.`min_id`
) dac ON u.`deviceId` = dac.`deviceId`

ORDER BY u.`id`;
