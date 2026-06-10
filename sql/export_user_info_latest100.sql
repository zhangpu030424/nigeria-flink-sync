-- 最新 N 条 user_info 导出（业务逻辑同 export_user_info.sql）
-- 子表 MAX(id) 仅在选中 user 范围内聚合，避免扫千万级全表
-- 验证: mysql -h... -u... -p ng_loan_market < sql/export_user_info_latest100.sql
-- 可调: 改下面 LIMIT 100 中的数字

DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
CREATE TEMPORARY TABLE tmp_u_pick (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE = Memory;

INSERT INTO tmp_u_pick (id)
SELECT id
FROM `user`
ORDER BY id DESC
LIMIT 100;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
CREATE TEMPORARY TABLE tmp_u_keys (
    id       BIGINT NOT NULL PRIMARY KEY,
    `appId`  INT    NOT NULL,
    mobile   VARCHAR(32) NOT NULL,
    `deviceId` BIGINT DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = Memory;

INSERT INTO tmp_u_keys (id, `appId`, mobile, `deviceId`)
SELECT u.id, u.`appId`, u.mobile, u.`deviceId`
FROM `user` u
INNER JOIN tmp_u_pick p ON p.id = u.id;

SELECT
    u.`id` AS `user_id`,
    IFNULL(ud.`bvn`, '') AS `id_number`,
    IFNULL(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), '') AS `full_name`,
    IFNULL(lup.`password`, '') AS `password`,
    CAST(NULL AS CHAR) AS `live_image`,
    CAST(NULL AS CHAR) AS `id_card`,
    JSON_OBJECT(
        'email', ud.`email`,
        'birthday', ud.`birthday`,
        'gender', ud.`gender`,
        'id_card', NULL,
        'live_image', NULL,
        'face_similarity', NULL,
        'address', JSON_OBJECT(
            'province', NULL,
            'city', ud.`addressState`,
            'district', ud.`addressDistrict`,
            'village', NULL,
            'detail', ud.`address`
        ),
        'company', ud.`company`,
        'education', ud.`education`,
        'loan_purpose', NULL,
        'marital', ud.`marital`,
        'job_type', NULL,
        'profession', ud.`profession`,
        'religion', NULL,
        'salary', ud.`salary`,
        'registration_ip', uri.`ip`,
        'registration_time', NULL,
        'children_num', ud.`numberOfChildren`,
        'pay_cycle', ud.`payCycle`,
        'salary_day', ud.`salaryDay`,
        'survey', JSON_OBJECT(
            'survey_loan_cnt', NULL,
            'survey_outstanding_cnt', NULL,
            'survey_overdue_max_days', NULL,
            'survey_overdue_6m', NULL,
            'survey_loan_amt_total', NULL
        ),
        'app', JSON_OBJECT(
            'name', a.`name`,
            'app_id', CAST(u.`appId` AS CHAR),
            'version', NULL
        ),
        'emergency_contacts', ud.`emergencyContact`,
        'install_source', dac.`channel`,
        'credit_limit', NULL
    ) AS `info`
FROM `user` u
INNER JOIN tmp_u_pick p ON p.id = u.id
LEFT JOIN (
    SELECT ud1.*
    FROM `user_data` ud1
    INNER JOIN (
        SELECT `userId`, MAX(`id`) AS `max_id`
        FROM `user_data`
        WHERE `userId` IN (SELECT id FROM tmp_u_pick)
        GROUP BY `userId`
    ) ud_max ON ud_max.`max_id` = ud1.`id`
) ud ON ud.`userId` = u.`id`
LEFT JOIN (
    SELECT l1.`appId`, l1.`mobile`, l1.`password`
    FROM `log_user_password` l1
    INNER JOIN (
        SELECT l.`appId`, l.`mobile`, MAX(l.`id`) AS `max_id`
        FROM `log_user_password` l
        INNER JOIN tmp_u_keys uk
            ON uk.`appId` = l.`appId` AND uk.mobile = l.mobile
        GROUP BY l.`appId`, l.mobile
    ) l_max ON l_max.`max_id` = l1.`id`
) lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN (
    SELECT dac1.`deviceId`, dac1.`channel`
    FROM `device_ad_channel` dac1
    INNER JOIN (
        SELECT `deviceId`, MAX(`id`) AS `max_id`
        FROM `device_ad_channel`
        WHERE `deviceId` IN (
            SELECT `deviceId`
            FROM tmp_u_keys
            WHERE `deviceId` IS NOT NULL
              AND CAST(`deviceId` AS CHAR) <> ''
              AND `deviceId` <> 0
        )
        GROUP BY `deviceId`
    ) dac_max ON dac_max.`max_id` = dac1.`id`
) dac ON dac.`deviceId` = u.`deviceId`
LEFT JOIN (
    SELECT r1.`userId`, r1.`ip`
    FROM `user_registration_ip` r1
    INNER JOIN (
        SELECT `userId`, MAX(`id`) AS `max_id`
        FROM `user_registration_ip`
        WHERE `userId` IN (SELECT id FROM tmp_u_pick)
        GROUP BY `userId`
    ) r_max ON r_max.`max_id` = r1.`id`
) uri ON uri.`userId` = u.`id`
LEFT JOIN `app` a ON a.`id` = u.`appId`
ORDER BY u.`id` DESC;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
