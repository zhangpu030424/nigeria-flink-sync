-- application 索引优化版校验（MySQL 直跑，对照 Flink lm/sql/04_sync_ng_application_opt.sql）

SELECT
    a.`applicationNo` AS `application_no`,
    CASE
        WHEN a.`mobile` LIKE '+234%' THEN a.`mobile`
        WHEN a.`mobile` LIKE '234%'  THEN CONCAT('+', a.`mobile`)
        WHEN a.`mobile` LIKE '0%'    THEN CONCAT('+234', SUBSTRING(a.`mobile`, 2))
        ELSE CONCAT('+234', a.`mobile`)
    END AS `mobile`,
    'ng01' AS `bid`,
    a.`appId` AS `app_id`,
    '1.0.0' AS `app_version`,
    a.`userId` AS `user_id`,
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
        a.`userId`
    ) AS `group_user_id`,
    a.`applicationNo` AS `sn`,
    0 AS `is_test`,
    CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS `is_first_apply`,
    0 AS `is_auto_apply`,
    IFNULL(ud.`bvn`, '') AS `id_number`,
    IFNULL(a.`gaid`, '') AS `gaid_idfa`,
    IFNULL(d.`deviceUUID`, '') AS `device_uuid`,
    NULL AS `session_id`,
    IFNULL(a.`bankCode`, '') AS `bank_code`,
    NULL AS `bank_account_name`,
    IFNULL(a.`bankAccount`, '') AS `bank_account_number`,
    CAST(a.`productId` AS CHAR) AS `product_id`,
    'PROD-002-D7' AS `product_scheme_id`,
    '1' AS `product_calculator_version`,
    JSON_OBJECT('penalty_rate', 0.05, 'upfront_rate', 0.35, 'interest_rate', 0, 'post_paid_rate', 0.05) AS `product_scheme_param`,
    a.`term` AS `term`, 1 AS `periods`, 1 AS `repayment_method`,
    JSON_OBJECT(
        'roll_sequence', 0, 'period', 1,
        'principal', a.`shouldLoanAmount`, 'disbursed_amount', a.`disburseAmount`,
        'interest', 0, 'admin_fee', GREATEST(a.`amount` - a.`shouldLoanAmount`, 0),
        'service_fee', 0, 'tax_fee', 0, 'reduction_amount', 0,
        'total_amount', a.`repayment`, 'term', a.`term`,
        'start_date', DATE(FROM_UNIXTIME(a.`applyDate`)),
        'due_date', DATE(FROM_UNIXTIME(a.`dueDate`)),
        'roll_allowed', 0
    ) AS `repayment_plan`,
    a.`amount` AS `credit_limit`, a.`amount` AS `loan_amount`,
    a.`shouldLoanAmount` AS `principal`, a.`repayment` AS `total_amount`, a.`disburseAmount` AS `disbursed_amount`,
    a.`applyDate` * 1000 AS `created_time`,
    IFNULL(ca.`apply_time`, 0) * 1000 AS `submited_time`,
    IFNULL(ca.`audit_time`, 0) * 1000 AS `reviewed_time`,
    a.`disburseTime` * 1000 AS `disbursed_time`,
    IFNULL(lpt.`last_paid_time`, 0) * 1000 AS `last_paid_time`,
    a.`paidTime` * 1000 AS `paid_off_time`,
    (a.`applyDate` + 7 * 86400) * 1000 AS `lock_expire_time`,
    DATE(FROM_UNIXTIME(a.`dueDate`)) AS `due_date`,
    DATE(FROM_UNIXTIME(a.`dueDate`)) AS `due_date_final`,
    CASE a.`status`
        WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 3
        WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7 WHEN 7 THEN 11 WHEN 9 THEN 13
        WHEN 12 THEN 15 WHEN 13 THEN 20 WHEN 14 THEN 20
        WHEN 15 THEN 23 WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
        ELSE a.`status`
    END AS `status`
FROM (
    SELECT *
    FROM `${LM_MYSQL_DATABASE}`.`application`
    WHERE `applicationNo` IS NOT NULL AND `applicationNo` <> ''
    ORDER BY `id` DESC
    LIMIT ${LM_MIGRATION_LIMIT}
) a
INNER JOIN `${LM_MYSQL_DATABASE}`.`user` u ON u.`id` = a.`userId`
LEFT JOIN (
    SELECT CAST(ac.`value` AS UNSIGNED) AS `sub_app_id`, ac.`appId` AS `main_app_id`
    FROM `${LM_MYSQL_DATABASE}`.`app_config` ac
    INNER JOIN (
        SELECT CAST(`value` AS UNSIGNED) AS `sub_app_id`, MAX(`id`) AS `max_id`
        FROM `${LM_MYSQL_DATABASE}`.`app_config` WHERE `key` = 'coreAppId'
        GROUP BY CAST(`value` AS UNSIGNED)
    ) pick ON pick.`max_id` = ac.`id`
) cam ON cam.`sub_app_id` = u.`appId`
LEFT JOIN `${LM_MYSQL_DATABASE}`.`user_data` ud
    ON ud.`userId` = a.`userId`
   AND ud.`id` = (
       SELECT MAX(ud2.`id`) FROM `${LM_MYSQL_DATABASE}`.`user_data` ud2 WHERE ud2.`userId` = a.`userId`
   )
LEFT JOIN `${LM_MYSQL_DATABASE}`.`device` d ON d.`id` = a.`deviceId`
LEFT JOIN `${LM_CORE_MYSQL_DATABASE}`.`application` ca
    ON ca.`ext_sn` = a.`applicationNo`
LEFT JOIN (
    SELECT ca2.`ext_sn`, MAX(rr.`repay_time`) AS `last_paid_time`
    FROM `${LM_CORE_MYSQL_DATABASE}`.`application` ca2
    INNER JOIN `${LM_CORE_MYSQL_DATABASE}`.`repay_record` rr ON rr.`sn` = ca2.`sn`
    INNER JOIN (
        SELECT `applicationNo` FROM `${LM_MYSQL_DATABASE}`.`application`
        WHERE `applicationNo` IS NOT NULL AND `applicationNo` <> ''
        ORDER BY `id` DESC LIMIT ${LM_MIGRATION_LIMIT}
    ) a20 ON a20.`applicationNo` = ca2.`ext_sn`
    GROUP BY ca2.`ext_sn`
) lpt ON lpt.`ext_sn` = a.`applicationNo`
ORDER BY a.`id` DESC;
