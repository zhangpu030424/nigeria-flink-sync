-- 全量 GPT user_info：MySQL 侧拼好 JSON（VIEW，非物化表）
-- 依赖: v_flink_mkt_user / v_flink_ud_latest / v_flink_lup_latest / v_flink_dac_latest
--       v_flink_uri_latest / v_flink_mkt_app（可选，缺表时由 setup 脚本 strip）
-- 主库执行后等同步到从库，Flink 单表 JDBC 读此 VIEW

CREATE OR REPLACE VIEW v_flink_gpt_user_info_sink AS
SELECT
    u.id                                              AS user_id,
    COALESCE(ud.bvn, '')                              AS id_number,
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), '') AS full_name,
    COALESCE(lup.password, '')                        AS password,
    CAST('' AS CHAR)                                  AS live_image,
    CAST('' AS CHAR)                                  AS id_card,
    JSON_OBJECT(
        'email', ud.email,
        'birthday', ud.birthday,
        'gender', ud.gender,
        'id_card', NULL,
        'live_image', NULL,
        'face_similarity', NULL,
        'address', JSON_OBJECT(
            'province', NULL,
            'city', ud.`addressState`,
            'district', ud.`addressDistrict`,
            'village', NULL,
            'detail', ud.address
        ),
        'company', ud.company,
        'education', ud.education,
        'loan_purpose', NULL,
        'marital', ud.marital,
        'job_type', NULL,
        'profession', ud.profession,
        'religion', NULL,
        'salary', ud.salary,
        'registration_ip', uri.ip,
        'registration_time', UNIX_TIMESTAMP(u.created) * 1000,
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
            'name', app.`name`,
            'app_id', u.`appId`,
            'version', NULL
        ),
        'emergency_contacts', ud.`emergencyContact`,
        'install_source', ldc.channel,
        'credit_limit', NULL
    )                                                 AS info
FROM v_flink_mkt_user u
LEFT JOIN v_flink_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN v_flink_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN v_flink_dac_latest ldc ON ldc.`deviceId` = u.`deviceId`
LEFT JOIN v_flink_uri_latest uri ON uri.`userId` = u.id
LEFT JOIN v_flink_mkt_app app ON app.id = u.`appId`
WHERE u.id IS NOT NULL AND TRIM(u.id) <> '';
