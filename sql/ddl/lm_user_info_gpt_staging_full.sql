-- 全量：从 flink_stg_* 拼 GPT 版 JSON → flink_stg_user_info_ready（不依赖 VIEW）
-- 前置: bash scripts/refresh-lm-user-info-staging.sh
-- 执行: bash scripts/refresh-lm-user-info-gpt-full.sh

DROP TABLE IF EXISTS flink_stg_user_info_ready;
CREATE TABLE flink_stg_user_info_ready (
    user_id_part DECIMAL(20, 0) NOT NULL,
    user_id      VARCHAR(32)     NOT NULL,
    id_number    VARCHAR(64)     NOT NULL DEFAULT '',
    full_name    VARCHAR(512)    NOT NULL DEFAULT '',
    password     VARCHAR(256)    NOT NULL DEFAULT '',
    live_image   VARCHAR(256)    DEFAULT NULL,
    id_card      VARCHAR(256)    DEFAULT NULL,
    info         JSON            NOT NULL,
    KEY idx_user_id_part (user_id_part),
    KEY idx_user_id (user_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

INSERT INTO flink_stg_user_info_ready (
    user_id_part, user_id, id_number, full_name, password, live_image, id_card, info
)
SELECT
    u.id_part,
    u.id,
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    COALESCE(lup.password, ''),
    '',
    '',
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
    )
FROM flink_stg_mkt_user u
LEFT JOIN flink_stg_ud_latest ud ON ud.`userId` = u.id
LEFT JOIN flink_stg_lup_latest lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN flink_stg_dac_latest ldc ON ldc.`deviceId` = u.`deviceId`
LEFT JOIN (
    SELECT CAST(r1.`userId` AS CHAR) AS `userId`, CAST(r1.`ip` AS CHAR) AS ip
    FROM user_registration_ip r1
    INNER JOIN (
        SELECT `userId`, MAX(id) AS max_id
        FROM user_registration_ip
        GROUP BY `userId`
    ) rx ON rx.max_id = r1.id
) uri ON uri.`userId` = u.id
LEFT JOIN (
    SELECT CAST(a.id AS CHAR) AS id, CAST(a.`name` AS CHAR) AS `name`
    FROM `app` a
) app ON app.id = u.`appId`;
