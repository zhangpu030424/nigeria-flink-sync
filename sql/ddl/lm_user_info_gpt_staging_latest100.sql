-- GPT 版 sink_user_info（最新 N 条 user，子表 MAX(id) 仅在选中范围内聚合）
-- 来源: GPT版本SQL.md — dwd_latest_* + m_app + registration_time
-- 执行: LM_PICK_N=100 bash scripts/refresh-lm-user-info-gpt-latest100.sh

SET @pick_n := ${LM_PICK_N};

DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
CREATE TEMPORARY TABLE tmp_u_pick (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE = Memory;

INSERT INTO tmp_u_pick (id)
SELECT id FROM `user` ORDER BY id DESC LIMIT @pick_n;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
CREATE TEMPORARY TABLE tmp_u_keys (
    id         BIGINT NOT NULL PRIMARY KEY,
    `appId`    INT    NOT NULL,
    mobile     VARCHAR(32) NOT NULL,
    `deviceId` BIGINT DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = Memory;

INSERT INTO tmp_u_keys (id, `appId`, mobile, `deviceId`)
SELECT u.id, u.`appId`, u.mobile, u.`deviceId`
FROM `user` u
INNER JOIN tmp_u_pick p ON p.id = u.id;

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
    CAST(u.id AS DECIMAL(20, 0)),
    CAST(u.id AS CHAR),
    COALESCE(ud.bvn, ''),
    COALESCE(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    COALESCE(lup.password, ''),
    '',
    '',
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
            'app_id', CAST(u.`appId` AS CHAR),
            'version', NULL
        ),
        'emergency_contacts', ud.`emergencyContact`,
        'install_source', ldc.`channel`,
        'credit_limit', NULL
    )
FROM `user` u
INNER JOIN tmp_u_pick p ON p.id = u.id
LEFT JOIN (
    SELECT ud1.*
    FROM user_data ud1
    INNER JOIN (
        SELECT `userId`, MAX(id) AS max_id
        FROM user_data
        WHERE `userId` IN (SELECT id FROM tmp_u_pick)
        GROUP BY `userId`
    ) x ON x.max_id = ud1.id
) ud ON ud.`userId` = u.id
LEFT JOIN (
    SELECT l1.`appId`, l1.mobile, l1.password
    FROM log_user_password l1
    INNER JOIN (
        SELECT l.`appId`, l.mobile, MAX(l.id) AS max_id
        FROM log_user_password l
        INNER JOIN tmp_u_keys uk ON uk.`appId` = l.`appId` AND uk.mobile = l.mobile
        GROUP BY l.`appId`, l.mobile
    ) x ON x.max_id = l1.id
) lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN (
    SELECT r1.`userId`, r1.`ip`
    FROM user_registration_ip r1
    INNER JOIN (
        SELECT `userId`, MAX(id) AS max_id
        FROM user_registration_ip
        WHERE `userId` IN (SELECT id FROM tmp_u_pick)
        GROUP BY `userId`
    ) x ON x.max_id = r1.id
) uri ON uri.`userId` = u.id
LEFT JOIN `app` app ON app.id = u.`appId`
LEFT JOIN (
    SELECT dac1.`deviceId`, dac1.channel
    FROM device_ad_channel dac1
    INNER JOIN (
        SELECT `deviceId`, MAX(id) AS max_id
        FROM device_ad_channel
        WHERE `deviceId` IN (
            SELECT `deviceId` FROM tmp_u_keys
            WHERE `deviceId` IS NOT NULL AND CAST(`deviceId` AS CHAR) <> '' AND `deviceId` <> 0
        )
        GROUP BY `deviceId`
    ) x ON x.max_id = dac1.id
) ldc ON ldc.`deviceId` = u.`deviceId`;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
