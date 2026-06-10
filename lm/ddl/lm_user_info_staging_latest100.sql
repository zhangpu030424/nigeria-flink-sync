-- 老库：export_user_info_latest100 逻辑落地 flink_stg_user_info_ready
-- 执行: LM_PICK_N=100 bash lm/scripts/refresh-lm-user-info-latest100.sh
-- 勿直接 mysql < 本文件；须经 refresh 脚本 envsubst 替换 ${LM_PICK_N}

DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
CREATE TEMPORARY TABLE tmp_u_pick (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE = Memory;

INSERT INTO tmp_u_pick (id)
SELECT id FROM `user` ORDER BY id DESC LIMIT ${LM_PICK_N};

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
CREATE TEMPORARY TABLE tmp_u_keys (
    id         BIGINT NOT NULL PRIMARY KEY,
    `appId`    BIGINT NOT NULL,
    mobile     VARCHAR(32) NOT NULL,
    `deviceId` BIGINT DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = Memory;

INSERT INTO tmp_u_keys (id, `appId`, mobile, `deviceId`)
SELECT u.id, u.`appId`, u.mobile, u.`deviceId`
FROM `user` u
INNER JOIN tmp_u_pick p ON p.id = u.id;

DROP TEMPORARY TABLE IF EXISTS tmp_u_pick2;
CREATE TEMPORARY TABLE tmp_u_pick2 (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE = Memory;
INSERT INTO tmp_u_pick2 SELECT id FROM tmp_u_pick;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys2;
CREATE TEMPORARY TABLE tmp_u_keys2 (
    id         BIGINT NOT NULL PRIMARY KEY,
    `appId`    BIGINT NOT NULL,
    mobile     VARCHAR(32) NOT NULL,
    `deviceId` BIGINT DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = Memory;
INSERT INTO tmp_u_keys2 SELECT id, `appId`, mobile, `deviceId` FROM tmp_u_keys;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys3;
CREATE TEMPORARY TABLE tmp_u_keys3 (
    id         BIGINT NOT NULL PRIMARY KEY,
    `appId`    BIGINT NOT NULL,
    mobile     VARCHAR(32) NOT NULL,
    `deviceId` BIGINT DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = Memory;
INSERT INTO tmp_u_keys3 SELECT id, `appId`, mobile, `deviceId` FROM tmp_u_keys;

DROP TABLE IF EXISTS flink_stg_user_info_ready;
CREATE TABLE flink_stg_user_info_ready (
    user_id_part BIGINT NOT NULL,
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
    u.id,
    CAST(u.id AS CHAR),
    IFNULL(ud.bvn, ''),
    IFNULL(TRIM(CONCAT_WS(' ', ud.`firstName`, ud.`middleName`, ud.`lastName`)), ''),
    IFNULL(lup.password, ''),
    NULL,
    NULL,
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
    )
FROM `user` u
INNER JOIN tmp_u_keys k ON k.id = u.id
LEFT JOIN (
    SELECT ud1.*
    FROM user_data ud1
    INNER JOIN (
        SELECT ud.`userId`, MAX(ud.id) AS max_id
        FROM user_data ud
        INNER JOIN tmp_u_pick pick ON pick.id = ud.`userId`
        GROUP BY ud.`userId`
    ) ud_max ON ud_max.max_id = ud1.id
) ud ON ud.`userId` = u.id
LEFT JOIN (
    SELECT l1.`appId`, l1.mobile, l1.password
    FROM log_user_password l1
    INNER JOIN (
        SELECT l.`appId`, l.mobile, MAX(l.id) AS max_id
        FROM log_user_password l
        INNER JOIN tmp_u_keys2 uk ON uk.`appId` = l.`appId` AND uk.mobile = l.mobile
        GROUP BY l.`appId`, l.mobile
    ) l_max ON l_max.max_id = l1.id
) lup ON lup.`appId` = u.`appId` AND lup.mobile = u.mobile
LEFT JOIN (
    SELECT dac1.`deviceId`, dac1.channel
    FROM device_ad_channel dac1
    INNER JOIN (
        SELECT dac.`deviceId`, MAX(dac.id) AS max_id
        FROM device_ad_channel dac
        INNER JOIN tmp_u_keys3 uk3
            ON uk3.`deviceId` = dac.`deviceId`
           AND uk3.`deviceId` IS NOT NULL
           AND CAST(uk3.`deviceId` AS CHAR) <> ''
           AND uk3.`deviceId` <> 0
        GROUP BY dac.`deviceId`
    ) dac_max ON dac_max.max_id = dac1.id
) dac ON dac.`deviceId` = u.`deviceId`
LEFT JOIN (
    SELECT r1.`userId`, r1.`ip`
    FROM user_registration_ip r1
    INNER JOIN (
        SELECT r.`userId`, MAX(r.id) AS max_id
        FROM user_registration_ip r
        INNER JOIN tmp_u_pick2 pick ON pick.id = r.`userId`
        GROUP BY r.`userId`
    ) r_max ON r_max.max_id = r1.id
) uri ON uri.`userId` = u.`id`
LEFT JOIN `app` a ON a.id = u.`appId`;

DROP TEMPORARY TABLE IF EXISTS tmp_u_keys3;
DROP TEMPORARY TABLE IF EXISTS tmp_u_keys2;
DROP TEMPORARY TABLE IF EXISTS tmp_u_pick2;
DROP TEMPORARY TABLE IF EXISTS tmp_u_keys;
DROP TEMPORARY TABLE IF EXISTS tmp_u_pick;
