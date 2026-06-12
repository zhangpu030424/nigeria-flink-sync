-- 一步完成：建 vt_token_cache + 灌入全部 VT 类型去重明文（status=0 待 /v2t）
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_token_cache_init_all.sql
--
-- 覆盖类型:
--   mobile       ← user.mobile（+234 规范化）
--   gaid_idfa    ← user.gps_adid / user.idfa / device_ids.aaid / device_ids.idfa
--   bank_account ← user_bank_info.bank_account（deleted=0）
--   id_number         ← user_personal_info.bvn
--   emergency_contact ← user_emergency_contact.contact_number（+234 规范化，同 mobile）
--   id2               ← 待业务确认，本脚本暂不灌
--
-- INSERT IGNORE：可重复执行，不覆盖已有 (vt_type, raw_value)

CREATE TABLE IF NOT EXISTS vt_token_cache (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    vt_type      ENUM('mobile','gaid_idfa','bank_account','id_number','emergency_contact','id2') NOT NULL,
    raw_value    VARCHAR(128) NOT NULL COMMENT '源明文（mobile 为规范化后）',
    token        VARCHAR(128) NULL,
    masking      VARCHAR(128) NULL,
    status       TINYINT      NOT NULL DEFAULT 0 COMMENT '0待VT 1成功 2失败',
    retry_count  INT          NOT NULL DEFAULT 0,
    last_error   VARCHAR(512) NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_type_raw (vt_type, raw_value),
    KEY idx_status (status, vt_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='VT 去重字典';

-- ---------- mobile ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'mobile', norm.mobile_norm, 0
FROM (
    SELECT DISTINCT
        CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END AS mobile_norm
    FROM `user` u
) norm
WHERE norm.mobile_norm IS NOT NULL AND norm.mobile_norm <> '';

-- ---------- gaid_idfa（多源 UNION 去重）----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'gaid_idfa', v.val, 0
FROM (
    SELECT DISTINCT TRIM(u.gps_adid) AS val FROM `user` u
    WHERE u.gps_adid IS NOT NULL AND TRIM(u.gps_adid) <> ''
    UNION
    SELECT DISTINCT TRIM(u.idfa) FROM `user` u
    WHERE u.idfa IS NOT NULL AND TRIM(u.idfa) <> ''
    UNION
    SELECT DISTINCT TRIM(d.aaid) FROM device_ids d
    WHERE d.aaid IS NOT NULL AND TRIM(d.aaid) <> ''
    UNION
    SELECT DISTINCT TRIM(d.idfa) FROM device_ids d
    WHERE d.idfa IS NOT NULL AND TRIM(d.idfa) <> ''
) v;

-- ---------- bank_account ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'bank_account', TRIM(b.bank_account), 0
FROM user_bank_info b
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL
  AND TRIM(b.bank_account) <> '';

-- ---------- id_number (BVN) ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'id_number', TRIM(p.bvn), 0
FROM user_personal_info p
WHERE p.bvn IS NOT NULL
  AND TRIM(p.bvn) <> '';

-- ---------- emergency_contact ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'emergency_contact', norm.mobile_norm, 0
FROM (
    SELECT DISTINCT
        CASE
            WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = '' THEN NULL
            WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
            WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
            WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
            ELSE CONCAT('+234', TRIM(ec.contact_number))
        END AS mobile_norm
    FROM user_emergency_contact ec
) norm
WHERE norm.mobile_norm IS NOT NULL AND norm.mobile_norm <> '';

-- ---------- 统计 ----------
SELECT 'vt_token_cache 全类型初始化完成' AS msg;
SELECT vt_type, status, COUNT(*) AS cnt
FROM vt_token_cache
GROUP BY vt_type, status
ORDER BY vt_type, status;
