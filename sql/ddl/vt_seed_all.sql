-- 从源表灌 vt_token_cache 明文（INSERT IGNORE，status=0 待 /v2t）
-- 类型: 1=mobile  2=gaid  3=bank_account  4=id_number(BVN)  5=emergency_contact(紧急联系人手机)
-- 源业务表多为 utf8mb3；勿对 utf8mb3 表达式写 COLLATE utf8mb4_bin（1253）
-- 经 BINARY 中转再 CONVERT utf8mb4，写入 raw_value(utf8mb4_bin) 列
-- 下一步: ./scripts/vt-preload.sh --vt-type all  →  source_all_sync_staging.sql
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_seed_all.sql

SET NAMES utf8mb4;

-- 全量 seed 期间勿触发 vt_token_cache 脏队列入队（rebuild-all-staging 在 preload 后才建 TRIGGER）
DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_au;

-- ---------- 1 mobile（user.mobile）----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 1,
       CONVERT(CAST(norm.mobile_norm AS BINARY(128)) USING utf8mb4),
       0
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
WHERE norm.mobile_norm IS NOT NULL AND norm.mobile_norm <> ''
  AND CHAR_LENGTH(norm.mobile_norm) <= 128;

-- ---------- 2 gaid_idfa ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 2,
       CONVERT(CAST(v.val AS BINARY(128)) USING utf8mb4),
       0
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
) v
WHERE CHAR_LENGTH(v.val) <= 128;

-- ---------- 3 bank_account ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 3,
       CONVERT(CAST(TRIM(b.bank_account) AS BINARY(128)) USING utf8mb4),
       0
FROM user_bank_info b
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL
  AND TRIM(b.bank_account) <> ''
  AND CHAR_LENGTH(TRIM(b.bank_account)) <= 128;

-- ---------- 4 id_number (BVN) ----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 4,
       CONVERT(CAST(TRIM(p.bvn) AS BINARY(128)) USING utf8mb4),
       0
FROM user_personal_info p
WHERE p.bvn IS NOT NULL
  AND TRIM(p.bvn) <> ''
  AND CHAR_LENGTH(TRIM(p.bvn)) <= 128;

-- ---------- 5 emergency_contact（user_emergency_contact.contact_number，+234 规范化）----------
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 5,
       CONVERT(CAST(norm.mobile_norm AS BINARY(128)) USING utf8mb4),
       0
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
WHERE norm.mobile_norm IS NOT NULL AND norm.mobile_norm <> ''
  AND CHAR_LENGTH(norm.mobile_norm) <= 128;

SELECT vt_type, status, COUNT(*) AS cnt
FROM vt_token_cache
GROUP BY vt_type, status
ORDER BY vt_type, status;
