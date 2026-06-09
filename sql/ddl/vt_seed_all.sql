-- 增量：从源表补灌 vt_token_cache（INSERT IGNORE，只增新值）
-- 建议 cron：先本脚本 → vt-preload.sh --vt-type all → 刷新各宽表
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_seed_all.sql

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

INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'bank_account', TRIM(b.bank_account), 0
FROM user_bank_info b
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL
  AND TRIM(b.bank_account) <> '';

INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'id_number', TRIM(p.bvn), 0
FROM user_personal_info p
WHERE p.bvn IS NOT NULL
  AND TRIM(p.bvn) <> '';

SELECT vt_type, status, COUNT(*) AS cnt
FROM vt_token_cache
GROUP BY vt_type, status
ORDER BY vt_type, status;
