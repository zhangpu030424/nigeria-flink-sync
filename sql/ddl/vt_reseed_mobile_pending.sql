-- delete_insert 失败导致数据丢失后：从 user 重新灌 mobile 待 VT 明文（INSERT IGNORE）
-- mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_reseed_mobile_pending.sql

INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 1, norm.mobile_norm, 0
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

SELECT status, COUNT(*) AS cnt
FROM vt_token_cache WHERE vt_type = 1 GROUP BY status;
