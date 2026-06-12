-- 灌 emergency_contact 明文（vt_type=5）；表须已为 TINYINT，见 vt_token_cache_rebuild.sql
INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 5, norm.mobile_norm, 0
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

SELECT vt_type, status, COUNT(*) AS cnt
FROM vt_token_cache
WHERE vt_type = 5
GROUP BY vt_type, status;
