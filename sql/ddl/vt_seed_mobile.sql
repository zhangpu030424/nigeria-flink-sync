-- 从 user 表抽取 DISTINCT 规范化 mobile，写入 vt_token_cache（仅新增，不覆盖已有 token）
-- 规范化规则与 MobileNormalizer / UserSyncFastJob SQL 一致
--
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_seed_mobile.sql

INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'mobile' AS vt_type,
       norm.mobile_norm AS raw_value,
       0 AS status
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
WHERE norm.mobile_norm IS NOT NULL
  AND norm.mobile_norm <> '';

-- 查看待处理数量
SELECT status, COUNT(*) AS cnt
FROM vt_token_cache
WHERE vt_type = 'mobile'
GROUP BY status;
