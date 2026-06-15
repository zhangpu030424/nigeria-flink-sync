-- 诊断：vt_token_cache / 源表是否存在超长 raw_value（>128）
-- 用法: mysql -h ... -u ... -p nigeria_backend < sql/verify/vt_raw_value_length_diag.sql

-- 1) cache 里是否已有超长（严格 VARCHAR(128) 下正常应为 0）
SELECT 'vt_token_cache 超长' AS check_name,
       vt_type,
       status,
       COUNT(*) AS cnt,
       MAX(CHAR_LENGTH(raw_value)) AS max_chars,
       MAX(LENGTH(raw_value)) AS max_bytes
FROM vt_token_cache
WHERE raw_value IS NOT NULL
  AND CHAR_LENGTH(raw_value) > 128
GROUP BY vt_type, status;

-- 2) 源表 user.mobile 规范化后超长（mobile vt_type=1）
SELECT 'user.mobile 规范化后>128' AS check_name,
       COUNT(*) AS cnt,
       MAX(CHAR_LENGTH(mobile_norm)) AS max_chars
FROM (
    SELECT
        CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END AS mobile_norm
    FROM `user` u
) t
WHERE mobile_norm IS NOT NULL
  AND CHAR_LENGTH(mobile_norm) > 128;

-- 3) 样例：最长的 10 条 mobile（看是否真像手机号）
SELECT 'user.mobile 样例 TOP10' AS check_name,
       u.id,
       CHAR_LENGTH(u.mobile) AS src_chars,
       LEFT(u.mobile, 80) AS mobile_preview,
       CASE
           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
           ELSE CONCAT('+234', TRIM(u.mobile))
       END AS mobile_norm,
       CHAR_LENGTH(
           CASE
               WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
               WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
               WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
               WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
               ELSE CONCAT('+234', TRIM(u.mobile))
           END
       ) AS norm_chars
FROM `user` u
WHERE u.mobile IS NOT NULL AND TRIM(u.mobile) <> ''
ORDER BY norm_chars DESC
LIMIT 10;

-- 4) cache 待 VT 的 mobile 最长几条
SELECT 'cache pending mobile TOP10' AS check_name,
       id,
       CHAR_LENGTH(raw_value) AS chars,
       LEFT(raw_value, 80) AS raw_preview
FROM vt_token_cache
WHERE vt_type = 1 AND status = 0
ORDER BY CHAR_LENGTH(raw_value) DESC
LIMIT 10;

-- 5) 列定义确认
SHOW CREATE TABLE vt_token_cache\G
