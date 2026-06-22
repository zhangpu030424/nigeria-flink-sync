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

-- 6) status=0 但含换行/制表符（mysql -B 读会拆行，preload 曾认领 0 条）
SELECT 'cache status=0 含换行或制表符' AS check_name,
       vt_type,
       COUNT(*) AS cnt
FROM vt_token_cache
WHERE status = 0
  AND (raw_value LIKE CONCAT('%', CHAR(10), '%') OR raw_value LIKE CONCAT('%', CHAR(9), '%'))
GROUP BY vt_type;

-- 7) 对比：status=0 总数 vs 可认领（<=128 且无空）
SELECT vt_type,
       SUM(status = 0) AS pending_all,
       SUM(status = 0 AND raw_value IS NOT NULL AND TRIM(raw_value) <> ''
           AND CHAR_LENGTH(raw_value) <= 128) AS pending_claimable
FROM vt_token_cache
GROUP BY vt_type;

-- 8) raw_value 含 \\0 填充（历史 CAST AS BINARY(128) 脏数据）
SELECT 'cache 含 NUL 填充' AS check_name,
       vt_type,
       COUNT(*) AS cnt
FROM vt_token_cache
WHERE LOCATE(CHAR(0), raw_value) > 0
GROUP BY vt_type;

-- 9) 同 vt_type 下去 NUL 后重复（GUI 看起来一样、uk 却有两条）
SELECT vt_type,
       REPLACE(raw_value, CHAR(0), '') AS raw_clean,
       COUNT(*) AS dup_cnt,
       GROUP_CONCAT(id ORDER BY id) AS ids,
       GROUP_CONCAT(status ORDER BY id) AS statuses
FROM vt_token_cache
GROUP BY vt_type, raw_clean
HAVING dup_cnt > 1
ORDER BY dup_cnt DESC
LIMIT 20;

-- 8) raw_value 含 \\0 填充（历史 CAST AS BINARY(128) 脏数据）
SELECT 'cache 含 NUL 填充' AS check_name,
       vt_type,
       COUNT(*) AS cnt
FROM vt_token_cache
WHERE LOCATE(CHAR(0), raw_value) > 0
GROUP BY vt_type;

-- 9) 同 vt_type 下去 NUL 后重复（GUI 看起来一样、uk 却有两条）
SELECT vt_type,
       REPLACE(raw_value, CHAR(0), '') AS raw_clean,
       COUNT(*) AS dup_cnt,
       GROUP_CONCAT(id ORDER BY id) AS ids,
       GROUP_CONCAT(status ORDER BY id) AS statuses
FROM vt_token_cache
GROUP BY vt_type, raw_clean
HAVING dup_cnt > 1
ORDER BY dup_cnt DESC
LIMIT 20;
