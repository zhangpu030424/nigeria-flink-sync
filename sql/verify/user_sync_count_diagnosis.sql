-- user 宽表 vs 目标库 分项诊断（在源库 nigeria_backend 执行）
-- 目标 user_id = 源 id + 100000000
-- 目标表主键 (mobile_token, app_id, closed_time)：同 token+app 多 id 合并 1 行。
-- sync-jobs.conf / sync-job-auto 已按 DISTINCT(token,app) 去重计数，与目标 COUNT(*) 对齐。

-- 1) 基础计数（与 sync-jobs.conf 口径一致）
SELECT 'staging_mobile_norm' AS metric, COUNT(*) AS cnt
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> '';

SELECT 'staging_has_token' AS metric, COUNT(*) AS cnt
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
  AND mobile_token IS NOT NULL AND TRIM(mobile_token) <> '';

SELECT 'staging_need_vt' AS metric, COUNT(*) AS cnt
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
  AND (mobile_token IS NULL OR TRIM(mobile_token) = '');

-- 2) 按目标主键去重后的「理论最大行数」（有 token 部分）
SELECT 'staging_distinct_pk_has_token' AS metric, COUNT(*) AS cnt
FROM (
    SELECT DISTINCT mobile_token, app_code
    FROM user_sync_staging
    WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
      AND mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
) d;

-- 3) 同一 token+app 对应多个源用户（主键冲突，后写覆盖先写）
SELECT 'duplicate_token_app_groups' AS metric, COUNT(*) AS cnt
FROM (
    SELECT mobile_token, app_code
    FROM user_sync_staging
    WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
    GROUP BY mobile_token, app_code
    HAVING COUNT(*) > 1
) x;

SELECT 'duplicate_token_app_extra_rows' AS metric, COALESCE(SUM(c - 1), 0) AS cnt
FROM (
    SELECT COUNT(*) AS c
    FROM user_sync_staging
    WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
    GROUP BY mobile_token, app_code
    HAVING COUNT(*) > 1
) t;

-- 4) 样例：同 token+app 多 id（前 20 组）
SELECT mobile_token, app_code, COUNT(*) AS user_cnt, GROUP_CONCAT(id ORDER BY id LIMIT 10) AS sample_ids
FROM user_sync_staging
WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
GROUP BY mobile_token, app_code
HAVING COUNT(*) > 1
ORDER BY user_cnt DESC
LIMIT 20;

-- 5) 若目标库在同一 MySQL 实例，取消下面注释并改库名后执行：
-- 按 user_id 对齐（比 COUNT(*) 更能反映「每个源用户是否都有目标行」）
/*
SELECT 'target_total' AS metric, COUNT(*) AS cnt
FROM platform_db.`user`;

SELECT 'target_matched_by_user_id' AS metric, COUNT(*) AS cnt
FROM platform_db.`user` t
INNER JOIN user_sync_staging s ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> '';

SELECT 'staging_missing_in_target_by_user_id' AS metric, COUNT(*) AS cnt
FROM user_sync_staging s
LEFT JOIN platform_db.`user` t ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> ''
  AND t.user_id IS NULL;
*/
