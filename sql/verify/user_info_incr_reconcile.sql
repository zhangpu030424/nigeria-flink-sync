-- user_info 全量/增量对账（源库执行；目标 user_id = 源 user_id + 100000000）
-- 用法: mysql ... nigeria_backend < sql/verify/user_info_incr_reconcile.sql
-- 或配合 scripts/verify-user-info-reconcile.sh（跨源/目标库）

SELECT '=== 1. 行数 ===' AS section;
SELECT (SELECT COUNT(*) FROM user_info_sync_staging) AS staging_cnt;
SELECT (SELECT COUNT(*) FROM user_info_dirty) AS dirty_cnt;

SELECT '=== 2. 组装一致性 staging vs bundle（应全 match）===' AS section;
SELECT
    CASE
        WHEN mismatch_cnt = 0 THEN 'PASS'
        ELSE CONCAT('FAIL mismatch=', mismatch_cnt)
    END AS staging_bundle_check
FROM (
    SELECT COUNT(*) AS mismatch_cnt
    FROM user_info_sync_staging s
             INNER JOIN user_info_incr_bundle_lookup b ON b.user_id = s.user_id
    WHERE TRIM(COALESCE(s.full_name, '')) <> TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, '')))
) t;

SELECT '=== 3. sink 会被过滤的用户（有 BVN 无 token，增量/全量都不写）===' AS section;
SELECT COUNT(*) AS sink_filtered_cnt
FROM user_info_incr_bundle_lookup b
WHERE b.bvn IS NOT NULL AND TRIM(b.bvn) <> ''
  AND (b.vt_token IS NULL OR TRIM(b.vt_token) = '');

SELECT '=== 4. 脏队列抽样：bundle 期望 vs staging（前 20）===' AS section;
SELECT d.user_id,
       d.updated_at AS dirty_at,
       TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))) AS bundle_full_name,
       s.full_name AS staging_full_name,
       CASE
           WHEN s.user_id IS NULL THEN 'no_staging'
           WHEN TRIM(COALESCE(s.full_name, '')) = TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, '')))
               THEN 'match'
           ELSE 'mismatch'
       END AS chk,
       CASE
           WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN 'ok_no_bvn'
           WHEN b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN 'ok_token'
           ELSE 'sink_filtered'
       END AS sink_hint
FROM user_info_dirty d
         INNER JOIN user_info_incr_bundle_lookup b ON b.user_id = d.user_id
         LEFT JOIN user_info_sync_staging s ON s.user_id = d.user_id
ORDER BY d.updated_at DESC
LIMIT 20;
