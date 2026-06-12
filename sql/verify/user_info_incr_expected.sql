-- 单用户：增量 sink 期望 vs 目标（源库 user_id 未加偏移）
-- 用法: mysql ... -e "SET @uid=211038;" < sql/verify/user_info_incr_expected.sql
-- 或:   mysql ... --init-command="SET @uid=211038" < sql/verify/user_info_incr_expected.sql

SET @tgt_uid := @uid + 100000000;

SELECT @uid AS src_user_id, @tgt_uid AS tgt_user_id;

-- bundle Lookup（增量 Flink 组装的数据源）
SELECT 'bundle_lookup' AS layer,
       b.user_id,
       TRIM(b.bvn) AS bvn_raw,
       b.vt_token,
       b.vt_status,
       TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))) AS expected_full_name,
       CASE
           WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN 'yes_no_bvn'
           WHEN b.vt_status = 1 AND b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN 'yes_has_token'
           ELSE 'needs_vt_tokenize_or_miss'
       END AS sink_filter_hint
FROM user_info_incr_bundle_lookup b
WHERE b.user_id = @uid;

-- 脏队列是否在等待 CDC
SELECT 'dirty_queue' AS layer,
       d.user_id,
       d.updated_at,
       CASE WHEN d.user_id IS NULL THEN 'not_queued' ELSE 'queued' END AS status
FROM (SELECT @uid AS user_id) x
LEFT JOIN user_info_dirty d ON d.user_id = x.user_id;

-- 全量宽表对照（若 staging 已刷新，应与 bundle 字段一致）
SELECT 'staging_vs_bundle' AS layer,
       s.user_id,
       s.full_name AS staging_full_name,
       TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))) AS bundle_full_name,
       CASE
           WHEN s.user_id IS NULL THEN 'no_staging_row'
           WHEN TRIM(COALESCE(s.full_name, '')) = TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, '')))
               THEN 'match'
           ELSE 'mismatch'
       END AS full_name_check
FROM user_info_incr_bundle_lookup b
LEFT JOIN user_info_sync_staging s ON s.user_id = b.user_id
WHERE b.user_id = @uid;
