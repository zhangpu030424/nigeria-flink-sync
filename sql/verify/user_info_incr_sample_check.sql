-- 抽样：脏队列中随机 20 个用户，对比 bundle full_name vs 目标（需能连目标库时改库名或分两次查）
-- 源库执行部分：期望 full_name + sink 是否可写

SELECT b.user_id,
       b.user_id + 100000000 AS tgt_user_id,
       TRIM(CONCAT(COALESCE(b.first_name, ''), ' ', COALESCE(b.sur_name, ''))) AS expected_full_name,
       CASE
           WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN 1
           WHEN b.vt_status = 1 AND b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN 1
           ELSE 0
       END AS sink_eligible_without_udf,
       d.updated_at AS dirty_updated_at
FROM user_info_dirty d
         INNER JOIN user_info_incr_bundle_lookup b ON b.user_id = d.user_id
ORDER BY d.updated_at DESC
LIMIT 20;
