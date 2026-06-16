-- user_info 脏队列分片完整性检查（源库执行）
-- 用法: mysql -h ... -u ... nigeria_backend < sql/verify/user_info_dirty_shards_check.sql

SELECT '=== 1. 对象类型 ===' AS section;

SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = DATABASE()
  AND (table_name = 'user_info_dirty'
    OR table_name REGEXP '^user_info_dirty_[0-3]$')
ORDER BY table_name;

SELECT '=== 2. 分片行数 vs 视图 ===' AS section;

SELECT
    (SELECT COUNT(*) FROM user_info_dirty_0) AS shard_0,
    (SELECT COUNT(*) FROM user_info_dirty_1) AS shard_1,
    (SELECT COUNT(*) FROM user_info_dirty_2) AS shard_2,
    (SELECT COUNT(*) FROM user_info_dirty_3) AS shard_3,
    (SELECT COUNT(*) FROM user_info_dirty_0)
        + (SELECT COUNT(*) FROM user_info_dirty_1)
        + (SELECT COUNT(*) FROM user_info_dirty_2)
        + (SELECT COUNT(*) FROM user_info_dirty_3) AS shard_sum,
    (SELECT COUNT(*) FROM user_info_dirty) AS view_cnt,
    CASE
        WHEN (SELECT COUNT(*) FROM user_info_dirty_0)
                 + (SELECT COUNT(*) FROM user_info_dirty_1)
                 + (SELECT COUNT(*) FROM user_info_dirty_2)
                 + (SELECT COUNT(*) FROM user_info_dirty_3)
             = (SELECT COUNT(*) FROM user_info_dirty)
            THEN 'PASS'
        ELSE 'FAIL'
    END AS view_sum_check;

SELECT '=== 3. 错分片行数（user_id % 4 与表后缀不一致，应全 0）===' AS section;

SELECT wrong_shard_rows FROM (
    SELECT (
        (SELECT COUNT(*) FROM user_info_dirty_0 WHERE MOD(user_id, 4) <> 0)
            + (SELECT COUNT(*) FROM user_info_dirty_1 WHERE MOD(user_id, 4) <> 1)
            + (SELECT COUNT(*) FROM user_info_dirty_2 WHERE MOD(user_id, 4) <> 2)
            + (SELECT COUNT(*) FROM user_info_dirty_3 WHERE MOD(user_id, 4) <> 3)
    ) AS wrong_shard_rows
) t;

SELECT '=== 4. 跨分片重复 user_id（应无）===' AS section;

SELECT dup_user_id, cnt
FROM (
    SELECT user_id, COUNT(*) AS cnt
    FROM (
        SELECT user_id FROM user_info_dirty_0
        UNION ALL SELECT user_id FROM user_info_dirty_1
        UNION ALL SELECT user_id FROM user_info_dirty_2
        UNION ALL SELECT user_id FROM user_info_dirty_3
    ) u
    GROUP BY user_id
    HAVING COUNT(*) > 1
) d
LIMIT 10;

SELECT '=== 5. 存储过程 / TRIGGER ===' AS section;

SELECT routine_name
FROM information_schema.routines
WHERE routine_schema = DATABASE()
  AND routine_type = 'PROCEDURE'
  AND routine_name IN (
    'sp_user_info_dirty_upsert_one',
    'sp_user_info_dirty_enqueue',
    'sp_user_info_dirty_enqueue_bvn',
    'sp_user_info_dirty_enqueue_adid',
    'sp_user_info_dirty_enqueue_emergency_mobile'
)
ORDER BY routine_name;

SELECT COUNT(*) AS trigger_cnt
FROM information_schema.triggers
WHERE trigger_schema = DATABASE()
  AND trigger_name LIKE 'trg_user_info_dirty_%';
