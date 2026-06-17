-- 生产库分片脏队列回迁单表（user_info_dirty_0..3 → user_info_dirty）
-- 背景: 曾用 MOD(user_id,4) 分片 + 写 dirty_0..3 的存储过程；Flink CDC 只监听 user_info_dirty。
--
-- 执行后须覆盖存储过程（写单表）:
--   mysql ... < sql/ddl/user_info_dirty_enqueue.sql
-- 或: ./scripts/migrate-user-info-dirty-unshard.sh
--
-- 可选: 验证无误后 DROP TABLE user_info_dirty_0..3（见文末注释）

CREATE TABLE IF NOT EXISTS user_info_dirty (
    user_id     BIGINT        NOT NULL COMMENT '源库 user.id',
    updated_at  TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'user_info 增量触发队列（Flink CDC 单表）';

-- 合并分片表（若某张不存在会报错，说明已是单表环境可跳过本文件）
INSERT INTO user_info_dirty (user_id, updated_at)
SELECT user_id, MAX(updated_at) AS updated_at
FROM (
    SELECT user_id, updated_at FROM user_info_dirty_0
    UNION ALL
    SELECT user_id, updated_at FROM user_info_dirty_1
    UNION ALL
    SELECT user_id, updated_at FROM user_info_dirty_2
    UNION ALL
    SELECT user_id, updated_at FROM user_info_dirty_3
) AS merged
GROUP BY user_id
ON DUPLICATE KEY UPDATE updated_at = GREATEST(user_info_dirty.updated_at, VALUES(updated_at));

SELECT 'user_info_dirty' AS tbl, COUNT(*) AS cnt FROM user_info_dirty
UNION ALL SELECT 'user_info_dirty_0', COUNT(*) FROM user_info_dirty_0
UNION ALL SELECT 'user_info_dirty_1', COUNT(*) FROM user_info_dirty_1
UNION ALL SELECT 'user_info_dirty_2', COUNT(*) FROM user_info_dirty_2
UNION ALL SELECT 'user_info_dirty_3', COUNT(*) FROM user_info_dirty_3;

-- 验证通过且 Flink 已消费后，DBA 可手动删除分片表（勿在合并前执行）:
-- DROP TABLE IF EXISTS user_info_dirty_0, user_info_dirty_1, user_info_dirty_2, user_info_dirty_3;
