-- 脏队列只读视图（UNION 4 分片），供对账/验证脚本查询
-- Flink CDC 读物理分片表 user_info_dirty_0..3，不读此视图

DROP VIEW IF EXISTS user_info_dirty;

CREATE VIEW user_info_dirty AS
SELECT user_id, updated_at FROM user_info_dirty_0
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_1
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_2
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_3;
