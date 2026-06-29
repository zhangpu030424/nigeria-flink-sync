-- 目标库一次性修复：group_user_id 从 unsigned 改为 signed
-- 原因：Flink JDBC Lookup 读 unsigned 返回 BigInteger，无法转为 Long/String
-- 在目标库执行：mysql -h ... -u ... -p platform_db < sql/migrate/user_bankcard_group_user_id_signed.sql

ALTER TABLE user_bankcard
    MODIFY COLUMN group_user_id BIGINT NOT NULL COMMENT 'App Group User ID';
