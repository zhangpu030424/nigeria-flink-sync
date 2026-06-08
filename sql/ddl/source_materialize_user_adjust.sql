-- 全量同步前在源库执行：把 v_user_adjust_latest 物化为表，避免 Lookup 每次跑 correlated subquery
-- 执行后 Flink dim 改为查 user_adjust_cache 表
--
-- mysql -h <源库> -u ... -p nigeria_backend < sql/ddl/source_materialize_user_adjust.sql

-- 依赖 v_user_adjust_latest（先执行 source_views_adjust.sql）
DROP TABLE IF EXISTS user_adjust_cache;

CREATE TABLE user_adjust_cache AS
SELECT * FROM v_user_adjust_latest;

ALTER TABLE user_adjust_cache
    ADD PRIMARY KEY (user_id);

-- 全量跑完后可定时刷新；增量阶段 UTM 变更不频繁时可接受
-- TRUNCATE user_adjust_cache; INSERT INTO user_adjust_cache SELECT * FROM v_user_adjust_latest;
