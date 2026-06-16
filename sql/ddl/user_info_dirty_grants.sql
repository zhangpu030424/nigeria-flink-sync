-- user_info 脏队列：存储过程 + TRIGGER 授权（DBA 用 root 执行）
-- flink_cdc 通常只能 SELECT/CREATE VIEW，无法 CREATE PROCEDURE / TRIGGER
--
-- 部署顺序:
--   mysql ... nigeria_backend < sql/ddl/user_info_dirty_enqueue.sql
--   mysql ... nigeria_backend < sql/ddl/user_info_dirty.sql
-- 或: mysql ... nigeria_backend < sql/ddl/user_info_dirty_dba_deploy.sql

USE nigeria_backend;

-- 若需 flink_cdc 自行部署（一般不建议），可放开 ROUTINE（仍无法建 TRIGGER）
-- GRANT CREATE ROUTINE, ALTER ROUTINE, EXECUTE ON nigeria_backend.* TO 'flink_cdc'@'%';

-- Flink CDC 读脏队列表
GRANT SELECT ON nigeria_backend.user_info_dirty TO 'flink_cdc'@'%';

FLUSH PRIVILEGES;
