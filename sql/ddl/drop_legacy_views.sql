-- 删除已废弃、Flink 不再引用的 Lookup 视图（源库 DBA 执行）
-- 前置: Cancel 全部 Flink Job，避免 metadata lock
--
-- mysql -h <host> -u root -p nigeria_backend < sql/ddl/drop_legacy_views.sql

USE nigeria_backend;

DROP VIEW IF EXISTS application_order_id_by_order_no_lookup;
DROP VIEW IF EXISTS vt_id_number_lookup;
DROP VIEW IF EXISTS user_info_user_lookup;
DROP VIEW IF EXISTS user_id_by_bvn_lookup;
DROP VIEW IF EXISTS device_uuid_user_lookup;
DROP VIEW IF EXISTS session_uuid_user_lookup;
DROP VIEW IF EXISTS v_user_adjust_latest;

SELECT 'legacy views dropped' AS msg;
