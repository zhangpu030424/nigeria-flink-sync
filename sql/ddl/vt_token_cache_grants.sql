-- vt-preload.py / 增量 Lookup 视图授权（须源库 DBA 在 mysql 客户端执行，勿用云 DMS 查询窗）
-- 若报 [5030] You are not allowed to create a user with GRANT：
--   1) flink_cdc 用户已存在且能跑 CDC → 多半已有 SELECT，可跳过本文件
--   2) 或请 DBA 先 CREATE USER，再执行本文件
-- user_info 已直查 user/app_config/vt_token_cache，通常无需本文件里的视图 GRANT
-- 注意：建表用 root 执行 sql/ddl/vt_token_cache.sql；flink_cdc 不需要 CREATE

USE nigeria_backend;

-- 先建表（root）: mysql ... < sql/ddl/vt_token_cache.sql

-- 默认 write_mode=update_id 只需 SELECT + UPDATE（INSERT 供 stream/失败行）
GRANT SELECT, INSERT, UPDATE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'101.47.31.184';
-- 仅 delete_insert 模式额外需要:
-- GRANT DELETE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'101.47.31.184';

-- stream 模式还需读源表（CDC 用户通常已有 SELECT）
GRANT SELECT ON nigeria_backend.`user` TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.device_ids TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bank_info TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_personal_info TO 'flink_cdc'@'101.47.31.184';

-- 增量 Lookup 视图（sql/ddl/source_lookup_views.sql）
GRANT SELECT ON nigeria_backend.user_bank_default_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bvn_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.device_ids_latest_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.risk_approval_latest_by_order TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_repay_paid_latest_by_order TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_order_installment_overdue TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_repay_paid_by_order_period TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_order_loan_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_info_user_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_work_latest_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.application_user_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.vt_id_number_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.vt_token_cache_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.app_config_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_info_incr_bundle_lookup TO 'flink_cdc'@'101.47.31.184';
-- user_info 增量脏队列 CDC（表由 deploy-source-ddl / root 建；flink_cdc 只需 SELECT）
GRANT SELECT ON nigeria_backend.user_info_dirty TO 'flink_cdc'@'101.47.31.184';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'flink_cdc'@'101.47.31.184';
