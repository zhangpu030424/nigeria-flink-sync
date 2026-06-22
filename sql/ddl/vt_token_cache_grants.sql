-- vt-preload / 增量 Lookup 授权（DBA 按需执行；flink_cdc 已有 SELECT 可跳过）
-- 视图清单以 sql/ddl/source_lookup_views.sql 为准

USE nigeria_backend;

GRANT SELECT, INSERT, UPDATE, DELETE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_info_dirty TO 'flink_cdc'@'101.47.31.184';

-- Lookup 视图（deploy-source-ddl.sh 部署后）
GRANT SELECT ON nigeria_backend.user_info_incr_bundle_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.vt_token_cache_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_incr_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.users_by_adid_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bankcard_incr_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bankcard_id_by_account_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_product_latest_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.application_order_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.application_user_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bank_default_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_bvn_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.device_ids_latest_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.risk_approval_latest_by_order TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_repay_paid_latest_by_order TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_order_installment_overdue TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_repay_paid_by_order_period TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_order_loan_lookup TO 'flink_cdc'@'101.47.31.184';
GRANT SELECT ON nigeria_backend.user_order_installment_loan_lookup TO 'flink_cdc'@'101.47.31.184';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'flink_cdc'@'101.47.31.184';
