-- 禁止 BI/Metabase 只读账号访问 Flink 内部对象（Lookup 视图、vt_token_cache、脏队列）
-- 须 root 或具备 REVOKE 权限的账号执行
--
-- 用法:
--   mysql -h HOST -u root -p nigeria_backend < sql/ddl/flink_lookup_revoke_readonly.sql
-- 或:
--   ./scripts/block-metabase-lookup.sh --revoke

USE nigeria_backend;

-- 按需改用户名；常见为 NGuserReadonly_backend@'%' 或 @'47.236.253.249'
SET @readonly_user = 'NGuserReadonly_backend';
SET @readonly_host = '%';

-- vt / 脏队列（非业务表）
SET @sql = CONCAT('REVOKE SELECT ON nigeria_backend.vt_token_cache FROM ''', @readonly_user, '''@''', @readonly_host, '''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @sql = CONCAT('REVOKE SELECT ON nigeria_backend.user_info_dirty FROM ''', @readonly_user, '''@''', @readonly_host, '''');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

-- 全部 *_lookup 视图（deploy-source-ddl.sh 部署清单）
SET @views = 'user_bank_default_lookup,user_bvn_lookup,device_ids_latest_lookup,risk_approval_latest_by_order,user_repay_paid_latest_by_order,user_order_installment_overdue,application_user_lookup,user_repay_paid_by_order_period,user_order_loan_lookup,application_order_lookup,user_order_installment_loan_lookup,vt_token_cache_lookup,user_personal_latest_lookup,app_config_lookup,user_work_latest_lookup,user_credit_latest_lookup,user_reg_ip_lookup,user_emergency_contacts_lookup,user_info_install_source_lookup,user_info_incr_bundle_lookup,users_by_adid_lookup,user_incr_lookup,user_bankcard_id_by_account_lookup,user_bankcard_incr_lookup,user_product_latest_lookup';

DROP TEMPORARY TABLE IF EXISTS _flink_lookup_revoke_views;
CREATE TEMPORARY TABLE _flink_lookup_revoke_views (view_name VARCHAR(128) PRIMARY KEY);

SET @i = 1;
SET @n = (LENGTH(@views) - LENGTH(REPLACE(@views, ',', '')) + 1);
WHILE @i <= @n DO
  SET @v = TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(@views, ',', @i), ',', -1));
  INSERT IGNORE INTO _flink_lookup_revoke_views VALUES (@v);
  SET @sql = CONCAT(
    'REVOKE SELECT ON nigeria_backend.', @v,
    ' FROM ''', @readonly_user, '''@''', @readonly_host, ''''
  );
  PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
  SET @i = @i + 1;
END WHILE;

FLUSH PRIVILEGES;

SELECT CONCAT('REVOKE 完成: ', @readonly_user, '@', @readonly_host, ' — 请执行 SHOW GRANTS 核对') AS result;
