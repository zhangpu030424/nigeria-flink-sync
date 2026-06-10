-- 创建 DWD 中间库 ng_migration_dwd（与老库 ng_loan_market 同 MySQL 实例）
-- 用法: bash scripts/init-dwd-database.sh

CREATE DATABASE IF NOT EXISTS ng_migration_dwd
  DEFAULT CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;
