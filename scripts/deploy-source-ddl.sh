#!/usr/bin/env bash
# 源库 DDL 一键部署（adjust + Lookup 视图 + user_info 脏队列）
# 默认用 SOURCE_MYSQL_USER（flink_cdc）；仅权限不足时可用 SOURCE_MYSQL_ROOT_* 兜底
# 用法: ./scripts/deploy-source-ddl.sh
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "请先: cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh
# shellcheck source=scripts/lib/user-info-dirty-deploy.sh
source scripts/lib/user-info-dirty-deploy.sh

echo ">> deploy-source-ddl: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"

VIEW_DDL_FILES=(
  sql/ddl/source_views_adjust.sql
  sql/ddl/source_lookup_views.sql
)

for f in "${VIEW_DDL_FILES[@]}"; do
  mysql_source_file "$f"
done

ensure_user_info_dirty_deploy

echo ""
echo ">> 校验关键对象"
CHECK_VIEWS=(
  v_adjust_latest_by_adid
  user_personal_latest_lookup
  app_config_lookup
  vt_token_cache_lookup
  user_work_latest_lookup
  user_credit_latest_lookup
  user_reg_ip_lookup
  user_emergency_contacts_lookup
  user_info_install_source_lookup
  user_info_incr_bundle_lookup
  users_by_adid_lookup
  user_incr_lookup
  user_bankcard_id_by_account_lookup
  user_bankcard_incr_lookup
  user_product_latest_lookup
  application_order_lookup
  user_order_installment_loan_lookup
  application_user_lookup
  user_order_loan_lookup
  user_repay_paid_by_order_period
  user_bank_default_lookup
  user_bvn_lookup
  device_ids_latest_lookup
  risk_approval_latest_by_order
  user_repay_paid_latest_by_order
  user_order_installment_overdue
)
failed=0
for v in "${CHECK_VIEWS[@]}"; do
  if view_exists "$v"; then
    echo "  ✓ ${v}"
  else
    echo "  ✗ ${v} 缺失"
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo "ERR: 源库视图部署未完整，请检查 flink_cdc 是否有 CREATE VIEW 权限"
  exit 1
fi

echo ""
echo ">> 校验 user_info_dirty 表"
if table_exists user_info_dirty; then
  echo "  ✓ user_info_dirty 表"
else
  echo "  ✗ user_info_dirty 表缺失"
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "ERR: 源库 DDL 部署未完整"
  exit 1
fi

echo ">> 源库 DDL 部署完成"
