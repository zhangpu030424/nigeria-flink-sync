#!/usr/bin/env bash
# 源库 DDL 一键部署（adjust 视图 + 增量 Lookup 视图；无需 DMS / GRANT）
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

echo ">> deploy-source-ddl: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"

DDL_FILES=(
  sql/ddl/source_views_adjust.sql
  sql/ddl/source_lookup_views.sql
)

for f in "${DDL_FILES[@]}"; do
  mysql_source_file "$f"
done

echo ""
echo ">> 校验关键对象"
CHECK_VIEWS=(
  v_adjust_latest_by_adid
  user_info_user_lookup
  user_personal_latest_lookup
  user_id_by_bvn_lookup
  device_uuid_user_lookup
  session_uuid_user_lookup
  app_config_lookup
  vt_token_cache_lookup
  user_work_latest_lookup
  user_credit_latest_lookup
  user_reg_ip_lookup
  user_emergency_contacts_lookup
  user_info_install_source_lookup
  application_user_lookup
  user_order_loan_lookup
  user_repay_paid_by_order_period
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

echo ">> 源库 DDL 部署完成"
