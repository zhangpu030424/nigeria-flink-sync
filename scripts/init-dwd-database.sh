#!/usr/bin/env bash
# 创建 DWD 库 ng_migration_dwd（与老库 ng_loan_market 同 MySQL 实例，非目标库）
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  export "$line"
done < .env
set +a

: "${LM_MYSQL_HOST:?}"
# shellcheck source=lib/dwd-mysql.sh
source "$(dirname "$0")/lib/dwd-mysql.sh"
dwd_mysql_export_env

echo ">> CREATE DATABASE ${DWD_MYSQL_DATABASE} @ $(dwd_mysql_write_host):$(dwd_mysql_write_port)（老库实例）"
dwd_mysql_exec_write_sql -e "CREATE DATABASE IF NOT EXISTS \`${DWD_MYSQL_DATABASE}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

dwd_mysql_exec_write_sql "$DWD_MYSQL_DATABASE" < sql/ddl/dwd_user_info_staging.sql

echo ">> 完成 ${DWD_MYSQL_DATABASE} @ 老库实例（dwd_* 表已就绪）"
