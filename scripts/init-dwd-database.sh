#!/usr/bin/env bash
# 创建 DWD 库 ng_migration_dwd（同 TARGET 实例，独立 database）
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

export DWD_MYSQL_HOST="${DWD_MYSQL_HOST:-${TARGET_MYSQL_HOST:?}}"
export DWD_MYSQL_PORT="${DWD_MYSQL_PORT:-${TARGET_MYSQL_PORT:-3306}}"
export DWD_MYSQL_USER="${DWD_MYSQL_USER:-${TARGET_MYSQL_USER:?}}"
export DWD_MYSQL_PASSWORD="${DWD_MYSQL_PASSWORD:-${TARGET_MYSQL_PASSWORD:?}}"
export DWD_MYSQL_DATABASE="${DWD_MYSQL_DATABASE:-ng_migration_dwd}"

echo ">> CREATE DATABASE ${DWD_MYSQL_DATABASE} @ ${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}"
MYSQL_PWD="$DWD_MYSQL_PASSWORD" mysql --connect-timeout=30 \
  -h "$DWD_MYSQL_HOST" -P "$DWD_MYSQL_PORT" -u "$DWD_MYSQL_USER" \
  -e "CREATE DATABASE IF NOT EXISTS \`${DWD_MYSQL_DATABASE}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

MYSQL_PWD="$DWD_MYSQL_PASSWORD" mysql --connect-timeout=30 \
  -h "$DWD_MYSQL_HOST" -P "$DWD_MYSQL_PORT" -u "$DWD_MYSQL_USER" "$DWD_MYSQL_DATABASE" \
  < sql/ddl/dwd_user_info_staging.sql

echo ">> 完成 ${DWD_MYSQL_DATABASE}（dwd_* 表已就绪）"
