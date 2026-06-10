#!/usr/bin/env bash
# 在老库 MySQL 直跑索引优化版校验 SQL（与 Flink opt 对照）
# 用法:
#   LM_MIGRATION_LIMIT=20 bash lm/scripts/run-lm-verify-mysql.sh user
#   LM_MIGRATION_LIMIT=20 bash lm/scripts/run-lm-verify-mysql.sh all
set -euo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${line%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

: "${LM_MYSQL_HOST:?}"
: "${LM_MYSQL_USER:?}"
: "${LM_MYSQL_PASSWORD:?}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"
LM_CORE_MYSQL_DATABASE="${LM_CORE_MYSQL_DATABASE:-ng_loan_core}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"

TARGET="${1:-user}"
VERIFY_DIR="lm/sql/verify"

run_one() {
  local key=$1
  local file=""
  case "$key" in
    user) file="${VERIFY_DIR}/01_user.sql" ;;
    *)
      echo "WARN: 校验 SQL [$key] 尚未放入 ${VERIFY_DIR}/，跳过"
      return 0
      ;;
  esac
  [[ -f "$file" ]] || { echo "ERR: 缺少 $file"; exit 1; }
  local prep="/tmp/lm-verify-${key}-$$.sql"
  envsubst '${LM_MIGRATION_LIMIT} ${LM_MYSQL_DATABASE} ${LM_CORE_MYSQL_DATABASE}' < "$file" > "$prep"
  echo ">> [$key] LIMIT=${LM_MIGRATION_LIMIT} @ ${LM_MYSQL_DATABASE}"
  MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
    --connect-timeout=30 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
    -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    < "$prep"
  rm -f "$prep"
}

if [[ "$TARGET" == "all" ]]; then
  run_one user
else
  run_one "$TARGET"
fi
