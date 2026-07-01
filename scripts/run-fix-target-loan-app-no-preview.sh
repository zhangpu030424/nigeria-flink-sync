#!/usr/bin/env bash
# 预览目标库 loan application_no 修正影响行数（只读）
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
source scripts/lib/load-project-env.sh
load_project_env .

for v in TARGET_MYSQL_HOST TARGET_MYSQL_PORT TARGET_MYSQL_USER TARGET_MYSQL_PASSWORD TARGET_MYSQL_DATABASE; do
  [[ -n "${!v:-}" ]] || { echo ">> ERR: .env 缺少 ${v}"; exit 1; }
done

SQL="sql/verify/fix_target_loan_application_no_preview.sql"
echo ">> 目标: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}"
echo ">> 预览（只读）: $SQL"
echo ""

if command -v mysql >/dev/null 2>&1; then
  MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql \
    -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT:-3306}" \
    -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" < "$SQL"
else
  docker run --rm -i \
    -e MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" \
    mysql:8.0 mysql \
    -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT:-3306}" \
    -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" < "$SQL"
fi
