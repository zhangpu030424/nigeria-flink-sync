#!/usr/bin/env bash
# 目标库 loan：application_no 前缀>6 时去掉 ng 后多余 0（如 ng05011→ng5011）
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
source scripts/lib/load-project-env.sh
load_project_env .

for v in TARGET_MYSQL_HOST TARGET_MYSQL_PORT TARGET_MYSQL_USER TARGET_MYSQL_PASSWORD TARGET_MYSQL_DATABASE; do
  [[ -n "${!v:-}" ]] || { echo ">> ERR: .env 缺少 ${v}"; exit 1; }
done

SQL="sql/migrate/fix_target_loan_application_no_from_application.sql"
[[ -f "$SQL" ]] || { echo ">> ERR: 缺少 $SQL"; exit 1; }

echo ">> 目标: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}"
echo ">> 执行: $SQL"

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

echo ">> 完成"
