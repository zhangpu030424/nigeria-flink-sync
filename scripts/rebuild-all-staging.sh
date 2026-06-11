#!/usr/bin/env bash
# 一键重建全部宽表（VT 需已灌满 vt_token_cache）
# 用法: ./scripts/rebuild-all-staging.sh
set -euo pipefail
cd "$(dirname "$0")/.."

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
  export "$line"
done < .env
set +a

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"
SQL="sql/ddl/source_all_sync_staging.sql"

echo ">> 重建全部宽表: $SQL"
echo ">> 源库: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"

MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
  -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" < "$SQL"

echo ">> 完成。missing_token 行由全量阶段 2（vt_tokenize）补全，无需手工 preload 到 0"
echo ">> 若单独执行本脚本，请紧接: ./scripts/sync-pipeline-auto.sh --skip-staging"
echo ">> （勿分两次跑；完整一键请直接: ./scripts/sync-pipeline-auto.sh）"
