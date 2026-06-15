#!/usr/bin/env bash
# 一键重建全部宽表
#
# 顺序（与 sql/ddl/vt_seed_all.sql 注释一致）:
#   1. vt_token_cache 建表 + TRIGGER
#   2. vt_seed_all.sql — INSERT IGNORE 灌明文（status=0）
#   3. vt-preload.sh — 批量 /v2t（status=0 → 1）
#   4. source_all_sync_staging.sql — 重建宽表（JOIN token）
#
# 用法:
#   ./scripts/rebuild-all-staging.sh
#   ./scripts/rebuild-all-staging.sh --skip-vt   # 跳过 seed/preload（VT 已最新）
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_VT=0
for arg in "$@"; do
  [[ "$arg" == "--skip-vt" ]] && SKIP_VT=1
done

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

run_sql_file() {
  local f=$1
  echo ""
  echo ">> SQL: $f"
  MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
    -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" < "$f"
}

echo ">> 源库: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"

if [[ "$SKIP_VT" -eq 0 ]]; then
  echo ""
  echo ">> [1/5] 建表 vt_token_cache（TINYINT）"
  run_sql_file sql/ddl/vt_token_cache.sql

  echo ""
  echo ">> [2/5] 重建 vt_token_cache TRIGGER（user_info 脏队列入队）"
  run_sql_file sql/ddl/vt_token_cache_vt_triggers.sql

  echo ""
  echo ">> [3/5] 灌明文 vt_seed_all.sql（status=0）"
  run_sql_file sql/ddl/vt_seed_all.sql

  echo ""
  echo ">> [4/5] VT 批量 /v2t（vt-preload.sh --vt-type all）"
  ./scripts/vt-preload.sh --mode fast --vt-type all --skip-count \
    --workers "${VT_PRELOAD_WORKERS:-4}" \
    --http-batch-size "${VT_PRELOAD_HTTP_BATCH:-50000}"
else
  echo ""
  echo ">> 跳过 VT seed/preload（--skip-vt）"
fi

echo ""
echo ">> [5/5] 重建全部宽表: sql/ddl/source_all_sync_staging.sql"
run_sql_file sql/ddl/source_all_sync_staging.sql

echo ""
echo ">> 完成。missing_token 行由全量阶段 2（vt_tokenize）补全"
echo ">> 紧接: ./scripts/sync-bulk-auto.sh --skip-staging"
echo ">> 完整一键: ./scripts/sync-migrate-auto.sh"
