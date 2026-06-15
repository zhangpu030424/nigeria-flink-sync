#!/usr/bin/env bash
# 一键重建全部宽表
#
# 顺序:
#   1. vt_token_cache 建表
#   2. vt_seed_all.sql — INSERT IGNORE 灌明文（status=0，无 VT 脏队列 TRIGGER）
#   3. vt-preload.sh — 批量 /v2t（status=0 → 1）
#   4. vt_token_cache_vt_triggers.sql — 增量脏队列入队（seed/preload 后再建，避免百万行触发）
#   5. source_all_sync_staging.sql + id_mapping_sync_staging.sql — 重建宽表
#
# 用法:
#   ./scripts/rebuild-all-staging.sh
#   ./scripts/rebuild-all-staging.sh --skip-vt   # 跳过 seed/preload（VT 已最新）
#   ./scripts/rebuild-all-staging.sh --keep-user-info-dirty  # 保留脏队列（一般不推荐）
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_VT=0
KEEP_DIRTY=0
for arg in "$@"; do
  [[ "$arg" == "--skip-vt" ]] && SKIP_VT=1
  [[ "$arg" == "--keep-user-info-dirty" ]] && KEEP_DIRTY=1
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
  echo ">> SQL: $f （$(date '+%H:%M:%S') 开始；大查询可能较久）"
  local t0=$SECONDS
  local init="SET SESSION wait_timeout=28800,net_read_timeout=7200,net_write_timeout=7200;"
  MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
    -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" \
    --max-allowed-packet=512M \
    --init-command="${init}" \
    < "$f"
  echo ">> 完成: $f （耗时 $((SECONDS - t0))s）"
}

echo ">> 源库: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"

if [[ "$SKIP_VT" -eq 0 ]]; then
  echo ""
  echo ">> [1/5] 建表 vt_token_cache（TINYINT）"
  run_sql_file sql/ddl/vt_token_cache.sql

  echo ""
  echo ">> [2/5] 灌明文 vt_seed_all.sql（status=0）"
  run_sql_file sql/ddl/vt_seed_all.sql

  echo ""
  echo ">> [3/5] VT 批量 /v2t（vt-preload.sh --vt-type all）"
  ./scripts/vt-preload.sh --mode fast --vt-type all --skip-count \
    --workers "${VT_PRELOAD_WORKERS:-4}" \
    --http-batch-size "${VT_PRELOAD_HTTP_BATCH:-50000}"

  echo ""
  echo ">> [4/5] 部署 vt_token_cache 脏队列 TRIGGER（seed/preload 完成后再建）"
  run_sql_file sql/ddl/vt_token_cache_vt_triggers.sql
else
  echo ""
  echo ">> 跳过 VT seed/preload（--skip-vt）"
  echo ">> 确保 vt_token_cache VT TRIGGER 已部署: sql/ddl/vt_token_cache_vt_triggers.sql"
fi

echo ""
echo ">> [5/5] 重建宽表 1-6: sql/ddl/source_all_sync_staging.sql"
if [[ "$KEEP_DIRTY" -eq 0 ]]; then
  echo ">> 建宽表前清空 user_info_dirty（vt-preload TRIGGER 可能已写入，全量将覆盖）"
  # shellcheck source=scripts/lib/mysql-source.sh
  source scripts/lib/mysql-source.sh
  # shellcheck source=scripts/lib/user-info-dirty.sh
  source scripts/lib/user-info-dirty.sh
  truncate_user_info_dirty
else
  echo ">> 保留 user_info_dirty（--keep-user-info-dirty）"
fi
run_sql_file sql/ddl/source_all_sync_staging.sql

echo ""
echo ">> [5/5] 重建 id_mapping: sql/ddl/id_mapping_sync_staging.sql"
run_sql_file sql/ddl/id_mapping_sync_staging.sql

echo ""
echo ">> 完成。missing_token 行由全量阶段 2（vt_tokenize）补全"
echo ">> 紧接: ./scripts/sync-bulk-auto.sh --skip-staging"
echo ">> 完整一键: ./scripts/sync-migrate-auto.sh"
