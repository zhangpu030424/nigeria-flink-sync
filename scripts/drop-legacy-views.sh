#!/usr/bin/env bash
# 删除废弃 Lookup 视图（sql/ddl/drop_legacy_views.sql）
#
# 前置:
#   1. Cancel Flink Job: bash scripts/cancel-flink-jobs.sh --yes
#   2. 停 Metabase 对 user_info_incr_bundle_lookup 的查询，或:
#        bash scripts/drop-legacy-views.sh --kill-readers
#
# 用法:
#   ./scripts/drop-legacy-views.sh
#   ./scripts/drop-legacy-views.sh --kill-readers   # 杀占 MDL 的长查询（Metabase 等）
#   ./scripts/drop-legacy-views.sh --dry-run
#
set -euo pipefail
cd "$(dirname "$0")/.."

KILL_READERS=0
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    --kill-readers) KILL_READERS=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $arg"
      exit 1
      ;;
  esac
done

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

LEGACY_VIEWS=(
  application_order_id_by_order_no_lookup
  vt_id_number_lookup
  user_info_user_lookup
  user_id_by_bvn_lookup
  device_uuid_user_lookup
  session_uuid_user_lookup
  v_user_adjust_latest
)

echo "=========================================="
echo "删除废弃视图 (${#LEGACY_VIEWS[@]} 个)"
echo "  库: ${SOURCE_MYSQL_DATABASE}@${SOURCE_MYSQL_HOST}"
echo "=========================================="

if [[ "$KILL_READERS" -eq 1 ]]; then
  echo ""
  echo ">> 查找占 Lookup 视图/VT 的长连接（Time>=60s）..."
  blockers=$(mysql_source_cmd -N -e "
    SELECT ID
    FROM information_schema.PROCESSLIST
    WHERE DB = '${SOURCE_MYSQL_DATABASE}'
      AND ID <> CONNECTION_ID()
      AND COMMAND != 'Daemon'
      AND TIME >= 60
      AND (
        INFO LIKE '%user_info_incr_bundle_lookup%'
        OR INFO LIKE '%vt_token_cache_lookup%'
        OR INFO LIKE '%vt_id_number_lookup%'
        OR INFO LIKE '%application_order_id_by_order_no%'
        OR INFO LIKE '%Metabase%'
      );
  " 2>/dev/null || true)
  if [[ -n "$blockers" ]]; then
    while read -r pid; do
      [[ -z "$pid" ]] && continue
      echo ">> KILL ${pid}"
      [[ "$DRY_RUN" -eq 0 ]] && mysql_source_query "KILL ${pid};" || true
    done <<< "$blockers"
    sleep 2
  else
    echo ">> 未发现明显阻塞连接"
  fi
fi

echo ""
for v in "${LEGACY_VIEWS[@]}"; do
  exists=$(mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.VIEWS WHERE TABLE_SCHEMA='${SOURCE_MYSQL_DATABASE}' AND TABLE_NAME='${v}';" \
    2>/dev/null || echo "0")
  if [[ "$exists" != "1" ]]; then
    echo "  - ${v}（不存在，跳过）"
    continue
  fi
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "  - DROP VIEW ${v}（dry-run）"
    continue
  fi
  echo -n "  - DROP VIEW ${v} ... "
  if mysql_source_query "DROP VIEW IF EXISTS \`${v}\`;"; then
    echo "OK"
  else
    echo "FAIL（可能被 Metabase/Flink 占锁，先 --kill-readers 或手动 KILL）"
    exit 1
  fi
done

echo ""
echo ">> 完成。保留视图请执行: ./scripts/deploy-source-ddl.sh"
