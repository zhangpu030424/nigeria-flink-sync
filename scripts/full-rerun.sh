#!/usr/bin/env bash
# 从零重跑：Cancel Job → 重建 vt_token_cache(TINYINT) → 清 dirty → 全量 → 增量
#
# 用法:
#   ./scripts/full-rerun.sh
#   ./scripts/full-rerun.sh --rebuild-vt-swap        # 大表推荐：RENAME 换表（默认）
#   ./scripts/full-rerun.sh --rebuild-vt-purge       # 分批删空再 DROP
#   ./scripts/full-rerun.sh --rebuild-vt-drop        # 直接 DROP（小表）
#   ./scripts/full-rerun.sh --skip-vt-rebuild
#   ./scripts/full-rerun.sh --jobs user,user_info
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_VT_REBUILD=0
REBUILD_VT_MODE="swap"
PIPELINE_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --skip-vt-rebuild|--skip-vt-emergency-enum) SKIP_VT_REBUILD=1 ;;
    --rebuild-vt-swap) REBUILD_VT_MODE="swap" ;;
    --rebuild-vt-purge|--rebuild-vt-purge-drop) REBUILD_VT_MODE="purge-drop" ;;
    --rebuild-vt-drop) REBUILD_VT_MODE="drop" ;;
    *) PIPELINE_ARGS+=("$arg") ;;
  esac
done

[[ -f .env ]] || { echo "请先: cp .env.example .env 并填写"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

echo "=========================================="
echo "全量+增量 从零重跑"
echo "  FLINK_PARALLELISM_BULK=${FLINK_PARALLELISM_BULK:-?}"
echo "  FLINK_PARALLELISM_INCR=${FLINK_PARALLELISM_INCR:-?}"
echo "  CDC_SERVER_ID_UI_DIRTY=${CDC_SERVER_ID_UI_DIRTY:-?}"
echo "=========================================="

echo ""
echo ">> [1/4] 取消全部 Running Flink Job"
bash scripts/cancel-flink-jobs.sh --yes

echo ""
echo ">> [2/4] vt_token_cache（TINYINT vt_type，避免 ENUM ALTER 锁表）"
if [[ "$SKIP_VT_REBUILD" -eq 1 ]]; then
  echo "  跳过 DROP 重建（--skip-vt-rebuild）"
else
  echo "  ⚠️  DROP 会清空全部 VT token，须 root/DBA；之后流水线会 vt_seed + vt-preload"
  mysql_source_file sql/ddl/vt_token_cache_rebuild.sql
fi

echo ""
echo ">> [3/4] 清空 user_info_dirty"
if table_exists user_info_dirty; then
  mysql_source_query "TRUNCATE TABLE user_info_dirty;" && echo "  ✓ TRUNCATE user_info_dirty"
else
  echo "  user_info_dirty 不存在，跳过"
fi

echo ""
echo ">> [4/5] sync-bulk-auto.sh（DDL → bulk-start-ms → VT+宽表 → 全量）"
echo "    日志: logs/sync-<job>-auto.log"
echo ""

./scripts/sync-bulk-auto.sh "${PIPELINE_ARGS[@]}"

echo ""
echo ">> [5/5] sync-incr-auto.sh（timestamp 增量，正确性优先）"
./scripts/sync-incr-auto.sh "${PIPELINE_ARGS[@]}"
