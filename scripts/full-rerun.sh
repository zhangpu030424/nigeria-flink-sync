#!/usr/bin/env bash
# 从零重跑：Cancel Job → DROP 重建 vt_token_cache(TINYINT) → 清 dirty → 一键流水线
#
# 用法:
#   ./scripts/full-rerun.sh
#   ./scripts/full-rerun.sh --skip-vt-rebuild    # 表已是 TINYINT 且不想清空 VT 缓存
#   ./scripts/full-rerun.sh --jobs user,user_info
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_VT_REBUILD=0
PIPELINE_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --skip-vt-rebuild|--skip-vt-emergency-enum) SKIP_VT_REBUILD=1 ;;
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
echo ">> [4/4] sync-pipeline-auto.sh（DDL → bulk-start-ms → VT+宽表 → 全量→增量）"
echo "    日志: logs/sync-<job>-auto.log"
echo ""

exec ./scripts/sync-pipeline-auto.sh "${PIPELINE_ARGS[@]}"
