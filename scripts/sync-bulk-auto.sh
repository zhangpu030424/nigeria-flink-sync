#!/usr/bin/env bash
# 全量迁移自动化（只做 bulk，不切增量）
#
# 流程:
#   1. 可选 Cancel Flink Job
#   2. 可选 root DROP 重建 vt_token_cache (TINYINT)
#   3. deploy-source-ddl（视图 + user_info 脏队列 TRIGGER）
#   4. 锁定 bulk-start-ms → logs/bulk-start-ms.env（增量必用，勿删）
#   5. vt_seed + vt-preload + 重建宽表
#   6. 按 config/sync-jobs.conf 顺序跑各表全量（--bulk-only）
#
# 用法:
#   ./scripts/sync-bulk-auto.sh
#   ./scripts/sync-bulk-auto.sh --jobs user,user_info
#   ./scripts/sync-bulk-auto.sh --skip-staging          # 宽表已建好
#   ./scripts/sync-bulk-auto.sh --skip-vt               # 跳过 vt_seed/preload
#   ./scripts/sync-bulk-auto.sh --rebuild-vt-swap       # 大表 RENAME 换表（推荐）
#   ./scripts/sync-bulk-auto.sh --rebuild-vt-purge      # 分批删空再 DROP
#   ./scripts/sync-bulk-auto.sh --rebuild-vt-cache      # 直接 DROP（小表）
#   ./scripts/sync-bulk-auto.sh --keep-jobs             # 不 Cancel 存量 Job
#
set -euo pipefail
cd "$(dirname "$0")/.."

CANCEL_JOBS=1
SKIP_STAGING=0
SKIP_VT=0
SKIP_DDL=0
REBUILD_VT=0
REBUILD_VT_MODE="drop"
JOBS_FILTER=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobs=*) JOBS_FILTER="${1#--jobs=}" ;;
    --jobs)
      shift
      JOBS_FILTER="${1:-}"
      ;;
    --skip-staging) SKIP_STAGING=1 ;;
    --skip-vt) SKIP_VT=1 ;;
    --skip-ddl) SKIP_DDL=1 ;;
    --rebuild-vt-cache) REBUILD_VT=1; REBUILD_VT_MODE="drop" ;;
    --rebuild-vt-swap) REBUILD_VT=1; REBUILD_VT_MODE="swap" ;;
    --rebuild-vt-purge|--rebuild-vt-purge-drop) REBUILD_VT=1; REBUILD_VT_MODE="purge-drop" ;;
    --keep-jobs) CANCEL_JOBS=0 ;;
    --user-info-latest-offset|--keep-user-info-dirty|--truncate-user-info-dirty|--verify) ;;  # incr 参数，bulk 忽略
    --startup-mode=*|--startup-mode|--incr-startup-mode=*|--incr-startup-mode) ;;  # incr 参数
    -h|--help)
      sed -n '2,20p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $1（--help 查看）"
      exit 1
      ;;
  esac
  shift
done

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/bulk-start-ms.sh
source scripts/lib/bulk-start-ms.sh

# shellcheck source=scripts/lib/sync-jobs.sh
source scripts/lib/sync-jobs.sh

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

BULK_PAR="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
SLOTS="${FLINK_TASK_SLOTS:-16}"

echo "=========================================="
echo "全量迁移 sync-bulk-auto"
echo "  FLINK_PARALLELISM_BULK=${BULK_PAR}  FLINK_TASK_SLOTS=${SLOTS}"
echo "=========================================="

if [[ "$CANCEL_JOBS" -eq 1 ]]; then
  echo ""
  echo ">> [1] Cancel 全部 Running Flink Job"
  bash scripts/cancel-flink-jobs.sh --yes
else
  echo ""
  echo ">> [1] 保留存量 Job（--keep-jobs）"
fi

if [[ "$REBUILD_VT" -eq 1 ]]; then
  echo ""
  echo ">> [2] 重建 vt_token_cache（${REBUILD_VT_MODE}，须 root/DBA）"
  # shellcheck source=scripts/lib/mysql-source.sh
  source scripts/lib/mysql-source.sh
  # shellcheck source=scripts/lib/vt-token-cache-rebuild.sh
  source scripts/lib/vt-token-cache-rebuild.sh
  vt_token_cache_rebuild "$REBUILD_VT_MODE"
else
  echo ""
  echo ">> [2] 跳过 vt_token_cache 重建（--rebuild-vt-cache 可开启）"
fi

if [[ "$SKIP_DDL" -eq 0 ]]; then
  echo ""
  echo ">> [3] 源库 DDL（adjust + Lookup + user_info_dirty）"
  if [[ "$CANCEL_JOBS" -eq 0 ]]; then
    ./scripts/deploy-source-ddl.sh --skip-if-ok
  else
    ./scripts/deploy-source-ddl.sh --force-views
  fi
else
  echo ""
  echo ">> [3] 跳过 DDL（--skip-ddl）"
fi

echo ""
echo ">> [4] 锁定 bulk-start-ms（写入 logs/bulk-start-ms.env，增量阶段必用）"
record_bulk_start_ms "$LOG_DIR"
SHARED_MS="${BULK_START_MS}"

if [[ "$SKIP_STAGING" -eq 0 ]]; then
  echo ""
  echo ">> [5] VT + 重建宽表"
  REBUILD_ARGS=()
  [[ "$SKIP_VT" -eq 1 ]] && REBUILD_ARGS+=(--skip-vt)
  ./scripts/rebuild-all-staging.sh "${REBUILD_ARGS[@]}"
else
  echo ""
  echo ">> [5] 跳过宽表（--skip-staging）"
fi

sync_jobs_load "$JOBS_FILTER"
sync_jobs_print_plan "全量"

echo ""
if ! ./scripts/check-flink-slots.sh; then
  echo "ERR: slot 检查未通过"
  exit 1
fi

need_slots="$BULK_PAR"
if [[ "$need_slots" -gt "$SLOTS" ]]; then
  echo "WARN: 全量并行 ${need_slots} > slots ${SLOTS}"
fi

echo ""
echo ">> [6] 顺序全量（--bulk-only，不切增量）"
first=1
for job in "${SYNC_ENABLED_JOBS[@]}"; do
  echo ""
  echo "########################################"
  echo "# 全量 Job: ${job}"
  echo "########################################"
  if [[ "$first" -eq 1 ]]; then
    ./scripts/sync-job-auto.sh "$job" --bulk-only --bulk-start-ms "$SHARED_MS"
    first=0
  else
    ./scripts/sync-job-auto.sh "$job" --bulk-only --bulk-start-ms "$SHARED_MS" --keep-other-jobs
  fi
done

echo ""
echo "=========================================="
echo "全量迁移完成"
echo "  bulk-start-ms=${SHARED_MS}"
echo "  日志: logs/sync-<job>-auto.log"
echo "  下一步: ./scripts/sync-incr-auto.sh"
echo "  对账:   bash scripts/verify-user-info-reconcile.sh --sample 200"
echo "=========================================="
