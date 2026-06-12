#!/usr/bin/env bash
# 一键全自动：源库 DDL → 锁定起始时间戳 → 重建宽表 → 顺序全量(含 VT 两阶段) → 各表切增量
#
# 无需 DMS 手动操作；Lookup 视图由 deploy-source-ddl.sh 自动部署。
#
# 用法:
#   ./scripts/sync-pipeline-auto.sh
#   ./scripts/sync-pipeline-auto.sh --skip-staging     # 宽表已建好，仍用已存 bulk-start-ms
#   ./scripts/sync-pipeline-auto.sh --skip-vt          # 重建宽表时跳过 vt_seed + vt-preload
#   ./scripts/sync-pipeline-auto.sh --jobs user,user_info
#   ./scripts/sync-pipeline-auto.sh --incr-only      # 仅提交增量（读 logs/bulk-start-ms.env）
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_STAGING=0
SKIP_VT=0
INCR_ONLY=0
JOBS_FILTER=""
for arg in "$@"; do
  [[ "$arg" == "--skip-staging" ]] && SKIP_STAGING=1
  [[ "$arg" == "--skip-vt" ]] && SKIP_VT=1
  [[ "$arg" == "--incr-only" ]] && INCR_ONLY=1
  [[ "$arg" == --jobs=* ]] && JOBS_FILTER="${arg#--jobs=}"
done

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/bulk-start-ms.sh
source scripts/lib/bulk-start-ms.sh

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "步骤 A: 源库 DDL（adjust + Lookup 视图）"
echo "=========================================="
./scripts/deploy-source-ddl.sh

if [[ "$INCR_ONLY" -eq 1 ]]; then
  if ! load_bulk_start_ms "${LOG_DIR}/bulk-start-ms.env"; then
    echo "ERR: --incr-only 需要 logs/bulk-start-ms.env（先跑过一次完整流水线）"
    exit 1
  fi
else
  echo "=========================================="
  echo "步骤 0: 锁定增量 binlog 起始时刻（早于宽表重建）"
  record_bulk_start_ms "$LOG_DIR"
fi

SHARED_MS="${BULK_START_MS}"

BULK_PAR="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
SLOTS="${FLINK_TASK_SLOTS:-16}"

echo "=========================================="
echo "流水线同步"
echo "  bulk-start-ms=${SHARED_MS}"
echo "  bulk并行=${BULK_PAR}  incr并行=${INCR_PAR}  slots=${SLOTS}"
echo "  增量模式: CDC initial（先快照补全量漏写，再追 binlog）"
echo "=========================================="

ENABLED_JOBS=()
while IFS= read -r row || [[ -n "$row" ]]; do
  row="${row%%#*}"
  row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$row" ]] && continue
  key="${row%%|*}"
  enabled="${row##*|}"
  [[ "$enabled" != "1" ]] && continue
  if [[ -n "$JOBS_FILTER" ]]; then
    echo ",${JOBS_FILTER}," | grep -q ",${key}," || continue
  fi
  ENABLED_JOBS+=("$key")
done < config/sync-jobs.conf

if [[ ${#ENABLED_JOBS[@]} -eq 0 ]]; then
  echo "无 ENABLED=1 的 Job"
  exit 1
fi

if [[ "$INCR_ONLY" -eq 1 ]]; then
  echo ">> --incr-only：跳过宽表/全量，提交增量 Job: ${ENABLED_JOBS[*]}"
  first=1
  for job in "${ENABLED_JOBS[@]}"; do
    if [[ "$first" -eq 1 ]]; then
      ./scripts/sync-job-auto.sh "$job" --incr-only --bulk-start-ms "$SHARED_MS"
      first=0
    else
      ./scripts/sync-job-auto.sh "$job" --incr-only --bulk-start-ms "$SHARED_MS" --keep-other-jobs
    fi
  done
  exit 0
fi

need_slots=$((BULK_PAR + ${#ENABLED_JOBS[@]} * INCR_PAR))
if [[ "$need_slots" -gt "$SLOTS" ]]; then
  echo "WARN: 预估峰值 slot 需求 ${need_slots} > FLINK_TASK_SLOTS=${SLOTS}"
fi

if [[ "$SKIP_STAGING" -eq 0 ]]; then
  echo ""
  echo ">> [1/2] VT 补灌 + 重建全部宽表（vt_seed_all → vt-preload → source_all_sync_staging）"
  echo ">>       起始时刻已锁定，此期间源库 binlog 由后续增量补"
  REBUILD_ARGS=()
  [[ "$SKIP_VT" -eq 1 ]] && REBUILD_ARGS+=(--skip-vt)
  ./scripts/rebuild-all-staging.sh "${REBUILD_ARGS[@]}"
else
  echo ">> 跳过宽表重建（--skip-staging）"
fi

echo ""
echo ">> [2/2] 顺序同步 Job: ${ENABLED_JOBS[*]}"

if ! ./scripts/check-flink-slots.sh; then
  echo "slot 检查未通过，已中止"
  exit 1
fi

first=1
for job in "${ENABLED_JOBS[@]}"; do
  echo ""
  echo "########################################"
  echo "# 流水线 Job: ${job}"
  echo "########################################"
  if [[ "$first" -eq 1 ]]; then
    ./scripts/sync-job-auto.sh "$job" --bulk-start-ms "$SHARED_MS"
    first=0
  else
    ./scripts/sync-job-auto.sh "$job" --bulk-start-ms "$SHARED_MS" --keep-other-jobs
  fi
done

echo ""
echo "=========================================="
echo "流水线完成。bulk-start-ms=${SHARED_MS}"
echo "增量 Job 数（预期 ${#ENABLED_JOBS[@]}）:"
docker exec "${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}" ./bin/flink list 2>/dev/null || true
echo "监控: ./scripts/monitor-sync.sh <表名> 60"
echo "日志: logs/sync-<job>-auto.log"
echo "恢复增量: ./scripts/sync-pipeline-auto.sh --incr-only"
