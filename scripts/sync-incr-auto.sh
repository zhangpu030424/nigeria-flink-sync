#!/usr/bin/env bash
# 增量迁移自动化（只提交 incr Job，不跑全量）
#
# 流程:
#   1. 读取 logs/bulk-start-ms.env（或 --bulk-start-ms）
#   2. deploy-source-ddl
#   3. 默认保留 user_info_dirty（全量期间入队变更由增量 CDC timestamp 消费；清队列仅在 rebuild-all-staging 建宽表前）
#   4. 可选 Cancel 存量 Job
#   5. 默认 CDC_STARTUP_MODE=timestamp（正确性优先，补 bulk-start 后 binlog）
#   6. 按 config/sync-jobs.conf 顺序提交各表增量 Job
#
# 用法:
#   ./scripts/sync-incr-auto.sh
#   ./scripts/sync-incr-auto.sh --jobs user_info
#   ./scripts/sync-incr-auto.sh --startup-mode latest-offset    # 仅追新变更（全量已覆盖缺口时）
#   ./scripts/sync-incr-auto.sh --user-info-latest-offset       # 仅 user_info 用 latest-offset
#   ./scripts/sync-incr-auto.sh --truncate-user-info-dirty   # 可选：增量前再清脏队列（单独重提 incr 时用）
#   ./scripts/sync-incr-auto.sh --keep-user-info-dirty       # 同默认，兼容旧参数
#   ./scripts/sync-incr-auto.sh --bulk-start-ms 1781240247171
#   ./scripts/sync-incr-auto.sh --keep-jobs                     # 不 Cancel
#   ./scripts/sync-incr-auto.sh --verify                        # 提交后跑 user_info 对账
#
set -euo pipefail
cd "$(dirname "$0")/.."

CANCEL_JOBS=1
SKIP_DDL=0
JOBS_FILTER=""
BULK_START_MS_ARG=""
STARTUP_MODE="${CDC_STARTUP_MODE:-timestamp}"
USER_INFO_LATEST=0
TRUNCATE_DIRTY=0
RUN_VERIFY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --jobs=*) JOBS_FILTER="${1#--jobs=}" ;;
    --jobs)
      shift
      JOBS_FILTER="${1:-}"
      ;;
    --bulk-start-ms=*) BULK_START_MS_ARG="${1#--bulk-start-ms=}" ;;
    --bulk-start-ms)
      shift
      BULK_START_MS_ARG="${1:-}"
      ;;
    --startup-mode=*)
      STARTUP_MODE="${1#--startup-mode=}"
      ;;
    --startup-mode)
      shift
      STARTUP_MODE="${1:-timestamp}"
      ;;
    --user-info-latest-offset) USER_INFO_LATEST=1 ;;
    --keep-user-info-dirty) TRUNCATE_DIRTY=0 ;;
    --truncate-user-info-dirty) TRUNCATE_DIRTY=1 ;;
    --skip-ddl) SKIP_DDL=1 ;;
    --keep-jobs) CANCEL_JOBS=0 ;;
    --verify) RUN_VERIFY=1 ;;
    --skip-staging|--skip-vt|--rebuild-vt-cache) ;;  # 全量参数，incr 忽略
    -h|--help)
      sed -n '2,22p' "$0"
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

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

echo "=========================================="
echo "增量迁移 sync-incr-auto"
echo "  FLINK_PARALLELISM_INCR=${FLINK_PARALLELISM_INCR:-?}"
echo "  FLINK_PARALLELISM_USER_INFO=$(sync_job_parallelism user_info incr)（user_info 专用）"
echo "  CDC_STARTUP_MODE=${STARTUP_MODE}"
echo "=========================================="

resolve_bulk_start_ms "$BULK_START_MS_ARG"
SHARED_MS="${BULK_START_MS}"
echo ">> bulk-start-ms=${SHARED_MS}"

if [[ "$SKIP_DDL" -eq 0 ]]; then
  echo ""
  echo ">> [1] 源库 DDL"
  ./scripts/deploy-source-ddl.sh --skip-if-ok
else
  echo ""
  echo ">> [1] 跳过 DDL（--skip-ddl）"
fi

if [[ "$TRUNCATE_DIRTY" -eq 1 ]]; then
  echo ""
  echo ">> [2] 清空 user_info_dirty（--truncate-user-info-dirty）"
  # shellcheck source=scripts/lib/user-info-dirty.sh
  source scripts/lib/user-info-dirty.sh
  truncate_user_info_dirty
  export TRUNCATE_USER_INFO_DIRTY=1
else
  echo ""
  echo ">> [2] 保留 user_info_dirty（默认；bulk 期间入队由增量 timestamp 消费）"
  export SKIP_TRUNCATE_USER_INFO_DIRTY=1
fi

if [[ "$CANCEL_JOBS" -eq 1 ]]; then
  echo ""
  echo ">> [3] Cancel 全部 Running Flink Job"
  bash scripts/cancel-flink-jobs.sh --yes
else
  echo ""
  echo ">> [3] 保留存量 Job（--keep-jobs）"
fi

sync_jobs_load "$JOBS_FILTER"
sync_jobs_print_plan "增量"

echo ""
if ! ./scripts/check-flink-slots.sh; then
  echo "WARN: slot 检查有告警，继续提交增量"
fi

echo ""
echo ">> [4] 提交增量 Job"
first=1
for job in "${SYNC_ENABLED_JOBS[@]}"; do
  job_mode="$STARTUP_MODE"
  if [[ "$job" == "user_info" && "$USER_INFO_LATEST" -eq 1 ]]; then
    job_mode="latest-offset"
  fi
  echo ""
  echo "########################################"
  echo "# 增量 Job: ${job}  mode=${job_mode}"
  echo "########################################"
  export CDC_STARTUP_MODE="$job_mode"
  if [[ "$first" -eq 1 ]]; then
    ./scripts/sync-job-auto.sh "$job" --incr-only --bulk-start-ms "$SHARED_MS"
    first=0
  else
    ./scripts/sync-job-auto.sh "$job" --incr-only --bulk-start-ms "$SHARED_MS" --keep-other-jobs
  fi
done

echo ""
echo "=========================================="
echo "增量 Job 已提交"
docker exec "${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}" ./bin/flink list -r 2>/dev/null || true
echo "  监控: ./scripts/monitor-sync.sh <表名> 60"
echo "  日志: logs/sync-<job>-auto.log"
echo "  对账: bash scripts/verify-user-info-reconcile.sh --sample 500"
echo "=========================================="

if [[ "$RUN_VERIFY" -eq 1 ]]; then
  echo ""
  echo ">> 等待 30s 后跑 user_info 抽样对账..."
  sleep 30
  bash scripts/verify-user-info-reconcile.sh --sample 200 || true
fi
