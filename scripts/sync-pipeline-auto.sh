#!/usr/bin/env bash
# 一键流水线：重建全部宽表 → 按序全量(打满核) → 各表自动切增量
#
# 规则:
#   - 每个表全量用 FLINK_PARALLELISM_BULK（默认=FLINK_PARALLELISM）打满 slot
#   - 全量达标后切增量（FLINK_PARALLELISM_INCR，默认 1），仅 cancel 本表全量 Job
#   - 下一表全量开始时，上一表及更早的增量 Job 保持 RUNNING（--keep-other-jobs）
#
# 用法:
#   ./scripts/sync-pipeline-auto.sh                 # 重建宽表 + 顺序同步 6 表
#   ./scripts/sync-pipeline-auto.sh --skip-staging  # 宽表已建好，直接同步
#   ./scripts/sync-pipeline-auto.sh --jobs user,user_bankcard  # 只跑指定 Job
#
# Slot 建议: FLINK_TASK_SLOTS >= FLINK_PARALLELISM_BULK + N表 * FLINK_PARALLELISM_INCR
#   例: slots=25, bulk=20, incr=1, 6表 → 20+6=26 略紧，可 bulk=18 或 slots=30
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_STAGING=0
JOBS_FILTER=""
for arg in "$@"; do
  [[ "$arg" == "--skip-staging" ]] && SKIP_STAGING=1
  [[ "$arg" == --jobs=* ]] && JOBS_FILTER="${arg#--jobs=}"
done

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

BULK_PAR="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
SLOTS="${FLINK_TASK_SLOTS:-16}"
SHARED_MS=$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))")

echo "=========================================="
echo "流水线同步"
echo "  bulk并行=${BULK_PAR}  incr并行=${INCR_PAR}  slots=${SLOTS}"
echo "  共享 bulk-start-ms=${SHARED_MS}"
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

need_slots=$((BULK_PAR + ${#ENABLED_JOBS[@]} * INCR_PAR))
if [[ "$need_slots" -gt "$SLOTS" ]]; then
  echo "WARN: 预估峰值 slot 需求 ${need_slots} > FLINK_TASK_SLOTS=${SLOTS}"
  echo "      末段可能排队；建议增大 slots 或降低 FLINK_PARALLELISM_BULK"
fi

if [[ "$SKIP_STAGING" -eq 0 ]]; then
  echo ""
  echo ">> [1/2] 重建全部宽表 sql/ddl/source_all_sync_staging.sql"
  ./scripts/rebuild-all-staging.sh
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
echo "流水线完成。当前 RUNNING Job（应为 ${#ENABLED_JOBS[@]} 个增量）:"
docker exec "${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}" ./bin/flink list 2>/dev/null || true
echo "监控: ./scripts/monitor-sync.sh <表名> 60"
echo "日志: logs/sync-<job>-auto.log"
