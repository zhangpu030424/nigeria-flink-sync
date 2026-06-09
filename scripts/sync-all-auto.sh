#!/usr/bin/env bash
# 按 config/sync-jobs.conf 顺序跑多个 Job：每个全量完成 → 切增量（最终 N 个增量 Job 并行）
#
# 用法:
#   ./scripts/sync-all-auto.sh              # 跑所有 ENABLED=1 的 Job
#   ./scripts/sync-all-auto.sh --incr-only  # 仅提交各 Job 增量（共用同一 bulk-start-ms）
#   ./scripts/sync-all-auto.sh --no-vt
#
set -euo pipefail
cd "$(dirname "$0")/.."

INCR_ONLY=0
EXTRA_ARGS=()
for arg in "$@"; do
  [[ "$arg" == "--incr-only" ]] && INCR_ONLY=1
  [[ "$arg" == "--no-vt" ]] && EXTRA_ARGS+=("--no-vt")
done

CONF="config/sync-jobs.conf"
[[ -f "$CONF" ]] || { echo "缺少 $CONF"; exit 1; }

JOBS=()
while IFS= read -r row || [[ -n "$row" ]]; do
  row="${row%%#*}"
  row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$row" ]] && continue
  key="${row%%|*}"
  enabled="${row##*|}"
  [[ "$enabled" == "1" ]] && JOBS+=("$key")
done < "$CONF"

if [[ ${#JOBS[@]} -eq 0 ]]; then
  echo "无 ENABLED=1 的 Job，请编辑 $CONF"
  exit 1
fi

SHARED_BULK_MS=$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))")
echo ">> 多 Job 编排: ${JOBS[*]}"
echo ">> 共享 bulk-start-ms: ${SHARED_BULK_MS}（各 Job 增量从此时间点补 binlog）"

run_job() {
  local job=$1
  shift
  if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    ./scripts/sync-job-auto.sh "$job" "$@" "${EXTRA_ARGS[@]}"
  else
    ./scripts/sync-job-auto.sh "$job" "$@"
  fi
}

if [[ "$INCR_ONLY" -eq 1 ]]; then
  for job in "${JOBS[@]}"; do
    echo ""
    echo "========== 增量: $job =========="
    run_job "$job" --incr-only --keep-other-jobs --bulk-start-ms "$SHARED_BULK_MS"
  done
  exit 0
fi

first=1
for job in "${JOBS[@]}"; do
  echo ""
  echo "========== Job: $job =========="
  if [[ "$first" -eq 1 ]]; then
    run_job "$job" --bulk-start-ms "$SHARED_BULK_MS"
  else
    run_job "$job" --bulk-start-ms "$SHARED_BULK_MS" --keep-other-jobs
  fi
  first=0
done

echo ""
echo ">> 全部 Job 已切增量。当前 RUNNING:"
docker exec "${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}" ./bin/flink list 2>/dev/null || true
