#!/usr/bin/env bash
# 检查 TaskManager slot 数 vs .env 中 FLINK_PARALLELISM
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"

if [[ -f .env ]]; then
  set -a
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    export "$line"
  done < .env
  set +a
fi

PAR="${FLINK_PARALLELISM:-16}"
SLOTS_ENV="${FLINK_TASK_SLOTS:-16}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"

echo ">> .env: FLINK_PARALLELISM=${PAR}  FLINK_TASK_SLOTS=${SLOTS_ENV}"

TOTAL_SLOTS=0
FREE_SLOTS=0
RUNNING_JOBS=0

if docker ps --format '{{.Names}}' | grep -qx "$JM"; then
  echo ">> Flink Web UI TaskManagers:"
  TM_JSON=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/taskmanagers" 2>/dev/null || true)
  if [[ -n "$TM_JSON" ]]; then
    while IFS= read -r line; do
      case "$line" in
        STATS*)
          read -r TOTAL_SLOTS FREE_SLOTS <<< "${line#STATS }"
          ;;
        *)
          echo "  $line"
          ;;
      esac
    done < <(echo "$TM_JSON" | python3 -c "
import json, sys
d = json.load(sys.stdin)
t = f = 0
for tm in d.get('taskmanagers', []):
    s = int(tm.get('slotsNumber', 0) or 0)
    fs = int(tm.get('freeSlots', 0) or 0)
    t += s
    f += fs
    tid = str(tm.get('id', ''))[:8]
    print(f'TM {tid}... slots={s} free={fs}')
print(f'合计 slots={t} 空闲={f}')
print(f'STATS {t} {f}')
" 2>/dev/null || echo "REST API 解析失败")
  else
    echo "  REST API 不可用（curl http://127.0.0.1:${FLINK_WEB_PORT}/taskmanagers）"
  fi

  RUNNING_JOBS=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | wc -l | tr -d ' ' || true)
  RUNNING_JOBS="${RUNNING_JOBS:-0}"
  echo ">> Running Jobs: ${RUNNING_JOBS}"
else
  echo ">> JobManager 未运行"
fi

echo ""
if (( PAR > SLOTS_ENV )); then
  echo "✗ FLINK_PARALLELISM(${PAR}) > FLINK_TASK_SLOTS(${SLOTS_ENV})"
  echo "  请降低并行或增大 FLINK_TASK_SLOTS"
  exit 1
fi

if (( RUNNING_JOBS > 0 && FREE_SLOTS < PAR )); then
  echo "⚠ 存量 Job=${RUNNING_JOBS}，空闲 slot=${FREE_SLOTS} < 并行 ${PAR}"
  echo "  全量迁移建议: bash scripts/cancel-flink-jobs.sh --yes"
elif (( PAR == SLOTS_ENV && FREE_SLOTS >= PAR && RUNNING_JOBS == 0 )); then
  echo "✓ 独占全量模式: ${PAR} 并行 + ${FREE_SLOTS} 空闲 slot，可直接跑 bulk-max"
elif (( PAR > SLOTS_ENV / 2 )); then
  echo "⚠ parallelism=${PAR} 占 slot 比例较高（${PAR}/${SLOTS_ENV}）"
  echo "  单 Job 独占 Batch 全量时通常 OK；与 incr 同跑请先 cancel-flink-jobs"
else
  echo "✓ 并行 ${PAR} / slots ${SLOTS_ENV}，配置合理"
fi

echo ">> 列表页 Tasks=算子个数；真并行看 Job Overview 的 Parallelism 列"
exit 0
