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

echo ">> .env: FLINK_PARALLELISM=${PAR}  FLINK_TASK_SLOTS=${SLOTS_ENV}"

if docker ps --format '{{.Names}}' | grep -qx "$JM"; then
  echo ">> Flink Web UI TaskManagers:"
  FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/taskmanagers" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    total=0
    for tm in d.get('taskmanagers',[]):
        s=tm.get('slotsNumber',0)
        f=tm.get('freeSlots',0)
        total+=s
        print(f\"  TM {tm.get('id','')[:8]}... slots={s} free={f}\")
    print(f'  合计 slots={total}')
except Exception as e:
    print('  无法解析 TaskManagers:', e)
" 2>/dev/null || echo "  REST API 不可用"
else
  echo ">> JobManager 未运行"
fi

echo ""
if (( PAR > SLOTS_ENV )); then
  echo "✗ FLINK_PARALLELISM(${PAR}) > FLINK_TASK_SLOTS(${SLOTS_ENV})"
  echo "  JDBC Sink 作业通常还需 Source+Sink 各一组 slot，建议:"
  echo "    FLINK_PARALLELISM <= FLINK_TASK_SLOTS / 2"
  echo "  例如 slots=16 时 parallelism 用 8"
  exit 1
fi
if (( PAR > SLOTS_ENV / 2 )); then
  echo "⚠ parallelism=${PAR} 偏高，单 TM ${SLOTS_ENV} slot 时可能触发 NoResourceAvailableException"
  echo "  建议 FLINK_PARALLELISM=8（slots=16）或 FLINK_TASK_SLOTS=40 + parallelism=20"
fi
echo ">> 当前配置可尝试启动；若 Job 一直 RESTARTING，请降低 parallelism"
