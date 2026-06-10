#!/usr/bin/env bash
# 读 flink_stg_user_info_ready → 目标 user_info（单表 JDBC，全量/限量）
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

# 先 export BULK 并行，再 load .env（避免 FLINK_PARALLELISM=1 覆盖）
SLOTS="${FLINK_TASK_SLOTS:-40}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"

set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${line%%=*}"
  [[ "$key" == "FLINK_PARALLELISM" ]] && continue
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

# 再次锁定 bulk 并行（.env 不能改回去）
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-40}}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-gpt-ready-sql.log"
mkdir -p "$LOG_DIR"

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY CAST(user_id AS UNSIGNED) LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量"
fi

echo "[$(date '+%F %T')] Flink 写 user_info（parallelism=${FLINK_PARALLELISM} slots=${FLINK_TASK_SLOTS:-?} LIMIT=${LIMIT_DESC}）"

bash scripts/check-flink-slots.sh || true
TM_SLOTS=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/taskmanagers" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(int(t.get('slotsNumber',0) or 0) for t in d.get('taskmanagers',[])))
" 2>/dev/null || echo "0")
if [[ "$TM_SLOTS" =~ ^[0-9]+$ && "$TM_SLOTS" -gt 0 && "$TM_SLOTS" -lt "$FLINK_PARALLELISM" ]]; then
  echo "ERR: TaskManager 实际 slots=${TM_SLOTS} < 请求并行 ${FLINK_PARALLELISM}"
  echo "  请: FLINK_TASK_SLOTS=${FLINK_PARALLELISM} docker compose up -d --force-recreate taskmanager"
  exit 1
fi

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

BEFORE=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | tr '\n' ' ')
bash scripts/run-sql.sh sql/04_sync_ng_user_info_latest100.sql 2>&1 | tee "$SQL_LOG"

sleep 3
JOB_ID=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | while read -r j; do
  [[ " $BEFORE " == *" $j "* ]] && continue
  echo "$j"
done | head -1)

if [[ -n "${JOB_ID:-}" ]]; then
  echo ">> Job=${JOB_ID} 算子并行度:"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for v in d.get('vertices',[]):
    print(f\"  {v.get('name','')[:70]}  parallelism={v.get('parallelism')}\")
" || true
  MIN_P=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
ps=[v.get('parallelism',0) for v in d.get('vertices',[]) if 'Source' in v.get('name','')]
print(min(ps) if ps else 0)
" || echo "0")
  if [[ "$MIN_P" =~ ^[0-9]+$ && "$MIN_P" -lt 8 ]]; then
    echo "ERR: Source 并行=${MIN_P}，期望≥8。cancel Job 并检查 .env FLINK_PARALLELISM_BULK"
    docker exec "$JM" ./bin/flink cancel "$JOB_ID" 2>/dev/null || true
    exit 1
  fi
fi

echo "[$(date '+%F %T')] 已提交。Web UI → Job Overview → Parallelism 列应≈${FLINK_PARALLELISM}"
