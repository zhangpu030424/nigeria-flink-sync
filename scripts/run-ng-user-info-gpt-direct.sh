#!/usr/bin/env bash
# GPT user_info：MySQL VIEW 拼 JSON + Flink 单表写（无物化表、无分区、无 Flink 多表 JOIN）
# 用法: bash scripts/run-ng-user-info-gpt-direct.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-direct.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-20}}"
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

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-20}}"
if [[ "${FLINK_PARALLELISM}" -gt "${FLINK_TASK_SLOTS:-20}" ]]; then
  export FLINK_PARALLELISM="${FLINK_TASK_SLOTS}"
  export FLINK_PARALLELISM_BULK="${FLINK_TASK_SLOTS}"
fi
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

export LM_SRC_TABLE_READY="${LM_SRC_TABLE_READY:-v_flink_gpt_user_info_sink}"
export LM_USER_ID_RANGE_CLAUSE="${LM_USER_ID_RANGE_CLAUSE:-}"

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY CAST(user_id AS BIGINT) LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量${LM_USER_ID_RANGE_CLAUSE:+（分段）}"
fi

# shellcheck source=lib/lm-mysql-write.sh
source "$(dirname "$0")/lib/lm-mysql-write.sh"
# shellcheck source=lib/lm-user-info-gpt-setup.sh
source "$(dirname "$0")/lib/lm-user-info-gpt-setup.sh"

if [[ "$LM_SRC_TABLE_READY" == "v_flink_gpt_user_info_sink" ]]; then
  lm_gpt_ensure_ready
  lm_gpt_probe_sink_read
else
  echo ">> 读实体表 ${LM_SRC_TABLE_READY}（跳过 VIEW 检查）"
fi

echo "[$(date '+%F %T')] GPT user_info 单表 Flink（源=${LM_SRC_TABLE_READY}）"
echo "  读: ${LM_MYSQL_HOST}:${LM_MYSQL_PORT:-3306}/${LM_MYSQL_DATABASE}"
echo "  parallelism.default=${FLINK_PARALLELISM}  LIMIT=${LIMIT_DESC}"
[[ -n "$LM_USER_ID_RANGE_CLAUSE" ]] && echo "  分段: ${LM_USER_ID_RANGE_CLAUSE}"

bash scripts/check-flink-slots.sh || true

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

BEFORE=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | tr '\n' ' ' || true)
bash scripts/run-sql.sh sql/04_sync_ng_gpt_user_info_one.sql

sleep 3
JOB_ID=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | while read -r j; do
  [[ " $BEFORE " == *" $j "* ]] && continue
  echo "$j"
done | head -1)

if [[ -n "${JOB_ID:-}" ]]; then
  echo ">> Job=${JOB_ID}  Web UI: http://127.0.0.1:${FLINK_WEB_PORT}/#/job/${JOB_ID}/overview"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for v in d.get('vertices',[]):
    print(f\"  {v.get('name','')[:72]}  p={v.get('parallelism')}\")
" || true
  echo "$JOB_ID"
fi

echo "[$(date '+%F %T')] 已提交 sink_user_info Job${JOB_ID:+ id=${JOB_ID}}"
