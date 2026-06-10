#!/usr/bin/env bash
# 读 flink_stg_user_info_ready → 目标 user_info（单表 JDBC，全量/限量）
# 前置: refresh-lm-user-info-gpt-full.sh 或 refresh-lm-user-info-latest100.sh
set -euo pipefail
cd "$(dirname "$0")/.."

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
  key="${line%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
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

SLOTS="${FLINK_TASK_SLOTS:-30}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-30}}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

echo "[$(date '+%F %T')] Flink 写 user_info（flink_stg_user_info_ready → sink）"
echo "  LIMIT=${LIMIT_DESC}  并行=${FLINK_PARALLELISM}"

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

bash scripts/run-sql.sh sql/04_sync_ng_user_info_latest100.sql 2>&1 | tee "$SQL_LOG"
echo "[$(date '+%F %T')] 已提交。Web UI 查看 Job；验证: SELECT COUNT(*) FROM user_info;"
