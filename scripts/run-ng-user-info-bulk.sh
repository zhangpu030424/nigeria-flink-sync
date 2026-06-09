#!/usr/bin/env bash
# 老库 ng_loan_market → 目标库 user_info（01_user_info.sql）
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

: "${LM_MYSQL_HOST:?请在 .env 填写 LM_MYSQL_HOST}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-2147483647}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-bulk-sql.log"
mkdir -p "$LOG_DIR"

SLOTS="${FLINK_TASK_SLOTS:-16}"
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
running_n=0
while read -r _jid; do
  [[ -z "$_jid" ]] && continue
  running_n=$((running_n + 1))
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true)
max_bulk=$((SLOTS - running_n * INCR_PAR - 2))
(( max_bulk < 1 )) && max_bulk=1
(( BULK_PARALLEL > max_bulk )) && BULK_PARALLEL=$max_bulk
export FLINK_PARALLELISM="${BULK_PARALLEL}"

echo "[$(date '+%F %T')] user_info 迁移"
echo "  源: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo "  目标: ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"
echo "  LIMIT=${LM_MIGRATION_LIMIT}  并行度=${FLINK_PARALLELISM}"

bash scripts/run-sql.sh sql/04_sync_ng_user_info_bulk.sql 2>&1 | tee "$SQL_LOG"
