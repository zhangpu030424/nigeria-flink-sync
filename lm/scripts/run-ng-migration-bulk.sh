#!/usr/bin/env bash
# 老库 ng_loan_market + ng_loan_core → 目标库 5 表批量迁移
# SQL: sql/04_sync_ng_migration_bulk.sql（ng_migration_flink.sql）
# 试跑: LM_MIGRATION_LIMIT=20  全量: LM_MIGRATION_LIMIT=2147483647（默认）
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

export LM_CORE_MYSQL_HOST="${LM_CORE_MYSQL_HOST:-$LM_MYSQL_HOST}"
export LM_CORE_MYSQL_PORT="${LM_CORE_MYSQL_PORT:-${LM_MYSQL_PORT:-3306}}"
export LM_CORE_MYSQL_USER="${LM_CORE_MYSQL_USER:-$LM_MYSQL_USER}"
export LM_CORE_MYSQL_PASSWORD="${LM_CORE_MYSQL_PASSWORD:-$LM_MYSQL_PASSWORD}"
export LM_CORE_MYSQL_DATABASE="${LM_CORE_MYSQL_DATABASE:-ng_loan_core}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-2147483647}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-ng-migration-bulk.log"
SQL_LOG="${LOG_DIR}/sync-ng-migration-bulk-sql.log"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

list_running_job_ids() {
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

SLOTS="${FLINK_TASK_SLOTS:-16}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
SLOT_BUFFER="${SYNC_SLOT_BUFFER:-2}"
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"

running_n=0
while read -r _jid; do
  [[ -z "$_jid" ]] && continue
  running_n=$((running_n + 1))
done < <(list_running_job_ids)

reserved=$((running_n * INCR_PAR + SLOT_BUFFER))
max_bulk=$((SLOTS - reserved))
(( max_bulk < 1 )) && max_bulk=1
if (( BULK_PARALLEL > max_bulk )); then
  log "WARN: 并行度 ${BULK_PARALLEL} → ${max_bulk}（slots=${SLOTS} 存量Job=${running_n}）"
  BULK_PARALLEL=$max_bulk
fi
export FLINK_PARALLELISM="${BULK_PARALLEL}"

log "老库 market: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
log "老库 core:  ${LM_CORE_MYSQL_DATABASE}@${LM_CORE_MYSQL_HOST}:${LM_CORE_MYSQL_PORT}"
log "目标库:     ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}"
log "LIMIT:      ${LM_MIGRATION_LIMIT}  并行度: ${FLINK_PARALLELISM}"
log "提交: sql/04_sync_ng_migration_bulk.sql"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  log "ERR: 容器 ${JM} 未运行"
  exit 1
fi

set +e
bash scripts/run-sql.sh sql/04_sync_ng_migration_bulk.sql 2>&1 | tee "$SQL_LOG"
SQL_RC=${PIPESTATUS[0]}
set -e

if [[ "$SQL_RC" -ne 0 ]]; then
  log "ERR: sql-client 失败 exit=${SQL_RC}，见 ${SQL_LOG}"
  exit "$SQL_RC"
fi

log "完成。5 步 INSERT 已提交（batch）。验证目标库:"
log "  user_info / user_bankcard / user_product / application / loan COUNT(*)"
