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
LOG_FILE="${LOG_DIR}/sync-ng-user-info-bulk.log"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-bulk-sql.log"
mkdir -p "$LOG_DIR"

log() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"; }

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

user_info_views_ok() {
  local db="${LM_MYSQL_DATABASE:-ng_loan_market}"
  local cnt
  for v in v_flink_mkt_user v_flink_mkt_user_data v_flink_mkt_log_user_password v_flink_mkt_device_ad_channel; do
    cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
      "$LM_MYSQL_PASSWORD" "$db" \
      "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${db}' AND table_name='${v}';")
    [[ "$cnt" == "1" ]] || return 1
  done
  return 0
}

ensure_flink_views() {
  log "---- 检查老库 VIEW v_flink_mkt_* ----"
  if [[ "${SKIP_LM_VIEW_CREATE:-0}" == "1" ]]; then
    log "SKIP_LM_VIEW_CREATE=1，跳过 VIEW 创建"
    return 0
  fi
  if user_info_views_ok && [[ "${LM_VIEW_REFRESH:-0}" != "1" ]]; then
    log "VIEW 已存在，跳过创建（强制刷新: LM_VIEW_REFRESH=1）"
    return 0
  fi
  if [[ ! -f sql/ddl/lm_user_info_flink_views.sql ]]; then
    log "ERR: 缺少 sql/ddl/lm_user_info_flink_views.sql"
    exit 1
  fi
  if ! MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" \
      -u "$LM_MYSQL_USER" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
      < sql/ddl/lm_user_info_flink_views.sql 2>>"$LOG_FILE"; then
    if user_info_views_ok; then
      log "WARN: CREATE VIEW 无权限，但 v_flink_mkt_* 已存在，继续提交 Job"
      return 0
    fi
    log "ERR: 老库创建 VIEW 失败，请手动执行 sql/ddl/lm_user_info_flink_views.sql"
    exit 1
  fi
  log "VIEW v_flink_mkt_* 已创建/刷新"
}

cancel_stale_jobs() {
  log "---- 取消残留 sink_user_info Job ----"
  local jid
  while read -r jid; do
    [[ -z "$jid" ]] && continue
    log "取消 Job: $jid"
    docker exec "$JM" ./bin/flink cancel "$jid" 2>>"$LOG_FILE" || true
  done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
    | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)
}

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

ensure_flink_views
cancel_stale_jobs

log "user_info 迁移（01_user_info v2 + v_flink_mkt_* VIEW）"
log "  源: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT:-3306}"
log "  目标: ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"
log "  user.id<=${LM_MIGRATION_LIMIT}  fetch=${LM_JDBC_FETCH_SIZE:-20000}  并行度=${FLINK_PARALLELISM}"

bash scripts/run-sql.sh sql/04_sync_ng_user_info_bulk.sql 2>&1 | tee "$SQL_LOG"
