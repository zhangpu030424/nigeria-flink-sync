#!/usr/bin/env bash
# 老库 ng_loan_market 多表 → 目标 user_info（VIEW 预聚合 + JSON，独立 Batch Job）
set -euo pipefail
cd "$(dirname "$0")/.."

MONITOR_ONLY=0
JOB_ID_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --monitor-only)
      MONITOR_ONLY=1
      JOB_ID_ARG="${2:-}"
      shift 2
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
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
  key="${line%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

: "${LM_MYSQL_HOST:?请在 .env 填写 LM_MYSQL_HOST}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
POLL_SEC="${LM_SYNC_POLL_SEC:-5}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-ng-user-info-bulk.log"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-bulk-sql.log"
mkdir -p "$LOG_DIR"

REQUIRED_VIEWS=(
  v_flink_mkt_user
  v_flink_ud_latest
  v_flink_lup_latest
  v_flink_dac_latest
)

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY user_id LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
elif [[ "$LM_MIGRATION_LIMIT" == "2147483647" ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量（已忽略 2147483647）"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量"
fi

MYSQL_PROBE_TIMEOUT="${LM_MYSQL_PROBE_TIMEOUT:-15}"

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql --connect-timeout=10 -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

mysql_probe() {
  local sql=$1
  timeout "$MYSQL_PROBE_TIMEOUT" env MYSQL_PWD="$LM_MYSQL_PASSWORD" \
    mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" \
    "${LM_MYSQL_DATABASE:-ng_loan_market}" -N -e "$sql" 2>/dev/null
}

view_in_schema() {
  local name=$1
  local cnt
  cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}' AND table_name='${name}';")
  [[ "$cnt" == "1" ]]
}

view_probe_one_row() {
  local name=$1
  mysql_probe "SELECT 1 FROM \`${name}\` LIMIT 1;" | grep -q '^1$'
}

all_views_ready() {
  local v
  for v in "${REQUIRED_VIEWS[@]}"; do
    log "  元数据检查 ${v} ..."
    view_in_schema "$v" || return 1
  done
  return 0
}

ensure_flink_views() {
  log "---- 检查老库 user_info VIEW（仅 information_schema，不做 COUNT 全表）----"
  if [[ "${SKIP_LM_VIEW_CREATE:-0}" == "1" ]]; then
    log "SKIP_LM_VIEW_CREATE=1，跳过 VIEW 创建/检查"
    return 0
  fi
  if all_views_ready && [[ "${LM_VIEW_REFRESH:-0}" != "1" ]]; then
    log "VIEW 元数据已存在: ${REQUIRED_VIEWS[*]}"
    return 0
  fi
  if [[ ! -f sql/ddl/lm_user_info_flink_views.sql ]]; then
    log "ERR: 缺少 sql/ddl/lm_user_info_flink_views.sql"
    exit 1
  fi
  if ! MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
      -u "$LM_MYSQL_USER" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
      < sql/ddl/lm_user_info_flink_views.sql 2>>"$LOG_FILE"; then
    if all_views_ready; then
      log "WARN: CREATE VIEW 无权限，但 VIEW 已存在，继续"
      return 0
    fi
    log "ERR: 请用有权限账号手动执行 sql/ddl/lm_user_info_flink_views.sql"
    exit 1
  fi
  log "VIEW 已创建/刷新"
}

preflight_check() {
  log "---- 提交前检查 ----"
  local v
  for v in "${REQUIRED_VIEWS[@]}"; do
    if ! view_in_schema "$v"; then
      log "ERR: information_schema 中无 VIEW ${v}"
      exit 1
    fi
  done
  if [[ "${LM_SKIP_VIEW_PROBE:-0}" != "1" ]]; then
    log "探测 VIEW 可读性（${MYSQL_PROBE_TIMEOUT}s 超时，v_flink_ud_latest 聚合慢可设 LM_SKIP_VIEW_PROBE=1）"
    for v in "${REQUIRED_VIEWS[@]}"; do
      log "  探测 SELECT 1 FROM ${v} LIMIT 1 ..."
      if ! view_probe_one_row "$v"; then
        log "WARN: ${v} 探测超时/失败；若 VIEW 已手动建好可 LM_SKIP_VIEW_PROBE=1 继续"
        if [[ "${LM_SKIP_VIEW_PROBE:-0}" != "1" ]]; then
          log "ERR: 探测失败。大表 VIEW 首次聚合很慢，可: LM_SKIP_VIEW_PROBE=1 bash scripts/run-ng-user-info-bulk.sh"
          exit 1
        fi
      fi
    done
  else
    log "LM_SKIP_VIEW_PROBE=1，跳过 VIEW 数据探测"
  fi
  local tgt_ok
  tgt_ok=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${TARGET_MYSQL_DATABASE}' AND table_name='user_info';")
  log "目标 user_info 表存在=${tgt_ok}（源表行数不在此 COUNT，避免扫两千万 VIEW）"
  if [[ "$tgt_ok" != "1" ]]; then
    log "ERR: 目标库无 user_info 表"
    exit 1
  fi
}

list_running_job_ids() {
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

latest_job_id() {
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/overview" 2>/dev/null \
    | grep -oE '"jid":"[a-f0-9]{32}"' | tail -1 | cut -d'"' -f4 || true
}

capture_new_job_id() {
  local before="$1" i id
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 2
    for id in $(list_running_job_ids); do
      if ! echo " $before " | grep -q " $id "; then echo "$id"; return 0; fi
    done
    id=$(latest_job_id || true)
    if [[ -n "$id" ]] && ! echo " $before " | grep -q " $id "; then echo "$id"; return 0; fi
  done
  list_running_job_ids | tail -1
}

flink_job_state() {
  local jid="${1:-}"
  [[ -z "$jid" ]] && echo "unknown" && return 0
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}" 2>/dev/null \
    | grep -oE '"state":"[A-Z_]+"' | head -1 | cut -d'"' -f4 || echo "unknown"
}

flink_records_out() {
  local jid="${1:-}"
  [[ -z "$jid" ]] && echo "n/a" && return 0
  local raw
  raw=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}/metrics?get=0.numRecordsOut" 2>/dev/null || true)
  echo "$raw" | grep -oE '"value":"[0-9]+"' | head -1 | grep -oE '[0-9]+' || echo "n/a"
}

print_job_exception() {
  local jid="${1:-}" raw
  [[ -z "$jid" ]] && return 0
  raw=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}/exceptions?maxExceptions=5" 2>/dev/null || true)
  [[ -z "$raw" ]] && return 0
  log "---- Flink 异常 ----"
  echo "$raw" | python3 -c "
import json,sys,html,re
try:
    d=json.loads(sys.stdin.read())
    for i,x in enumerate(d.get('all-exceptions',[])[:2],1):
        e=html.unescape(x.get('exception','') or '')
        e=re.sub(r'<br/?>','\n',e,flags=re.I)
        e=re.sub(r'<[^>]+>','',e)
        print('---',i,'---'); print(e[:5000])
except Exception: print(sys.stdin.read()[:3000])
" 2>/dev/null | tee -a "$LOG_FILE" || true
}

monitor_loop() {
  local job_id="${1:-}"
  log "监控开始 Job=${job_id:-?} 间隔=${POLL_SEC}s"
  local prev="" prev_ts round=0
  prev_ts=$(date +%s)
  while true; do
    round=$((round + 1))
    local now_ts tgt_cnt delta rate job_state out
    now_ts=$(date +%s)
    tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
      "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "SELECT COUNT(*) FROM user_info;")
    delta="n/a"; rate="n/a"
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev" =~ ^[0-9]+$ ]]; then
      delta=$((tgt_cnt - prev))
      local el=$((now_ts - prev_ts))
      (( el > 0 )) && rate=$(awk "BEGIN {printf \"%.1f\", $delta / $el}")
    fi
    job_state=$(flink_job_state "$job_id")
    out=$(flink_records_out "$job_id")
    log "[#${round}] Job=${job_state} 目标user_info=${tgt_cnt} 本段+${delta} 速率≈${rate}条/秒 Flink写出=${out}"
    if [[ -n "$job_id" && "$job_state" =~ ^(FINISHED|FAILED|CANCELED)$ ]]; then
      [[ "$job_state" == "FAILED" ]] && print_job_exception "$job_id"
      break
    fi
    prev="$tgt_cnt"; prev_ts=$now_ts
    sleep "$POLL_SEC"
  done
}

if [[ "$MONITOR_ONLY" -eq 1 ]]; then
  monitor_loop "$JOB_ID_ARG"
  exit 0
fi

SLOTS="${FLINK_TASK_SLOTS:-16}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
USER_INFO_MAX_PAR="${LM_USER_INFO_MAX_PARALLEL:-16}"
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-16}}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

running_n=0
while read -r _jid; do [[ -n "$_jid" ]] && running_n=$((running_n + 1)); done < <(list_running_job_ids)
max_bulk=$((SLOTS - running_n * INCR_PAR - 2))
(( max_bulk < 1 )) && max_bulk=1
(( BULK_PARALLEL > max_bulk )) && BULK_PARALLEL=$max_bulk
(( BULK_PARALLEL > USER_INFO_MAX_PAR )) && BULK_PARALLEL=$USER_INFO_MAX_PAR
export FLINK_PARALLELISM="${BULK_PARALLEL}"

log "user_info 迁移"
log "  源: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
log "  目标: ${TARGET_MYSQL_DATABASE}.user_info @ ${TARGET_MYSQL_HOST}"
log "  LIMIT=${LIMIT_DESC}  并行度=${FLINK_PARALLELISM}"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  log "ERR: 容器 ${JM} 未运行"
  exit 1
fi

ensure_flink_views
preflight_check

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
set +e
bash scripts/run-sql.sh sql/04_sync_ng_user_info_bulk.sql 2>&1 | tee "$SQL_LOG"
SQL_RC=${PIPESTATUS[0]}
set -e

JOB_ID=$(capture_new_job_id "$BEFORE_JOBS" || true)
log "sql-client 退出码=${SQL_RC}  Job=${JOB_ID:-?}"
[[ "$SQL_RC" -ne 0 ]] && exit "$SQL_RC"

monitor_loop "${JOB_ID:-}"
final_state=$(flink_job_state "${JOB_ID:-}")
[[ "$final_state" == "FAILED" ]] && exit 1
