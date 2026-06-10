#!/usr/bin/env bash
# 老库 ng_loan_market.id_add_user 宽表 → 目标 user（独立 Batch Job，不改 SOURCE_MYSQL_* incr）
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

# shellcheck disable=SC1091
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
POLL_SEC="${LM_SYNC_POLL_SEC:-3}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-id-add-user-bulk.log"
SQL_LOG="${LOG_DIR}/sync-id-add-user-bulk-sql.log"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# 全量默认不加 LIMIT（2147483647 会触发 Flink SortLimit OOM）
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY user_id LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
elif [[ "$LM_MIGRATION_LIMIT" == "2147483647" ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量（已忽略 2147483647，避免 SortLimit OOM）"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量"
fi

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

list_running_job_ids() {
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

latest_job_id() {
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/overview" 2>/dev/null \
    | grep -oE '"jid":"[a-f0-9]{32}"' | tail -1 | cut -d'"' -f4 || true
}

capture_new_job_id() {
  local before="$1"
  local i id
  for i in 1 2 3 4 5 6 7 8 9 10; do
    sleep 2
    for id in $(list_running_job_ids); do
      if ! echo " $before " | grep -q " $id "; then
        echo "$id"
        return 0
      fi
    done
    id=$(latest_job_id || true)
    if [[ -n "$id" ]] && ! echo " $before " | grep -q " $id "; then
      echo "$id"
      return 0
    fi
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
  local jid="${1:-}"
  [[ -z "$jid" ]] && return 0
  local raw
  raw=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}/exceptions?maxExceptions=5" 2>/dev/null || true)
  [[ -z "$raw" ]] && return 0
  log "---- Flink 异常堆栈 ----"
  echo "$raw" | python3 -c "
import json, sys, html, re
raw = sys.stdin.read().strip()
if not raw:
    sys.exit(0)
try:
    d = json.loads(raw)
except json.JSONDecodeError:
    print(raw[:4000])
    sys.exit(0)
for i, item in enumerate(d.get('all-exceptions', [])[:3], 1):
    ts = item.get('timestamp', '')
    exc = item.get('exception', '') or ''
    exc = html.unescape(exc)
    exc = re.sub(r'<br/?>', '\n', exc, flags=re.I)
    exc = re.sub(r'<[^>]+>', '', exc)
    print(f'--- exception #{i} @ {ts} ---')
    print(exc[:6000])
" 2>/dev/null | tee -a "$LOG_FILE" || true
}

view_exists() {
  local cnt
  cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}' AND table_name='v_id_add_user_flink';")
  [[ "$cnt" == "1" ]] && return 0
  cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM v_id_add_user_flink LIMIT 1;")
  [[ "$cnt" =~ ^[0-9]+$ ]]
}

ensure_flink_view() {
  log "---- 检查老库 VIEW v_id_add_user_flink ----"
  if [[ "${SKIP_LM_VIEW_CREATE:-0}" == "1" ]]; then
    log "SKIP_LM_VIEW_CREATE=1，跳过 VIEW 创建"
    return 0
  fi
  if view_exists && [[ "${LM_VIEW_REFRESH:-0}" != "1" ]]; then
    log "VIEW 已存在，跳过创建（已手动建 VIEW 时正常；强制刷新: LM_VIEW_REFRESH=1）"
    return 0
  fi
  if [[ ! -f sql/ddl/lm_id_add_user_flink_view.sql ]]; then
    log "ERR: 缺少 sql/ddl/lm_id_add_user_flink_view.sql"
    exit 1
  fi
  if ! MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
      -u "$LM_MYSQL_USER" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
      < sql/ddl/lm_id_add_user_flink_view.sql 2>>"$LOG_FILE"; then
    if view_exists; then
      log "WARN: CREATE VIEW 无权限，但 v_id_add_user_flink 已存在，继续提交 Job"
      return 0
    fi
    log "ERR: 老库创建 VIEW 失败且无可用 VIEW，请手动执行 sql/ddl/lm_id_add_user_flink_view.sql"
    exit 1
  fi
  log "VIEW v_id_add_user_flink 已创建/刷新"
}

preflight_check() {
  log "---- 提交前检查 ----"
  local src_ok view_ok tgt_ok bad_cnt valid_cnt
  src_ok=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}' AND table_name='id_add_user';")
  view_ok=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}' AND table_name='v_id_add_user_flink';")
  tgt_ok=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${TARGET_MYSQL_DATABASE}' AND table_name='user';")
  bad_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM id_add_user WHERE user_id IS NULL OR app_id IS NULL OR mobile IS NULL OR TRIM(mobile)='';")
  valid_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM id_add_user WHERE user_id IS NOT NULL AND app_id IS NOT NULL AND mobile IS NOT NULL AND TRIM(mobile)<>'';")

  log "源表 id_add_user 存在=${src_ok}  VIEW=${view_ok}  可同步行数=${valid_cnt}  无效行=${bad_cnt}"
  log "目标表 user 存在=${tgt_ok}  库=${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"

  if [[ "$src_ok" != "1" ]]; then
    log "ERR: 源库找不到 id_add_user"
    exit 1
  fi
  if [[ "$view_ok" != "1" ]] && ! view_exists; then
    log "ERR: 源库找不到 v_id_add_user_flink VIEW（可手动建 VIEW 或 SKIP_LM_VIEW_CREATE=1）"
    exit 1
  fi
  if [[ "$tgt_ok" != "1" ]]; then
    log "ERR: 目标库找不到 user 表，请先执行 Target.sql 建表"
    exit 1
  fi
  if [[ "$valid_cnt" == "0" || "$valid_cnt" == "ERR" ]]; then
    log "ERR: 源表没有可同步的有效行（user_id/mobile 为空）"
    exit 1
  fi
}

print_job_diagnostics() {
  log "---- Flink 诊断 ----"
  docker exec "$JM" ./bin/flink list 2>&1 | tee -a "$LOG_FILE" || true
  if [[ -f "$SQL_LOG" ]]; then
    log "---- sql-client 末尾 40 行 ----"
    tail -40 "$SQL_LOG" | tee -a "$LOG_FILE"
  fi
  docker logs --tail 80 "${FLINK_TASKMANAGER_CONTAINER:-nigeria-flink-taskmanager}" 2>&1 \
    | grep -iE 'Exception|Error|FAILED|SQLException|id_add_user|sink_user' | tail -30 \
    | tee -a "$LOG_FILE" || true
}

monitor_loop() {
  local job_id="${1:-}"
  local src_sql="SELECT COUNT(*) FROM id_add_user;"
  local tgt_sql="SELECT COUNT(*) FROM \`user\`;"

  log "监控开始 间隔=${POLL_SEC}s  (Ctrl+C 停止)"
  [[ -n "$job_id" ]] && log "Flink Job: ${job_id}"

  local prev_target="" prev_ts
  prev_ts=$(date +%s)
  local round=0

  while true; do
    round=$((round + 1))
    local now_ts
    now_ts=$(date +%s)

    local src_cnt tgt_cnt
    src_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
      "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" "$src_sql")
    tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
      "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$tgt_sql")

    local delta="n/a" rate_sec="n/a" progress="n/a"
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
      delta=$((tgt_cnt - prev_target))
      local elapsed=$((now_ts - prev_ts))
      if (( elapsed > 0 )); then
        rate_sec=$(awk "BEGIN {printf \"%.1f\", $delta / $elapsed}")
      fi
    fi
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
      progress=$(awk "BEGIN {printf \"%.2f%%\", $tgt_cnt * 100 / $src_cnt}")
    fi

    local job_state flink_out
    job_state=$(flink_job_state "$job_id")
    flink_out=$(flink_records_out "$job_id")

    log "[#${round}] Job=${job_state} 源id_add_user=${src_cnt} 目标user=${tgt_cnt} 进度=${progress} 本段+${delta} 速率≈${rate_sec}条/秒 Flink写出=${flink_out}"

    if [[ -n "$job_id" && "$job_state" =~ ^(FINISHED|FAILED|CANCELED)$ ]]; then
      log "Job 已结束 state=${job_state}"
      if [[ "$job_state" == "FAILED" ]]; then
        print_job_exception "$job_id"
        print_job_diagnostics
      fi
      break
    fi

    prev_target="$tgt_cnt"
    prev_ts=$now_ts
    sleep "$POLL_SEC"
  done
}

if [[ "$MONITOR_ONLY" -eq 1 ]]; then
  [[ -z "$JOB_ID_ARG" ]] && { echo "用法: bash $0 --monitor-only <job_id>"; exit 1; }
  monitor_loop "$JOB_ID_ARG"
  exit 0
fi

SLOTS="${FLINK_TASK_SLOTS:-16}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
# 小表直读无分区，默认并行度 1 更稳；可 export FLINK_PARALLELISM=4 覆盖
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-1}}"

running_n=0
while read -r _jid; do
  [[ -z "$_jid" ]] && continue
  running_n=$((running_n + 1))
done < <(list_running_job_ids)

max_bulk=$((SLOTS - running_n * INCR_PAR - 2))
(( max_bulk < 1 )) && max_bulk=1
(( BULK_PARALLEL > max_bulk )) && BULK_PARALLEL=$max_bulk
(( BULK_PARALLEL > 4 )) && BULK_PARALLEL=4
export FLINK_PARALLELISM="${BULK_PARALLEL}"

log "源: ${LM_MYSQL_DATABASE:-ng_loan_market}.id_add_user @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
log "目标: ${TARGET_MYSQL_DATABASE}.user @ ${TARGET_MYSQL_HOST}"
log "模式: 宽表直传  LIMIT=${LIMIT_DESC}  并行度=${FLINK_PARALLELISM}"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  log "ERR: 容器 ${JM} 未运行"
  exit 1
fi

ensure_flink_view
preflight_check

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
log "提交前 RUNNING: ${BEFORE_JOBS:-<无>}"

set +e
bash scripts/run-sql.sh sql/05_sync_id_add_user_bulk.sql 2>&1 | tee "$SQL_LOG"
SQL_RC=${PIPESTATUS[0]}
set -e

JOB_ID=$(capture_new_job_id "$BEFORE_JOBS" || true)
log "sql-client 退出码=${SQL_RC}  Job id=${JOB_ID:-<未捕获>}"

if [[ "$SQL_RC" -ne 0 ]]; then
  log "ERR: sql-client 失败"
  print_job_diagnostics
  exit "$SQL_RC"
fi

if [[ -z "${JOB_ID:-}" ]]; then
  JOB_ID=$(latest_job_id || true)
  log "最近 Job: ${JOB_ID:-<无>}（batch 可能已秒完成）"
fi

monitor_loop "${JOB_ID:-}"

final_state=$(flink_job_state "${JOB_ID:-}")
if [[ "$final_state" == "FAILED" ]]; then
  exit 1
fi
