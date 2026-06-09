#!/usr/bin/env bash
# 老库 ng_loan_market 一次性 user 全量 → 目标库（独立 Job，不改 SOURCE_MYSQL_* incr）
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
LOG_FILE="${LOG_DIR}/sync-user-lm-bulk.log"
SQL_LOG="${LOG_DIR}/sync-user-lm-bulk-sql.log"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

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

print_job_diagnostics() {
  log "---- Flink 诊断 ----"
  docker exec "$JM" ./bin/flink list 2>&1 | tee -a "$LOG_FILE" || true
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/overview" 2>/dev/null | tee -a "$LOG_FILE" || true
  if [[ -f "$SQL_LOG" ]]; then
    log "---- sql-client 末尾 40 行 ----"
    tail -40 "$SQL_LOG" | tee -a "$LOG_FILE"
  fi
}

monitor_loop() {
  local job_id="${1:-}"
  local src_sql="SELECT COUNT(*) FROM \`user\`;"
  local tgt_sql="SELECT COUNT(*) FROM \`user\`;"

  log "监控开始 间隔=${POLL_SEC}s 日志=${LOG_FILE}  (Ctrl+C 停止监控)"
  [[ -n "$job_id" ]] && log "Flink Job: ${job_id}"
  log "----------------------------------------"

  local prev_target="" prev_ts
  prev_ts=$(date +%s)
  local round=0

  while true; do
    round=$((round + 1))
    local now now_ts
    now=$(date '+%Y-%m-%d %H:%M:%S')
    now_ts=$(date +%s)

    local src_cnt tgt_cnt
    src_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
      "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" "$src_sql")
    tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
      "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$tgt_sql")

    local delta="n/a" rate_min="n/a" rate_sec="n/a" progress="n/a"
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
      delta=$((tgt_cnt - prev_target))
      local elapsed=$((now_ts - prev_ts))
      if (( elapsed > 0 )); then
        rate_min=$(awk "BEGIN {printf \"%.1f\", $delta * 60 / $elapsed}")
        rate_sec=$(awk "BEGIN {printf \"%.1f\", $delta / $elapsed}")
      fi
    fi
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
      progress=$(awk "BEGIN {printf \"%.2f%%\", $tgt_cnt * 100 / $src_cnt}")
    fi

    local job_state flink_out
    job_state=$(flink_job_state "$job_id")
    flink_out=$(flink_records_out "$job_id")

    log "[${now}] #${round} Job=${job_state} 目标=${tgt_cnt} 老库user=${src_cnt} 进度=${progress} 本段+${delta} 速率≈${rate_sec}条/秒(${rate_min}条/分) Flink写出=${flink_out}"

    if [[ -n "$job_id" && "$job_state" =~ ^(FINISHED|FAILED|CANCELED)$ ]]; then
      log "Job 已结束 state=${job_state}"
      break
    fi
    if [[ -z "$job_id" && "$round" -ge 3 && "$delta" == "n/a" && "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" == "$tgt_cnt" ]]; then
      log "WARN: 无 Job 且目标库无增长，请查看 ${SQL_LOG}"
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

log "老库: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
log "目标: ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"
log "模式: 无 VT，mobile 明文直传"
log "提交: sql/03_sync_user_lm_bulk.sql"

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  log "ERR: 容器 ${JM} 未运行，请先 docker compose up -d"
  exit 1
fi

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
log "提交前 RUNNING: ${BEFORE_JOBS:-<无>}"

set +e
bash scripts/run-sql.sh sql/03_sync_user_lm_bulk.sql 2>&1 | tee "$SQL_LOG"
SQL_RC=${PIPESTATUS[0]}
set -e

JOB_ID=$(capture_new_job_id "$BEFORE_JOBS" || true)
log "sql-client 退出码=${SQL_RC}  Job id=${JOB_ID:-<未捕获>}"

if [[ "$SQL_RC" -ne 0 ]]; then
  log "ERR: sql-client 失败，未提交 Job"
  print_job_diagnostics
  exit "$SQL_RC"
fi

if [[ -z "${JOB_ID:-}" ]]; then
  log "WARN: sql-client 成功但未发现 RUNNING Job（batch 可能已秒完成或仍在排队）"
  JOB_ID=$(latest_job_id || true)
  log "最近 Job: ${JOB_ID:-<无>}"
  print_job_diagnostics
fi

monitor_loop "${JOB_ID:-}"
