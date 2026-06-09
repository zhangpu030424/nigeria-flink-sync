#!/usr/bin/env bash
# 老库 ng_loan_market 一次性 user 全量 → 目标库（独立 Job，不改 SOURCE_MYSQL_* incr）
# 用法:
#   bash scripts/lm-vt-seed-mobile.sh
#   bash scripts/vt-preload.sh --mode fast --vt-type mobile --skip-count --workers 2
#   bash scripts/run-user-lm-bulk.sh
#   bash scripts/run-user-lm-bulk.sh --monitor-only <job_id>   # 仅监控已提交 Job
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

capture_new_job_id() {
  local before="$1"
  sleep 8
  local after
  after=$(list_running_job_ids | tr '\n' ' ')
  for id in $after; do
    if ! echo " $before " | grep -q " $id "; then
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

monitor_loop() {
  local job_id="${1:-}"
  local src_sql="SELECT COUNT(*) FROM \`user\`;"
  local tgt_sql="SELECT COUNT(*) FROM \`user\`;"

  log "监控开始 间隔=${POLL_SEC}s 日志=${LOG_FILE}  (Ctrl+C 停止监控，不取消 Job)"
  log "老库源: ${LM_MYSQL_USER}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE:-ng_loan_market}"
  log "目标库: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}"
  [[ -n "$job_id" ]] && log "Flink Job: ${job_id}  Web UI: http://127.0.0.1:${FLINK_WEB_PORT}/#/job/${job_id}/overview"
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

    if [[ "$job_state" == "FINISHED" || "$job_state" == "FAILED" || "$job_state" == "CANCELED" ]]; then
      log "Job 已结束 state=${job_state}，停止监控"
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
log "查询: generate_user.py 等价 SELECT（内嵌 JDBC，不建 VIEW）"
log "VT 字典: ${SOURCE_MYSQL_DATABASE}@${SOURCE_MYSQL_HOST}"
log "目标: ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"
log "提交 Job: sql/03_sync_user_lm_bulk.sql（batch 模式，不影响 incr）"

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
log "提交前 RUNNING Job: ${BEFORE_JOBS:-<无>}"

bash scripts/run-sql.sh sql/03_sync_user_lm_bulk.sql &
SQL_PID=$!

JOB_ID=$(capture_new_job_id "$BEFORE_JOBS" || true)
if [[ -z "${JOB_ID:-}" ]]; then
  log "WARN: 未捕获到新 Job ID，请 docker exec ${JM} ./bin/flink list 查看"
  JOB_ID=$(list_running_job_ids | tail -1 || true)
fi
log "全量 Job id=${JOB_ID:-unknown}"

if [[ -n "${JOB_ID:-}" ]]; then
  monitor_loop "$JOB_ID"
else
  log "无 Job ID，仍按目标库计数监控（无 Flink 状态）"
  monitor_loop ""
fi

wait "$SQL_PID" || true
log "sql-client 已退出"
