#!/usr/bin/env bash
# 单 Job：全量 → 监控达标 → 切增量（增量 VT：Lookup 优先，miss 则 UDF 调 /v2t）
#
# 用法:
#   ./scripts/sync-job-auto.sh user
#   ./scripts/sync-job-auto.sh user --incr-only
#   ./scripts/sync-job-auto.sh user --no-vt
#   ./scripts/sync-job-auto.sh user --keep-other-jobs    # 不取消其他 RUNNING Job（多 Job 串联用）
#   ./scripts/sync-job-auto.sh user --bulk-start-ms 1710000000000
#
set -euo pipefail
cd "$(dirname "$0")/.."

JOB_KEY="${1:-}"
shift || true
[[ -z "$JOB_KEY" ]] && { echo "用法: $0 <job_key> [--incr-only] [--no-vt] [--keep-other-jobs] [--bulk-start-ms MS]"; exit 1; }

INCR_ONLY=0
NO_VT=0
KEEP_OTHER=0
BULK_START_MS_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --incr-only) INCR_ONLY=1 ;;
    --no-vt) NO_VT=1 ;;
    --keep-other-jobs) KEEP_OTHER=1 ;;
    --bulk-start-ms=*) BULK_START_MS_ARG="${1#--bulk-start-ms=}" ;;
    --bulk-start-ms)
      shift
      BULK_START_MS_ARG="${1:-}"
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
  shift
done

CONF="config/sync-jobs.conf"
[[ -f "$CONF" ]] || { echo "缺少 $CONF"; exit 1; }

line=""
while IFS= read -r row || [[ -n "$row" ]]; do
  row="${row%%#*}"
  row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$row" ]] && continue
  [[ "$row" == "$JOB_KEY|"* ]] && line="$row" && break
done < "$CONF"

[[ -n "$line" ]] || { echo "未知 Job: $JOB_KEY（见 $CONF）"; exit 1; }

IFS='|' read -r _key _desc FULL_SQL INCR_SQL FULL_RUNNER SRC_CNT_SQL TGT_CNT_SQL MONITOR_TABLE ENABLED <<< "$line"

if [[ "$ENABLED" != "1" ]]; then
  echo "Job [$JOB_KEY] ENABLED=0，跳过。SQL 就绪后在 $CONF 改为 1"
  exit 0
fi

if [[ "$NO_VT" -eq 1 ]]; then
  case "$JOB_KEY" in
    user)
      FULL_SQL="sql/02_sync_user_fast_no_vt.sql"
      INCR_SQL="sql/02_sync_user_incr_no_vt.sql"
      ;;
    *)
      echo "Job [$JOB_KEY] 暂无 --no-vt SQL，请补 *_no_vt.sql"
      exit 1
      ;;
  esac
fi

[[ -f "$FULL_SQL" ]] || { echo "全量 SQL 不存在: $FULL_SQL"; exit 1; }
[[ -f "$INCR_SQL" ]] || { echo "增量 SQL 不存在: $INCR_SQL"; exit 1; }

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

set -a
while IFS= read -r envline || [[ -n "$envline" ]]; do
  envline="${envline%%#*}"
  envline="$(echo "$envline" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$envline" ]] && continue
  [[ "$envline" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${envline%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$envline"
done < .env
set +a

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-${JOB_KEY}-auto.log"
mkdir -p "$LOG_DIR"

SYNC_THRESHOLD_PCT="${SYNC_THRESHOLD_PCT:-99.5}"
POLL_SEC="${SYNC_POLL_SEC:-3}"
STABLE_ROUNDS="${SYNC_STABLE_ROUNDS:-3}"
MIN_RATE_TO_STABLE="${SYNC_MIN_RATE:-200}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$JOB_KEY] $*" | tee -a "$LOG_FILE"
}

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

list_running_job_ids() {
  # 无 RUNNING Job 时 grep 退出码=1；set -euo pipefail 下会误杀脚本
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

cancel_all_jobs() {
  log "取消所有 RUNNING Job..."
  while read -r job_id; do
    [[ -z "$job_id" ]] && continue
    docker exec "$JM" ./bin/flink cancel "$job_id" 2>/dev/null || true
  done < <(list_running_job_ids)
  sleep 5
}

cancel_job_id() {
  local job_id="${1:-}"
  [[ -z "$job_id" ]] && return 0
  log "取消本 Job 全量: $job_id"
  docker exec "$JM" ./bin/flink cancel "$job_id" 2>/dev/null || true
  sleep 3
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

start_incr() {
  local bulk_start_ms="${1:-}"
  local bulk_parallel="${FLINK_PARALLELISM:-8}"
  local incr_parallel="${FLINK_PARALLELISM_INCR:-1}"

  export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-timestamp}"
  export CDC_STARTUP_TIMESTAMP_MILLIS="${bulk_start_ms:-$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s000)}"
  if [[ "$CDC_STARTUP_MODE" == "latest-offset" ]]; then
    export CDC_STARTUP_TIMESTAMP_MILLIS="0"
  fi

  export FLINK_PARALLELISM="${incr_parallel}"
  log "切换增量：并行度 ${bulk_parallel} → ${FLINK_PARALLELISM}"
  log "增量 SQL: ${INCR_SQL} mode=${CDC_STARTUP_MODE} ts=${CDC_STARTUP_TIMESTAMP_MILLIS}"
  ./scripts/run-sql.sh "$INCR_SQL"
  log "增量 Job 已提交。监控: ./scripts/monitor-sync.sh ${MONITOR_TABLE} 60"
}

submit_bulk() {
  if [[ -n "$FULL_RUNNER" && -x "scripts/${FULL_RUNNER}.sh" ]]; then
    "./scripts/${FULL_RUNNER}.sh"
  elif [[ -n "$FULL_RUNNER" && -f "scripts/${FULL_RUNNER}" ]]; then
    "./scripts/${FULL_RUNNER}"
  else
    ./scripts/run-sql.sh "$FULL_SQL"
  fi
}

if [[ "$INCR_ONLY" -eq 1 ]]; then
  if [[ "$KEEP_OTHER" -eq 0 ]]; then
    cancel_all_jobs
  fi
  start_incr "${BULK_START_MS_ARG:-}"
  exit 0
fi

BULK_START_MS="${BULK_START_MS_ARG:-$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))")}"
log "========== 阶段 1/2：全量 ${FULL_SQL} =========="
log "全量起始时间戳(ms): ${BULK_START_MS}"

if [[ "$KEEP_OTHER" -eq 0 ]]; then
  if ! ./scripts/check-flink-slots.sh 2>&1 | tee -a "$LOG_FILE"; then
    log "✗ slot/parallelism 不合理，已中止"
    exit 1
  fi
  cancel_all_jobs
else
  log "保留其他 RUNNING Job（--keep-other-jobs）"
fi

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
export FLINK_PARALLELISM="${BULK_PARALLEL}"
log "全量并行度: FLINK_PARALLELISM=${FLINK_PARALLELISM}"

submit_bulk
BULK_JOB_ID=$(capture_new_job_id "$BEFORE_JOBS")
log "全量 Job 已提交 id=${BULK_JOB_ID:-unknown}，监控 ${MONITOR_TABLE}..."

prev_target=""
stable=0
round=0

while true; do
  round=$((round + 1))
  src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$SRC_CNT_SQL")
  tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")

  progress="n/a"
  rate="n/a"
  if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
    progress=$(awk "BEGIN {printf \"%.2f\", $tgt_cnt * 100 / $src_cnt}")
  fi
  if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
    delta=$((tgt_cnt - prev_target))
    rate=$(awk "BEGIN {printf \"%.0f\", $delta * 60 / $POLL_SEC}")
  fi

  log "#${round} 目标=${tgt_cnt} 源=${src_cnt} 进度=${progress}% 速率≈${rate}/min 阈值=${SYNC_THRESHOLD_PCT}%"

  switch=0
  if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
    reached=$(awk "BEGIN {print ($tgt_cnt * 100 / $src_cnt) >= $SYNC_THRESHOLD_PCT}")
    if [[ "$reached" == "1" ]]; then
      if [[ "$rate" == "n/a" ]] || [[ "$rate" -lt "$MIN_RATE_TO_STABLE" ]]; then
        stable=$((stable + 1))
        log "进度已达阈值，稳定 ${stable}/${STABLE_ROUNDS}"
        [[ "$stable" -ge "$STABLE_ROUNDS" ]] && switch=1
      else
        stable=0
      fi
    fi
  fi

  [[ "$switch" -eq 1 ]] && break
  prev_target="$tgt_cnt"
  sleep "$POLL_SEC"
done

log "========== 阶段 2/2：增量 ${INCR_SQL} =========="
cancel_job_id "$BULK_JOB_ID"
start_incr "$BULK_START_MS"
log "[$JOB_KEY] 自动切换完成。日志: ${LOG_FILE}"
