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

SYNC_THRESHOLD_PCT="${SYNC_THRESHOLD_PCT:-95.5}"
POLL_SEC="${SYNC_POLL_SEC:-3}"
STABLE_ROUNDS="${SYNC_STABLE_ROUNDS:-3}"
MIN_RATE_TO_STABLE="${SYNC_MIN_RATE:-200}"
BULK_JOB_WAIT_SEC="${BULK_JOB_WAIT_SEC:-7200}"
BULK_JOB_POLL_SEC="${BULK_JOB_POLL_SEC:-5}"
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
  local protected_ids="${2:-}"
  [[ -z "$job_id" ]] && return 0
  if echo " $protected_ids " | grep -q " $job_id "; then
    log "WARN: ${job_id} 属于存量 Job（增量等），跳过 cancel"
    return 0
  fi
  if ! list_running_job_ids | grep -qx "$job_id"; then
    log "全量 Job ${job_id} 已结束，跳过 cancel"
    return 0
  fi
  log "取消本 Job 全量: $job_id"
  docker exec "$JM" ./bin/flink cancel "$job_id" 2>/dev/null || true
  sleep 3
}

capture_new_job_id() {
  local before="$1"
  local id=""
  local i
  # 短任务（如 VT miss）可能在单次 sleep 8 内已结束；逐秒轮询，且不用 tail -1 回退
  for i in $(seq 1 20); do
    sleep 1
    while read -r id; do
      [[ -z "$id" ]] && continue
      if ! echo " $before " | grep -q " $id "; then
        echo "$id"
        return 0
      fi
    done < <(list_running_job_ids)
  done
  echo ""
  return 1
}

start_incr() {
  local bulk_start_ms="${1:-}"
  local bulk_parallel="${FLINK_PARALLELISM:-8}"
  local incr_parallel="${FLINK_PARALLELISM_INCR:-1}"

  # initial：增量 Job 首次启动先并行快照全表（补全量漏写），再追 binlog；有 checkpoint 后重启从位点恢复
  # 仅追 binlog 可用 CDC_STARTUP_MODE=timestamp（需配合 bulk-start-ms）
  export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-initial}"
  export CDC_STARTUP_TIMESTAMP_MILLIS="${bulk_start_ms:-$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s000)}"
  if [[ "$CDC_STARTUP_MODE" == "latest-offset" ]]; then
    export CDC_STARTUP_TIMESTAMP_MILLIS="0"
  fi

  export FLINK_PARALLELISM="${incr_parallel}"
  log "切换增量：并行度 ${bulk_parallel} → ${FLINK_PARALLELISM}"
  log "增量 SQL: ${INCR_SQL} mode=${CDC_STARTUP_MODE} bulk-start-ms=${CDC_STARTUP_TIMESTAMP_MILLIS}"
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

# 全量 batch Job 跑完自然 FINISHED（不再按目标库计数达标后 cancel）
wait_for_bulk_job() {
  local job_id="$1"
  local phase_label="$2"
  local src_sql="${3:-}"
  local waited=0
  local prev_target=""

  [[ -z "$job_id" ]] && { log "[$phase_label] 无 Job id，跳过等待"; return 0; }

  while (( waited < BULK_JOB_WAIT_SEC )); do
    if ! list_running_job_ids | grep -qx "$job_id"; then
      log "[$phase_label] Job ${job_id} 已结束（FINISHED/CANCELED/FAILED）"
      return 0
    fi
    if [[ -n "$src_sql" ]]; then
      src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
        "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$src_sql")
      tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
        "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")
      progress="n/a"
      rate="n/a"
      if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
        progress=$(awk "BEGIN {printf \"%.2f\", $tgt_cnt * 100 / $src_cnt}")
      fi
      if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
        delta=$((tgt_cnt - prev_target))
        rate=$(awk "BEGIN {printf \"%.0f\", $delta * 60 / $BULK_JOB_POLL_SEC}")
      fi
      log "[$phase_label] 等待 Job ${job_id} … 目标=${tgt_cnt} 源=${src_cnt} 进度=${progress}% 速率≈${rate}/min (${waited}s)"
      prev_target="$tgt_cnt"
    else
      log "[$phase_label] 等待 Job ${job_id} … (${waited}s)"
    fi
    sleep "$BULK_JOB_POLL_SEC"
    waited=$((waited + BULK_JOB_POLL_SEC))
  done
  log "[$phase_label] ✗ 等待 Job ${job_id} 超时 ${BULK_JOB_WAIT_SEC}s"
  return 1
}

# 监控全量进度直至达标（进度≥阈值 且 低速稳定 SYNC_STABLE_ROUNDS 轮）— 已弃用，保留供手工调试
monitor_bulk_until_stable() {
  local phase_label="$1"
  local src_sql="$2"
  local prev_target=""
  local stable=0
  local round=0

  while true; do
    round=$((round + 1))
    src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
      "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$src_sql")
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

    log "[${phase_label}] #${round} 目标=${tgt_cnt} 源=${src_cnt} 进度=${progress}% 速率≈${rate}/min 阈值=${SYNC_THRESHOLD_PCT}%"

    switch=0
    if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
      reached=$(awk "BEGIN {print ($tgt_cnt * 100 / $src_cnt) >= $SYNC_THRESHOLD_PCT}")
      if [[ "$reached" == "1" ]]; then
        if [[ "$rate" == "n/a" ]] || [[ "$rate" -lt "$MIN_RATE_TO_STABLE" ]]; then
          stable=$((stable + 1))
          log "[${phase_label}] 进度已达阈值，稳定 ${stable}/${STABLE_ROUNDS}"
          [[ "$stable" -ge "$STABLE_ROUNDS" ]] && switch=1
        else
          stable=0
        fi
      fi
    fi

    [[ "$switch" -eq 1 ]] && return 0
    prev_target="$tgt_cnt"
    sleep "$POLL_SEC"
  done
}

# VT 两阶段全量：阶段 1 已有 token；阶段 2 无 token 行 UDF 调 /v2t
VT_MISS_RUNNER=""
VT_SRC_HAS_TOKEN_SQL=""
VT_SRC_MISS_SQL=""

resolve_vt_two_phase() {
  VT_MISS_RUNNER=""
  VT_SRC_HAS_TOKEN_SQL=""
  VT_SRC_MISS_SQL=""
  case "$JOB_KEY" in
    user)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM user_sync_staging WHERE mobile_token IS NOT NULL AND TRIM(mobile_token)<>''"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_sync_staging WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm)<>'' AND (mobile_token IS NULL OR TRIM(mobile_token)='')"
      VT_MISS_RUNNER="run-user-fast-vt-miss.sh"
      ;;
    user_info)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM user_info_sync_staging WHERE id_number_token IS NOT NULL AND TRIM(id_number_token)<>''"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_info_sync_staging WHERE bvn_raw IS NOT NULL AND TRIM(bvn_raw)<>'' AND (id_number_token IS NULL OR TRIM(id_number_token)='')"
      VT_MISS_RUNNER="run-user-info-fast-vt-miss.sh"
      ;;
    user_bankcard)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM user_bankcard_sync_staging WHERE bank_account_token IS NOT NULL AND TRIM(bank_account_token)<>''"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_bankcard_sync_staging WHERE bank_account_raw IS NOT NULL AND TRIM(bank_account_raw)<>'' AND (bank_account_token IS NULL OR TRIM(bank_account_token)='')"
      VT_MISS_RUNNER="run-user-bankcard-fast-vt-miss.sh"
      ;;
    application)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM application_sync_staging WHERE mobile_token IS NOT NULL AND TRIM(mobile_token)<>'' AND id_number_token IS NOT NULL AND TRIM(id_number_token)<>'' AND bank_account_token IS NOT NULL AND TRIM(bank_account_token)<>''"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM application_sync_staging WHERE ((mobile_token IS NULL OR TRIM(mobile_token)='') AND mobile_norm IS NOT NULL AND TRIM(mobile_norm)<>'') OR ((id_number_token IS NULL OR TRIM(id_number_token)='') AND bvn_raw IS NOT NULL AND TRIM(bvn_raw)<>'') OR ((bank_account_token IS NULL OR TRIM(bank_account_token)='') AND bank_account_raw IS NOT NULL AND TRIM(bank_account_raw)<>'')"
      VT_MISS_RUNNER="run-application-fast-vt-miss.sh"
      ;;
  esac
}

run_vt_bulk_two_phase() {
  log "========== ${JOB_KEY} 全量阶段 1/2：已有 VT token（不调 /v2t）=========="
  submit_bulk
  BULK_JOB_ID=$(capture_new_job_id "$BEFORE_JOBS")
  log "阶段 1 Job id=${BULK_JOB_ID:-unknown}"
  wait_for_bulk_job "$BULK_JOB_ID" "${JOB_KEY}-vt-hit" "$VT_SRC_HAS_TOKEN_SQL"

  local miss_cnt
  miss_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$VT_SRC_MISS_SQL")
  if [[ "$miss_cnt" =~ ^[0-9]+$ && "$miss_cnt" -gt 0 ]]; then
    log "========== ${JOB_KEY} 全量阶段 2/2：待 VT 补全 ${miss_cnt} 条，运行时 UDF 调 /v2t =========="
    local vt_miss_par="${FLINK_PARALLELISM_VT_MISS:-2}"
    log "VT 补全并行度: FLINK_PARALLELISM=${vt_miss_par}"
    local before_vt
    before_vt=$(list_running_job_ids | tr '\n' ' ')
    export FLINK_PARALLELISM="${vt_miss_par}"
    "./scripts/${VT_MISS_RUNNER}"
    BULK_JOB_ID=$(capture_new_job_id "$before_vt")
    log "阶段 2 Job id=${BULK_JOB_ID:-unknown}"
    wait_for_bulk_job "$BULK_JOB_ID" "${JOB_KEY}-vt-miss" "$SRC_CNT_SQL"
  else
    log "无待 VT 补全记录，跳过阶段 2"
    BULK_JOB_ID=""
  fi
}

# shellcheck source=scripts/lib/bulk-start-ms.sh
source "$(dirname "$0")/lib/bulk-start-ms.sh"

if [[ "$INCR_ONLY" -eq 1 ]]; then
  resolve_bulk_start_ms "${BULK_START_MS_ARG:-}"
  if [[ "$KEEP_OTHER" -eq 0 ]]; then
    cancel_all_jobs
  fi
  start_incr "$BULK_START_MS"
  exit 0
fi

resolve_bulk_start_ms "${BULK_START_MS_ARG:-}"
BULK_START_MS_ARG="$BULK_START_MS"
log "========== 全量 ${FULL_SQL} =========="
log "增量 binlog 起点 bulk-start-ms: ${BULK_START_MS}"

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
SLOTS="${FLINK_TASK_SLOTS:-16}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
SLOT_BUFFER="${SYNC_SLOT_BUFFER:-2}"

if [[ "$KEEP_OTHER" -eq 1 ]]; then
  running_n=0
  while read -r _jid; do
    [[ -z "$_jid" ]] && continue
    running_n=$((running_n + 1))
  done < <(list_running_job_ids)
  reserved=$((running_n * INCR_PAR + SLOT_BUFFER))
  max_bulk=$((SLOTS - reserved))
  (( max_bulk < 1 )) && max_bulk=1
  if (( BULK_PARALLEL > max_bulk )); then
    log "WARN: 全量并行 ${BULK_PARALLEL} → ${max_bulk}（slots=${SLOTS}，已保留 ${reserved} 给 ${running_n} 个存量增量 Job）"
    BULK_PARALLEL=$max_bulk
  fi
fi

export FLINK_PARALLELISM="${BULK_PARALLEL}"
log "全量并行度: FLINK_PARALLELISM=${FLINK_PARALLELISM}（slots=${SLOTS}）"

resolve_vt_two_phase
if [[ -n "$VT_MISS_RUNNER" ]]; then
  run_vt_bulk_two_phase
else
  submit_bulk
  BULK_JOB_ID=$(capture_new_job_id "$BEFORE_JOBS")
  log "全量 Job 已提交 id=${BULK_JOB_ID:-unknown}，等待 batch 完成 ${MONITOR_TABLE}..."
  wait_for_bulk_job "$BULK_JOB_ID" "bulk" "$SRC_CNT_SQL"
fi

log "========== 切增量 ${INCR_SQL} =========="
start_incr "$BULK_START_MS"
log "[$JOB_KEY] 自动切换完成。日志: ${LOG_FILE}"
