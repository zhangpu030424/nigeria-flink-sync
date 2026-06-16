#!/usr/bin/env bash
# 单 Job：全量 → 监控达标 → 切增量（增量 VT：Lookup 优先，miss 则 UDF 调 /v2t）
#
# 用法:
#   ./scripts/sync-job-auto.sh user
#   ./scripts/sync-job-auto.sh user --incr-only
#   ./scripts/sync-job-auto.sh user --bulk-only    # 只全量，不切增量
#   ./scripts/sync-job-auto.sh id_mapping --bulk-only --bulk-submit-only  # 提交后不监控
#   ./scripts/sync-job-auto.sh user [--incr-only|--bulk-only] [--keep-other-jobs] [--bulk-start-ms MS]
#   ./scripts/sync-job-auto.sh user --bulk-start-ms 1710000000000
#
set -euo pipefail
cd "$(dirname "$0")/.."

# 部署校验：日志里应出现本版本号；若仍见「等待 Job … 已结束」说明服务器脚本未更新
SYNC_SCRIPT_VERSION="monitor-v4-count-deficit-job-id"

JOB_KEY="${1:-}"
shift || true
[[ -z "$JOB_KEY" ]] && { echo "用法: $0 <job_key> [--incr-only|--bulk-only] [--keep-other-jobs] [--bulk-start-ms MS]"; exit 1; }

INCR_ONLY=0
BULK_ONLY=0
BULK_SUBMIT_ONLY=0
KEEP_OTHER=0
BULK_START_MS_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --incr-only) INCR_ONLY=1 ;;
    --bulk-only) BULK_ONLY=1 ;;
    --bulk-submit-only) BULK_SUBMIT_ONLY=1; BULK_ONLY=1 ;;
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

[[ -f "$FULL_SQL" ]] || { echo "全量 SQL 不存在: $FULL_SQL"; exit 1; }
[[ -f "$INCR_SQL" ]] || { echo "增量 SQL 不存在: $INCR_SQL"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-${JOB_KEY}-auto.log"
mkdir -p "$LOG_DIR"

SYNC_THRESHOLD_PCT="${SYNC_THRESHOLD_PCT:-100}"
SYNC_REQUIRE_EXACT_COUNT="${SYNC_REQUIRE_EXACT_COUNT:-1}"
SYNC_TARGET_BASELINE_AUTO="${SYNC_TARGET_BASELINE_AUTO:-1}"
POLL_SEC="${SYNC_POLL_SEC:-3}"
STABLE_ROUNDS="${SYNC_STABLE_ROUNDS:-5}"
MIN_RATE_TO_STABLE="${SYNC_MIN_RATE:-200}"
BULK_GRACE_ROUNDS="${BULK_GRACE_ROUNDS:-10}"
BULK_SHORT_RETRY_MAX="${BULK_SHORT_RETRY_MAX:-2}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"
TARGET_BASELINE=""
TARGET_BASELINE_FILE="${LOG_DIR}/sync-${JOB_KEY}-target-baseline.count"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$JOB_KEY] $*" | tee -a "$LOG_FILE"
}

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

# 全量达标（内部）：期望计数 expect_cnt vs 实际目标 tgt
bulk_count_reached_inner() {
  local expect_cnt="$1"
  local tgt="$2"
  [[ "$expect_cnt" =~ ^[0-9]+$ && "$tgt" =~ ^[0-9]+$ && "$expect_cnt" -gt 0 ]] || return 1
  if [[ "$SYNC_REQUIRE_EXACT_COUNT" == "1" ]]; then
    [[ "$tgt" -eq "$expect_cnt" ]] && return 0
    if [[ "$tgt" -gt "$expect_cnt" ]]; then
      local surplus=$((tgt - expect_cnt))
      local max_surplus="${SYNC_COUNT_MAX_SURPLUS:-100}"
      [[ "$surplus" -le "$max_surplus" ]] && return 0
    elif [[ "$tgt" -lt "$expect_cnt" ]]; then
      local deficit=$((expect_cnt - tgt))
      local max_deficit="${SYNC_COUNT_MAX_DEFICIT:-10}"
      [[ "$deficit" -le "$max_deficit" ]] && return 0
    fi
    return 1
  else
    awk "BEGIN {exit !(($tgt * 100 / $expect_cnt) >= $SYNC_THRESHOLD_PCT)}"
  fi
}

# 全量达标：宽表 src 条；目标库已有数据时按「基线 + 宽表 ≈ 目标总数」
bulk_count_reached() {
  local src="$1"
  local tgt="$2"
  if [[ -n "$TARGET_BASELINE" && "$TARGET_BASELINE" =~ ^[0-9]+$ ]]; then
    local expected=$((TARGET_BASELINE + src))
    bulk_count_reached_inner "$expected" "$tgt"
  else
    bulk_count_reached_inner "$src" "$tgt"
  fi
}

count_match_reason() {
  local src="$1"
  local tgt="$2"
  local expect_cnt="$src"
  local prefix=""
  if [[ -n "$TARGET_BASELINE" && "$TARGET_BASELINE" =~ ^[0-9]+$ ]]; then
    expect_cnt=$((TARGET_BASELINE + src))
    prefix="基线${TARGET_BASELINE}+宽表${src}="
  fi
  if [[ "$tgt" -eq "$expect_cnt" ]]; then
    echo "${prefix}一致(目标=${tgt})"
  elif [[ "$tgt" -gt "$expect_cnt" ]]; then
    echo "${prefix}目标多$((tgt - expect_cnt))（≤${SYNC_COUNT_MAX_SURPLUS:-100}视为达标）"
  else
    echo "${prefix}目标少$((expect_cnt - tgt))（≤${SYNC_COUNT_MAX_DEFICIT:-10}视为达标）"
  fi
}

capture_target_baseline() {
  if [[ "${SYNC_TARGET_BASELINE_AUTO}" != "1" ]]; then
    TARGET_BASELINE=""
    return 0
  fi
  if [[ -n "${SYNC_TARGET_BASELINE:-}" && "${SYNC_TARGET_BASELINE}" =~ ^[0-9]+$ ]]; then
    TARGET_BASELINE="${SYNC_TARGET_BASELINE}"
    log "使用手动目标基线 TARGET_BASELINE=${TARGET_BASELINE}"
    return 0
  fi
  local cnt
  cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")
  if [[ ! "$cnt" =~ ^[0-9]+$ ]]; then
    log "WARN: 无法读取目标基线计数（${cnt}），回退为宽表=目标总数对比"
    TARGET_BASELINE=""
    return 0
  fi
  TARGET_BASELINE="$cnt"
  echo "$cnt" > "$TARGET_BASELINE_FILE"
  log "目标库基线=${TARGET_BASELINE}（全量开始前快照；期望目标≈基线+宽表）"
}

verify_staging_target_count() {
  local src_cnt tgt_cnt
  src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$SRC_CNT_SQL")
  tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")
  if [[ ! "$src_cnt" =~ ^[0-9]+$ || ! "$tgt_cnt" =~ ^[0-9]+$ ]]; then
    log "✗ 无法读取宽表/目标计数（宽表=${src_cnt} 目标=${tgt_cnt}）"
    return 1
  fi
  if ! bulk_count_reached "$src_cnt" "$tgt_cnt"; then
    log "✗ 宽表与目标表数量未达标：宽表=${src_cnt} 目标=${tgt_cnt}（$(count_match_reason "$src_cnt" "$tgt_cnt")）"
    return 1
  fi
  log "✓ 宽表与目标表数量达标：宽表=${src_cnt} 目标=${tgt_cnt}（$(count_match_reason "$src_cnt" "$tgt_cnt")）"
  return 0
}

list_running_job_ids() {
  # 无 RUNNING Job 时 grep 退出码=1；set -euo pipefail 下会误杀脚本
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

read_submitted_job_id() {
  local f="${FLINK_LAST_JOB_ID_FILE:-logs/last-flink-job-id}"
  [[ -f "$f" ]] && tr -d '[:space:]' < "$f" || true
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
  local submitted
  submitted=$(read_submitted_job_id)
  if [[ -n "$submitted" ]] && ! echo " $before " | grep -q " $submitted "; then
    echo "$submitted"
    return 0
  fi
  # 短 batch（如 user_bankcard ~10s）sql-client 返回时 Job 常已 FINISHED；再轮询 RUNNING
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
  submitted=$(read_submitted_job_id)
  if [[ -n "$submitted" ]] && ! echo " $before " | grep -q " $submitted "; then
    echo "$submitted"
    return 0
  fi
  echo ""
  return 1
}

start_incr() {
  local bulk_start_ms="${1:-}"
  local bulk_parallel
  bulk_parallel=$(sync_job_parallelism "$JOB_KEY" bulk)
  local incr_parallel
  incr_parallel=$(sync_job_parallelism "$JOB_KEY" incr)

  # 有 bulk-start-ms 时默认 timestamp：只追 binlog，不重扫全表（每行 vt_tokenize 全表极慢）
  # 全量已写入历史数据；漏写窗口由 bulk-start-ms 覆盖。需全表快照补漏可 export CDC_STARTUP_MODE=initial
  export CDC_STARTUP_TIMESTAMP_MILLIS="${bulk_start_ms:-$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || date +%s000)}"
  if [[ -n "$bulk_start_ms" ]]; then
    export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-timestamp}"
  else
    export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-initial}"
  fi
  if [[ "$CDC_STARTUP_MODE" == "latest-offset" ]]; then
    export CDC_STARTUP_TIMESTAMP_MILLIS="0"
  fi

  export FLINK_PARALLELISM="${incr_parallel}"
  log "切换增量：并行度 ${bulk_parallel} → ${FLINK_PARALLELISM}（job=${JOB_KEY}）"
  if [[ "$JOB_KEY" == "user_info" && "${TRUNCATE_USER_INFO_DIRTY:-0}" == "1" ]]; then
    log "清空 user_info_dirty（--truncate-user-info-dirty）"
    # shellcheck source=scripts/lib/user-info-dirty.sh
    source "$(dirname "$0")/lib/user-info-dirty.sh"
    truncate_user_info_dirty >> "$LOG_FILE" 2>&1
  fi
  log "源库 DDL: deploy-source-ddl.sh --skip-if-ok"
  ./scripts/deploy-source-ddl.sh --skip-if-ok >> "$LOG_FILE" 2>&1
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

# 全量：进度≥阈值 且 低速稳定 STABLE_ROUNDS 轮 → cancel Job → 切增量
# batch Job 提前 FINISHED 且未达标：再等 BULK_GRACE_ROUNDS 轮刷盘，仍不足则失败
monitor_bulk_until_stable() {
  local phase_label="$1"
  local src_sql="$2"
  local job_id="${3:-}"
  local prev_target=""
  local stable=0
  local round=0
  local grace=0

  if [[ -z "$job_id" ]]; then
    log "[${phase_label}] WARN: 无 Job id（batch 可能已结束），仅按宽表/目标计数监控"
  fi

  while true; do
    round=$((round + 1))
    src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
      "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$src_sql")
    tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
      "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")

    progress="n/a"
    rate="n/a"
    reached=0
    if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
      if [[ -n "$TARGET_BASELINE" && "$TARGET_BASELINE" =~ ^[0-9]+$ ]]; then
        local synced=$((tgt_cnt - TARGET_BASELINE))
        (( synced < 0 )) && synced=0
        progress=$(awk "BEGIN {printf \"%.2f\", $synced * 100 / $src_cnt}")
      else
        progress=$(awk "BEGIN {printf \"%.2f\", $tgt_cnt * 100 / $src_cnt}")
      fi
      if bulk_count_reached "$src_cnt" "$tgt_cnt"; then
        reached=1
      fi
    fi
    if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
      delta=$((tgt_cnt - prev_target))
      rate=$(awk "BEGIN {printf \"%.0f\", $delta * 60 / $POLL_SEC}")
    fi

    job_running=0
    if [[ -n "$job_id" ]] && list_running_job_ids | grep -qx "$job_id"; then
      job_running=1
    fi
    job_state="DONE"
    [[ "$job_running" -eq 1 ]] && job_state="RUNNING"
    [[ -z "$job_id" ]] && job_state="DONE(no-id)"

    local match_hint="需一致"
    [[ "$SYNC_REQUIRE_EXACT_COUNT" != "1" ]] && match_hint="≥${SYNC_THRESHOLD_PCT}%"
    if [[ -n "$TARGET_BASELINE" && "$TARGET_BASELINE" =~ ^[0-9]+$ && "$src_cnt" =~ ^[0-9]+$ ]]; then
      local synced=$((tgt_cnt - TARGET_BASELINE))
      (( synced < 0 )) && synced=0
      local expected=$((TARGET_BASELINE + src_cnt))
      log "[${phase_label}] #${round} 宽表=${src_cnt} 基线=${TARGET_BASELINE} 目标=${tgt_cnt} 本次+${synced} 期望≈${expected} 进度=${progress}% 速率≈${rate}/min ${match_hint} job=${job_state}"
    else
      log "[${phase_label}] #${round} 宽表=${src_cnt} 目标=${tgt_cnt} 进度=${progress}% 速率≈${rate}/min ${match_hint} job=${job_state}"
    fi

    switch=0
    if [[ "$reached" == "1" ]]; then
      if [[ "$rate" == "n/a" ]] || [[ "$rate" -lt "$MIN_RATE_TO_STABLE" ]]; then
        stable=$((stable + 1))
        log "[${phase_label}] 宽表与目标数量达标，稳定 ${stable}/${STABLE_ROUNDS}"
        [[ "$stable" -ge "$STABLE_ROUNDS" ]] && switch=1
      else
        stable=0
      fi
    else
      stable=0
    fi

    if [[ "$switch" -eq 1 ]]; then
      if [[ "$job_running" -eq 1 ]]; then
        log "[${phase_label}] 达标且稳定，cancel 全量 Job ${job_id}"
        cancel_job_id "$job_id" ""
      fi
      log "[${phase_label}] ✓ 全量完成（宽表=${src_cnt} 目标=${tgt_cnt}）"
      return 0
    fi

    if [[ "$job_running" -eq 0 && "$reached" != "1" ]]; then
      grace=$((grace + 1))
      if [[ "$grace" -le "$BULK_GRACE_ROUNDS" ]]; then
        log "[${phase_label}] Job 已结束，宽表=${src_cnt} 目标=${tgt_cnt} 未一致，等待刷盘 ${grace}/${BULK_GRACE_ROUNDS}"
        stable=0
        prev_target="$tgt_cnt"
        sleep "$POLL_SEC"
        continue
      fi
      log "[${phase_label}] ✗ Job 已结束但宽表=${src_cnt} 目标=${tgt_cnt} 数量不一致（进度 ${progress}%），不切增量"
      return 1
    fi

    prev_target="$tgt_cnt"
    sleep "$POLL_SEC"
  done
}

log_bulk_sync_counts() {
  local phase_label="${1:-}"
  local hit_cnt miss_cnt total_cnt tgt_cnt
  hit_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "${VT_SRC_HAS_TOKEN_SQL:-SELECT 0}")
  miss_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "${VT_SRC_MISS_SQL:-SELECT 0}")
  total_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$SRC_CNT_SQL")
  tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")
  log "[${phase_label}] 计数快照: 有token=${hit_cnt} 待VT=${miss_cnt} 宽表总量=${total_cnt} 目标=${tgt_cnt}"
}

# 未达标时重提 batch（upsert 幂等）；返回 0=达标，1=彻底失败
monitor_bulk_with_retry() {
  local phase_label="$1"
  local src_sql="$2"
  local job_id="$3"
  local attempt=0

  while (( attempt <= BULK_SHORT_RETRY_MAX )); do
    if [[ "$attempt" -gt 0 ]]; then
      log "[${phase_label}] 未达标，重提 batch Job (${attempt}/${BULK_SHORT_RETRY_MAX})"
      submit_bulk
      job_id=$(read_submitted_job_id)
      if [[ -z "$job_id" ]]; then
        job_id=$(capture_new_job_id "$(list_running_job_ids | tr '\n' ' ')")
      fi
      [[ -z "$job_id" ]] && { log "[${phase_label}] ✗ 重试后未捕获 Job id（请 git pull 更新 run-sql.sh）"; return 1; }
      log "[${phase_label}] 重试 Job id=${job_id}"
    fi
    if monitor_bulk_until_stable "$phase_label" "$src_sql" "$job_id"; then
      BULK_JOB_ID="$job_id"
      return 0
    fi
    attempt=$((attempt + 1))
  done
  return 1
}

# 阶段 1 未达标但「目标=有token数」且仍有待 VT → 允许进阶段 2
phase1_done_with_miss_pending() {
  local hit_cnt miss_cnt tgt_cnt
  [[ -z "$VT_SRC_HAS_TOKEN_SQL" || -z "$VT_SRC_MISS_SQL" ]] && return 1
  hit_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$VT_SRC_HAS_TOKEN_SQL")
  miss_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$VT_SRC_MISS_SQL")
  tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL")
  [[ "$hit_cnt" =~ ^[0-9]+$ && "$miss_cnt" -gt 0 && "$tgt_cnt" =~ ^[0-9]+$ ]] || return 1
  local max_deficit="${SYNC_COUNT_MAX_DEFICIT:-10}"
  if [[ "$tgt_cnt" -eq "$hit_cnt" ]]; then
    return 0
  fi
  [[ "$tgt_cnt" -le "$hit_cnt" && $((hit_cnt - tgt_cnt)) -le "$max_deficit" ]]
}

# VT 两阶段全量：阶段 1 已有 token；阶段 2 无 token 行 UDF 调 /v2t
VT_MISS_RUNNER=""
VT_SRC_HAS_TOKEN_SQL=""
VT_SRC_PHASE1_SQL=""
VT_SRC_MISS_SQL=""

resolve_vt_two_phase() {
  VT_MISS_RUNNER=""
  VT_SRC_HAS_TOKEN_SQL=""
  VT_SRC_PHASE1_SQL=""
  VT_SRC_MISS_SQL=""
  case "$JOB_KEY" in
    user)
      # 目标 PK=(mobile_token,app_id)：同 token+app 多 id 合并为 1 行，计数按去重口径
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM (SELECT DISTINCT mobile_token,app_code FROM user_sync_staging WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm)<>'' AND mobile_token IS NOT NULL AND TRIM(mobile_token)<>'') d"
      VT_SRC_PHASE1_SQL="$VT_SRC_HAS_TOKEN_SQL"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_sync_staging WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm)<>'' AND (mobile_token IS NULL OR TRIM(mobile_token)='')"
      VT_MISS_RUNNER="run-user-fast-vt-miss.sh"
      ;;
    user_info)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM user_info_sync_staging WHERE id_number_token IS NOT NULL AND TRIM(id_number_token)<>''"
      VT_SRC_PHASE1_SQL="SELECT COUNT(*) FROM user_info_sync_staging WHERE (bvn_raw IS NULL OR TRIM(bvn_raw)='') OR (id_number_token IS NOT NULL AND TRIM(id_number_token)<>'')"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_info_sync_staging WHERE bvn_raw IS NOT NULL AND TRIM(bvn_raw)<>'' AND (id_number_token IS NULL OR TRIM(id_number_token)='')"
      VT_MISS_RUNNER="run-user-info-fast-vt-miss.sh"
      ;;
    user_bankcard)
      # sink PK=(group_user_id,bank_account_number)：同 user+token 多行合并，计数须去重
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM (SELECT DISTINCT user_id, bank_account_token FROM user_bankcard_sync_staging WHERE bank_account_token IS NOT NULL AND TRIM(bank_account_token)<>'') d"
      VT_SRC_PHASE1_SQL="$VT_SRC_HAS_TOKEN_SQL"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM user_bankcard_sync_staging WHERE bank_account_raw IS NOT NULL AND TRIM(bank_account_raw)<>'' AND (bank_account_token IS NULL OR TRIM(bank_account_token)='')"
      VT_MISS_RUNNER="run-user-bankcard-fast-vt-miss.sh"
      ;;
    application)
      VT_SRC_HAS_TOKEN_SQL="SELECT COUNT(*) FROM application_sync_staging WHERE mobile_token IS NOT NULL AND TRIM(mobile_token)<>'' AND id_number_token IS NOT NULL AND TRIM(id_number_token)<>'' AND bank_account_token IS NOT NULL AND TRIM(bank_account_token)<>''"
      VT_SRC_PHASE1_SQL="SELECT COUNT(*) FROM application_sync_staging WHERE mobile_token IS NOT NULL AND TRIM(mobile_token)<>'' AND bank_account_token IS NOT NULL AND TRIM(bank_account_token)<>'' AND ((bvn_raw IS NULL OR TRIM(bvn_raw)='') OR (id_number_token IS NOT NULL AND TRIM(id_number_token)<>''))"
      VT_SRC_MISS_SQL="SELECT COUNT(*) FROM application_sync_staging WHERE ((mobile_token IS NULL OR TRIM(mobile_token)='') AND mobile_norm IS NOT NULL AND TRIM(mobile_norm)<>'') OR ((id_number_token IS NULL OR TRIM(id_number_token)='') AND bvn_raw IS NOT NULL AND TRIM(bvn_raw)<>'') OR ((bank_account_token IS NULL OR TRIM(bank_account_token)='') AND bank_account_raw IS NOT NULL AND TRIM(bank_account_raw)<>'')"
      VT_MISS_RUNNER="run-application-fast-vt-miss.sh"
      ;;
  esac
}

run_vt_bulk_two_phase() {
  log "========== ${JOB_KEY} 全量阶段 1/2：已有 VT token（不调 /v2t）=========="
  log_bulk_sync_counts "phase1-before"
  log "阶段 1 监控口径: ${VT_SRC_PHASE1_SQL:-$VT_SRC_HAS_TOKEN_SQL}"
  submit_bulk
  BULK_JOB_ID=$(capture_new_job_id "$BEFORE_JOBS")
  log "阶段 1 Job id=${BULK_JOB_ID:-unknown}"
  if ! monitor_bulk_with_retry "${JOB_KEY}-vt-hit" "${VT_SRC_PHASE1_SQL:-$VT_SRC_HAS_TOKEN_SQL}" "$BULK_JOB_ID"; then
    if phase1_done_with_miss_pending; then
      log "WARN: 阶段 1 监控未过终检，但目标=有token数且仍有待VT，进入阶段 2"
      log_bulk_sync_counts "phase1-fallback"
    else
      log "✗ 阶段 1 未达标且无法进入阶段 2"
      exit 1
    fi
  fi

  local miss_cnt
  miss_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$VT_SRC_MISS_SQL")
  if [[ "$miss_cnt" =~ ^[0-9]+$ && "$miss_cnt" -gt 0 ]]; then
    log "========== ${JOB_KEY} 全量阶段 2/2：待 VT 补全 ${miss_cnt} 条，运行时 UDF 调 /v2t =========="
    log_bulk_sync_counts "phase2-before"
    local vt_miss_par="${FLINK_PARALLELISM_VT_MISS:-2}"
    log "VT 补全并行度: FLINK_PARALLELISM=${vt_miss_par}"
    local before_vt
    before_vt=$(list_running_job_ids | tr '\n' ' ')
    export FLINK_PARALLELISM="${vt_miss_par}"
    "./scripts/${VT_MISS_RUNNER}"
    BULK_JOB_ID=$(capture_new_job_id "$before_vt")
    log "阶段 2 Job id=${BULK_JOB_ID:-unknown}"
    monitor_bulk_with_retry "${JOB_KEY}-vt-miss" "$SRC_CNT_SQL" "$BULK_JOB_ID"
  else
    log "无待 VT 补全记录，跳过阶段 2"
    BULK_JOB_ID=""
  fi
}

# shellcheck source=scripts/lib/bulk-start-ms.sh
source "$(dirname "$0")/lib/bulk-start-ms.sh"

if [[ "$INCR_ONLY" -eq 1 && "$BULK_ONLY" -eq 1 ]]; then
  echo "ERR: --incr-only 与 --bulk-only 不能同时使用"
  exit 1
fi

# shellcheck source=scripts/lib/sync-jobs.sh
source "$(dirname "$0")/lib/sync-jobs.sh"

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
log "全量监控: ${SYNC_SCRIPT_VERSION} 稳定=${STABLE_ROUNDS}轮 一致=${SYNC_REQUIRE_EXACT_COUNT} 允差±${SYNC_COUNT_MAX_DEFICIT:-10}/+${SYNC_COUNT_MAX_SURPLUS:-100} 目标基线=${SYNC_TARGET_BASELINE_AUTO}"
log "增量 binlog 起点 bulk-start-ms: ${BULK_START_MS}"
capture_target_baseline

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
BULK_PARALLEL=$(sync_job_parallelism "$JOB_KEY" bulk)
SLOTS="${FLINK_TASK_SLOTS:-16}"
INCR_PAR=$(sync_job_parallelism "$JOB_KEY" incr)
SLOT_BUFFER="${SYNC_SLOT_BUFFER:-2}"

if [[ "$KEEP_OTHER" -eq 1 ]]; then
  running_n=0
  while read -r _jid; do
    [[ -z "$_jid" ]] && continue
    running_n=$((running_n + 1))
  done < <(list_running_job_ids)
  max_incr_par="${FLINK_PARALLELISM_INCR:-1}"
  ui_incr_par=$(sync_job_parallelism user_info incr)
  (( ui_incr_par > max_incr_par )) && max_incr_par=$ui_incr_par
  reserved=$((running_n * max_incr_par + SLOT_BUFFER))
  max_bulk=$((SLOTS - reserved))
  (( max_bulk < 1 )) && max_bulk=1
  if (( BULK_PARALLEL > max_bulk )); then
    log "WARN: 全量并行 ${BULK_PARALLEL} → ${max_bulk}（slots=${SLOTS}，已保留 ${reserved} 给 ${running_n} 个存量增量 Job）"
    BULK_PARALLEL=$max_bulk
  fi
fi

export FLINK_PARALLELISM="${BULK_PARALLEL}"
log "全量并行度: FLINK_PARALLELISM=${FLINK_PARALLELISM}（job=${JOB_KEY} slots=${SLOTS}）"

resolve_vt_two_phase
if [[ -n "$VT_MISS_RUNNER" ]]; then
  if [[ "$BULK_SUBMIT_ONLY" -eq 1 ]]; then
    log "WARN: --bulk-submit-only 与 VT 两阶段全量不兼容，仍走完整监控"
  fi
  run_vt_bulk_two_phase
else
  submit_bulk
  BULK_JOB_ID=$(capture_new_job_id "$BEFORE_JOBS")
  if [[ -z "$BULK_JOB_ID" ]]; then
    BULK_JOB_ID=$(read_submitted_job_id)
  fi
  log "全量 Job 已提交 id=${BULK_JOB_ID:-unknown}，监控 ${MONITOR_TABLE} 达标后 cancel..."
  if [[ "$BULK_SUBMIT_ONLY" -eq 1 ]]; then
    log "[$JOB_KEY] --bulk-submit-only：不等待监控，Job 在 Flink 后台继续（id=${BULK_JOB_ID:-n/a}）"
    log "[$JOB_KEY] 进度: ./scripts/diagnose-job.sh ${BULK_JOB_ID:-}  或 Web UI"
    exit 0
  fi
  monitor_bulk_with_retry "bulk" "$SRC_CNT_SQL" "$BULK_JOB_ID"
fi

log "========== 全量终检：宽表 vs 目标 =========="
verify_staging_target_count

if [[ "$BULK_ONLY" -eq 1 ]]; then
  log "[$JOB_KEY] --bulk-only：全量完成，未切增量。日志: ${LOG_FILE}"
  exit 0
fi

log "========== 切增量 ${INCR_SQL} =========="
start_incr "$BULK_START_MS"
log "[$JOB_KEY] 自动切换完成。日志: ${LOG_FILE}"
