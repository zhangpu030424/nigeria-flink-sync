#!/usr/bin/env bash
# 对比全量 fast：无 VT vs 有 VT（相同 parallel、相同宽表）
#
# 用法:
#   ./scripts/bench-vt-compare.sh           # 每轮跑 120 秒
#   ./scripts/bench-vt-compare.sh 180       # 每轮 180 秒
#   ./scripts/bench-vt-compare.sh 120 --no-truncate  # 不 TRUNCATE（第二次会 upsert 变慢）
#
# 前置: user_sync_staging 已建好；建议 FLINK_PARALLELISM=8, FLINK_TASK_SLOTS=16
set -euo pipefail
cd "$(dirname "$0")/.."

DURATION="${1:-120}"
NO_TRUNCATE=0
[[ "${2:-}" == "--no-truncate" ]] && NO_TRUNCATE=1

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
  export "$line"
done < .env
set +a

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
LOG_FILE="logs/bench-vt-compare-$(date +%Y%m%d-%H%M%S).log"
mkdir -p logs

log() {
  echo "[$(date '+%H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

count_target() {
  MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT}" \
    -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" -N -e "SELECT COUNT(*) FROM \`user\`;" 2>/dev/null || echo "ERR"
}

cancel_jobs() {
  while read -r job_id; do
    [[ -z "$job_id" ]] && continue
    docker exec "$JM" ./bin/flink cancel "$job_id" 2>/dev/null || true
  done < <(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u)
  sleep 3
}

truncate_target() {
  if [[ "$NO_TRUNCATE" -eq 1 ]]; then
    log "跳过 TRUNCATE"
    return
  fi
  log "TRUNCATE 目标 user 表..."
  MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT}" \
    -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" -e "TRUNCATE TABLE \`user\`;"
}

run_round() {
  local label=$1 sql=$2
  log "========== ${label} =========="
  log "SQL: ${sql}  时长: ${DURATION}s"

  cancel_jobs
  truncate_target

  local start_cnt end_cnt
  start_cnt=$(count_target)
  local t0
  t0=$(date +%s)

  ./scripts/run-sql.sh "$sql"
  log "Job 已提交，等待 ${DURATION}s ..."

  sleep "$DURATION"

  end_cnt=$(count_target)
  local t1 delta elapsed rate_per_min rate_per_sec
  t1=$(date +%s)
  elapsed=$((t1 - t0))

  if [[ "$start_cnt" =~ ^[0-9]+$ && "$end_cnt" =~ ^[0-9]+$ ]]; then
    delta=$((end_cnt - start_cnt))
    rate_per_min=$(awk "BEGIN {printf \"%.0f\", $delta * 60 / $elapsed}")
    rate_per_sec=$(awk "BEGIN {printf \"%.1f\", $delta / $elapsed}")
    log "${label} 结果: +${delta} 条 / ${elapsed}s ≈ ${rate_per_min} 条/分钟 (${rate_per_sec} 条/秒)"
    echo "${label}|${delta}|${elapsed}|${rate_per_min}|${rate_per_sec}" >> "$LOG_FILE.summary"
  else
    log "${label} 目标库查询失败 start=${start_cnt} end=${end_cnt}"
    echo "${label}|ERR|${elapsed}|0|0" >> "$LOG_FILE.summary"
  fi

  cancel_jobs
  sleep 5
}

log "VT 对比测试开始  parallel=${FLINK_PARALLELISM:-?} slots=${FLINK_TASK_SLOTS:-?}"
log "日志: ${LOG_FILE}"
: > "$LOG_FILE.summary"

./scripts/check-flink-slots.sh 2>&1 | tee -a "$LOG_FILE" || {
  log "slot 检查未通过，请先调整 .env"
  exit 1
}

run_round "无VT" "sql/02_sync_user_fast_no_vt.sql"
run_round "有VT" "sql/02_sync_user_fast.sql"

log ""
log "========== 对比汇总 =========="
printf "%-8s %10s %8s %12s %10s\n" "模式" "写入条数" "耗时(s)" "条/分钟" "条/秒"
while IFS='|' read -r label delta elapsed rpm rps; do
  [[ "$label" == "无VT" || "$label" == "有VT" ]] || continue
  printf "%-8s %10s %8s %12s %10s\n" "$label" "$delta" "$elapsed" "$rpm" "$rps"
done < "$LOG_FILE.summary"

if [[ -f "$LOG_FILE.summary" ]]; then
  no_vt=$(grep '^无VT|' "$LOG_FILE.summary" | cut -d'|' -f4)
  with_vt=$(grep '^有VT|' "$LOG_FILE.summary" | cut -d'|' -f4)
  if [[ "$no_vt" =~ ^[0-9]+$ && "$with_vt" =~ ^[0-9]+$ && "$with_vt" -gt 0 ]]; then
    ratio=$(awk "BEGIN {printf \"%.1f\", $no_vt / $with_vt}")
    log "无VT 约为 有VT 的 ${ratio}x 倍速"
  fi
fi

log "完成。Web UI 可看各轮 Job 的 Calc[2] Busy（有VT 时 Calc=vt_tokenize）"
