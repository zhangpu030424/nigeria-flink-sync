#!/usr/bin/env bash
# 全量（fast 宽表）→ 自动监控 → 切换增量（user CDC + binlog）
#
# 用法:
#   ./scripts/sync-user-auto.sh                 # 全量(含VT) → 增量(含VT)
#   ./scripts/sync-user-auto.sh --no-vt         # 全量无VT → 增量无VT（测速用）
#   ./scripts/sync-user-auto.sh --incr-only     # 仅增量
#   ./scripts/sync-user-auto.sh --incr-only --no-vt
#
# 前置（源库一次）:
#   sql/ddl/source_views_adjust.sql
#   sql/ddl/source_materialize_user_adjust.sql
#   sql/ddl/source_user_sync_staging.sql
set -euo pipefail
cd "$(dirname "$0")/.."

INCR_ONLY=0
NO_VT=0
for arg in "$@"; do
  [[ "$arg" == "--incr-only" ]] && INCR_ONLY=1
  [[ "$arg" == "--no-vt" ]] && NO_VT=1
done

FULL_SQL="sql/02_sync_user_fast.sql"
INCR_SQL="sql/02_sync_user_incr.sql"
if [[ "$NO_VT" -eq 1 ]]; then
  FULL_SQL="sql/02_sync_user_fast_no_vt.sql"
  INCR_SQL="sql/02_sync_user_incr_no_vt.sql"
fi

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
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-user-auto.log"
mkdir -p "$LOG_DIR"

SYNC_THRESHOLD_PCT="${SYNC_THRESHOLD_PCT:-99.5}"
POLL_SEC="${SYNC_POLL_SEC:-30}"
STABLE_ROUNDS="${SYNC_STABLE_ROUNDS:-3}"
MIN_RATE_TO_STABLE="${SYNC_MIN_RATE:-200}"

TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

mysql_count() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>/dev/null || echo "ERR"
}

cancel_jobs() {
  log "取消所有 RUNNING Job..."
  while read -r job_id; do
    [[ -z "$job_id" ]] && continue
    docker exec "$JM" ./bin/flink cancel "$job_id" 2>/dev/null || true
  done < <(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u)
  sleep 5
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

  # 全量结束后：并行度降为 1，只跑一个增量 Job
  export FLINK_PARALLELISM="${incr_parallel}"
  log "切换增量：并行度 ${bulk_parallel} → ${FLINK_PARALLELISM}（仅 1 个增量 Job）"
  log "启动增量 Job: ${INCR_SQL} mode=${CDC_STARTUP_MODE} ts=${CDC_STARTUP_TIMESTAMP_MILLIS}"
  ./scripts/run-sql.sh "$INCR_SQL"
  log "增量 Job 已提交，长期运行。监控: ./scripts/monitor-sync.sh user 60"
}

if [[ "$INCR_ONLY" -eq 1 ]]; then
  cancel_jobs
  start_incr ""
  exit 0
fi

BULK_START_MS=$(python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))")
log "========== 阶段 1/2：全量 fast（${FULL_SQL}）=========="
log "全量起始时间戳(ms): ${BULK_START_MS}"

if ! ./scripts/check-flink-slots.sh 2>&1 | tee -a "$LOG_FILE"; then
  log "✗ slot/parallelism 配置不合理，已中止。请修改 .env 后 docker compose up -d"
  exit 1
fi

cancel_jobs
BULK_PARALLEL="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
export FLINK_PARALLELISM="${BULK_PARALLEL}"
log "全量并行度: FLINK_PARALLELISM=${FLINK_PARALLELISM}"
if [[ "$NO_VT" -eq 1 ]]; then
  ./scripts/run-sql.sh "$FULL_SQL"
else
  ./scripts/run-user-fast-vt.sh
fi
log "全量 Job 已提交，开始监控目标库条数..."

prev_target=""
stable=0
round=0

while true; do
  round=$((round + 1))
  src_cnt=$(mysql_count "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "SELECT COUNT(*) FROM \`user\`;")
  tgt_cnt=$(mysql_count "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "SELECT COUNT(*) FROM \`user\`;")

  progress="n/a"
  rate="n/a"
  if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
    progress=$(awk "BEGIN {printf \"%.2f\", $tgt_cnt * 100 / $src_cnt}")
  fi
  if [[ "$tgt_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
    delta=$((tgt_cnt - prev_target))
    rate=$(awk "BEGIN {printf \"%.0f\", $delta * 60 / $POLL_SEC}")
  fi

  log "#${round} 目标=${tgt_cnt} 源=${src_cnt} 进度=${progress}% 速率≈${rate}条/分钟 阈值=${SYNC_THRESHOLD_PCT}%"

  switch=0
  if [[ "$src_cnt" =~ ^[0-9]+$ && "$tgt_cnt" =~ ^[0-9]+$ && "$src_cnt" -gt 0 ]]; then
    reached=$(awk "BEGIN {print ($tgt_cnt * 100 / $src_cnt) >= $SYNC_THRESHOLD_PCT}")
    if [[ "$reached" == "1" ]]; then
      if [[ "$rate" == "n/a" ]] || [[ "$rate" -lt "$MIN_RATE_TO_STABLE" ]]; then
        stable=$((stable + 1))
        log "进度已达阈值，速率偏低，稳定轮次 ${stable}/${STABLE_ROUNDS}"
        [[ "$stable" -ge "$STABLE_ROUNDS" ]] && switch=1
      else
        stable=0
      fi
    fi
  fi

  if [[ "$switch" -eq 1 ]]; then
    log "全量完成，准备切换增量..."
    break
  fi

  prev_target="$tgt_cnt"
  sleep "$POLL_SEC"
done

log "========== 阶段 2/2：增量（user CDC + adjust Lookup）=========="
cancel_jobs
start_incr "$BULK_START_MS"

log "自动切换完成。日志: ${LOG_FILE}"
