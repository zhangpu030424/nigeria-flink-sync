#!/usr/bin/env bash
# 监控同步进度：按固定间隔打印目标库增量、速率、与源库对比
#
# 用法:
#   ./scripts/monitor-sync.sh                    # 默认 user 表，60 秒一轮
#   ./scripts/monitor-sync.sh user 30            # 表名 + 间隔秒数
#   ./scripts/monitor-sync.sh user 60 <job_id>   # 附带 Flink 累计写出条数
set -euo pipefail
cd "$(dirname "$0")/.."

TABLE="${1:-user}"
INTERVAL="${2:-60}"
JOB_ID="${3:-}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-monitor.log"
LAST_MYSQL_ERR=""

mkdir -p "$LOG_DIR"

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写"
  exit 1
fi

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"
USER_ID_OFFSET="${USER_ID_OFFSET:-100000000}"
# shellcheck source=scripts/lib/sync-monitor-sql.sh
source scripts/lib/sync-monitor-sql.sh

SRC_CNT_SQL=""
TGT_CNT_SQL=""
MONITOR_COUNT_MODE="absolute"
if mapfile -t _sqls < <(lookup_job_monitor_sql "$TABLE"); then
  SRC_CNT_SQL="${_sqls[0]:-}"
  TGT_CNT_SQL="${_sqls[1]:-}"
  MONITOR_COUNT_MODE=$(resolve_monitor_count_mode "$TGT_CNT_SQL")
fi
if [[ -z "$TGT_CNT_SQL" ]]; then
  TGT_CNT_SQL="SELECT COUNT(*) FROM \`${TABLE}\`;"
  MONITOR_COUNT_MODE="absolute"
fi
if [[ -z "$SRC_CNT_SQL" ]]; then
  SRC_CNT_SQL="SELECT COUNT(*) FROM \`${TABLE}\`;"
fi

FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

mysql_q() {
  local host=$1 port=$2 user=$3 pass=$4 db=$5 sql=$6
  local err
  err=$(MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e "$sql" 2>&1) || {
    LAST_MYSQL_ERR="$err"
    return 1
  }
  echo "$err"
}

count_target() {
  mysql_q "$TARGET_MYSQL_HOST" "$TARGET_MYSQL_PORT" "$TARGET_MYSQL_USER" \
    "$TARGET_MYSQL_PASSWORD" "$TARGET_MYSQL_DATABASE" "$TGT_CNT_SQL"
}

count_source() {
  mysql_q "$SOURCE_MYSQL_HOST" "$SOURCE_MYSQL_PORT" "$SOURCE_MYSQL_USER" \
    "$SOURCE_MYSQL_PASSWORD" "$SOURCE_MYSQL_DATABASE" "$SRC_CNT_SQL"
}

flink_records_out() {
  [[ -z "$JOB_ID" ]] && echo "n/a" && return 0
  local raw
  raw=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}/metrics?get=0.numRecordsOut" 2>/dev/null || true)
  echo "$raw" | grep -oE '"value":"[0-9]+"' | head -1 | grep -oE '[0-9]+' || echo "n/a"
}

log_line() {
  echo "$1" | tee -a "$LOG_FILE"
}

log_line "========================================"
log_line "[$(date '+%Y-%m-%d %H:%M:%S')] 监控开始 表=${TABLE} 间隔=${INTERVAL}s 口径=${MONITOR_COUNT_MODE}"
log_line "源库: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"
log_line "目标: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}"
log_line "源计数 SQL: ${SRC_CNT_SQL}"
log_line "目标计数 SQL: ${TGT_CNT_SQL}"
[[ -n "$JOB_ID" ]] && log_line "Flink Job: ${JOB_ID}  Web UI: http://127.0.0.1:${FLINK_WEB_PORT}"
log_line "日志: ${LOG_FILE}  (Ctrl+C 停止)"

# 启动时测一次目标库，失败则打印原因
if ! count_target >/dev/null 2>&1; then
  log_line "[WARN] 目标库查询失败: ${LAST_MYSQL_ERR}"
  log_line "[WARN] 请在本机执行: mysql -h \$TARGET_MYSQL_HOST -u \$TARGET_MYSQL_USER -p \$TARGET_MYSQL_DATABASE -e 'SELECT COUNT(*) FROM ${TABLE};'"
fi
log_line "----------------------------------------"

prev_target=""
prev_ts=$(date +%s)
round=0
err_logged=0

while true; do
  round=$((round + 1))
  now=$(date '+%Y-%m-%d %H:%M:%S')
  now_ts=$(date +%s)

  if target_cnt=$(count_target 2>/dev/null); then
    :
  else
    target_cnt="ERR"
    if (( err_logged == 0 )) && [[ -n "$LAST_MYSQL_ERR" ]]; then
      log_line "[ERR] 目标库: ${LAST_MYSQL_ERR}"
      err_logged=1
    fi
  fi

  source_cnt=$(count_source 2>/dev/null || echo "ERR")

  delta="n/a"
  rate="n/a"
  progress="n/a"

  if [[ "$target_cnt" =~ ^[0-9]+$ && "$prev_target" =~ ^[0-9]+$ ]]; then
    delta=$((target_cnt - prev_target))
    elapsed=$((now_ts - prev_ts))
    if (( elapsed > 0 )); then
      rate=$(awk "BEGIN {printf \"%.1f\", $delta * 60 / $elapsed}")
    fi
  fi

  if [[ "$target_cnt" =~ ^[0-9]+$ && "$source_cnt" =~ ^[0-9]+$ && "$source_cnt" -gt 0 ]]; then
    progress=$(awk "BEGIN {printf \"%.2f%%\", $target_cnt * 100 / $source_cnt}")
  fi

  flink_out=$(flink_records_out)

  if [[ "${MONITOR_COUNT_MODE}" == "absolute" && "$source_cnt" =~ ^[0-9]+$ ]]; then
    log_line "[${now}] #${round} 宽表=${source_cnt} 目标=${target_cnt} 期望≈${source_cnt} 进度=${progress} 本段+${delta} 速率≈${rate}条/分钟 Flink写出=${flink_out}"
  else
    log_line "[${now}] #${round} 目标=${target_cnt} 源=${source_cnt} 进度=${progress} 本段+${delta} 速率≈${rate}条/分钟 Flink写出=${flink_out}"
  fi

  prev_target="$target_cnt"
  prev_ts=$now_ts
  sleep "$INTERVAL"
done
