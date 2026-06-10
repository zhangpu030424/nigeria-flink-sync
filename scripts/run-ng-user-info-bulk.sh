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
  local name=$1 db="${LM_MYSQL_DATABASE:-ng_loan_market}"
  local line cnt
  # 与手工 SHOW FULL TABLES 一致；information_schema.views 在部分只读账号下会误报不存在
  line=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$db" -N -e \
    "SHOW FULL TABLES LIKE '${name}';" 2>/dev/null | head -1 || true)
  [[ "$line" == *"VIEW"* ]] && return 0
  cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "$db" \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${db}' AND table_name='${name}' AND table_type='VIEW';")
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
  local view_ddl="sql/ddl/lm_user_info_flink_views.sql"
  local stg_cnt
  stg_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.tables
     WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}'
       AND table_name='flink_stg_mkt_user';")
  if [[ "$stg_cnt" == "1" && -f sql/ddl/lm_user_info_flink_views_staging.sql ]]; then
    view_ddl="sql/ddl/lm_user_info_flink_views_staging.sql"
    log "检测到 flink_stg_mkt_user，使用 ${view_ddl}"
  fi
  if [[ ! -f "$view_ddl" ]]; then
    log "ERR: 缺少 ${view_ddl}"
    exit 1
  fi
  if ! MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
      -u "$LM_MYSQL_USER" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
      < "$view_ddl" 2>>"$LOG_FILE"; then
    if all_views_ready; then
      log "WARN: CREATE VIEW 无权限，但 VIEW 已存在，继续"
      return 0
    fi
    log "ERR: 请用有权限账号手动执行 ${view_ddl}"
    exit 1
  fi
  log "VIEW 已创建/刷新"
}

base_table_exists() {
  local name=$1
  local cnt
  cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM information_schema.tables
     WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}'
       AND table_name='${name}' AND table_type='BASE TABLE';")
  [[ "$cnt" == "1" ]]
}

table_has_column() {
  local table_name=$1 col_name=$2 db="${LM_MYSQL_DATABASE:-ng_loan_market}"
  local line
  line=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$db" -N -e \
    "SHOW COLUMNS FROM \`${table_name}\` LIKE '${col_name}';" 2>/dev/null | head -1 || true)
  [[ -n "$line" ]]
}

staging_table_partition_ready() {
  local t=$1 col=$2
  base_table_exists "$t" && table_has_column "$t" "$col"
}

all_staging_tables_ready() {
  staging_table_partition_ready "flink_stg_mkt_user" "id_part" \
    && staging_table_partition_ready "flink_stg_ud_latest" "user_id_part" \
    && staging_table_partition_ready "flink_stg_lup_latest" "id_part" \
    && staging_table_partition_ready "flink_stg_dac_latest" "id_part"
}

resolve_source_tables() {
  if [[ "${LM_FORCE_VIEW_SOURCE:-0}" == "1" || "${LM_MYSQL_READ_REPLICA:-0}" == "1" ]]; then
    export LM_SRC_TABLE_MKT="v_flink_mkt_user"
    export LM_SRC_TABLE_UD="v_flink_ud_latest"
    export LM_SRC_TABLE_LUP="v_flink_lup_latest"
    export LM_SRC_TABLE_DAC="v_flink_dac_latest"
    export LM_SRC_MODE="view"
    log "源表模式: v_flink_* VIEW（从库/强制 VIEW，不读 flink_stg_*）"
  elif all_staging_tables_ready; then
    export LM_SRC_TABLE_MKT="flink_stg_mkt_user"
    export LM_SRC_TABLE_UD="flink_stg_ud_latest"
    export LM_SRC_TABLE_LUP="flink_stg_lup_latest"
    export LM_SRC_TABLE_DAC="flink_stg_dac_latest"
    export LM_SRC_MODE="staging"
    log "源表模式: flink_stg_* 实体表（4 张齐全）"
  else
    export LM_SRC_TABLE_MKT="v_flink_mkt_user"
    export LM_SRC_TABLE_UD="v_flink_ud_latest"
    export LM_SRC_TABLE_LUP="v_flink_lup_latest"
    export LM_SRC_TABLE_DAC="v_flink_dac_latest"
    export LM_SRC_MODE="view"
    log "源表模式: v_flink_* VIEW（flink_stg_* 未齐，常见于从库只读）"
  fi
  log "  mkt=${LM_SRC_TABLE_MKT} ud=${LM_SRC_TABLE_UD} lup=${LM_SRC_TABLE_LUP} dac=${LM_SRC_TABLE_DAC}"
}

check_source_partition_columns() {
  local -a specs=(
    "${LM_SRC_TABLE_MKT}:id_part"
    "${LM_SRC_TABLE_UD}:user_id_part"
    "${LM_SRC_TABLE_LUP}:id_part"
    "${LM_SRC_TABLE_DAC}:id_part"
  )
  local spec tbl col
  for spec in "${specs[@]}"; do
    tbl="${spec%%:*}"
    col="${spec##*:}"
    if ! table_has_column "$tbl" "$col"; then
      log "ERR: ${tbl} 缺少分区列 ${col} → Flink 报 Unknown column 'id_part' in where clause"
      log "  诊断 SHOW COLUMNS:"
      MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
        -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" \
        "${LM_MYSQL_DATABASE:-ng_loan_market}" -e "SHOW COLUMNS FROM \`${tbl}\`;" 2>&1 | tee -a "$LOG_FILE" || true
      if [[ "$tbl" == "flink_stg_lup_latest" || "$tbl" == "v_flink_lup_latest" ]]; then
        log "  常见原因: 早期建的 flink_stg_lup_latest 无 id_part；主库重跑 lm_user_info_flink_staging_tables.sql 或:"
        log "  ALTER TABLE flink_stg_lup_latest ADD COLUMN id_part DECIMAL(20,0) NOT NULL, ADD KEY idx_id_part(id_part);"
        log "  或强制 VIEW: LM_FORCE_VIEW_SOURCE=1 bash scripts/run-ng-user-info-bulk-max.sh"
      fi
      if [[ "$tbl" == "flink_stg_dac_latest" ]]; then
        log "  dac 可建空表: CREATE TABLE flink_stg_dac_latest (id_part DECIMAL(20,0), deviceId VARCHAR(128), channel VARCHAR(128), KEY(id_part));"
      fi
      return 1
    fi
    log "  ✓ ${tbl}.${col}"
  done
  return 0
}

partition_probe_source() {
  local tbl=$1 col=$2
  local row_cnt
  row_cnt=$(mysql_count "$LM_MYSQL_HOST" "$LM_MYSQL_PORT" "$LM_MYSQL_USER" \
    "$LM_MYSQL_PASSWORD" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    "SELECT COUNT(*) FROM \`${tbl}\`;")
  if [[ "$row_cnt" == "0" ]]; then
    log "  ${tbl} 空表(${row_cnt}行)，跳过分区 WHERE 探测"
    return 0
  fi
  log "  分区探测 SELECT 1 FROM ${tbl} WHERE ${col}>=1 LIMIT 1 ..."
  mysql_probe "SELECT 1 FROM \`${tbl}\` WHERE \`${col}\` >= 1 AND \`${col}\` < 1000000000 LIMIT 1;" | grep -q '^1$'
}

preflight_check() {
  log "---- 提交前检查 ----"
  resolve_source_tables

  if [[ "${LM_SRC_MODE:-view}" == "staging" ]]; then
    local t
    for t in "$LM_SRC_TABLE_MKT" "$LM_SRC_TABLE_UD" "$LM_SRC_TABLE_LUP" "$LM_SRC_TABLE_DAC"; do
      if ! base_table_exists "$t"; then
        log "ERR: 缺少实体表 ${t}"
        if [[ "$t" == "flink_stg_dac_latest" ]]; then
          log "  dac 允许空表，在老库执行:"
          log "  CREATE TABLE flink_stg_dac_latest (id_part DECIMAL(20,0) NOT NULL, deviceId VARCHAR(128) NOT NULL, channel VARCHAR(128), KEY idx_id_part(id_part)) ENGINE=InnoDB;"
        fi
        exit 1
      fi
    done
  else
    local v missing=()
    for v in "${REQUIRED_VIEWS[@]}"; do
      if ! view_in_schema "$v"; then
        missing+=("$v")
      fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
      log "诊断: 用 .env 账号探测 SHOW FULL TABLES（若此处空而你在别的客户端能看到 VIEW，说明 .env 连的不是同一库/账号）"
      MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
        -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" \
        "${LM_MYSQL_DATABASE:-ng_loan_market}" -e \
        "SHOW FULL TABLES LIKE 'v_flink_%'; SELECT @@hostname, @@port, DATABASE();" 2>&1 | tee -a "$LOG_FILE" || true
      log "ERR: 当前库 ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE:-ng_loan_market} 缺少 VIEW: ${missing[*]}"
      log "  从库只读时不能自己建 VIEW，请 DBA 在【主库】执行:"
      log "    mysql -h<主库> -u<写账号> -p ng_loan_market < sql/ddl/lm_user_info_flink_views.sql"
      log "  主从同步后在从库验证: SHOW FULL TABLES LIKE 'v_flink_%';"
      log "  Flink .env 保持从库地址，并设 LM_MYSQL_READ_REPLICA=1"
      log "  详见 docs/LM_REPLICA_MIGRATION.md"
      exit 1
    fi
  fi

  log "分区列检查（JDBC scan.partition.column）:"
  check_source_partition_columns || exit 1

  if [[ "${LM_SKIP_VIEW_PROBE:-0}" != "1" ]]; then
    log "Flink 分区 WHERE 探测（${MYSQL_PROBE_TIMEOUT}s 超时）:"
    partition_probe_source "$LM_SRC_TABLE_MKT" "id_part" || { log "ERR: ${LM_SRC_TABLE_MKT} 分区探测失败"; exit 1; }
    partition_probe_source "$LM_SRC_TABLE_UD" "user_id_part" || { log "ERR: ${LM_SRC_TABLE_UD} 分区探测失败"; exit 1; }
    partition_probe_source "$LM_SRC_TABLE_LUP" "id_part" || { log "ERR: ${LM_SRC_TABLE_LUP} 分区探测失败"; exit 1; }
    partition_probe_source "$LM_SRC_TABLE_DAC" "id_part" || { log "ERR: ${LM_SRC_TABLE_DAC} 分区探测失败"; exit 1; }
  else
    log "LM_SKIP_VIEW_PROBE=1，跳过分区 WHERE 探测"
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

verify_job_source_parallelism() {
  local jid="${1:-}" expect="${2:-30}"
  [[ -z "$jid" ]] && return 0
  local raw min_par
  raw=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}" 2>/dev/null || true)
  [[ -z "$raw" ]] && return 0
  log "---- Job 算子并行度（期望源表≥${expect}）----"
  echo "$raw" | python3 -c "
import json,sys,re
expect=int(sys.argv[1])
d=json.load(sys.stdin)
for v in d.get('vertices',[]):
    n=v.get('name','')
    if 'src_' not in n and 'Source:' not in n:
        continue
    p=v.get('parallelism',-1)
    print(f'  {n[:72]}  parallelism={p}')
" "$expect" 2>/dev/null | tee -a "$LOG_FILE" || true
  min_par=$(echo "$raw" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ps=[]
for v in d.get('vertices',[]):
    n=v.get('name','')
    if 'src_' in n or 'Source:' in n:
        ps.append(v.get('parallelism',0))
print(min(ps) if ps else 0)
" 2>/dev/null || echo "0")
  if [[ "$min_par" =~ ^[0-9]+$ && "$min_par" -lt "$expect" ]]; then
    log "WARN: 源算子并行=${min_par} < 期望${expect}！请 cancel 本 Job + 停 incr + 确认用 run-ng-user-info-bulk.sh 提交（勿直接 run-sql.sh）"
    log "  HashJoin 显示 CREATED/-1 常因 slot 被占满，下游算子排不上队"
  fi
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

SLOTS="${FLINK_TASK_SLOTS:-30}"
INCR_PAR="${FLINK_PARALLELISM_INCR:-1}"
# 32C64G 独占全量时 buffer=0 吃满 30 slot；与 incr 同跑可 export SYNC_SLOT_BUFFER=2
SLOT_BUFFER="${SYNC_SLOT_BUFFER:-0}"
USER_INFO_MAX_PAR="${LM_USER_INFO_MAX_PARALLEL:-30}"
REQ_PARALLEL="${FLINK_PARALLELISM:-${FLINK_PARALLELISM_BULK:-30}}"
BULK_PARALLEL="${REQ_PARALLEL}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

running_n=0
while read -r _jid; do [[ -n "$_jid" ]] && running_n=$((running_n + 1)); done < <(list_running_job_ids)
max_bulk=$((SLOTS - running_n * INCR_PAR - SLOT_BUFFER))
(( max_bulk < 1 )) && max_bulk=1
_CAP_BEFORE="${BULK_PARALLEL}"
if [[ "${SYNC_BULK_IGNORE_SLOT_CAP:-0}" != "1" ]]; then
  (( BULK_PARALLEL > max_bulk )) && BULK_PARALLEL=$max_bulk
fi
(( BULK_PARALLEL > USER_INFO_MAX_PAR )) && BULK_PARALLEL=$USER_INFO_MAX_PAR
export FLINK_PARALLELISM="${BULK_PARALLEL}"

log "user_info 迁移"
log "  源: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
log "  目标: ${TARGET_MYSQL_DATABASE}.user_info @ ${TARGET_MYSQL_HOST}"
log "  LIMIT=${LIMIT_DESC}"
log "  JDBC并行: 请求=${REQ_PARALLEL} slots=${SLOTS} 存量Job=${running_n} 空闲≤${max_bulk} 上限=${USER_INFO_MAX_PAR} → 实际=${FLINK_PARALLELISM}"
if [[ "$REQ_PARALLEL" != "$FLINK_PARALLELISM" ]]; then
  log "  WARN: 并行被压低！要满 30 核: .env FLINK_TASK_SLOTS=30、停 incr Job、或 SYNC_BULK_IGNORE_SLOT_CAP=1"
fi
if [[ "$FLINK_PARALLELISM" -lt 30 && "$REQ_PARALLEL" -ge 30 ]]; then
  log "  提示: 存量 incr 占 slot 时可用≤${max_bulk}；全量迁移建议先 cancel incr 再跑"
fi

print_slot_status() {
  log "---- 集群 slot 实况 ----"
  docker exec "$JM" ./bin/flink list 2>/dev/null | tee -a "$LOG_FILE" || true
  FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/taskmanagers" 2>/dev/null | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    t=f=0
    for tm in d.get('taskmanagers',[]):
        t+=tm.get('slotsNumber',0)
        f+=tm.get('freeSlots',0)
    print(f'TM slots合计={t} 空闲={f}（若合计≠.env FLINK_TASK_SLOTS 请 force-recreate taskmanager）')
except Exception as e:
    print('无法读取 TaskManagers:', e)
" 2>/dev/null | tee -a "$LOG_FILE" || true
}

if [[ "$FLINK_PARALLELISM" -lt "$REQ_PARALLEL" && "${SYNC_BULK_IGNORE_SLOT_CAP:-0}" != "1" ]]; then
  log "ERR: 并行 ${FLINK_PARALLELISM} < 请求 ${REQ_PARALLEL}（存量 Flink Job=${running_n} 占 slot）"
  log "  列表页 Tasks=4 是 4 个算子，不是 30 核；当前只会每个算子 ${FLINK_PARALLELISM} 路 JDBC"
  log "  解决: ① cancel 存量 incr  ② .env FLINK_TASK_SLOTS=30 + recreate TM  ③ 或 SYNC_BULK_IGNORE_SLOT_CAP=1"
  print_slot_status
  exit 1
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  log "ERR: 容器 ${JM} 未运行"
  exit 1
fi

print_slot_status
bash scripts/check-flink-slots.sh 2>&1 | tee -a "$LOG_FILE" || true

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
verify_job_source_parallelism "${JOB_ID:-}" "$FLINK_PARALLELISM"

monitor_loop "${JOB_ID:-}"
final_state=$(flink_job_state "${JOB_ID:-}")
[[ "$final_state" == "FAILED" ]] && exit 1
