#!/usr/bin/env bash
# user_info：CDC 分片快照 + Temporal Lookup（不在 MySQL 建 VIEW）
# 用法: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-cdc-lookup.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写 LM_* / TARGET_*"
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
: "${LM_MYSQL_USER:?}"
: "${LM_MYSQL_PASSWORD:?}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-2}"

# 多 CDC 源须错开 server-id；每段宽度 ≥ FLINK_PARALLELISM（CDC chunk 并行）
_base="${LM_CDC_SERVER_ID_BASE:-5700}"
_sid_span=$((FLINK_PARALLELISM + 2))
export LM_CDC_SERVER_ID_USER="${LM_CDC_SERVER_ID_USER:-$((_base + 1))-$((_base + _sid_span))}"
export LM_CDC_SERVER_ID_USER_DATA="${LM_CDC_SERVER_ID_USER_DATA:-$((_base + 20))-$((_base + 20 + _sid_span))}"
export LM_CDC_SERVER_ID_LUP="${LM_CDC_SERVER_ID_LUP:-$((_base + 40))-$((_base + 40 + _sid_span))}"
export LM_CDC_SERVER_ID_DAC="${LM_CDC_SERVER_ID_DAC:-$((_base + 60))-$((_base + 60 + _sid_span))}"
export LM_CDC_SERVER_ID_URI="${LM_CDC_SERVER_ID_URI:-$((_base + 80))-$((_base + 80 + _sid_span))}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-cdc-lookup-sql.log"
mkdir -p "$LOG_DIR"

list_running_job_ids() {
  docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u || true
}

latest_job_id() {
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/overview" 2>/dev/null \
    | grep -oE '"jid":"[a-f0-9]{32}"' | tail -1 | cut -d'"' -f4 || true
}

print_job_hint() {
  echo "[$(date '+%F %T')] 最近 Job（含 FINISHED/FAILED）:"
  docker exec "$JM" ./bin/flink list -a 2>/dev/null | tail -20 || true
  local jid
  jid=$(latest_job_id || true)
  [[ -n "${jid:-}" ]] && echo "[$(date '+%F %T')] Web UI: http://127.0.0.1:${FLINK_WEB_PORT}/#/job/${jid}/overview"
}

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

PREP="/tmp/sync-user-info-cdc-lookup-$$.sql"
cp lm/sql/04_sync_ng_user_info_cdc_lookup.sql "$PREP"

if [[ "$LM_MIGRATION_LIMIT" -lt 2147483647 ]]; then
  _snap_override=$',\n    '\''debezium.snapshot.select.statement.overrides'\'' = '\'''"${LM_MYSQL_DATABASE}"'.user'\'',\n    '\''debezium.snapshot.select.statement.overrides.'"${LM_MYSQL_DATABASE}"'.user'\'' = '\''SELECT * FROM user ORDER BY id DESC LIMIT '"${LM_MIGRATION_LIMIT}"'\'''
else
  _snap_override=""
fi
python3 - "$PREP" "$_snap_override" <<'PY'
import sys
path, override = sys.argv[1], sys.argv[2]
text = open(path, encoding="utf-8").read()
text = text.replace("__CDC_USER_SNAPSHOT_OVERRIDE__", override)
open(path, "w", encoding="utf-8").write(text)
PY

REG_IP_TABLE=""
for tbl in user_reg_ip user_registration_ip; do
  if table_exists "$tbl"; then
    REG_IP_TABLE="$tbl"
    break
  fi
done

python3 - "$PREP" "${REG_IP_TABLE}" "$(table_exists app && echo 1 || echo 0)" <<'PY'
import re, sys
path, reg_ip_table, has_app = sys.argv[1], sys.argv[2], sys.argv[3] == "1"
text = open(path, encoding="utf-8").read()

if not reg_ip_table:
    text = re.sub(
        r"-- registration_ip：.*?\nCREATE TABLE cdc_mkt_user_reg_ip.*?;\n\n",
        "",
        text,
        count=1,
        flags=re.S,
    )
    text = re.sub(
        r"CREATE TEMPORARY VIEW v_uri_latest AS.*?;\n\n",
        "",
        text,
        count=1,
        flags=re.S,
    )
    text = text.replace(
        "'registration_ip' VALUE uri.ip,",
        "'registration_ip' VALUE CAST(NULL AS STRING),",
    )
    text = text.replace("LEFT JOIN v_uri_latest uri ON uri.`userId` = u.id\n", "")
else:
    text = text.replace("${LM_USER_REG_IP_TABLE}", reg_ip_table)

if not has_app:
    text = re.sub(r"-- 小子表 app：.*?\nCREATE TABLE dim_mkt_app.*?;\n\n", "", text, count=1, flags=re.S)
    text = text.replace("'name' VALUE ap.name,", "'name' VALUE CAST(NULL AS STRING),")
    text = text.replace(
        "LEFT JOIN dim_mkt_app FOR SYSTEM_TIME AS OF u.proc_time AS ap\n    ON ap.id = u.`appId`\n",
        "",
    )

open(path, "w", encoding="utf-8").write(text)
PY

if [[ -n "$REG_IP_TABLE" ]]; then
  export LM_USER_REG_IP_TABLE="$REG_IP_TABLE"
  echo "[$(date '+%F %T')] registration_ip CDC 源: ${REG_IP_TABLE}"
else
  echo "[$(date '+%F %T')] WARN: 无 user_reg_ip / user_registration_ip，registration_ip 写 NULL"
fi

if table_exists "app"; then
  echo "[$(date '+%F %T')] app 维表: JDBC Temporal Lookup"
else
  echo "[$(date '+%F %T')] WARN: 无 app 表，app.name 写 NULL"
fi

echo "[$(date '+%F %T')] user_info CDC+Lookup LIMIT=${LM_MIGRATION_LIMIT} 并行=${FLINK_PARALLELISM}"
echo "[$(date '+%F %T')] CDC server-id: user=${LM_CDC_SERVER_ID_USER} ud=${LM_CDC_SERVER_ID_USER_DATA}"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行，请先 ./scripts/up.sh"
  exit 1
fi

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

BEFORE_JOBS=$(list_running_job_ids | tr '\n' ' ')
echo "[$(date '+%F %T')] 提交前 RUNNING: ${BEFORE_JOBS:-<无>}"

set +e
bash scripts/run-sql.sh "$PREP" 2>&1 | tee "$SQL_LOG"
SQL_RC=${PIPESTATUS[0]}
set -e
rm -f "$PREP"

if grep -qiE 'Exception|ERROR|ValidationException|TableException|SqlParserException' "$SQL_LOG" 2>/dev/null; then
  echo "[$(date '+%F %T')] ERR: sql 日志含异常，请查看 ${SQL_LOG}"
  grep -iE 'Exception|ERROR|ValidationException|TableException|SqlParserException' "$SQL_LOG" | tail -15 || true
fi

echo "[$(date '+%F %T')] sql-client 退出码=${SQL_RC}"
if [[ "$SQL_RC" -ne 0 ]]; then
  print_job_hint
  exit "$SQL_RC"
fi

JOB_ID=""
for _ in 1 2 3 4 5; do
  sleep 1
  for jid in $(list_running_job_ids); do
    if ! echo " $BEFORE_JOBS " | grep -q " $jid "; then
      JOB_ID="$jid"
      break 2
    fi
  done
done
JOB_ID="${JOB_ID:-$(latest_job_id || true)}"

if [[ -z "${JOB_ID:-}" ]]; then
  echo "[$(date '+%F %T')] WARN: 未发现 Job。Batch+CDC 若 planning 失败通常此处为空；请查 ${SQL_LOG}"
  print_job_hint
  exit 1
fi

echo "[$(date '+%F %T')] Job=${JOB_ID}（Batch 模式跑完即 FINISHED，不一定在 Running 里）"
print_job_hint
echo "[$(date '+%F %T')] 完成。验证: SELECT COUNT(*) FROM user_info;"
