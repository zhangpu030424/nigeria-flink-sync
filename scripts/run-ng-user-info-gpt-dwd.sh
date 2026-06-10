#!/usr/bin/env bash
# sql.md DWD 版 user_info：老库 JDBC → DWD 表（目标库）→ Lookup → user_info
# 用法: bash scripts/run-ng-user-info-gpt-dwd.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-dwd.sh
set -euo pipefail
cd "$(dirname "$0")/.."

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

export DWD_MYSQL_HOST="${DWD_MYSQL_HOST:-${TARGET_MYSQL_HOST:?}}"
export DWD_MYSQL_PORT="${DWD_MYSQL_PORT:-${TARGET_MYSQL_PORT:-3306}}"
export DWD_MYSQL_USER="${DWD_MYSQL_USER:-${TARGET_MYSQL_USER:?}}"
export DWD_MYSQL_PASSWORD="${DWD_MYSQL_PASSWORD:-${TARGET_MYSQL_PASSWORD:?}}"
export DWD_MYSQL_DATABASE="${DWD_MYSQL_DATABASE:-ng_migration_dwd}"

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-20}}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"

export LM_SRC_TABLE_URI_BASE="${LM_SRC_TABLE_URI_BASE:-user_registration_ip}"
export LM_SRC_TABLE_APP_BASE="${LM_SRC_TABLE_APP_BASE:-app}"
export LM_USER_ID_RANGE_CLAUSE="${LM_USER_ID_RANGE_CLAUSE:-}"

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY u.user_id LIMIT ${LM_MIGRATION_LIMIT}"
  export LM_MIGRATION_LIMIT_CLAUSE_DWD_USER="ORDER BY CAST(id AS UNSIGNED) LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  export LM_MIGRATION_LIMIT_CLAUSE_DWD_USER=""
  LIMIT_DESC="全量"
fi

mysql_dwd() {
  MYSQL_PWD="$DWD_MYSQL_PASSWORD" mysql --connect-timeout=15 \
    -h "$DWD_MYSQL_HOST" -P "$DWD_MYSQL_PORT" -u "$DWD_MYSQL_USER" "$DWD_MYSQL_DATABASE" \
    -N -e "$1"
}

mysql_lm_table_exists() {
  local tbl=$1
  MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0
}

wait_job() {
  local jid=$1
  local st i
  for ((i=0; i<86400; i+=15)); do
    st=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    case "$st" in
      FINISHED) echo ">> Job ${jid} FINISHED"; return 0 ;;
      FAILED|CANCELED) echo "ERR: Job ${jid} ${st}"; return 1 ;;
      *) [[ $((i % 60)) -eq 0 && -n "$st" ]] && echo "[$(date '+%F %T')] ${jid} ${st}..." ;;
    esac
    sleep 15
  done
  echo "ERR: Job ${jid} 超时"
  return 1
}

submit_and_wait() {
  local sql_file=$1
  local label=$2
  echo ""
  echo ">> ${label}: ${sql_file}"
  local before job_id
  before=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | tr '\n' ' ')
  bash scripts/run-sql.sh "$sql_file"
  sleep 5
  job_id=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | while read -r j; do
    [[ " $before " == *" $j "* ]] && continue
    echo "$j"
  done | head -1)
  if [[ "$job_id" =~ ^[a-f0-9]{32}$ ]]; then
    wait_job "$job_id"
  else
    echo "WARN: 未捕获 JobId，请到 Web UI 确认"
  fi
}

# reg_ip 灌数（追加到 load SQL 末尾）
build_dwd_load_sql() {
  local out="/tmp/dwd_load_ng_user_info-$$.sql"
  cp sql/06_dwd_load_ng_user_info_batch.sql "$out"
  if [[ "$(mysql_lm_table_exists "$LM_SRC_TABLE_URI_BASE")" == "1" ]]; then
    cat >> "$out" <<'EOSQL'

INSERT INTO dwd_latest_user_reg_ip
SELECT
    CAST(`userId` AS BIGINT) AS userId,
    CAST(id AS BIGINT) AS id,
    COALESCE(ip, '') AS ip,
    created
FROM (
    SELECT uri.*, ROW_NUMBER() OVER (PARTITION BY uri.`userId` ORDER BY uri.id DESC) AS rn
    FROM m_user_reg_ip uri
) WHERE rn = 1;
EOSQL
  else
    echo ">> WARN: 无 ${LM_SRC_TABLE_URI_BASE}，跳过 dwd_latest_user_reg_ip"
  fi
  echo "$out"
}

echo "========== DWD 版 user_info（sql.md）=========="
echo "  老库读: ${LM_MYSQL_HOST}:${LM_MYSQL_PORT:-3306}/${LM_MYSQL_DATABASE}"
echo "  DWD写:  ${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}/${DWD_MYSQL_DATABASE}"
echo "  目标:   ${TARGET_MYSQL_DATABASE}.user_info"
echo "  LIMIT:  ${LIMIT_DESC}"

echo ""
echo ">> Step 1/4: 创建 DWD 库 ${DWD_MYSQL_DATABASE}（同实例 ${DWD_MYSQL_HOST}:${DWD_MYSQL_PORT}）"
MYSQL_PWD="$DWD_MYSQL_PASSWORD" mysql --connect-timeout=30 \
  -h "$DWD_MYSQL_HOST" -P "$DWD_MYSQL_PORT" -u "$DWD_MYSQL_USER" \
  -e "CREATE DATABASE IF NOT EXISTS \`${DWD_MYSQL_DATABASE}\` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"

echo ">> Step 1/4: DWD 表 DDL"
MYSQL_PWD="$DWD_MYSQL_PASSWORD" mysql --connect-timeout=30 \
  -h "$DWD_MYSQL_HOST" -P "$DWD_MYSQL_PORT" -u "$DWD_MYSQL_USER" "$DWD_MYSQL_DATABASE" \
  < sql/ddl/dwd_user_info_staging.sql

if [[ "${DWD_TRUNCATE_BEFORE_LOAD:-1}" == "1" ]]; then
  echo ">> Step 2/4: TRUNCATE dwd_*"
  mysql_dwd "SET FOREIGN_KEY_CHECKS=0;
    TRUNCATE TABLE dwd_latest_user_reg_ip;
    TRUNCATE TABLE dwd_latest_device_channel;
    TRUNCATE TABLE dwd_latest_user_password;
    TRUNCATE TABLE dwd_latest_user_data;
    TRUNCATE TABLE dwd_user_base;
    SET FOREIGN_KEY_CHECKS=1;"
else
  echo ">> Step 2/4: 跳过 TRUNCATE（DWD_TRUNCATE_BEFORE_LOAD=0）"
fi

bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true

DWD_LOAD_SQL=$(build_dwd_load_sql)
submit_and_wait "$DWD_LOAD_SQL" "Step 3/4 Flink 灌 DWD"
rm -f "$DWD_LOAD_SQL"

cnt=$(mysql_dwd "SELECT COUNT(*) FROM dwd_user_base;" 2>/dev/null || echo "?")
echo ">> dwd_user_base=${cnt} 行"

if [[ "$(mysql_lm_table_exists "$LM_SRC_TABLE_APP_BASE")" != "1" ]]; then
  echo ">> WARN: 无 ${LM_SRC_TABLE_APP_BASE}，sync 去掉 app Lookup"
  export LM_SRC_TABLE_APP_BASE="app"
  SYNC_SQL="/tmp/dwd_sync_user_info-$$.sql"
  python3 - "$SYNC_SQL" <<'PY'
import re, sys
text = open("sql/06_sync_ng_user_info_from_dwd.sql", encoding="utf-8").read()
text = text.replace("'name' VALUE COALESCE(app.name, '')", "'name' VALUE CAST('' AS STRING)")
text = re.sub(
    r"LEFT JOIN m_app FOR SYSTEM_TIME AS OF PROCTIME\(\) AS app\s*ON app\.id = u\.app_id\s*",
    "",
    text,
    count=1,
)
text = re.sub(
    r"CREATE TABLE m_app \([\s\S]*?\);\s*",
    "",
    text,
    count=1,
)
open(sys.argv[1], "w", encoding="utf-8").write(text)
PY
  submit_and_wait "$SYNC_SQL" "Step 4/4 DWD → user_info"
  rm -f "$SYNC_SQL"
else
  submit_and_wait sql/06_sync_ng_user_info_from_dwd.sql "Step 4/4 DWD → user_info"
fi

echo ""
echo ">> 完成。验证: SELECT COUNT(*) FROM user_info;"
