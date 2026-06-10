#!/usr/bin/env bash
# user_info Flink 多表 Join（JDBC 分区读版）：大表按 id 分片并行，仍全量 HashAggregate
# 用法: FLINK_PARALLELISM=8 LM_MIGRATION_ID_UPPER=500000000 bash lm/scripts/run-ng-user-info-opt-partition.sh
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
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-2147483647}"
export LM_MIGRATION_ID_UPPER="${LM_MIGRATION_ID_UPPER:-500000000}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-${FLINK_PARALLELISM_BULK:-8}}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-opt-partition-sql.log"
mkdir -p "$LOG_DIR"

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

PREP="/tmp/sync-user-info-opt-part-$$.sql"
cp lm/sql/04_sync_ng_user_info_opt_partition.sql "$PREP"

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
        r"-- registration_ip 源表：.*?\nCREATE TABLE src_mkt_user_reg_ip.*?;\n\n",
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
    text = re.sub(r"CREATE TABLE src_mkt_app.*?;\n\n", "", text, count=1, flags=re.S)
    text = text.replace("'name' VALUE ap.name,", "'name' VALUE CAST(NULL AS STRING),")
    text = text.replace("LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`;\n", "")
    text = text.replace("LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`\n", "")

open(path, "w", encoding="utf-8").write(text)
PY

if [[ -n "$REG_IP_TABLE" ]]; then
  export LM_USER_REG_IP_TABLE="$REG_IP_TABLE"
  echo "[$(date '+%F %T')] registration_ip 源表: ${REG_IP_TABLE}"
else
  echo "[$(date '+%F %T')] WARN: 无 user_reg_ip / user_registration_ip，registration_ip 写 NULL"
fi

if table_exists "app"; then
  echo "[$(date '+%F %T')] app 源表: app"
else
  echo "[$(date '+%F %T')] WARN: 无 app 表，app.name 写 NULL"
fi

echo "[$(date '+%F %T')] user_info Join+分区读 并行=${FLINK_PARALLELISM} id_upper=${LM_MIGRATION_ID_UPPER} LIMIT=${LM_MIGRATION_LIMIT}"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行，请先 ./scripts/up.sh"
  exit 1
fi

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

bash scripts/run-sql.sh "$PREP" 2>&1 | tee "$SQL_LOG"
rm -f "$PREP"
echo "[$(date '+%F %T')] 完成。验证: SELECT COUNT(*) FROM user_info;"
