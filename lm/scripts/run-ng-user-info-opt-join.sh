#!/usr/bin/env bash
# user_info Flink 多表 Join（VIEW 减轻版）：MySQL 过滤 + Flink 轻量 Join
# 用法: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-opt-join.sh
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

: "${LM_MYSQL_HOST:?}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-2}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-opt-join-sql.log"
mkdir -p "$LOG_DIR"

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="${LM_MYSQL_PASSWORD}" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" -u "$LM_MYSQL_USER" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE:-ng_loan_market}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

echo "[$(date '+%F %T')] user_info Join+VIEW 试跑 LIMIT=${LM_MIGRATION_LIMIT}"
bash lm/scripts/refresh-flink-migration-pick.sh

PREP="/tmp/sync-user-info-opt-join-$$.sql"
cp lm/sql/04_sync_ng_user_info_opt_join.sql "$PREP"

if ! table_exists "user_reg_ip" && ! table_exists "user_registration_ip"; then
  python3 - "$PREP" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
text = re.sub(r"-- registration_ip 源：.*?\nCREATE TABLE src_pick_uri.*?;\n\n", "", text, count=1, flags=re.S)
text = text.replace("'registration_ip' VALUE uri.ip,", "'registration_ip' VALUE CAST(NULL AS STRING),")
text = text.replace("LEFT JOIN src_pick_uri uri ON uri.`userId` = u.id\n", "")
open(path, "w", encoding="utf-8").write(text)
PY
fi

if ! table_exists "app"; then
  python3 - "$PREP" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
text = re.sub(r"CREATE TABLE src_mkt_app.*?;\n\n", "", text, count=1, flags=re.S)
text = text.replace("'name' VALUE ap.name,", "'name' VALUE CAST(NULL AS STRING),")
text = text.replace("LEFT JOIN src_mkt_app ap ON ap.id = u.`appId`;\n", "")
open(path, "w", encoding="utf-8").write(text)
PY
fi

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行"
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
