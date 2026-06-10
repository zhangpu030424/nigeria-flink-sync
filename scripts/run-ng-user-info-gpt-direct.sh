#!/usr/bin/env bash
# GPT user_info：Flink 直连 VIEW，无物化表、无 JDBC 分区（避免 Step3 大 INSERT 卡住）
# 用法: bash scripts/run-ng-user-info-gpt-direct.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-direct.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

SLOTS="${FLINK_TASK_SLOTS:-20}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"

set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${line%%=*}"
  [[ "$key" == "FLINK_PARALLELISM" ]] && continue
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-20}}"
if [[ "${FLINK_PARALLELISM}" -gt "${FLINK_TASK_SLOTS:-20}" ]]; then
  echo "WARN: FLINK_PARALLELISM=${FLINK_PARALLELISM} > FLINK_TASK_SLOTS=${FLINK_TASK_SLOTS}，降为 ${FLINK_TASK_SLOTS}"
  export FLINK_PARALLELISM="${FLINK_TASK_SLOTS}"
  export FLINK_PARALLELISM_BULK="${FLINK_TASK_SLOTS}"
fi
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY user_id LIMIT ${LM_MIGRATION_LIMIT}"
  LIMIT_DESC="${LM_MIGRATION_LIMIT}"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
  LIMIT_DESC="全量"
fi

# 只用 VIEW，不读 flink_stg_*
export LM_SRC_TABLE_MKT="v_flink_mkt_user"
export LM_SRC_TABLE_UD="v_flink_ud_latest"
export LM_SRC_TABLE_LUP="v_flink_lup_latest"
export LM_SRC_TABLE_DAC="v_flink_dac_latest"
export LM_SRC_TABLE_URI="v_flink_uri_latest"
export LM_SRC_TABLE_APP="v_flink_mkt_app"

# shellcheck source=lib/lm-mysql-write.sh
source "$(dirname "$0")/lib/lm-mysql-write.sh"

mysql_read() {
  lm_mysql_query_read "$1" 2>/dev/null || echo "ERR"
}

view_ok() {
  local v=$1
  [[ "$(mysql_read "SELECT COUNT(*) FROM information_schema.views
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';")" == "1" ]]
}

ensure_views() {
  local need=0
  for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest; do
    view_ok "$v" || need=1
  done
  if [[ "$need" == "0" ]]; then
    echo ">> 从库 VIEW 已就绪"
    return 0
  fi
  echo ">> 从库缺 VIEW，尝试主库创建（仅 CREATE VIEW，不物化表）..."
  if ! lm_mysql_assert_writable 2>/dev/null; then
    echo "ERR: 从库无 VIEW 且无法写主库。请 DBA 在主库执行:"
    echo "  sql/ddl/lm_user_info_flink_views.sql"
    echo "  sql/ddl/lm_user_info_gpt_views.sql"
    exit 1
  fi
  lm_mysql_exec_write sql/ddl/lm_user_info_flink_views.sql
  uri_cnt=$(lm_mysql_query_write "SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='user_registration_ip';" || echo 0)
  if [[ "$uri_cnt" == "1" ]]; then
    lm_mysql_exec_write sql/ddl/lm_user_info_gpt_views.sql || true
  else
    echo ">> WARN: 无 user_registration_ip，跳过 v_flink_uri_latest"
  fi
  echo ">> 等 VIEW 同步到从库 ${LM_MYSQL_HOST} 后再提交 Flink（通常数分钟）"
  sleep 5
  for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest; do
    view_ok "$v" || echo ">> WARN: 从库仍无 ${v}，若 Job 失败请稍等同步后重试"
  done
}

ensure_views

echo "[$(date '+%F %T')] GPT user_info 直连 VIEW（无分区、无物化）"
echo "  读: ${LM_MYSQL_HOST}:${LM_MYSQL_PORT:-3306}/${LM_MYSQL_DATABASE}"
echo "  源: ${LM_SRC_TABLE_MKT} + uri/app"
echo "  Flink parallelism.default=${FLINK_PARALLELISM}（JDBC 源单路读，Sink/Join 可并行）"
echo "  LIMIT: ${LIMIT_DESC}"

bash scripts/check-flink-slots.sh || true

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

BEFORE=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | tr '\n' ' ')
bash scripts/run-sql.sh sql/05_sync_ng_gpt_user_info_direct.sql

sleep 3
JOB_ID=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u | while read -r j; do
  [[ " $BEFORE " == *" $j "* ]] && continue
  echo "$j"
done | head -1)

if [[ -n "${JOB_ID:-}" ]]; then
  echo ">> Job=${JOB_ID}"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for v in d.get('vertices',[]):
    print(f\"  {v.get('name','')[:72]}  p={v.get('parallelism')}\")
" || true
fi

echo "[$(date '+%F %T')] 已提交。Web UI 看 Running Jobs → sink_user_info"
