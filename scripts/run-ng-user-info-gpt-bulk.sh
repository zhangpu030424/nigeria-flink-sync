#!/usr/bin/env bash
# GPT 版 user_info 全量/限量 Flink Batch（05_sync_ng_gpt_user_info_bulk.sql）
# 用法: bash scripts/run-ng-user-info-gpt-bulk.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-bulk.sh
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

: "${LM_MYSQL_HOST:?}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ && "$LM_MIGRATION_LIMIT" -gt 0 && "$LM_MIGRATION_LIMIT" -lt 10000000 ]]; then
  export LM_MIGRATION_LIMIT_CLAUSE="ORDER BY user_id LIMIT ${LM_MIGRATION_LIMIT}"
else
  export LM_MIGRATION_LIMIT_CLAUSE=""
fi

mysql_count() {
  MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "$1" 2>/dev/null || echo "ERR"
}

# 源表：优先 flink_stg_*，否则 v_flink_*
if [[ "$(mysql_count "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='flink_stg_mkt_user'")" == "1" ]]; then
  export LM_SRC_TABLE_MKT="flink_stg_mkt_user"
  export LM_SRC_TABLE_UD="flink_stg_ud_latest"
  export LM_SRC_TABLE_LUP="flink_stg_lup_latest"
  export LM_SRC_TABLE_DAC="flink_stg_dac_latest"
else
  export LM_SRC_TABLE_MKT="v_flink_mkt_user"
  export LM_SRC_TABLE_UD="v_flink_ud_latest"
  export LM_SRC_TABLE_LUP="v_flink_lup_latest"
  export LM_SRC_TABLE_DAC="v_flink_dac_latest"
  if [[ -f sql/ddl/lm_user_info_flink_views.sql ]]; then
    MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" \
      -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" < sql/ddl/lm_user_info_flink_views.sql 2>/dev/null || true
  fi
fi

export LM_SRC_TABLE_URI="${LM_SRC_TABLE_URI:-v_flink_uri_latest}"
export LM_SRC_TABLE_APP="${LM_SRC_TABLE_APP:-v_flink_mkt_app}"
if [[ -f sql/ddl/lm_user_info_gpt_views.sql ]]; then
  MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql -h "$LM_MYSQL_HOST" -P "${LM_MYSQL_PORT:-3306}" \
    -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" < sql/ddl/lm_user_info_gpt_views.sql 2>/dev/null || true
fi

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"

echo "[$(date '+%F %T')] GPT 版 user_info Flink 同步"
echo "  源: mkt=${LM_SRC_TABLE_MKT} uri=${LM_SRC_TABLE_URI} app=${LM_SRC_TABLE_APP}"
echo "  LIMIT: ${LM_MIGRATION_LIMIT_CLAUSE:-全量}"

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

bash scripts/run-sql.sh sql/05_sync_ng_gpt_user_info_bulk.sql
