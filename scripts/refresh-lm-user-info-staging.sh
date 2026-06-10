#!/usr/bin/env bash
# 老库落地 user_info 同步用实体表（聚合一次），再刷新 VIEW 指向实体表
# 用法: bash scripts/refresh-lm-user-info-staging.sh
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
: "${LM_MYSQL_USER:?}"
: "${LM_MYSQL_PASSWORD:?}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

echo ">> 老库落地实体表（可能需 10～60 分钟，视数据量）"
echo ">> ${LM_MYSQL_DATABASE} @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo ">> 开始: $(date '+%F %T')"

MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < sql/ddl/lm_user_info_flink_staging_tables.sql

echo ">> 刷新 VIEW → 实体表"
MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < sql/ddl/lm_user_info_flink_views_staging.sql

echo ">> 完成: $(date '+%F %T')"
for t in flink_stg_mkt_user flink_stg_ud_latest flink_stg_lup_latest flink_stg_dac_latest; do
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM ${t};" 2>/dev/null || echo "?")
  echo "   ${t}: ${cnt} 行"
done
