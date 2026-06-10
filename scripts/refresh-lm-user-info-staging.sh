#!/usr/bin/env bash
# 老库落地 user_info 同步用实体表（聚合一次），再刷新 VIEW 指向实体表
# 用法: bash scripts/refresh-lm-user-info-staging.sh
# DDL 走 LM_MYSQL_WRITE_*（主库）；未配置时等同 LM_MYSQL_*
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

# shellcheck source=lib/lm-mysql-write.sh
source "$(dirname "$0")/lib/lm-mysql-write.sh"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

lm_mysql_assert_writable || exit 1

echo ">> 老库落地实体表（可能需 10～60 分钟，视数据量）"
echo ">> ${LM_MYSQL_DATABASE} @ $(lm_mysql_write_host):$(lm_mysql_write_port)"
echo ">> 开始: $(date '+%F %T')"

lm_mysql_exec_write sql/ddl/lm_user_info_flink_staging_tables.sql

echo ">> 刷新 VIEW → 实体表"
lm_mysql_exec_write sql/ddl/lm_user_info_flink_views_staging.sql

echo ">> 完成: $(date '+%F %T')"
for t in flink_stg_mkt_user flink_stg_ud_latest flink_stg_lup_latest flink_stg_dac_latest; do
  cnt=$(lm_mysql_query_write "SELECT COUNT(*) FROM ${t};" || echo "?")
  echo "   ${t}: ${cnt} 行"
done
if [[ "$(lm_mysql_write_host)" != "$(lm_mysql_read_host)" ]]; then
  echo ">> 主从分离: 请等 flink_stg_* 同步到从库 $(lm_mysql_read_host) 后再跑 Flink"
fi
