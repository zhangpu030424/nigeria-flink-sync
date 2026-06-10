#!/usr/bin/env bash
# 预检 GPT user_info 链路（VIEW / 从库可读性）
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  export "$line"
done < .env
set +a
# shellcheck source=lib/lm-mysql-write.sh
source "$(dirname "$0")/lib/lm-mysql-write.sh"
# shellcheck source=lib/lm-user-info-gpt-setup.sh
source "$(dirname "$0")/lib/lm-user-info-gpt-setup.sh"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

echo ">> 读库 ${LM_MYSQL_HOST}:${LM_MYSQL_PORT:-3306}/${LM_MYSQL_DATABASE}"
for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest v_flink_gpt_user_info_sink; do
  ok=$(lm_mysql_query_read "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';" || echo 0)
  echo "  ${v}: $([[ "$ok" == 1 ]] && echo OK || echo MISSING)"
done
row=$(lm_mysql_query_read "SELECT user_id FROM v_flink_gpt_user_info_sink LIMIT 1;" 2>/dev/null || echo "")
if [[ -n "$row" ]]; then
  echo ">> SELECT 试读 OK sample user_id=${row}"
else
  echo ">> ERR: v_flink_gpt_user_info_sink 不可读或为空"
  echo ">> 运行: bash scripts/run-ng-user-info-gpt-direct.sh （会自动主库建 VIEW）"
  exit 1
fi
