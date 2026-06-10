#!/usr/bin/env bash
# 主库创建 GPT user_info 所需 VIEW（无物化表）
# 含: v_flink_* + v_flink_gpt_user_info_sink
# 用法: bash scripts/refresh-lm-user-info-gpt-views.sh
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
  export "$line"
done < .env
set +a

# shellcheck source=lib/lm-mysql-write.sh
source "$(dirname "$0")/lib/lm-mysql-write.sh"
# shellcheck source=lib/lm-user-info-gpt-setup.sh
source "$(dirname "$0")/lib/lm-user-info-gpt-setup.sh"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

W_HOST=$(lm_mysql_write_host)
W_PORT=$(lm_mysql_write_port)
R_HOST=$(lm_mysql_read_host)
R_PORT=$(lm_mysql_read_port)

echo ">> 主库建 VIEW: ${LM_MYSQL_DATABASE} @ ${W_HOST}:${W_PORT}"
lm_mysql_assert_writable || exit 1

lm_gpt_ensure_views_on_write

echo ">> 主库校验:"
for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest \
         v_flink_uri_latest v_flink_mkt_app v_flink_gpt_user_info_sink; do
  ok=$(lm_mysql_query_write "SELECT COUNT(*) FROM information_schema.views
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';" || echo 0)
  echo "  ${v}: $([[ "$ok" == "1" ]] && echo OK || echo MISSING)"
  [[ "$ok" != "1" && "$v" != "v_flink_uri_latest" && "$v" != "v_flink_mkt_app" ]] && {
    echo "ERR: 主库创建 ${v} 失败"; exit 1
  }
done

sample=$(lm_mysql_query_write "SELECT user_id FROM v_flink_gpt_user_info_sink LIMIT 1;" 2>/dev/null || echo "")
[[ -n "$sample" ]] || { echo "ERR: 主库 SELECT v_flink_gpt_user_info_sink 失败"; exit 1; }
echo ">> 主库试读 OK user_id=${sample}"

if [[ "$W_HOST" != "$R_HOST" || "$W_PORT" != "$R_PORT" ]]; then
  echo ""
  echo ">> 从库 ${R_HOST}:${R_PORT} 等待 VIEW 同步..."
  export LM_GPT_VIEW_WAIT_SEC="${LM_GPT_VIEW_WAIT_SEC:-300}"
  lm_gpt_wait_sink_view_read || {
    echo ""
    echo ">> 主库已有 VIEW，从库尚未同步。可:"
    echo "   1) 等几分钟后: bash scripts/preflight-lm-user-info-gpt.sh"
    echo "   2) 或临时让 Flink 读主库: LM_MYSQL_HOST=${W_HOST} bash scripts/run-ng-user-info-gpt-direct.sh"
    exit 1
  }
else
  echo ">> 读写同库，无需等同步"
fi

echo ">> 完成。下一步: bash scripts/run-ng-user-info-gpt-direct.sh"
