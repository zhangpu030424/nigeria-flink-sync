#!/usr/bin/env bash
# 预检 GPT user_info；缺 VIEW 时加 --fix 自动主库创建
# 用法: bash scripts/preflight-lm-user-info-gpt.sh
#       bash scripts/preflight-lm-user-info-gpt.sh --fix
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo "缺少 .env"; exit 1; }

FIX=0
[[ "${1:-}" == "--fix" ]] && FIX=1

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
R_HOST=$(lm_mysql_read_host)

echo ">> 从库(读) ${R_HOST}:${LM_MYSQL_PORT:-3306}/${LM_MYSQL_DATABASE}"
echo ">> 主库(写) ${W_HOST}:${LM_MYSQL_WRITE_PORT:-${LM_MYSQL_PORT:-3306}}"

missing=0
for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest v_flink_gpt_user_info_sink; do
  ok=$(lm_mysql_query_read "SELECT COUNT(*) FROM information_schema.views
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';" || echo 0)
  if [[ "$ok" == "1" ]]; then
    echo "  ${v}: OK"
  else
    echo "  ${v}: MISSING"
    missing=1
  fi
done

if [[ "$missing" == "1" ]]; then
  if [[ "$FIX" == "1" ]]; then
    echo ""
    exec bash scripts/refresh-lm-user-info-gpt-views.sh
  fi
  echo ""
  echo ">> 从库缺 VIEW。请在 .env 配置 LM_MYSQL_WRITE_HOST=主库 后执行:"
  echo "   bash scripts/refresh-lm-user-info-gpt-views.sh"
  echo "   或: bash scripts/preflight-lm-user-info-gpt.sh --fix"
  if [[ "$W_HOST" == "$R_HOST" ]]; then
    ro=$(MYSQL_PWD="$(lm_mysql_write_password)" mysql --connect-timeout=5 \
      -h "$W_HOST" -P "$(lm_mysql_write_port)" -u "$(lm_mysql_write_user)" \
      "${LM_MYSQL_DATABASE}" -N -e "SELECT @@read_only;" 2>/dev/null || echo "?")
    [[ "$ro" == "1" ]] && echo ">> 当前 LM_MYSQL_HOST 为只读从库，必须单独配置 LM_MYSQL_WRITE_HOST"
  fi
  exit 1
fi

if lm_gpt_view_exists_read "v_flink_gpt_user_info_sink"; then
  lm_gpt_probe_sink_read
else
  echo ">> ERR: v_flink_gpt_user_info_sink 不存在"
  exit 1
fi
