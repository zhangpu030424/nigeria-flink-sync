#!/usr/bin/env bash
# 检查老库（通常是从库）是否已有 Flink 所需的 4 个 VIEW + 分区列
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

: "${LM_MYSQL_HOST:?}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
DB="${LM_MYSQL_DATABASE:-ng_loan_market}"

echo ">> 检查 ${DB} @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"

ro=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" -N -e "SELECT @@read_only;" 2>/dev/null || echo "?")
echo ">> read_only=${ro} $([[ "$ro" == "1" ]] && echo '(从库只读，VIEW 须在主库创建)' || echo '(可写)')"

MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=15 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$DB" -e "
SHOW FULL TABLES WHERE Table_type='VIEW' AND Tables_in_${DB} LIKE 'v_flink_%';
SELECT table_name, column_name
FROM information_schema.columns
WHERE table_schema='${DB}'
  AND table_name IN ('v_flink_mkt_user','v_flink_ud_latest','v_flink_lup_latest','v_flink_dac_latest')
  AND column_name IN ('id_part','user_id_part')
ORDER BY table_name, column_name;
"

missing=0
for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest; do
  line=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$DB" -N -e \
    "SHOW FULL TABLES LIKE '${v}';" 2>/dev/null | head -1 || true)
  if [[ "$line" == *"VIEW"* ]]; then
    echo "✓ VIEW ${v}"
  else
    echo "✗ 缺少 VIEW ${v}（脚本探测: ${line:-连接失败或无权限}）"
    missing=1
  fi
done

if [[ "$missing" -eq 1 ]]; then
  echo ""
  echo "请在【主库】执行: sql/ddl/lm_user_info_flink_views.sql"
  echo "同步完成后再跑: bash scripts/run-ng-user-info-bulk-max.sh"
  exit 1
fi

echo ">> 全部 VIEW 已就绪，可提交 Flink Job"
