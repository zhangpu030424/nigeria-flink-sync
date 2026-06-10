#!/usr/bin/env bash
# 全量 GPT JSON 落地 flink_stg_user_info_ready（依赖 flink_stg_* 实体表）
# 用法: bash scripts/refresh-lm-user-info-gpt-full.sh
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

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

stg_cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_stg_mkt_user;" 2>/dev/null || echo 0)

if [[ ! "$stg_cnt" =~ ^[0-9]+$ ]] || [[ "$stg_cnt" -lt 1000 ]]; then
  echo ">> flink_stg_mkt_user 为空或不存在，先落地基础实体表..."
  bash scripts/refresh-lm-user-info-staging.sh
fi

echo ">> 创建 GPT 辅助 VIEW（uri / app）"
if [[ -f sql/ddl/lm_user_info_gpt_views.sql ]]; then
  MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=30 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    < sql/ddl/lm_user_info_gpt_views.sql 2>/dev/null || true
fi

PREP="/tmp/lm_user_info_gpt_staging_full-$$.sql"
cp sql/ddl/lm_user_info_gpt_staging_full.sql "$PREP"

if ! table_exists "user_registration_ip"; then
  echo ">> WARN: 无 user_registration_ip，registration_ip 写 NULL"
  sed -i.bak "s/'registration_ip', uri\.ip/'registration_ip', CAST(NULL AS CHAR)/" "$PREP"
  sed -i.bak "s/LEFT JOIN v_flink_uri_latest uri ON uri\.\`userId\` = u\.id/LEFT JOIN (SELECT CAST(NULL AS CHAR) AS userId, CAST(NULL AS CHAR) AS ip) uri ON 1=0/" "$PREP"
fi

if ! table_exists "app"; then
  echo ">> WARN: 无 app 表，app.name 写 NULL"
  sed -i.bak "s/'name', app\.\`name\`/'name', CAST(NULL AS CHAR)/" "$PREP"
  sed -i.bak "s/LEFT JOIN v_flink_mkt_app app ON app\.id = u\.\`appId\`/LEFT JOIN (SELECT CAST(NULL AS CHAR) AS id, CAST(NULL AS CHAR) AS name) app ON 1=0/" "$PREP"
fi

echo ">> 全量拼 GPT JSON → flink_stg_user_info_ready"
echo ">> ${LM_MYSQL_DATABASE} @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo ">> 开始: $(date '+%F %T')（约 10～40 分钟，视数据量）"

MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < "$PREP"

rm -f "$PREP" "$PREP.bak"

cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;" 2>/dev/null || echo "?")
echo ">> 完成: $(date '+%F %T')  flink_stg_user_info_ready=${cnt} 行"
