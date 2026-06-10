#!/usr/bin/env bash
# 刷新 Flink join 试跑用的 pick 表 + MySQL VIEW（范围过滤 + MAX(id) 在库内完成）
# 用法: LM_MIGRATION_LIMIT=20 bash lm/scripts/refresh-flink-migration-pick.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

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
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
[[ "$LM_MIGRATION_LIMIT" =~ ^[0-9]+$ ]] || { echo "ERR: LM_MIGRATION_LIMIT 须为正整数"; exit 1; }

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

REG_IP_TABLE=""
for tbl in user_reg_ip user_registration_ip; do
  if table_exists "$tbl"; then
    REG_IP_TABLE="$tbl"
    break
  fi
done

echo ">> 建表/视图: flink_migration_pick_views"
MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < lm/ddl/flink_migration_pick_views.sql

URI_VIEW_SQL=""
if [[ -n "$REG_IP_TABLE" ]]; then
  URI_VIEW_SQL="CREATE OR REPLACE VIEW v_flink_pick_uri_latest AS
SELECT r1.\`userId\`, r1.ip
FROM \`${REG_IP_TABLE}\` r1
INNER JOIN (
    SELECT r.\`userId\`, MAX(r.id) AS max_id
    FROM \`${REG_IP_TABLE}\` r
    INNER JOIN flink_migration_user_pick pick ON pick.id = r.\`userId\`
    GROUP BY r.\`userId\`
) t ON t.max_id = r1.id;"
  echo ">> registration_ip 视图: ${REG_IP_TABLE}"
else
  URI_VIEW_SQL="CREATE OR REPLACE VIEW v_flink_pick_uri_latest AS
SELECT CAST(NULL AS SIGNED) AS \`userId\`, CAST(NULL AS CHAR) AS ip
FROM flink_migration_user_pick WHERE 1 = 0;"
  echo ">> WARN: 无 user_reg_ip / user_registration_ip，uri 视图为空"
fi

PREP="/tmp/refresh-flink-migration-pick-$$.sql"
envsubst '${LM_MIGRATION_LIMIT}' < lm/sql/refresh_flink_migration_pick.sql > "$PREP"
echo "$URI_VIEW_SQL" >> "$PREP"

echo ">> 刷新 pick LIMIT=${LM_MIGRATION_LIMIT} @ ${LM_MYSQL_DATABASE}"
MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=60 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < "$PREP"
rm -f "$PREP"

pick_cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_migration_user_pick;" 2>/dev/null || echo "?")
echo ">> 完成: flink_migration_user_pick=${pick_cnt} 行"
