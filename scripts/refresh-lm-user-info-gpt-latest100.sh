#!/usr/bin/env bash
# GPT 版：最新 N 条 user_info 落地 flink_stg_user_info_ready
# 用法: LM_PICK_N=100 bash scripts/refresh-lm-user-info-gpt-latest100.sh
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
export LM_PICK_N="${LM_PICK_N:-100}"

table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${tbl}';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

PREP="/tmp/lm_user_info_gpt_staging_latest100-$$.sql"
envsubst '${LM_PICK_N}' < sql/ddl/lm_user_info_gpt_staging_latest100.sql > "$PREP"

if ! table_exists "user_registration_ip"; then
  echo ">> WARN: 无 user_registration_ip，registration_ip 写 NULL"
  python3 - "$PREP" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
text = text.replace("'registration_ip', uri.`ip`", "'registration_ip', CAST(NULL AS CHAR)")
text = re.sub(
    r"LEFT JOIN \(\s*SELECT r1\.`userId`, r1\.`ip`.*?LEFT JOIN `app`",
    "LEFT JOIN `app`",
    text,
    count=1,
    flags=re.S,
)
open(path, "w", encoding="utf-8").write(text)
PY
fi

if ! table_exists "app"; then
  echo ">> WARN: 无 app 表，app.name 写 NULL"
  python3 - "$PREP" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
text = text.replace("'name', app.`name`", "'name', CAST(NULL AS CHAR)")
text = re.sub(r"LEFT JOIN `app` app ON app\.id = u\.`appId`\s*", "", text, count=1)
open(path, "w", encoding="utf-8").write(text)
PY
fi

echo ">> GPT 版落地 flink_stg_user_info_ready（最新 ${LM_PICK_N} 条）"
echo ">> ${LM_MYSQL_DATABASE} @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo ">> 开始: $(date '+%F %T')"

MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < "$PREP"

rm -f "$PREP"

cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;" 2>/dev/null || echo "?")
echo ">> 完成: $(date '+%F %T')  flink_stg_user_info_ready=${cnt} 行"
