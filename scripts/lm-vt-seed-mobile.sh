#!/usr/bin/env bash
# 从 ng_loan_market 抽 DISTINCT 手机号 → 写入 nigeria_backend.vt_token_cache（status=0）
# 随后执行: ./scripts/vt-preload.sh --mode fast --vt-type mobile --skip-count
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

# shellcheck disable=SC1091
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

: "${LM_MYSQL_HOST:?请在 .env 填写 LM_MYSQL_HOST}"
: "${LM_MYSQL_PORT:?请在 .env 填写 LM_MYSQL_PORT}"
: "${LM_MYSQL_USER:?请在 .env 填写 LM_MYSQL_USER}"
: "${LM_MYSQL_PASSWORD:?请在 .env 填写 LM_MYSQL_PASSWORD}"
: "${LM_MYSQL_DATABASE:=ng_loan_market}"

TMP="/tmp/lm_mobile_norm_$$.tsv"
trap 'rm -f "$TMP"' EXIT

echo ">> 从 ${LM_MYSQL_DATABASE}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT} 抽取手机号..."
mysql -h"$LM_MYSQL_HOST" -P"$LM_MYSQL_PORT" -u"$LM_MYSQL_USER" -p"$LM_MYSQL_PASSWORD" \
  --batch --skip-column-names "$LM_MYSQL_DATABASE" -e "
SELECT DISTINCT
    CASE
        WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
        WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
        WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
        WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
        ELSE CONCAT('+234', TRIM(u.mobile))
    END AS mobile_norm
FROM user u
WHERE u.mobile IS NOT NULL AND TRIM(u.mobile) <> '';
" > "$TMP"

ROWS=$(wc -l < "$TMP" | tr -d ' ')
echo ">> 去重手机号: ${ROWS} 条"

if [[ "$ROWS" -eq 0 ]]; then
  echo "WARN: 无手机号可灌"
  exit 0
fi

BATCH=5000
INSERTED=0
while IFS= read -r mobile_norm; do
  [[ -z "$mobile_norm" ]] && continue
  esc="${mobile_norm//\'/\'\'}"
  printf "INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status) VALUES ('mobile', '%s', 0);\n" "$esc"
done < "$TMP" | mysql -h"$SOURCE_MYSQL_HOST" -P"$SOURCE_MYSQL_PORT" -u"$SOURCE_MYSQL_USER" -p"$SOURCE_MYSQL_PASSWORD" \
  "$SOURCE_MYSQL_DATABASE" --batch

echo ">> 已写入 ${SOURCE_MYSQL_DATABASE}.vt_token_cache（INSERT IGNORE）"
echo ">> 下一步: ./scripts/vt-preload.sh --mode fast --vt-type mobile --skip-count --workers 2"
