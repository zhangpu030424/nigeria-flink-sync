#!/usr/bin/env bash
# user_product 宽表 vs 源库 user_order 差集诊断
# 用法: ./scripts/diagnose-user-product-staging.sh
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "请先配置 .env"; exit 1; }

set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  export "$line"
done < .env
set +a

SRC_HOST="${SOURCE_MYSQL_HOST:?}"
SRC_PORT="${SOURCE_MYSQL_PORT:-3306}"
SRC_USER="${SOURCE_MYSQL_USER:?}"
SRC_PASS="${SOURCE_MYSQL_PASSWORD:?}"
SRC_DB="${SOURCE_MYSQL_DATABASE:?}"
TGT_HOST="${TARGET_MYSQL_HOST:?}"
TGT_PORT="${TARGET_MYSQL_PORT:-3306}"
TGT_USER="${TARGET_MYSQL_USER:?}"
TGT_PASS="${TARGET_MYSQL_PASSWORD:?}"
TGT_DB="${TARGET_MYSQL_DATABASE:?}"

mysql_src() {
  MYSQL_PWD="$SRC_PASS" mysql -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" "$SRC_DB" -e "$1"
}

echo "=========================================="
echo "user_product 宽表差集诊断"
echo "  源库: ${SRC_HOST}/${SRC_DB}"
echo "=========================================="
echo ""

mysql_src "
SELECT 'user_order_rows' AS metric, COUNT(*) AS cnt FROM user_order
UNION ALL SELECT 'user_order_distinct_pk', COUNT(*) FROM (SELECT DISTINCT user_id, product_id FROM user_order) d
UNION ALL SELECT 'staging_rows', COUNT(*) FROM user_product_sync_staging;
"

echo ""
echo ">> 源库应有、宽表没有（missing_in_staging）"
mysql_src "
SELECT COUNT(*) AS missing_in_staging FROM (
  SELECT user_id, product_id,
         ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
  FROM user_order
) e
LEFT JOIN user_product_sync_staging s ON s.user_id = e.user_id AND s.product_id = e.product_id
WHERE e.rn = 1 AND s.user_id IS NULL;
"

miss=$(MYSQL_PWD="$SRC_PASS" mysql -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" "$SRC_DB" -N -e "
SELECT COUNT(*) FROM (
  SELECT user_id, product_id,
         ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
  FROM user_order
) e
LEFT JOIN user_product_sync_staging s ON s.user_id = e.user_id AND s.product_id = e.product_id
WHERE e.rn = 1 AND s.user_id IS NULL;" 2>/dev/null || echo ERR)

if [[ "$miss" =~ ^[0-9]+$ && "$miss" -gt 0 ]]; then
  echo ""
  echo ">> 缺行样例（前 30）"
  mysql_src "
SELECT e.user_id, e.product_id, o.id AS order_id, o.order_time, o.amount_max
FROM (
  SELECT user_id, product_id,
         ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
  FROM user_order
) e
INNER JOIN user_order o ON o.user_id = e.user_id AND o.product_id = e.product_id
LEFT JOIN user_product_sync_staging s ON s.user_id = e.user_id AND s.product_id = e.product_id
WHERE e.rn = 1 AND s.user_id IS NULL
GROUP BY e.user_id, e.product_id, o.id, o.order_time, o.amount_max
ORDER BY e.user_id, e.product_id
LIMIT 30;
"
fi

echo ""
echo ">> 宽表有、user_order 没有（extra_in_staging / 宽表过期）"
mysql_src "
SELECT COUNT(*) AS extra_in_staging
FROM user_product_sync_staging s
LEFT JOIN (SELECT DISTINCT user_id, product_id FROM user_order) o
  ON o.user_id = s.user_id AND o.product_id = s.product_id
WHERE o.user_id IS NULL;
"

if [[ "$SRC_HOST" == "$TGT_HOST" ]]; then
  echo ""
  echo ">> 宽表有、目标库没有（staging_missing_in_target）"
  mysql_src "
SELECT COUNT(*) AS staging_missing_in_target
FROM user_product_sync_staging s
LEFT JOIN ${TGT_DB}.user_product t
  ON t.group_user_id = s.user_id + 100000000 AND t.product_id = s.product_id
WHERE t.group_user_id IS NULL;
"
else
  tgt_cnt=$(MYSQL_PWD="$TGT_PASS" mysql -h "$TGT_HOST" -P "$TGT_PORT" -u "$TGT_USER" "$TGT_DB" -N -e "SELECT COUNT(*) FROM user_product;" 2>/dev/null || echo ERR)
  echo ""
  echo ">> 目标库 user_product 行数: ${tgt_cnt}"
fi

echo ""
echo "完整 SQL: sql/verify/user_product_staging_gap.sql"
echo "完成。"
