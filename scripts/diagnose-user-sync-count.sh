#!/usr/bin/env bash
# user 宽表 vs 目标：待 VT、主键冲突(token+app 重复)、按 user_id 漏写
#
# 用法: ./scripts/diagnose-user-sync-count.sh
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

mysql_tgt() {
  MYSQL_PWD="$TGT_PASS" mysql -h "$TGT_HOST" -P "$TGT_PORT" -u "$TGT_USER" "$TGT_DB" -e "$1"
}

echo "=========================================="
echo "user 同步数量诊断"
echo "  源库: ${SRC_HOST}/${SRC_DB}"
echo "  目标: ${TGT_HOST}/${TGT_DB}"
echo "=========================================="
echo ""

echo ">> [1] 基础计数（监控用 expected = staging_expected_pk，忽略 token+app 重复 id）"
mysql_src "
SELECT 'staging_mobile_norm_raw' AS metric, COUNT(*) AS cnt
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
UNION ALL
SELECT 'staging_expected_pk', COUNT(*)
FROM (
  SELECT DISTINCT COALESCE(NULLIF(TRIM(mobile_token), ''), mobile_norm), app_code
  FROM user_sync_staging
  WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
) d
UNION ALL
SELECT 'staging_has_token_raw', COUNT(*)
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
  AND mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
UNION ALL
SELECT 'staging_need_vt', COUNT(*)
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND TRIM(mobile_norm) <> ''
  AND (mobile_token IS NULL OR TRIM(mobile_token) = '');
"

echo ""
echo ">> [2] 主键冲突：同一 mobile_token + app_code 对应多个源 id"
mysql_src "
SELECT 'duplicate_token_app_groups' AS metric, COUNT(*) AS cnt
FROM (
  SELECT mobile_token, app_code
  FROM user_sync_staging
  WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
  GROUP BY mobile_token, app_code
  HAVING COUNT(*) > 1
) x
UNION ALL
SELECT 'duplicate_token_app_extra_rows', COALESCE(SUM(c - 1), 0)
FROM (
  SELECT COUNT(*) AS c
  FROM user_sync_staging
  WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
  GROUP BY mobile_token, app_code
  HAVING COUNT(*) > 1
) t;
"

dup_groups=$(MYSQL_PWD="$SRC_PASS" mysql -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" "$SRC_DB" -N -e "
SELECT COUNT(*) FROM (
  SELECT 1 FROM user_sync_staging
  WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
  GROUP BY mobile_token, app_code HAVING COUNT(*) > 1
) x;" 2>/dev/null || echo 0)

if [[ "${dup_groups:-0}" -gt 0 ]]; then
  echo ""
  echo ">> [3] 冲突样例（前 20 组：token+app → 多个 id）"
  mysql_src "
SELECT mobile_token, app_code, COUNT(*) AS user_cnt,
       SUBSTRING(GROUP_CONCAT(id ORDER BY id SEPARATOR ','), 1, 200) AS sample_ids
FROM user_sync_staging
WHERE mobile_token IS NOT NULL AND TRIM(mobile_token) <> ''
GROUP BY mobile_token, app_code
HAVING COUNT(*) > 1
ORDER BY user_cnt DESC
LIMIT 20;
"
else
  echo ""
  echo ">> [3] 无 mobile_token+app_code 重复组"
fi

echo ""
echo ">> [4] 目标库 COUNT(*)"
mysql_tgt "SELECT COUNT(*) AS target_user_total FROM \`user\`;"

if [[ "$SRC_HOST" == "$TGT_HOST" ]]; then
  echo ""
  echo ">> [5] 源/目标同实例：按 user_id 对齐"
  mysql_src "
SELECT 'target_matched_by_user_id' AS metric, COUNT(*) AS cnt
FROM ${TGT_DB}.\`user\` t
INNER JOIN user_sync_staging s ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> ''
UNION ALL
SELECT 'staging_missing_in_target_by_user_id', COUNT(*)
FROM user_sync_staging s
LEFT JOIN ${TGT_DB}.\`user\` t ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> ''
  AND t.user_id IS NULL;
"
  miss=$(MYSQL_PWD="$SRC_PASS" mysql -h "$SRC_HOST" -P "$SRC_PORT" -u "$SRC_USER" "$SRC_DB" -N -e "
SELECT COUNT(*) FROM user_sync_staging s
LEFT JOIN ${TGT_DB}.\`user\` t ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> ''
  AND t.user_id IS NULL;" 2>/dev/null || echo ERR)
  if [[ "$miss" =~ ^[0-9]+$ && "$miss" -gt 0 && "$miss" -le 50 ]]; then
    echo ""
    echo ">> [6] 漏写样例（宽表有、目标无，最多 30 条）"
    mysql_src "
SELECT s.id, s.id + 100000000 AS expect_user_id, s.app_code, s.mobile_norm,
       CASE WHEN s.mobile_token IS NULL OR TRIM(s.mobile_token) = '' THEN 'need_vt' ELSE 'has_token' END AS token_state
FROM user_sync_staging s
LEFT JOIN ${TGT_DB}.\`user\` t ON t.user_id = s.id + 100000000
WHERE s.mobile_norm IS NOT NULL AND TRIM(s.mobile_norm) <> ''
  AND t.user_id IS NULL
ORDER BY s.id
LIMIT 30;
"
  fi
else
  echo ""
  echo ">> [5] 源/目标不同实例，跳过跨库 user_id 对齐（需在能连两库的环境跑 sql/verify/user_sync_count_diagnosis.sql）"
fi

echo ""
echo "完成。"
