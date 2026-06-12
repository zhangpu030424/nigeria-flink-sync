#!/usr/bin/env bash
# user_info 增量正确性批量对账：源 bundle 期望 vs 目标库实际
# 用法:
#   bash scripts/verify-user-info-reconcile.sh           # 脏队列最近 200 + 随机 staging 200
#   bash scripts/verify-user-info-reconcile.sh --sample 500
set -euo pipefail
cd "$(dirname "$0")/.."

SAMPLE=200
while [[ $# -gt 0 ]]; do
  case "$1" in
    --sample) SAMPLE="${2:-200}"; shift 2 ;;
    *) echo "未知参数: $1"; exit 1 ;;
  esac
done

[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
# shellcheck source=scripts/lib/load-env.sh
source "$(dirname "$0")/lib/load-env.sh"
set -a
load_env_file .env
set +a

OFFSET="${USER_ID_OFFSET:-100000000}"
TMP="$(mktemp)"
trap 'rm -f "$TMP" "${TMP}.tgt"' EXIT

mysql_src() {
  MYSQL_PWD="$SOURCE_MYSQL_PASSWORD" mysql -h "$SOURCE_MYSQL_HOST" -P "$SOURCE_MYSQL_PORT" \
    -u "$SOURCE_MYSQL_USER" "$SOURCE_MYSQL_DATABASE" -N "$@"
}

mysql_tgt() {
  MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
    -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" -N "$@"
}

echo "=== user_info 增量正确性对账（sample=${SAMPLE}）==="
echo

echo "[1] 源库 staging vs bundle"
mysql_src -e "
SELECT
  (SELECT COUNT(*) FROM user_info_sync_staging) AS staging_cnt,
  (SELECT COUNT(*) FROM user_info_dirty) AS dirty_cnt,
  (SELECT COUNT(*) FROM user_info_incr_bundle_lookup b
   INNER JOIN user_info_sync_staging s ON s.user_id = b.user_id
   WHERE TRIM(COALESCE(s.full_name,'')) <> TRIM(CONCAT(COALESCE(b.first_name,''), ' ', COALESCE(b.sur_name,'')))
  ) AS staging_bundle_mismatch;
" | awk -F'\t' '{printf "  staging=%s dirty=%s staging/bundle不一致=%s\n", $1,$2,$3}'

echo
echo "[2] 抽样对账 bundle → 目标（脏队列最近 ${SAMPLE} 条）"
mysql_src -e "
SELECT b.user_id,
       TRIM(CONCAT(COALESCE(b.first_name,''), ' ', COALESCE(b.sur_name,''))) AS expect_fn,
       CASE
         WHEN b.bvn IS NULL OR TRIM(b.bvn) = '' THEN 1
         WHEN b.vt_token IS NOT NULL AND TRIM(b.vt_token) <> '' THEN 1
         ELSE 0
       END AS sink_ok
FROM user_info_dirty d
INNER JOIN user_info_incr_bundle_lookup b ON b.user_id = d.user_id
ORDER BY d.updated_at DESC
LIMIT ${SAMPLE};
" > "$TMP"

if [[ ! -s "$TMP" ]]; then
  echo "  无抽样数据"
  exit 1
fi

# 目标库批量查
uids=$(cut -f1 "$TMP" | paste -sd, -)
mysql_tgt -e "
SELECT user_id, COALESCE(full_name,'')
FROM user_info
WHERE user_id IN ($(awk -F'\t' -v o=$OFFSET '{print $1+o}' "$TMP" | paste -sd, -));
" > "${TMP}.tgt" || true

ok=0 stale=0 missing=0 filtered=0 skipped=0
while IFS=$'\t' read -r uid expect sink_ok; do
  [[ -z "$uid" ]] && continue
  tgt_uid=$((uid + OFFSET))
  tgt_fn=$(grep -E "^${tgt_uid}	" "${TMP}.tgt" 2>/dev/null | cut -f2 || true)
  if [[ "$sink_ok" == "0" ]]; then
    filtered=$((filtered + 1))
    continue
  fi
  if [[ -z "$tgt_fn" ]]; then
    missing=$((missing + 1))
    [[ $missing -le 5 ]] && echo "  MISSING user_id=${uid} expect=${expect}"
  elif [[ "$(echo "$expect" | xargs)" == "$(echo "$tgt_fn" | xargs)" ]]; then
    ok=$((ok + 1))
  else
    stale=$((stale + 1))
    [[ $stale -le 5 ]] && echo "  STALE user_id=${uid} expect=${expect} target=${tgt_fn}"
  fi
done < "$TMP"

checked=$((ok + stale + missing))
total=$((checked + filtered))
echo
echo "  抽样 ${total} 条: PASS=${ok} STALE=${stale} MISSING=${missing} sink过滤=${filtered}"
echo

if [[ $stale -eq 0 && $missing -eq 0 ]]; then
  echo "结论: 抽样对账 PASS（sink 过滤 ${filtered} 条属预期行为）"
  exit 0
fi
echo "结论: 对账未通过 — STALE/MISSING 说明增量未追上或全量缺口；勿用 latest-offset 截断"
echo "建议: timestamp + bulk-start-ms 跑到 STALE=0，或重跑 user_info 全量"
exit 2
