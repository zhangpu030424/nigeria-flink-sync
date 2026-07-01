#!/usr/bin/env bash
# 按订单号列表灌源库宽表 loan_dk_ld_sync_staging
# 用法:
#   1. sn-list.txt 每行一个 applicationNo
#   2. bash scripts/run-lm-loan-backfill-by-sn.sh sn-list.txt
#   3. bash scripts/run-lm-loan-dk-ld-sync.sh   # 宽表 → 目标 loan
set -euo pipefail
cd "$(dirname "$0")/.."

SN_FILE="${1:-}"

if [[ -z "$SN_FILE" || ! -f "$SN_FILE" ]]; then
  echo "用法: $0 <sn列表文件>"
  echo "  sn 文件每行一个 applicationNo"
  exit 1
fi

# shellcheck disable=SC1091
source scripts/lib/load-project-env.sh
load_project_env .

# shellcheck disable=SC1091
source scripts/lib/mysql-lm.sh

echo ">> 建表 lm_loan_backfill_sn（若无）"
mysql_lm_cmd < sql/ddl/lm_loan_backfill_sn.sql

echo ">> 灌入 SN 列表: $SN_FILE"
mysql_lm_cmd -e "TRUNCATE TABLE lm_loan_backfill_sn;"
_batch=()
while IFS= read -r line || [[ -n "$line" ]]; do
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/^"//;s/"$//;s/,$//')"
  [[ -z "$line" ]] && continue
  _batch+=("$line")
  if [[ ${#_batch[@]} -ge 500 ]]; then
    vals=$(printf "('%s')," "${_batch[@]}")
    vals="${vals%,}"
    mysql_lm_cmd -e "INSERT IGNORE INTO lm_loan_backfill_sn (sn) VALUES ${vals};"
    _batch=()
  fi
done < "$SN_FILE"
if [[ ${#_batch[@]} -gt 0 ]]; then
  vals=$(printf "('%s')," "${_batch[@]}")
  vals="${vals%,}"
  mysql_lm_cmd -e "INSERT IGNORE INTO lm_loan_backfill_sn (sn) VALUES ${vals};"
fi

cnt=$(mysql_lm_query "SELECT COUNT(*) FROM lm_loan_backfill_sn;" 2>/dev/null || echo "?")
echo ">> lm_loan_backfill_sn 行数: ${cnt}"

echo ">> 诊断 + 补宽表"
mysql_lm_cmd < sql/migrate/backfill_lm_loan_dk_ld_by_sn_list.sql

echo ">> 完成；下一步: bash scripts/run-lm-loan-dk-ld-sync.sh"
