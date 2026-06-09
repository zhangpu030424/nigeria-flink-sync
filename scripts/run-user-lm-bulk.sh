#!/usr/bin/env bash
# 老库 ng_loan_market 一次性 user 全量 → 目标库（独立 Job，不改 SOURCE_MYSQL_* incr）
# 用法:
#   ./scripts/lm-vt-seed-mobile.sh
#   ./scripts/vt-preload.sh --mode fast --vt-type mobile --skip-count --workers 2
#   ./scripts/run-user-lm-bulk.sh
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

echo ">> 老库: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo ">> 查询: generate_user.py 等价 SELECT（内嵌 JDBC，不建 VIEW）"
echo ">> VT 字典: ${SOURCE_MYSQL_DATABASE}@${SOURCE_MYSQL_HOST}"
echo ">> 目标: ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST}"
echo ">> 提交 Job: sql/03_sync_user_lm_bulk.sql（batch 模式，不影响 incr）"

export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"
./scripts/run-sql.sh sql/03_sync_user_lm_bulk.sql
