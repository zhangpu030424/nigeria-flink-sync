#!/usr/bin/env bash
# 老库 user_info 单表试跑（索引优化版）
# 用法: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-user-info-opt.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写 LM_* / TARGET_*"
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

: "${LM_MYSQL_HOST:?请在 .env 填写 LM_MYSQL_HOST}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-opt-sql.log"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] user_info 优化版试跑 LIMIT=${LM_MIGRATION_LIMIT}"
echo "  SQL: lm/sql/04_sync_ng_user_info_opt.sql"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行，请先 ./scripts/up.sh"
  exit 1
fi

bash scripts/run-sql.sh lm/sql/04_sync_ng_user_info_opt.sql 2>&1 | tee "$SQL_LOG"
echo "[$(date '+%F %T')] 完成。验证: SELECT COUNT(*) FROM user_info;"
