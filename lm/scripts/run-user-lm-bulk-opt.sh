#!/usr/bin/env bash
# 老库 user 全量（索引优化版）→ 目标库
# 试跑: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-user-lm-bulk-opt.sh
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

: "${LM_MYSQL_HOST:?请在 .env 填写 LM_MYSQL_HOST}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-user-lm-bulk-opt-sql.log"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] 【优化版】user 同步 LIMIT=${LM_MIGRATION_LIMIT}"
echo "  SQL: lm/sql/03_sync_user_lm_bulk_opt.sql"
echo "  MySQL 对照: bash lm/scripts/run-lm-verify-mysql.sh user"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行"
  exit 1
fi

bash scripts/run-sql.sh lm/sql/03_sync_user_lm_bulk_opt.sql 2>&1 | tee "$SQL_LOG"
