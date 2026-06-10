#!/usr/bin/env bash
# 老库 application 单表试跑（索引优化版）
# 用法: LM_MIGRATION_LIMIT=20 bash lm/scripts/run-ng-application-opt.sh
set -euo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写 LM_* / LM_CORE_* / TARGET_*"
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

export LM_CORE_MYSQL_HOST="${LM_CORE_MYSQL_HOST:-$LM_MYSQL_HOST}"
export LM_CORE_MYSQL_PORT="${LM_CORE_MYSQL_PORT:-${LM_MYSQL_PORT:-3306}}"
export LM_CORE_MYSQL_USER="${LM_CORE_MYSQL_USER:-$LM_MYSQL_USER}"
export LM_CORE_MYSQL_PASSWORD="${LM_CORE_MYSQL_PASSWORD:-$LM_MYSQL_PASSWORD}"
export LM_CORE_MYSQL_DATABASE="${LM_CORE_MYSQL_DATABASE:-ng_loan_core}"
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-20}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-application-opt-sql.log"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] application 优化版试跑 LIMIT=${LM_MIGRATION_LIMIT}"
echo "  market: ${LM_MYSQL_DATABASE:-ng_loan_market}@${LM_MYSQL_HOST}"
echo "  core:   ${LM_CORE_MYSQL_DATABASE}@${LM_CORE_MYSQL_HOST}"
echo "  SQL:    lm/sql/04_sync_ng_application_opt.sql"
echo "  MySQL对照: LM_MIGRATION_LIMIT=${LM_MIGRATION_LIMIT} bash lm/scripts/run-lm-verify-mysql.sh application"

if ! docker ps --format '{{.Names}}' | grep -q "^${JM}$"; then
  echo "ERR: 容器 ${JM} 未运行，请先 ./scripts/up.sh"
  exit 1
fi

bash scripts/run-sql.sh lm/sql/04_sync_ng_application_opt.sql 2>&1 | tee "$SQL_LOG"
echo "[$(date '+%F %T')] 完成。验证: SELECT COUNT(*) FROM application;"
