#!/usr/bin/env bash
# GPT 版 user_info 试同步：MySQL 落地（GPT JSON）→ Flink 单表写目标
# 用法: bash scripts/run-ng-user-info-gpt-latest100.sh
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
export LM_PICK_N="${LM_PICK_N:-100}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-gpt-latest100-sql.log"
mkdir -p "$LOG_DIR"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"

echo "[$(date '+%F %T')] GPT 版 user_info 试同步（最新 ${LM_PICK_N} 条）"
bash scripts/refresh-lm-user-info-gpt-latest100.sh

while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

bash scripts/run-sql.sh sql/04_sync_ng_user_info_latest100.sql 2>&1 | tee "$SQL_LOG"
echo "[$(date '+%F %T')] 完成。验证: SELECT user_id, JSON_EXTRACT(info,'$.registration_time') FROM user_info ORDER BY user_id DESC LIMIT 5;"
