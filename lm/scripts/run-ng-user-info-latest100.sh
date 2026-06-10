#!/usr/bin/env bash
# 最新 N 条 user_info 试同步：MySQL 侧 export 逻辑落地 → Flink 单表写入目标库
# 用法: bash lm/scripts/run-ng-user-info-latest100.sh
#       LM_PICK_N=100 bash lm/scripts/run-ng-user-info-latest100.sh
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
export LM_PICK_N="${LM_PICK_N:-100}"

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
LOG_DIR="logs"
SQL_LOG="${LOG_DIR}/sync-ng-user-info-latest100-sql.log"
mkdir -p "$LOG_DIR"

# 100 行试跑无需高并行
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"

echo "[$(date '+%F %T')] user_info 最新 ${LM_PICK_N} 条试同步"
echo "  1) 老库落地 flink_stg_user_info_ready（export_user_info_latest100 逻辑）"
echo "  2) Flink 单表读 → 目标 user_info"

bash "$(dirname "$0")/refresh-lm-user-info-latest100.sh"

echo "[$(date '+%F %T')] 取消残留 sink_user_info Job（如有）"
while read -r jid; do
  [[ -z "$jid" ]] && continue
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -i 'sink_user_info' -B1 | grep -oE '[a-f0-9]{32}' | sort -u || true)

echo "[$(date '+%F %T')] 提交 Flink Job: lm/sql/04_sync_ng_user_info_latest100.sql"
bash scripts/run-sql.sh lm/sql/04_sync_ng_user_info_latest100.sql 2>&1 | tee "$SQL_LOG"

echo "[$(date '+%F %T')] 完成。请到 Flink Web UI 查看 Job 状态，目标库: SELECT COUNT(*) FROM user_info;"
