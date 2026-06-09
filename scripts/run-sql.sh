#!/usr/bin/env bash
# 读取 .env 替换占位符，在 JobManager 容器内执行 Flink SQL 文件
# 用法: ./scripts/run-sql.sh sql/02_sync_user_test.sql
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写"
  exit 1
fi

SQL_FILE="${1:-}"
if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
  echo "用法: $0 <sql文件路径>"
  echo "示例:"
  echo "  $0 sql/01_cdc_smoke.sql"
  echo "  $0 sql/02_sync_user_test.sql"
  exit 1
fi

# shellcheck disable=SC1091
set -a
# 只加载 KEY=VALUE 行；已在环境中的变量不覆盖（便于切增量前 export FLINK_PARALLELISM=1）
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
export USER_ID_OFFSET="${USER_ID_OFFSET:-100000000}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-16}"
export FLINK_MINI_BATCH_SIZE="${FLINK_MINI_BATCH_SIZE:-10000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-10000}"
export FLINK_CDC_CHUNK_SIZE="${FLINK_CDC_CHUNK_SIZE:-100000}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-10000}"
export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-timestamp}"
export CDC_STARTUP_TIMESTAMP_MILLIS="${CDC_STARTUP_TIMESTAMP_MILLIS:-0}"

VARS='${SOURCE_MYSQL_HOST} ${SOURCE_MYSQL_PORT} ${SOURCE_MYSQL_USER} ${SOURCE_MYSQL_PASSWORD} ${SOURCE_MYSQL_DATABASE} ${LM_MYSQL_HOST} ${LM_MYSQL_PORT} ${LM_MYSQL_USER} ${LM_MYSQL_PASSWORD} ${LM_MYSQL_DATABASE} ${TARGET_MYSQL_HOST} ${TARGET_MYSQL_PORT} ${TARGET_MYSQL_USER} ${TARGET_MYSQL_PASSWORD} ${TARGET_MYSQL_DATABASE} ${FLINK_PARALLELISM} ${FLINK_MINI_BATCH_SIZE} ${FLINK_SINK_BUFFER_ROWS} ${FLINK_CDC_CHUNK_SIZE} ${FLINK_CDC_FETCH_SIZE} ${CDC_STARTUP_MODE} ${CDC_STARTUP_TIMESTAMP_MILLIS}'

PREPARED="/tmp/nigeria-flink-run-$$.sql"
envsubst "$VARS" < "$SQL_FILE" > "$PREPARED"

CONTAINER="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
REMOTE="/tmp/nigeria-flink-run.sql"

echo ">> 执行: $SQL_FILE"
docker cp "$PREPARED" "${CONTAINER}:${REMOTE}"
docker exec "$CONTAINER" ./bin/sql-client.sh -f "$REMOTE"
rm -f "$PREPARED"

echo ">> 完成。INSERT 类语句会提交长期 Job，请到 Web UI 查看 Running Jobs。"
