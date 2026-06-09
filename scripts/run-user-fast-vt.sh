#!/usr/bin/env bash
# 全量 user 同步（宽表 CDC + 批量 VT，默认 1 万条/次 HTTP）
# 用法: ./scripts/run-user-fast-vt.sh
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

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
PAR="${FLINK_PARALLELISM:-8}"

echo ">> 提交批量 VT 全量 Job（VT_BATCH_SIZE=${VT_BATCH_SIZE:-10000}, parallel=${PAR}）"
docker exec \
  -e SOURCE_MYSQL_HOST -e SOURCE_MYSQL_PORT -e SOURCE_MYSQL_USER -e SOURCE_MYSQL_PASSWORD -e SOURCE_MYSQL_DATABASE \
  -e TARGET_MYSQL_HOST -e TARGET_MYSQL_PORT -e TARGET_MYSQL_USER -e TARGET_MYSQL_PASSWORD -e TARGET_MYSQL_DATABASE \
  -e FLINK_PARALLELISM="${PAR}" \
  -e FLINK_CDC_CHUNK_SIZE="${FLINK_CDC_CHUNK_SIZE:-100000}" \
  -e FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-10000}" \
  -e FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-10000}" \
  -e VT_BASE_URL="${VT_BASE_URL:-http://101.47.27.225}" \
  -e VT_BATCH_SIZE="${VT_BATCH_SIZE:-10000}" \
  -e VT_BATCH_FLUSH_MS="${VT_BATCH_FLUSH_MS:-5000}" \
  -e VT_BATCH_TIMEOUT_SEC="${VT_BATCH_TIMEOUT_SEC:-300}" \
  -e VT_BATCH_MAX_RETRIES="${VT_BATCH_MAX_RETRIES:-3}" \
  "$JM" ./bin/flink run \
    -c com.nigeria.flink.job.UserSyncFastJob \
    -p "${PAR}" \
    /opt/flink/lib/flink-sync-udf.jar

echo ">> Job 已提交。日志中搜 'VT /v2t batch done' 可见每批条数与耗时。"
