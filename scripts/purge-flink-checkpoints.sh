#!/usr/bin/env bash
# 清空 Flink checkpoint / savepoint 目录，确保下次提交是「全新 Job」，不会 restore 旧 binlog 位点
# 用法: bash scripts/purge-flink-checkpoints.sh
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
TM="${FLINK_TASKMANAGER_CONTAINER:-nigeria-flink-taskmanager}"

mkdir -p data/flink-checkpoints data/flink-savepoints
echo ">> 清理宿主机 data/flink-checkpoints、data/flink-savepoints"
find data/flink-checkpoints -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true
find data/flink-savepoints -mindepth 1 -maxdepth 1 -exec rm -rf {} + 2>/dev/null || true

for c in "$JM" "$TM"; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo ">> 清理容器 ${c} 内 /tmp/flink-checkpoints、/tmp/flink-savepoints"
    docker exec "$c" bash -c \
      'rm -rf /tmp/flink-checkpoints/* /tmp/flink-savepoints/* 2>/dev/null || true' || true
  fi
done

echo ">> checkpoint/savepoint 已清空（下次 CDC 只能用 scan.startup.* 新起点，不会 restore 旧位点）"
