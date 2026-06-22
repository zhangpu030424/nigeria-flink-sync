#!/usr/bin/env bash
# 取消所有 Running Flink Job，释放 slot 给全量迁移
# 用法: bash scripts/cancel-flink-jobs.sh          # 交互确认
#       bash scripts/cancel-flink-jobs.sh --yes    # 直接取消
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
YES=0
[[ "${1:-}" == "--yes" || "${1:-}" == "-y" ]] && YES=1

if ! docker ps --format '{{.Names}}' | grep -qx "$JM"; then
  echo "ERR: ${JM} 未运行"
  exit 1
fi

mapfile -t JIDS < <(docker exec "$JM" ./bin/flink list 2>/dev/null \
  | grep -oE '[a-f0-9]{32}' | sort -u || true)

if [[ ${#JIDS[@]} -eq 0 ]]; then
  echo ">> 无 Running Job，slot 已空闲"
  exit 0
fi

echo ">> 将取消 ${#JIDS[@]} 个 Job:"
docker exec "$JM" ./bin/flink list 2>/dev/null || true

if [[ "$YES" -ne 1 ]]; then
  read -r -p "确认 cancel? [y/N] " ans
  [[ "${ans,,}" == "y" || "${ans,,}" == "yes" ]] || { echo "已取消"; exit 0; }
fi

for jid in "${JIDS[@]}"; do
  echo ">> cancel $jid"
  docker exec "$JM" ./bin/flink cancel "$jid" 2>/dev/null || true
done

sleep 3
echo ">> 剩余 Job:"
docker exec "$JM" ./bin/flink list 2>/dev/null || true
bash scripts/check-flink-slots.sh
