#!/usr/bin/env bash
# 列出并取消所有 Flink Job（释放 slot，排查用）
set -euo pipefail
CONTAINER="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"

echo ">> 当前 Job 列表："
docker exec "$CONTAINER" ./bin/flink list -a || exit 1

echo ""
read -r -p "取消上述所有 RUNNING/RESTARTING Job？(y/N) " ans
if [[ "${ans,,}" != "y" ]]; then
  echo "已取消操作"
  exit 0
fi

while read -r job_id; do
  [[ -z "$job_id" ]] && continue
  echo ">> 取消 Job $job_id"
  docker exec "$CONTAINER" ./bin/flink cancel "$job_id" || true
done < <(docker exec "$CONTAINER" ./bin/flink list -a 2>/dev/null | grep -oE '[a-f0-9]{32}' | sort -u)

echo ">> 完成"
