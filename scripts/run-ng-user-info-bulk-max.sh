#!/usr/bin/env bash
# user_info 全量「独占满速」：停所有 Job → 检查 30 slot → 跑 30 并行
#
# 首次或源表变更后，先落地实体表（大幅提速）:
#   bash scripts/refresh-lm-user-info-staging.sh
#
# 然后:
#   bash scripts/run-ng-user-info-bulk-max.sh
#
# 试跑: LM_MIGRATION_LIMIT=1000 bash scripts/run-ng-user-info-bulk-max.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "========== user_info 全量独占满速 =========="
echo ">> Step 1/3: 释放 slot（cancel 所有 Running Job）"
bash scripts/cancel-flink-jobs.sh --yes

echo ""
echo ">> Step 2/3: 确认 TaskManager slots"
if ! bash scripts/check-flink-slots.sh; then
  echo "ERR: slot/parallelism 配置不合理，请检查 .env FLINK_TASK_SLOTS / FLINK_PARALLELISM"
  exit 1
fi

SLOTS="${FLINK_TASK_SLOTS:-30}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-${FLINK_PARALLELISM_BULK:-30}}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-30}"
export SYNC_SLOT_BUFFER=0
export SKIP_LM_VIEW_CREATE=1
export LM_SKIP_VIEW_PROBE=1

if [[ "${FLINK_PARALLELISM}" -gt "${SLOTS}" ]]; then
  echo "WARN: FLINK_PARALLELISM=${FLINK_PARALLELISM} > slots=${SLOTS}，降为 ${SLOTS}"
  export FLINK_PARALLELISM="${SLOTS}"
fi

echo ""
echo ">> Step 3/3: 提交 Job（FLINK_PARALLELISM=${FLINK_PARALLELISM}）"
echo "   列表页 Tasks 固定=4（4个源算子）；真并行看 Job→Overview→Parallelism 列"
echo "   有效 JDBC 读并发 ≈ 4源 × ${FLINK_PARALLELISM} = $((4 * FLINK_PARALLELISM)) 连接（受 ${SLOTS} slot 调度）"
echo ""

exec bash scripts/run-ng-user-info-bulk.sh
