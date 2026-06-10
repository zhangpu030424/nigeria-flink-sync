#!/usr/bin/env bash
# GPT 版 user_info 全量独占满速：停 Job → 落地 GPT JSON → Flink 单表写目标
#
# 首次或源表变更:
#   bash scripts/refresh-lm-user-info-staging.sh   # 可选，gpt-full 会自动触发
#
# 全量:
#   bash scripts/run-ng-user-info-gpt-bulk-max.sh
#
# 限量试跑:
#   LM_MIGRATION_LIMIT=1000 bash scripts/run-ng-user-info-gpt-bulk-max.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -f .env ]]; then
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
fi

# 全量：忽略 .env 里 LM_MIGRATION_LIMIT=2147483647
export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
if [[ "$LM_MIGRATION_LIMIT" == "2147483647" ]]; then
  export LM_MIGRATION_LIMIT=0
fi

echo "========== GPT user_info 全量 =========="
echo ">> Step 1/4: 释放 slot"
bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true

echo ""
echo ">> Step 2/4: 检查 TaskManager slots"
bash scripts/check-flink-slots.sh 2>/dev/null || true

SLOTS="${FLINK_TASK_SLOTS:-30}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-30}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"
export SYNC_SLOT_BUFFER=0

if [[ "${FLINK_PARALLELISM}" -gt "${SLOTS}" ]]; then
  echo "WARN: FLINK_PARALLELISM=${FLINK_PARALLELISM} > slots=${SLOTS}，降为 ${SLOTS}"
  export FLINK_PARALLELISM="${SLOTS}"
fi

echo ""
echo ">> Step 3/4: MySQL 全量拼 GPT JSON（flink_stg_user_info_ready）"
bash scripts/refresh-lm-user-info-gpt-full.sh

echo ""
echo ">> Step 4/4: Flink 单表写入目标 user_info（并行=${FLINK_PARALLELISM}）"
exec bash scripts/run-ng-user-info-gpt-bulk-ready.sh
