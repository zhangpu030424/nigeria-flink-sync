#!/usr/bin/env bash
# GPT 版 user_info 全量独占满速：停 Job → 落地 GPT JSON → Flink 单表写目标
#
# 40 核并行（.env 示例）:
#   FLINK_TASK_SLOTS=40 FLINK_PARALLELISM_BULK=40 LM_USER_INFO_MAX_PARALLEL=40
# 20 核够用:
#   FLINK_TASK_SLOTS=20 FLINK_PARALLELISM_BULK=20 LM_USER_INFO_MAX_PARALLEL=20
#   docker compose up -d --force-recreate taskmanager
#
# 全量: bash scripts/run-ng-user-info-gpt-bulk-max.sh
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

export LM_MIGRATION_LIMIT="${LM_MIGRATION_LIMIT:-0}"
[[ "$LM_MIGRATION_LIMIT" == "2147483647" ]] && export LM_MIGRATION_LIMIT=0

SLOTS="${FLINK_TASK_SLOTS:-40}"
MAX_PAR="${LM_USER_INFO_MAX_PARALLEL:-${SLOTS}}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"
export SYNC_SLOT_BUFFER=0

if [[ "${FLINK_PARALLELISM}" -gt "${MAX_PAR}" ]]; then
  echo "WARN: FLINK_PARALLELISM_BULK=${FLINK_PARALLELISM} > LM_USER_INFO_MAX_PARALLEL=${MAX_PAR}，降为 ${MAX_PAR}"
  export FLINK_PARALLELISM="${MAX_PAR}"
  export FLINK_PARALLELISM_BULK="${MAX_PAR}"
fi
if [[ "${FLINK_PARALLELISM}" -gt "${SLOTS}" ]]; then
  echo "WARN: FLINK_PARALLELISM=${FLINK_PARALLELISM} > FLINK_TASK_SLOTS=${SLOTS}，降为 ${SLOTS}"
  export FLINK_PARALLELISM="${SLOTS}"
  export FLINK_PARALLELISM_BULK="${SLOTS}"
fi

echo "========== GPT user_info 全量（Flink 并行=${FLINK_PARALLELISM} / slots=${SLOTS}）=========="
echo ">> Step 1/4: 释放 slot"
bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true

echo ""
echo ">> Step 2/4: 确认 TaskManager slots（须 ≥ ${FLINK_PARALLELISM}）"
bash scripts/check-flink-slots.sh 2>/dev/null || true

echo ""
echo ">> Step 3/4: MySQL 全量拼 GPT JSON（单线程 SQL，与 Flink 核数无关）"
echo ">>         Flink ${FLINK_PARALLELISM} 路并行在 Step 4 才生效"
bash scripts/refresh-lm-user-info-gpt-full.sh

echo ""
echo ">> Step 4/4: Flink 单表写入 user_info（parallelism.default=${FLINK_PARALLELISM}）"
exec bash scripts/run-ng-user-info-gpt-bulk-ready.sh
