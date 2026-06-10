#!/usr/bin/env bash
# GPT 版 user_info 全量：停 Job → Flink 直连 VIEW（无物化、无 JDBC 分区）
#
# .env 示例（20 核）:
#   FLINK_TASK_SLOTS=20 FLINK_PARALLELISM_BULK=20
#
# 全量: bash scripts/run-ng-user-info-gpt-bulk-max.sh
# 旧路径（MySQL 物化 flink_stg_user_info_ready）: LM_USER_INFO_MATERIALIZE=1 bash scripts/run-ng-user-info-gpt-bulk-max.sh
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

if [[ "${LM_USER_INFO_MATERIALIZE:-0}" == "1" ]]; then
  SLOTS="${FLINK_TASK_SLOTS:-40}"
  MAX_PAR="${LM_USER_INFO_MAX_PARALLEL:-${SLOTS}}"
  export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
  export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"
  export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-50000}"
  export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-50000}"
  export SYNC_SLOT_BUFFER=0
  if [[ "${FLINK_PARALLELISM}" -gt "${MAX_PAR}" ]]; then
    export FLINK_PARALLELISM="${MAX_PAR}"
    export FLINK_PARALLELISM_BULK="${MAX_PAR}"
  fi
  if [[ "${FLINK_PARALLELISM}" -gt "${SLOTS}" ]]; then
    export FLINK_PARALLELISM="${SLOTS}"
    export FLINK_PARALLELISM_BULK="${SLOTS}"
  fi
  echo "========== GPT user_info 物化表模式（LM_USER_INFO_MATERIALIZE=1）=========="
  bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true
  bash scripts/check-flink-slots.sh 2>/dev/null || true
  bash scripts/refresh-lm-user-info-gpt-full.sh
  exec bash scripts/run-ng-user-info-gpt-bulk-ready.sh
fi

SLOTS="${FLINK_TASK_SLOTS:-20}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK}"
if [[ "${FLINK_PARALLELISM}" -gt "${SLOTS}" ]]; then
  export FLINK_PARALLELISM="${SLOTS}"
  export FLINK_PARALLELISM_BULK="${SLOTS}"
fi

echo "========== GPT user_info 全量（直连 VIEW，无物化/无分区）=========="
echo ">> Step 1/2: 释放 slot"
bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true

echo ""
echo ">> Step 2/2: Flink 直连老库 VIEW → 目标 user_info"
exec bash scripts/run-ng-user-info-gpt-direct.sh
