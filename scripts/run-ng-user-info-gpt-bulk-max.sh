#!/usr/bin/env bash
# GPT user_info 全量：VIEW + Flink 单表（无 Step3 物化、无 JDBC 分区）
# 可选分段: LM_USER_INFO_CHUNK_ROWS=500000（按 user.id 分段，避免一次 Job 过大）
#
# 全量: bash scripts/run-ng-user-info-gpt-bulk-max.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-direct.sh
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
  echo "========== 物化表模式 LM_USER_INFO_MATERIALIZE=1 =========="
  bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true
  bash scripts/refresh-lm-user-info-gpt-full.sh
  export LM_SRC_TABLE_READY=flink_stg_user_info_ready
  exec bash scripts/run-ng-user-info-gpt-direct.sh
fi

SLOTS="${FLINK_TASK_SLOTS:-20}"
export FLINK_PARALLELISM_BULK="${FLINK_PARALLELISM_BULK:-${SLOTS}}"
if [[ "${FLINK_PARALLELISM_BULK}" -gt "${SLOTS}" ]]; then
  export FLINK_PARALLELISM_BULK="${SLOTS}"
fi

CHUNK="${LM_USER_INFO_CHUNK_ROWS:-0}"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"

wait_job() {
  local jid=$1
  local i st
  for ((i=0; i<86400; i+=15)); do
    st=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${jid}" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    case "$st" in
      FINISHED) echo ">> Job ${jid} FINISHED"; return 0 ;;
      FAILED|CANCELED) echo "ERR: Job ${jid} ${st}"; return 1 ;;
      *) [[ $((i % 60)) -eq 0 && -n "$st" ]] && echo "[$(date '+%F %T')] Job ${jid} ${st}..." ;;
    esac
    sleep 15
  done
  echo "ERR: Job ${jid} 超时未完成"
  return 1
}

echo "========== GPT user_info（VIEW + 单表 Flink）=========="
bash scripts/cancel-flink-jobs.sh --yes 2>/dev/null || true
bash scripts/check-flink-slots.sh 2>/dev/null || true

if [[ "$CHUNK" =~ ^[0-9]+$ && "$CHUNK" -gt 0 && "$LM_MIGRATION_LIMIT" == "0" ]]; then
  # shellcheck source=lib/lm-mysql-write.sh
  source "$(dirname "$0")/lib/lm-mysql-write.sh"
  max_id=$(lm_mysql_query_read "SELECT COALESCE(MAX(id),0) FROM \`user\`;" 2>/dev/null || echo 0)
  if ! [[ "$max_id" =~ ^[0-9]+$ ]] || [[ "$max_id" -eq 0 ]]; then
    echo "ERR: 无法读 user 表 MAX(id)"
    exit 1
  fi
  echo ">> 分段全量: chunk=${CHUNK}  max_user_id=${max_id}"
  lo=1
  seg=0
  while [[ "$lo" -le "$max_id" ]]; do
    hi=$((lo + CHUNK - 1))
    seg=$((seg + 1))
    export LM_USER_ID_RANGE_CLAUSE="AND u.user_id BETWEEN ${lo} AND ${hi}"
    export LM_MIGRATION_LIMIT_CLAUSE=""
    echo ""
    echo ">> 段 ${seg}: user.id ${lo} ~ ${hi}"
    job_id=$(bash scripts/run-ng-user-info-gpt-direct.sh | grep -oE '[a-f0-9]{32}' | tail -1)
    if [[ "$job_id" =~ ^[a-f0-9]{32}$ ]]; then
      wait_job "$job_id" || exit 1
    else
      echo "WARN: 未捕获 JobId，请 Web UI 确认"
    fi
    lo=$((hi + 1))
  done
  echo ">> 全部分段完成"
  exit 0
fi

export LM_USER_ID_RANGE_CLAUSE=""
exec bash scripts/run-ng-user-info-gpt-direct.sh
