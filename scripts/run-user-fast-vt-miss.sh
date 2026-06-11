#!/usr/bin/env bash
# 全量阶段 2：无 VT token 用户，Flink UDF 运行时调 /v2t
# 用法: FLINK_PARALLELISM_VT_MISS=2 ./scripts/run-user-fast-vt-miss.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

export FLINK_PARALLELISM="${FLINK_PARALLELISM_VT_MISS:-2}"
echo ">> 提交 VT 补全 Job（sql/02_sync_user_fast_vt_miss.sql）并行=${FLINK_PARALLELISM}"
echo ">> 前置: 阶段 1 已完成；TaskManager 需能访问 VT_BASE_URL"
./scripts/run-sql.sh sql/02_sync_user_fast_vt_miss.sql
