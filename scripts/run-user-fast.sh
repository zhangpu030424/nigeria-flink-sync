#!/usr/bin/env bash
# 全量 user 同步：宽表已含 mobile_token，Flink 不调 /v2t
# 用法: ./scripts/run-user-fast.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

echo ">> 提交全量阶段 1 Job（sql/02_sync_user_fast.sql，仅有 mobile_token 的用户）"
echo ">> 前置: user_sync_staging 已重建；无 token 用户由 sync-job-auto 阶段 2 或 run-user-fast-vt-miss.sh 补全"
./scripts/run-sql.sh sql/02_sync_user_fast.sql
