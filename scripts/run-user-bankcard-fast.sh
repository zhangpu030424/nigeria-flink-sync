#!/usr/bin/env bash
# 全量 user_bankcard：宽表已含 bank_account_token，Flink 不调 /v2t
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

echo ">> 提交 user_bankcard 全量 Job（sql/02_sync_user_bankcard_fast.sql）"
echo ">> 前置: vt_token_cache bank_account 已灌满 + source_user_bankcard_sync_staging.sql 已重建"
./scripts/run-sql.sh sql/02_sync_user_bankcard_fast.sql
