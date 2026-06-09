#!/usr/bin/env bash
# 全量 user 同步：宽表已含 mobile_token，Flink 不调 /v2t
# 用法: ./scripts/run-user-fast-pre-vt.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

echo ">> 提交预 VT 全量 Job（sql/02_sync_user_fast_pre_vt.sql）"
echo ">> 前置: vt_token_cache 已灌满 + user_sync_staging 已重建（source_user_sync_staging_vt.sql）"
./scripts/run-sql.sh sql/02_sync_user_fast_pre_vt.sql
