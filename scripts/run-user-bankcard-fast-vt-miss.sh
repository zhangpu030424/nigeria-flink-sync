#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export FLINK_PARALLELISM="${FLINK_PARALLELISM_VT_MISS:-2}"
echo ">> user_bankcard VT 补全 Job 并行=${FLINK_PARALLELISM}"
./scripts/run-sql.sh sql/02_sync_user_bankcard_fast_vt_miss.sql
