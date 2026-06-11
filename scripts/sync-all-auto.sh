#!/usr/bin/env bash
# 兼容入口 → sync-pipeline-auto.sh（建宽表 + 锁定 bulk-start-ms + 全量 + 增量）
#
# 用法与 sync-pipeline-auto.sh 相同，例如:
#   ./scripts/sync-all-auto.sh
#   ./scripts/sync-all-auto.sh --skip-staging
#   ./scripts/sync-all-auto.sh --incr-only
#
exec "$(dirname "$0")/sync-pipeline-auto.sh" "$@"
