#!/usr/bin/env bash
# 兼容入口 → sync-pipeline-auto.sh（每表全量→增量串行）
# 新推荐: sync-migrate-auto.sh（先全部全量，再全部增量）
#
# 用法:
#   ./scripts/sync-all-auto.sh                    # 兼容：每表 bulk→incr 串行
#   ./scripts/sync-migrate-auto.sh                # 推荐：bulk 阶段 + incr 阶段
#   ./scripts/sync-all-auto.sh --skip-staging
#   ./scripts/sync-all-auto.sh --incr-only
#   ./scripts/sync-all-auto.sh --jobs=user_info,application
#
exec "$(dirname "$0")/sync-pipeline-auto.sh" "$@"
