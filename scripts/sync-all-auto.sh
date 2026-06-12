#!/usr/bin/env bash
# 兼容入口 → sync-pipeline-auto.sh（源库 DDL + 建宽表 + 锁定 bulk-start-ms + 全量 + 增量）
# 无需 DMS 手动建视图 / GRANT，一条命令全自动。
#
# 用法:
#   ./scripts/sync-all-auto.sh                    # 从零：DDL + 宽表 + 全量 + 增量
#   ./scripts/sync-all-auto.sh --skip-staging     # 宽表已有，全量 + 增量
#   ./scripts/sync-all-auto.sh --incr-only        # 仅重提增量 Job
#   ./scripts/sync-all-auto.sh --jobs=user_info,application
#
exec "$(dirname "$0")/sync-pipeline-auto.sh" "$@"
