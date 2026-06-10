#!/usr/bin/env bash
# GPT 版 user_info Flink Batch（默认直连 VIEW，无物化/无分区）
# 用法: bash scripts/run-ng-user-info-gpt-bulk.sh
# 试跑: LM_MIGRATION_LIMIT=100 bash scripts/run-ng-user-info-gpt-bulk.sh
exec bash "$(dirname "$0")/run-ng-user-info-gpt-direct.sh"
