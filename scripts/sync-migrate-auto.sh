#!/usr/bin/env bash
# 完整迁移：全量 → 增量（两阶段，推荐）
#
# 等价于:
#   ./scripts/sync-bulk-auto.sh [bulk 参数]
#   ./scripts/sync-incr-auto.sh [incr 参数]
#
# 用法:
#   ./scripts/sync-migrate-auto.sh
#   ./scripts/sync-migrate-auto.sh --jobs user,user_info
#   ./scripts/sync-migrate-auto.sh --skip-staging
#   ./scripts/sync-migrate-auto.sh --bulk-submit-only-jobs id_mapping
#   ./scripts/sync-migrate-auto.sh --incr-startup-mode latest-offset
#   ./scripts/sync-migrate-auto.sh --truncate-user-info-dirty
#
# 默认会先 cancel 全部 Job 并清空 checkpoint，避免 CDC restore 已 purge 的 binlog 位点。
#
set -euo pipefail
cd "$(dirname "$0")/.."

BULK_ARGS=()
INCR_ARGS=()
KEEP_JOBS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --incr-startup-mode=*)
      INCR_ARGS+=(--startup-mode="${1#--incr-startup-mode=}")
      ;;
    --incr-startup-mode)
      shift
      INCR_ARGS+=(--startup-mode="${1:-timestamp}")
      ;;
    --user-info-latest-offset) INCR_ARGS+=("$1") ;;
    --keep-user-info-dirty) ;;
    --truncate-user-info-dirty) INCR_ARGS+=("$1") ;;
    --verify) INCR_ARGS+=("$1") ;;
    --bulk-submit-only-jobs=*)
      BULK_ARGS+=("$1")
      ;;
    --bulk-submit-only-jobs)
      shift
      BULK_ARGS+=(--bulk-submit-only-jobs="${1:-id_mapping}")
      ;;
    --keep-jobs)
      KEEP_JOBS=1
      BULK_ARGS+=("$1")
      INCR_ARGS+=("$1")
      ;;
    *)
      BULK_ARGS+=("$1")
      ;;
  esac
  shift
done

echo "=========================================="
echo "完整迁移 sync-migrate-auto（全量 → 增量）"
echo "=========================================="

if [[ "$KEEP_JOBS" -eq 0 ]]; then
  echo ">> [0] Cancel 全部 Job + 清空 checkpoint（禁止 restore 旧 binlog 位点）"
  bash scripts/cancel-flink-jobs.sh --yes
  # 子脚本不再二次 cancel
  BULK_ARGS+=(--keep-jobs)
  INCR_ARGS+=(--keep-jobs)
else
  echo ">> [0] --keep-jobs：保留存量 Job / checkpoint"
fi

./scripts/sync-bulk-auto.sh "${BULK_ARGS[@]}"

echo ""
echo ">> 全量阶段结束，开始增量..."
./scripts/sync-incr-auto.sh "${INCR_ARGS[@]}"
