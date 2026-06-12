#!/usr/bin/env bash
# 从零重跑：Cancel 全部 Job → 清 user_info 脏队列 → 一键流水线（DDL + VT + 宽表 + 6 表全量 + 增量）
#
# 用法:
#   ./scripts/full-rerun.sh
#   ./scripts/full-rerun.sh --skip-vt-emergency-enum   # ENUM 已扩过则跳过
#   ./scripts/full-rerun.sh --jobs user,user_info      # 只跑部分表（传给 sync-pipeline-auto.sh）
#
# 前置:
#   - .env 已配置；Flink 容器 RUNNING
#   - TRIGGER / 存储过程须 root：若 deploy-source-ddl 报权限错，先:
#       mysql -u root ... < sql/ddl/user_info_dirty_enqueue.sql
#       mysql -u root ... < sql/ddl/user_info_dirty.sql
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_EMERGENCY_ENUM=0
PIPELINE_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --skip-vt-emergency-enum) SKIP_EMERGENCY_ENUM=1 ;;
    *) PIPELINE_ARGS+=("$arg") ;;
  esac
done

[[ -f .env ]] || { echo "请先: cp .env.example .env 并填写"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

echo "=========================================="
echo "全量+增量 从零重跑"
echo "  FLINK_PARALLELISM_BULK=${FLINK_PARALLELISM_BULK:-?}"
echo "  FLINK_PARALLELISM_INCR=${FLINK_PARALLELISM_INCR:-?}"
echo "  CDC_SERVER_ID_UI_DIRTY=${CDC_SERVER_ID_UI_DIRTY:-?}"
echo "=========================================="

echo ""
echo ">> [1/4] 取消全部 Running Flink Job"
bash scripts/cancel-flink-jobs.sh --yes

if [[ "$SKIP_EMERGENCY_ENUM" -eq 0 ]] && [[ -f sql/ddl/vt_token_cache_add_emergency_contact.sql ]]; then
  echo ""
  echo ">> [2/4] 扩展 vt_token_cache ENUM + 灌 emergency_contact 明文（可重复执行）"
  # shellcheck source=scripts/lib/mysql-source.sh
  source scripts/lib/mysql-source.sh
  mysql_source_file sql/ddl/vt_token_cache_add_emergency_contact.sql || {
    echo "WARN: emergency_contact ENUM/灌数失败（可能须 root 执行上述 SQL），继续流水线..."
  }
else
  echo ""
  echo ">> [2/4] 跳过 emergency_contact ENUM（--skip-vt-emergency-enum）"
fi

echo ""
echo ">> [3/4] 清空 user_info_dirty（避免上次积压 binlog 干扰）"
# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh
if table_exists user_info_dirty; then
  mysql_source_query "TRUNCATE TABLE user_info_dirty;" && echo "  ✓ TRUNCATE user_info_dirty"
else
  echo "  user_info_dirty 不存在，跳过（deploy-source-ddl 会建）"
fi

echo ""
echo ">> [4/4] 启动一键流水线 sync-pipeline-auto.sh"
echo "    顺序: deploy DDL → 锁 bulk-start-ms → VT+宽表 → user→user_info→…→loan 全量→增量"
echo "    日志: logs/sync-<job>-auto.log  预计数小时，勿中断"
echo ""

exec ./scripts/sync-pipeline-auto.sh "${PIPELINE_ARGS[@]}"
