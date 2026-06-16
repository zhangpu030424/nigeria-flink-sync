#!/usr/bin/env bash
# 源库 DDL 一键部署（adjust + Lookup 视图 + user_info 脏队列）
#
# CREATE OR REPLACE VIEW 需 MDL 排他锁。
# - Metabase 长查询：部署前 KILL 即可
# - Flink 增量 Job（flink_cdc JDBC Lookup）：Job 在跑时无法重建视图，须先 Cancel Job
#
# 用法:
#   ./scripts/deploy-source-ddl.sh                    # 增量启动推荐：视图已存在则跳过重建
#   ./scripts/deploy-source-ddl.sh --force-views      # 强制重建（须先 cancel-flink-jobs）
#   ./scripts/deploy-source-ddl.sh --force-views --cancel-flink
#   ./scripts/deploy-source-ddl.sh --list-blockers
#   ./scripts/deploy-source-ddl.sh --kill-readers
#
set -euo pipefail
cd "$(dirname "$0")/.."

KILL_READERS=1
LIST_ONLY=0
KILL_ONLY=0
SKIP_IF_OK=1
FORCE_VIEWS=0
CANCEL_FLINK=0

for arg in "$@"; do
  case "$arg" in
    --no-kill) KILL_READERS=0 ;;
    --kill-readers) KILL_ONLY=1; KILL_READERS=1 ;;
    --list-blockers) LIST_ONLY=1 ;;
    --skip-if-ok) SKIP_IF_OK=1 ;;
    --force-views) FORCE_VIEWS=1; SKIP_IF_OK=0 ;;
    --cancel-flink) CANCEL_FLINK=1 ;;
    -h|--help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $arg（--help 查看）"
      exit 1
      ;;
  esac
done

# .env 可覆盖默认：增量启动 skip-if-ok=1；显式 --force-views 优先
if [[ "$FORCE_VIEWS" -eq 0 && "${DEPLOY_SOURCE_DDL_SKIP_IF_OK:-1}" == "0" ]]; then
  SKIP_IF_OK=0
fi

[[ -f .env ]] || { echo "请先: cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"
SOURCE_DDL_LOCK_WAIT_TIMEOUT="${SOURCE_DDL_LOCK_WAIT_TIMEOUT:-120}"
SOURCE_DDL_KILL_MIN_TIME="${SOURCE_DDL_KILL_MIN_TIME:-3}"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh
# shellcheck source=scripts/lib/mysql-source-ddl.sh
source scripts/lib/mysql-source-ddl.sh
# shellcheck source=scripts/lib/user-info-dirty-deploy.sh
source scripts/lib/user-info-dirty-deploy.sh

echo ">> deploy-source-ddl: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"
echo ">> lock_wait=${SOURCE_DDL_LOCK_WAIT_TIMEOUT}s  skip_if_ok=${SKIP_IF_OK}  force_views=${FORCE_VIEWS}"

if [[ "$LIST_ONLY" -eq 1 ]]; then
  echo ""
  echo ">> Flink Running Jobs: $(flink_running_job_count)"
  echo ">> Lookup / MDL 阻塞会话:"
  mysql_source_list_ddl_blockers
  exit 0
fi

if [[ "$KILL_ONLY" -eq 1 ]]; then
  mysql_source_kill_ddl_blockers
  exit 0
fi

SKIP_VIEW_DDL=0
if [[ "$SKIP_IF_OK" -eq 1 ]] && source_lookup_views_all_exist; then
  echo ""
  echo ">> Lookup 视图已全部存在，跳过 CREATE OR REPLACE（避免与 Flink JDBC Lookup 抢 MDL 锁）"
  echo ">> 若 git pull 更新了 source_lookup_views.sql，请先 cancel Job 再:"
  echo ">>   ./scripts/deploy-source-ddl.sh --force-views --cancel-flink"
  SKIP_VIEW_DDL=1
fi

if [[ "$SKIP_VIEW_DDL" -eq 0 ]]; then
  mysql_source_require_no_flink_for_view_ddl "$CANCEL_FLINK" || exit 1

  if [[ "$KILL_READERS" -eq 1 ]]; then
    echo ""
    echo ">> [0] 清理 Lookup 读阻塞（Metabase 等，Time>=${SOURCE_DDL_KILL_MIN_TIME}s）"
    mysql_source_kill_ddl_blockers
  fi

  VIEW_DDL_FILES=(
    sql/ddl/source_views_adjust.sql
    sql/ddl/source_lookup_views.sql
  )

  for f in "${VIEW_DDL_FILES[@]}"; do
    if [[ "$f" == *source_lookup_views.sql ]]; then
      mysql_source_ddl_views_file "$f"
    else
      mysql_source_ddl_file "$f"
    fi
  done
fi

ensure_user_info_dirty_deploy

echo ""
echo ">> 校验关键对象"
failed=0
for v in "${SOURCE_LOOKUP_CHECK_VIEWS[@]}"; do
  if view_exists "$v"; then
    echo "  ✓ ${v}"
  else
    echo "  ✗ ${v} 缺失"
    failed=1
  fi
done

if [[ "$failed" -ne 0 ]]; then
  echo "ERR: 源库视图未完整。若 Flink Job 在跑，请先 cancel 再 --force-views"
  exit 1
fi

echo ""
echo ">> 校验 user_info_dirty 分片表"
if user_info_dirty_shards_ok 2>/dev/null; then
  echo "  ✓ user_info_dirty_0..$(( ${USER_INFO_DIRTY_SHARDS:-4} - 1 ))"
else
  echo "  ✗ user_info_dirty 分片表缺失（期望 user_info_dirty_0..$(( ${USER_INFO_DIRTY_SHARDS:-4} - 1 ))）"
  failed=1
fi

if [[ "$failed" -ne 0 ]]; then
  echo "ERR: 源库 DDL 部署未完整"
  exit 1
fi

echo ">> 源库 DDL 部署完成"
