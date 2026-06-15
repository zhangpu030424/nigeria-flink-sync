#!/usr/bin/env bash
# DBA 部署 user_info 脏队列：存储过程 + TRIGGER（须 root 或 TRIGGER/CREATE ROUTINE 权限）
#
# 用法:
#   SOURCE_MYSQL_USER=root SOURCE_MYSQL_PASSWORD=xxx ./scripts/deploy-user-info-dirty-dba.sh
#   或交互输入密码:
#   SOURCE_MYSQL_USER=root ./scripts/deploy-user-info-dirty-dba.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

SOURCE_MYSQL_USER="${SOURCE_MYSQL_USER:-root}"
SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

echo ">> DBA 部署 user_info_dirty（${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}）"
mysql_source_file sql/ddl/user_info_dirty_enqueue.sql
mysql_source_file sql/ddl/user_info_dirty.sql

echo ""
echo ">> 校验存储过程"
for p in sp_user_info_dirty_enqueue sp_user_info_dirty_enqueue_bvn \
         sp_user_info_dirty_enqueue_adid sp_user_info_dirty_enqueue_emergency_mobile; do
  cnt=$(mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema='${SOURCE_MYSQL_DATABASE}' AND routine_name='${p}';" \
    | tr -d '[:space:]')
  [[ "${cnt:-0}" -ge 1 ]] && echo "  ✓ ${p}" || { echo "  ✗ ${p}"; exit 1; }
done

trg_cnt=$(mysql_source_query \
  "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema='${SOURCE_MYSQL_DATABASE}' AND trigger_name LIKE 'trg_user_info_dirty_%';" \
  | tr -d '[:space:]')
echo "  ✓ TRIGGER 数量=${trg_cnt}（期望≥14）"
[[ "${trg_cnt:-0}" -ge 14 ]] || exit 1

echo ">> user_info_dirty DBA 部署完成"
