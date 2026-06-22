#!/usr/bin/env bash
# DBA 部署 user_info 脏队列：存储过程 + TRIGGER
# 等价于 deploy-source-ddl 中的 ensure_user_info_dirty_deploy（强制重跑 SQL）
#
# 用法:
#   ./scripts/deploy-user-info-dirty-dba.sh
#   SOURCE_MYSQL_ROOT_USER=root SOURCE_MYSQL_ROOT_PASSWORD=xxx ./scripts/deploy-user-info-dirty-dba.sh
#
# 生产曾用 user_info_dirty_0..3 分片时，先跑:
#   ./scripts/migrate-user-info-dirty-unshard.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh
# shellcheck source=scripts/lib/user-info-dirty-deploy.sh
source scripts/lib/user-info-dirty-deploy.sh

dba_user="${SOURCE_MYSQL_ROOT_USER:-${SOURCE_MYSQL_USER:-root}}"
dba_pass="${SOURCE_MYSQL_ROOT_PASSWORD:-${SOURCE_MYSQL_PASSWORD:-}}"

echo ">> DBA 部署 user_info_dirty（${dba_user}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}）"
deploy_user_info_dirty_sql "$dba_user" "$dba_pass"
_verify_user_info_dirty_objects || exit 1
echo ">> user_info_dirty DBA 部署完成"
