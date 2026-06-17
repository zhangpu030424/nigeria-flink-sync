#!/usr/bin/env bash
# 生产库 user_info_dirty 分片 → 单表，对齐仓库（方案 A）
#
# 步骤:
#   1. 合并 user_info_dirty_0..3 → user_info_dirty
#   2. 覆盖 4 个入队存储过程（写单表，无动态 SQL）
#   3. 校验 TRIGGER ≥14
#
# 用法:
#   SOURCE_MYSQL_ROOT_USER=root SOURCE_MYSQL_ROOT_PASSWORD=xxx ./scripts/migrate-user-info-dirty-unshard.sh
#   ./scripts/migrate-user-info-dirty-unshard.sh --skip-merge   # 分片表已空或不存在，只重部署 SP
#
set -euo pipefail
cd "$(dirname "$0")/.."

SKIP_MERGE=0
for arg in "$@"; do
  [[ "$arg" == "--skip-merge" ]] && SKIP_MERGE=1
done

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

_mysql_source_file_as() {
  local user=$1 pass=$2 file=$3
  MYSQL_PWD="$pass" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
    -u "$user" "${SOURCE_MYSQL_DATABASE}" < "$file"
}

echo ">> user_info_dirty 回迁单表（${dba_user}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}）"

if [[ "$SKIP_MERGE" -eq 0 ]]; then
  echo ">> [1/3] 合并 user_info_dirty_0..3 → user_info_dirty"
  _mysql_source_file_as "$dba_user" "$dba_pass" sql/ddl/user_info_dirty_unshard_migrate.sql
else
  echo ">> [1/3] 跳过合并（--skip-merge）"
  _mysql_source_file_as "$dba_user" "$dba_pass" sql/ddl/user_info_dirty.sql
fi

echo ">> [2/3] 覆盖入队存储过程（单表版）"
_mysql_source_file_as "$dba_user" "$dba_pass" sql/ddl/user_info_dirty_enqueue.sql

echo ">> [3/3] 校验 TRIGGER + 存储过程"
if ! _verify_user_info_dirty_objects; then
  echo ">> TRIGGER 不足时执行: mysql ... < sql/ddl/user_info_dirty.sql"
  exit 1
fi

echo ""
echo ">> 完成。请确认:"
echo "   1. user_info_dirty 行数 ≥ 分片表合计（或符合预期）"
echo "   2. SHOW CREATE PROCEDURE sp_user_info_dirty_enqueue 写入 user_info_dirty（非 dirty_0）"
echo "   3. 测试: INSERT INTO user_info_dirty (user_id,updated_at) VALUES (<uid>, NOW(3));"
echo "   4. Flink sink_user_info Records Sent 增加"
echo "   5. 稳定后可 DROP user_info_dirty_0..3（见 sql/ddl/user_info_dirty_unshard_migrate.sql 文末）"
