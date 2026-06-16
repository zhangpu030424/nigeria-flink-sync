#!/usr/bin/env bash
# 将旧单表 user_info_dirty 迁移到分片表 user_info_dirty_0..3 并建 UNION 视图
# 用法: ./scripts/migrate-user-info-dirty-shards.sh
set -euo pipefail
cd "$(dirname "$0")/.."

[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh
# shellcheck source=scripts/lib/user-info-dirty.sh
source scripts/lib/user-info-dirty.sh

migrate_user_info_dirty_to_shards
echo ">> 完成。请 cancel 旧 sink_user_info Job 后重提增量。"
