#!/usr/bin/env bash
# nigeria-flink-sync 表级对账入口（源库业务表 ↔ 目标库；user_id>1亿 / 指定 app）
# 用法见 scripts/reconcile/reconcile_tables.py 文档头
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/lib/load-project-env.sh
source "${ROOT}/scripts/lib/load-project-env.sh"
load_project_env "${ROOT}" || exit 1

export PYTHONUNBUFFERED=1
exec python3 "${ROOT}/scripts/reconcile/reconcile_tables.py" --env "${ROOT}/.env" "$@"
