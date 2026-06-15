#!/usr/bin/env bash
# Metabase 等 BI 误查 Flink Lookup 视图时：杀长连接 +（可选）REVOKE 只读权限
#
# Lookup 视图 / vt_token_cache 仅供 Flink JDBC Lookup，Metabase 应查目标库 platform_db.user_info
#
# 用法:
#   ./scripts/block-metabase-lookup.sh --list
#   ./scripts/block-metabase-lookup.sh --kill              # 杀占锁查询（Time>=5s）
#   ./scripts/block-metabase-lookup.sh --kill --user NGuserReadonly_backend
#   ./scripts/block-metabase-lookup.sh --revoke            # root REVOKE（需 SOURCE_MYSQL_ROOT_*）
#   ./scripts/block-metabase-lookup.sh --kill --revoke
#
set -euo pipefail
cd "$(dirname "$0")/.."

KILL=0
REVOKE=0
LIST=0
DRY_RUN=0
READONLY_USER="${METABASE_READONLY_USER:-NGuserReadonly_backend}"
MIN_TIME_SEC=5

for arg in "$@"; do
  case "$arg" in
    --kill) KILL=1 ;;
    --revoke) REVOKE=1 ;;
    --list) LIST=1 ;;
    --dry-run) DRY_RUN=1 ;;
    --user=*) READONLY_USER="${arg#--user=}" ;;
    --user)
      echo "ERR: 请用 --user=NGuserReadonly_backend"
      exit 1
      ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $arg"
      exit 1
      ;;
  esac
done

[[ "$KILL" -eq 1 || "$REVOKE" -eq 1 || "$LIST" -eq 1 ]] || LIST=1

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

lookup_like="(
  INFO LIKE '%_lookup%'
  OR INFO LIKE '%vt_token_cache%'
  OR INFO LIKE '%user_info_dirty%'
  OR INFO LIKE '%Metabase%'
)"

echo "=========================================="
echo "Flink Lookup 读保护"
echo "  库: ${SOURCE_MYSQL_DATABASE}@${SOURCE_MYSQL_HOST}"
echo "  目标只读用户: ${READONLY_USER}"
echo "=========================================="

if [[ "$LIST" -eq 1 ]]; then
  echo ""
  echo ">> 当前可疑连接:"
  mysql_source_cmd -e "
    SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE,
           LEFT(COALESCE(INFO, ''), 120) AS INFO_PREVIEW
    FROM information_schema.PROCESSLIST
    WHERE DB = '${SOURCE_MYSQL_DATABASE}'
      AND ID <> CONNECTION_ID()
      AND COMMAND != 'Daemon'
      AND (
        USER = '${READONLY_USER}'
        OR ${lookup_like}
      )
    ORDER BY TIME DESC;
  " 2>/dev/null || true
fi

if [[ "$KILL" -eq 1 ]]; then
  echo ""
  echo ">> 杀连接（USER=${READONLY_USER} 或查 Lookup，Time>=${MIN_TIME_SEC}s）..."
  blockers=$(mysql_source_cmd -N -e "
    SELECT ID
    FROM information_schema.PROCESSLIST
    WHERE DB = '${SOURCE_MYSQL_DATABASE}'
      AND ID <> CONNECTION_ID()
      AND COMMAND != 'Daemon'
      AND TIME >= ${MIN_TIME_SEC}
      AND (
        USER = '${READONLY_USER}' AND ${lookup_like}
      );
  " 2>/dev/null || true)
  if [[ -n "$blockers" ]]; then
    while read -r pid; do
      [[ -z "$pid" ]] && continue
      echo ">> KILL ${pid}"
      [[ "$DRY_RUN" -eq 0 ]] && mysql_source_query "KILL ${pid};" || true
    done <<< "$blockers"
  else
    echo ">> 未发现需杀的连接"
  fi
fi

if [[ "$REVOKE" -eq 1 ]]; then
  dba_user="${SOURCE_MYSQL_ROOT_USER:-root}"
  dba_pass="${SOURCE_MYSQL_ROOT_PASSWORD:-}"
  if [[ -z "$dba_pass" ]]; then
    echo ""
    echo "ERR: --revoke 需要 SOURCE_MYSQL_ROOT_PASSWORD（.env 中配置）"
    exit 1
  fi
  echo ""
  echo ">> REVOKE ${READONLY_USER} 对 Flink 内部 Lookup / vt_token_cache ..."
  if [[ "$DRY_RUN" -eq 1 ]]; then
    echo ">> dry-run: sql/ddl/flink_lookup_revoke_readonly.sql"
  else
    MYSQL_PWD="${dba_pass}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
      -u "${dba_user}" "${SOURCE_MYSQL_DATABASE}" < sql/ddl/flink_lookup_revoke_readonly.sql
    echo ">> REVOKE 完成；Metabase 查这些对象将报 Access denied"
  fi
fi

echo ""
echo ">> 提示: Metabase 应连目标库 ${TARGET_MYSQL_DATABASE:-platform_db}.user_info，勿查源库 *_lookup"
