#!/usr/bin/env bash
# 源库 DDL 辅助：清理 Lookup 读阻塞、按视图分批部署（避免整文件无输出卡死）
# 依赖: mysql_source_cmd / mysql_source_query（scripts/lib/mysql-source.sh）

SOURCE_DDL_LOCK_WAIT_TIMEOUT="${SOURCE_DDL_LOCK_WAIT_TIMEOUT:-120}"
SOURCE_DDL_KILL_MIN_TIME="${SOURCE_DDL_KILL_MIN_TIME:-3}"

# deploy-source-ddl.sh 校验清单（与 source_lookup_views.sql 一致）
SOURCE_LOOKUP_CHECK_VIEWS=(
  v_adjust_latest_by_adid
  user_personal_latest_lookup
  app_config_lookup
  vt_token_cache_lookup
  user_work_latest_lookup
  user_credit_latest_lookup
  user_reg_ip_lookup
  user_emergency_contacts_lookup
  user_info_install_source_lookup
  user_info_incr_bundle_lookup
  users_by_adid_lookup
  user_incr_lookup
  user_bankcard_id_by_account_lookup
  user_bankcard_incr_lookup
  user_product_latest_lookup
  application_order_lookup
  user_order_installment_loan_lookup
  application_user_lookup
  user_order_loan_lookup
  user_repay_paid_by_order_period
  user_bank_default_lookup
  user_bvn_lookup
  device_ids_latest_lookup
  risk_approval_latest_by_order
  user_repay_paid_latest_by_order
  user_order_installment_overdue
)

source_lookup_views_all_exist() {
  local v
  for v in "${SOURCE_LOOKUP_CHECK_VIEWS[@]}"; do
    view_exists "$v" || return 1
  done
  return 0
}

flink_running_job_count() {
  local jm="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
  if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$jm"; then
    echo 0
    return 0
  fi
  docker exec "$jm" ./bin/flink list 2>/dev/null \
    | grep -oE '[a-f0-9]{32}' | sort -u | wc -l | tr -d '[:space:]' || echo 0
}

# CREATE OR REPLACE VIEW 与运行中 Flink JDBC Lookup 互斥（flink_cdc 会持续占 SHARED_READ）
mysql_source_require_no_flink_for_view_ddl() {
  local cancel_flink="${1:-0}"
  local running
  running=$(flink_running_job_count)
  if [[ "${running:-0}" -eq 0 ]]; then
    return 0
  fi
  if [[ "$cancel_flink" -eq 1 ]]; then
    echo ">> Flink Job=${running} 个，--cancel-flink：先 Cancel 再部署视图"
    bash scripts/cancel-flink-jobs.sh --yes
    sleep 5
    mysql_source_kill_ddl_blockers 0 0
    return 0
  fi
  echo ""
  echo "ERR: 有 ${running} 个 Flink Job 在跑，JDBC Lookup（flink_cdc）占 Lookup 视图 SHARED_READ 锁。"
  echo "  CREATE OR REPLACE VIEW 需要 EXCLUSIVE 锁，与运行中 Job 互斥；KILL MySQL 连接无效（Job 会立即重连）。"
  echo ""
  echo "  视图 SQL 未改 / 已部署过："
  echo "    ./scripts/deploy-source-ddl.sh --skip-if-ok          # 增量启动默认，只校验不重建"
  echo ""
  echo "  git pull 后需更新 Lookup 视图："
  echo "    ./scripts/cancel-flink-jobs.sh --yes"
  echo "    ./scripts/deploy-source-ddl.sh --force-views"
  echo "    ./scripts/sync-incr-auto.sh --keep-jobs              # 再提交增量"
  return 1
}

_mysql_source_ddl_preamble() {
  mysql_source_query "SET SESSION lock_wait_timeout=${SOURCE_DDL_LOCK_WAIT_TIMEOUT};" >/dev/null 2>&1 || true
}

# 列出/杀掉占用 Lookup 视图 MDL 的会话（Metabase、Flink JDBC Lookup 等）
mysql_source_list_ddl_blockers() {
  mysql_source_cmd -e "
    SELECT p.ID, p.USER, p.HOST, p.TIME, p.STATE,
           LEFT(COALESCE(p.INFO, ''), 100) AS INFO_PREVIEW
    FROM information_schema.PROCESSLIST p
    WHERE p.DB = '${SOURCE_MYSQL_DATABASE}'
      AND p.ID <> CONNECTION_ID()
      AND p.COMMAND != 'Daemon'
      AND (
        p.INFO LIKE '%_lookup%'
        OR p.INFO LIKE '%vt_token_cache%'
        OR p.INFO LIKE '%v_adjust_latest%'
        OR p.INFO LIKE '%Metabase%'
      )
    ORDER BY p.TIME DESC;
  " 2>/dev/null || true
}

mysql_source_kill_ddl_blockers() {
  local min_time="${1:-${SOURCE_DDL_KILL_MIN_TIME}}"
  local dry_run="${2:-0}"
  local blockers killed=0

  blockers=$(mysql_source_cmd -N -e "
    SELECT ID
    FROM information_schema.PROCESSLIST
    WHERE DB = '${SOURCE_MYSQL_DATABASE}'
      AND ID <> CONNECTION_ID()
      AND COMMAND != 'Daemon'
      AND TIME >= ${min_time}
      AND (
        INFO LIKE '%_lookup%'
        OR INFO LIKE '%vt_token_cache%'
        OR INFO LIKE '%v_adjust_latest%'
      );
  " 2>/dev/null || true)

  if [[ -z "$blockers" ]]; then
    echo ">> 未发现 Lookup 长查询（Time>=${min_time}s）"
    return 0
  fi

  while read -r pid; do
    [[ -z "$pid" ]] && continue
    if [[ "$dry_run" -eq 1 ]]; then
      echo ">> [dry-run] KILL ${pid}"
    else
      echo ">> KILL ${pid}（释放 Lookup MDL 锁）"
      mysql_source_query "KILL ${pid};" 2>/dev/null || true
    fi
    killed=$((killed + 1))
  done <<< "$blockers"

  if [[ "$killed" -gt 0 && "$dry_run" -eq 0 ]]; then
    sleep 2
  fi
  echo ">> 已处理 ${killed} 个阻塞连接"
}

# 执行单条 SQL（带 lock_wait_timeout）
mysql_source_exec_sql() {
  local sql="$1"
  _mysql_source_ddl_preamble
  mysql_source_cmd -e "$sql"
}

# 整文件 DDL（adjust 等小文件）
mysql_source_ddl_file() {
  local f="$1"
  [[ -f "$f" ]] || { echo "ERR: SQL 文件不存在: $f" >&2; return 1; }
  echo ">> 源库 DDL: $f （$(date '+%H:%M:%S') 开始；lock_wait=${SOURCE_DDL_LOCK_WAIT_TIMEOUT}s）"
  local t0=$SECONDS
  _mysql_source_ddl_preamble
  mysql_source_cmd < "$f"
  echo ">> 完成: $f （耗时 $((SECONDS - t0))s）"
}

# 按 CREATE OR REPLACE VIEW 拆分部署，便于定位卡在哪一个视图
mysql_source_ddl_views_file() {
  local f="$1"
  [[ -f "$f" ]] || { echo "ERR: SQL 文件不存在: $f" >&2; return 1; }

  local total=0 ok=0 fail=0
  local view_name t0 elapsed sqlf tmpdir
  local -a files=()

  echo ">> 源库视图 DDL: $f （逐视图部署；lock_wait=${SOURCE_DDL_LOCK_WAIT_TIMEOUT}s）"
  _mysql_source_ddl_preamble

  tmpdir=$(mktemp -d "${TMPDIR:-/tmp}/nigeria-ddl-views.XXXXXX")

  awk -v dir="$tmpdir" '
    BEGIN { n=0; buf="" }
    /^CREATE OR REPLACE VIEW/ {
      if (buf != "") {
        n++
        f=sprintf("%s/%03d.sql", dir, n)
        print buf > f
        close(f)
        buf=""
      }
      buf=$0
      next
    }
    { if (buf != "") buf=buf "\n" $0 }
    END {
      if (buf != "") {
        n++
        f=sprintf("%s/%03d.sql", dir, n)
        print buf > f
        close(f)
      }
    }
  ' "$f"

  shopt -s nullglob
  files=("$tmpdir"/*.sql)
  shopt -u nullglob

  if [[ ${#files[@]} -eq 0 ]]; then
    rm -rf "$tmpdir"
    echo "ERR: 未从 $f 解析出任何 CREATE VIEW 语句"
    return 1
  fi

  for sqlf in "${files[@]}"; do
    view_name=$(sed -n "s/^CREATE OR REPLACE VIEW \`\?\([a-zA-Z0-9_]*\)\`\?.*/\1/p" "$sqlf" | head -1)
    [[ -z "$view_name" ]] && view_name="$(basename "$sqlf")"
    total=$((total + 1))
    t0=$SECONDS
    printf "  [%02d] %s ... " "$total" "$view_name"
    if mysql_source_cmd < "$sqlf" 2>/tmp/nigeria-ddl-err-$$.log; then
      elapsed=$((SECONDS - t0))
      echo "OK (${elapsed}s)"
      ok=$((ok + 1))
    else
      elapsed=$((SECONDS - t0))
      echo "FAIL (${elapsed}s)"
      sed 's/^/      /' /tmp/nigeria-ddl-err-$$.log 2>/dev/null || true
      fail=$((fail + 1))
      rm -f /tmp/nigeria-ddl-err-$$.log
      rm -rf "$tmpdir"
      echo "ERR: 视图 ${view_name} 部署失败"
      echo "  常见原因: Flink 增量 Job 正在 JDBC Lookup（flink_cdc 占 SHARED_READ）"
      echo "  处理: cancel-flink-jobs → deploy-source-ddl.sh --force-views"
      echo "  排查: ./scripts/deploy-source-ddl.sh --list-blockers"
      return 1
    fi
  done

  rm -rf "$tmpdir"
  rm -f /tmp/nigeria-ddl-err-$$.log
  echo ">> 完成: $f （${ok}/${total} 视图 OK）"
  [[ "$fail" -eq 0 ]]
}
