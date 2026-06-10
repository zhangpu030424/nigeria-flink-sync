# shellcheck shell=bash
# GPT user_info：主库建 VIEW、从库校验（无 flink_stg_* 物化）
# source scripts/lib/lm-user-info-gpt-setup.sh

lm_gpt_setup_patch_sink_view() {
  local prep=$1
  python3 - "$prep" <<'PY'
import re, sys, os
path = sys.argv[1]
text = open(path, encoding="utf-8").read()

def strip_uri(sql):
    sql = sql.replace("'registration_ip', uri.ip", "'registration_ip', CAST(NULL AS CHAR)")
    sql = re.sub(
        r"LEFT JOIN v_flink_uri_latest uri ON uri\.`userId` = u\.id\s*",
        "",
        sql,
        count=1,
    )
    return sql

def strip_app(sql):
    sql = sql.replace("'name', app.`name`", "'name', CAST(NULL AS CHAR)")
    sql = re.sub(
        r"LEFT JOIN v_flink_mkt_app app ON app\.id = u\.`appId`\s*",
        "",
        sql,
        count=1,
    )
    return sql

if os.environ.get("STRIP_URI") == "1":
    text = strip_uri(text)
if os.environ.get("STRIP_APP") == "1":
    text = strip_app(text)
open(path, "w", encoding="utf-8").write(text)
PY
}

lm_gpt_ensure_views_on_write() {
  local prep="/tmp/lm_gpt_view_sink-$$.sql"
  lm_mysql_exec_write sql/ddl/lm_user_info_flink_views.sql

  local uri_cnt app_cnt
  uri_cnt=$(lm_mysql_query_write "SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='user_registration_ip';" || echo 0)
  app_cnt=$(lm_mysql_query_write "SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='app';" || echo 0)

  if [[ "$uri_cnt" == "1" ]]; then
    lm_mysql_exec_write sql/ddl/lm_user_info_gpt_view_uri.sql
    export STRIP_URI=0
  else
    echo ">> WARN: 无 user_registration_ip，registration_ip 写 NULL"
    export STRIP_URI=1
  fi
  if [[ "$app_cnt" == "1" ]]; then
    lm_mysql_exec_write sql/ddl/lm_user_info_gpt_view_app.sql
    export STRIP_APP=0
  else
    echo ">> WARN: 无 app 表，app.name 写 NULL"
    export STRIP_APP=1
  fi

  cp sql/ddl/lm_user_info_gpt_view_sink.sql "$prep"
  [[ "$STRIP_URI" == "1" || "$STRIP_APP" == "1" ]] && lm_gpt_setup_patch_sink_view "$prep"
  lm_mysql_exec_write "$prep"
  rm -f "$prep"
}

lm_gpt_view_exists_read() {
  local v=$1
  [[ "$(lm_mysql_query_read "SELECT COUNT(*) FROM information_schema.views
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';" 2>/dev/null || echo 0)" == "1" ]]
}

lm_gpt_view_exists_write() {
  local v=$1
  [[ "$(lm_mysql_query_write "SELECT COUNT(*) FROM information_schema.views
    WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${v}';" 2>/dev/null || echo 0)" == "1" ]]
}

lm_gpt_wait_sink_view_read() {
  local i max="${LM_GPT_VIEW_WAIT_SEC:-300}"
  for ((i=5; i<=max; i+=5)); do
    if lm_gpt_view_exists_read "v_flink_gpt_user_info_sink"; then
      echo ">> 从库 VIEW v_flink_gpt_user_info_sink 已就绪（${i}s）"
      return 0
    fi
    echo ">> 等待 VIEW 同步到从库… ${i}s / ${max}s"
    sleep 5
  done
  echo "ERR: 从库 ${LM_MYSQL_HOST} 仍无 v_flink_gpt_user_info_sink"
  if lm_gpt_view_exists_write "v_flink_gpt_user_info_sink"; then
    echo "  （主库已有，仅主从延迟；可加大 LM_GPT_VIEW_WAIT_SEC 或临时 Flink 读主库）"
  else
    echo "  （主库也没有，请 bash scripts/refresh-lm-user-info-gpt-views.sh 看报错）"
  fi
  return 1
}

lm_gpt_ensure_ready() {
  LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"
  local need=0
  for v in v_flink_mkt_user v_flink_ud_latest v_flink_lup_latest v_flink_dac_latest v_flink_gpt_user_info_sink; do
    lm_gpt_view_exists_read "$v" || need=1
  done
  if [[ "$need" == "0" ]]; then
    echo ">> 从库 VIEW 齐全（含 v_flink_gpt_user_info_sink）"
    return 0
  fi
  echo ">> 从库缺 VIEW，主库创建（仅 VIEW，不物化表）..."
  if ! lm_mysql_assert_writable; then
    echo "ERR: 配置 LM_MYSQL_WRITE_HOST 为主库，或 DBA 手动执行:"
    echo "  sql/ddl/lm_user_info_flink_views.sql"
    echo "  sql/ddl/lm_user_info_gpt_views.sql"
    echo "  sql/ddl/lm_user_info_gpt_view_sink.sql"
    return 1
  fi
  lm_gpt_ensure_views_on_write
  if ! lm_gpt_view_exists_write "v_flink_gpt_user_info_sink"; then
    echo "ERR: 主库创建 v_flink_gpt_user_info_sink 失败"
    return 1
  fi
  lm_gpt_wait_sink_view_read
}

lm_gpt_preflight_read() {
  local cnt
  cnt=$(lm_mysql_query_read "SELECT COUNT(*) FROM v_flink_gpt_user_info_sink LIMIT 1;" 2>/dev/null || echo "ERR")
  if [[ "$cnt" == "ERR" ]] || ! [[ "$cnt" =~ ^[0-9]+$ ]]; then
    echo "ERR: 无法 SELECT v_flink_gpt_user_info_sink @ ${LM_MYSQL_HOST}"
    return 1
  fi
  echo ">> 预检 OK: v_flink_gpt_user_info_sink 约 ${cnt} 行"
  return 0
}
