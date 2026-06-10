# shellcheck shell=bash
# 老库 DDL/INSERT 用可写主库；Flink JDBC 读仍用 LM_MYSQL_HOST（可为从库）
# source scripts/lib/lm-mysql-write.sh

lm_mysql_write_host() {
  echo "${LM_MYSQL_WRITE_HOST:-${LM_MYSQL_HOST:?}}"
}

lm_mysql_write_port() {
  echo "${LM_MYSQL_WRITE_PORT:-${LM_MYSQL_PORT:-3306}}"
}

lm_mysql_write_user() {
  echo "${LM_MYSQL_WRITE_USER:-${LM_MYSQL_USER:?}}"
}

lm_mysql_write_password() {
  echo "${LM_MYSQL_WRITE_PASSWORD:-${LM_MYSQL_PASSWORD:?}}"
}

lm_mysql_read_host() {
  echo "${LM_MYSQL_HOST:?}"
}

lm_mysql_read_port() {
  echo "${LM_MYSQL_PORT:-3306}"
}

lm_mysql_is_readonly() {
  local host=$1 port=$2 user=$3 pass=$4
  local ro
  ro=$(MYSQL_PWD="$pass" mysql --connect-timeout=10 \
    -h "$host" -P "$port" -u "$user" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    -N -e "SELECT @@read_only;" 2>/dev/null || echo "?")
  [[ "$ro" == "1" ]]
}

lm_mysql_assert_writable() {
  local host port user pass
  host=$(lm_mysql_write_host)
  port=$(lm_mysql_write_port)
  user=$(lm_mysql_write_user)
  pass=$(lm_mysql_write_password)
  if lm_mysql_is_readonly "$host" "$port" "$user" "$pass"; then
    echo "ERR: ${host}:${port} 为只读从库 (read_only=1)，不能 CREATE/INSERT"
    echo "  请在 .env 配置可写主库:"
    echo "    LM_MYSQL_WRITE_HOST=<主库地址>"
    echo "    LM_MYSQL_WRITE_PORT=34057   # 可选，默认同 LM_MYSQL_PORT"
    echo "  或在主库手动执行 sql/ddl/lm_user_info_gpt_staging_full.sql 后等同步到从库"
    echo "  详见 docs/LM_REPLICA_MIGRATION.md"
    return 1
  fi
  echo ">> 可写库: ${host}:${port} (read_only=0)"
  return 0
}

lm_mysql_exec_write() {
  local sql_file=$1
  MYSQL_PWD="$(lm_mysql_write_password)" mysql \
    --connect-timeout=30 \
    -h "$(lm_mysql_write_host)" -P "$(lm_mysql_write_port)" \
    -u "$(lm_mysql_write_user)" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    < "$sql_file"
}

lm_mysql_query_read() {
  MYSQL_PWD="${LM_MYSQL_PASSWORD:?}" mysql --connect-timeout=10 \
    -h "$(lm_mysql_read_host)" -P "$(lm_mysql_read_port)" \
    -u "${LM_MYSQL_USER:?}" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    -N -e "$1" 2>/dev/null
}

lm_mysql_query_write() {
  MYSQL_PWD="$(lm_mysql_write_password)" mysql --connect-timeout=10 \
    -h "$(lm_mysql_write_host)" -P "$(lm_mysql_write_port)" \
    -u "$(lm_mysql_write_user)" "${LM_MYSQL_DATABASE:-ng_loan_market}" \
    -N -e "$1" 2>/dev/null
}
