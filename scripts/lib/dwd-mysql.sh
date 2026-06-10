# shellcheck shell=bash
# ng_migration_dwd：与老库 ng_loan_market 同 MySQL 实例（DDL/写入走可写主库）
# source scripts/lib/dwd-mysql.sh

dwd_mysql_write_host() {
  echo "${DWD_MYSQL_WRITE_HOST:-${LM_MYSQL_WRITE_HOST:-${LM_MYSQL_HOST:?}}}"
}

dwd_mysql_write_port() {
  echo "${DWD_MYSQL_WRITE_PORT:-${LM_MYSQL_WRITE_PORT:-${LM_MYSQL_PORT:-3306}}}"
}

dwd_mysql_write_user() {
  echo "${DWD_MYSQL_WRITE_USER:-${LM_MYSQL_WRITE_USER:-${LM_MYSQL_USER:?}}}"
}

dwd_mysql_write_password() {
  echo "${DWD_MYSQL_WRITE_PASSWORD:-${LM_MYSQL_WRITE_PASSWORD:-${LM_MYSQL_PASSWORD:?}}}"
}

# Flink JDBC 读 DWD（默认可写主库；主从同步后可设 DWD_MYSQL_HOST=从库）
dwd_mysql_read_host() {
  echo "${DWD_MYSQL_HOST:-$(dwd_mysql_write_host)}"
}

dwd_mysql_read_port() {
  echo "${DWD_MYSQL_PORT:-$(dwd_mysql_write_port)}"
}

dwd_mysql_read_user() {
  echo "${DWD_MYSQL_USER:-$(dwd_mysql_write_user)}"
}

dwd_mysql_read_password() {
  echo "${DWD_MYSQL_PASSWORD:-$(dwd_mysql_write_password)}"
}

dwd_mysql_export_env() {
  export DWD_MYSQL_DATABASE="${DWD_MYSQL_DATABASE:-ng_migration_dwd}"
  export DWD_MYSQL_HOST="$(dwd_mysql_read_host)"
  export DWD_MYSQL_PORT="$(dwd_mysql_read_port)"
  export DWD_MYSQL_USER="$(dwd_mysql_read_user)"
  export DWD_MYSQL_PASSWORD="$(dwd_mysql_read_password)"
}

dwd_mysql_exec_write_sql() {
  MYSQL_PWD="$(dwd_mysql_write_password)" mysql --connect-timeout=30 \
    -h "$(dwd_mysql_write_host)" -P "$(dwd_mysql_write_port)" \
    -u "$(dwd_mysql_write_user)" "$@"
}

dwd_mysql_query_write() {
  dwd_mysql_exec_write_sql -N -e "$1" 2>/dev/null
}
