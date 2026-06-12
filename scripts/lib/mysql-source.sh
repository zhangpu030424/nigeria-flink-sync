#!/usr/bin/env bash
# 源库 MySQL 执行（宿主机 mysql 或 docker mysql:8.0 兜底）
# 依赖环境变量: SOURCE_MYSQL_HOST/PORT/USER/PASSWORD/DATABASE

mysql_source_cmd() {
  local -a args=("$@")
  if command -v mysql >/dev/null 2>&1; then
    MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
      -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" "${args[@]}"
  else
    docker run --rm -i \
      -e MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" \
      mysql:8.0 mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
      -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" "${args[@]}"
  fi
}

mysql_source_file() {
  local f="$1"
  [[ -f "$f" ]] || { echo "ERR: SQL 文件不存在: $f" >&2; return 1; }
  echo ">> 源库 DDL: $f"
  mysql_source_cmd < "$f"
}

mysql_source_query() {
  mysql_source_cmd -N -e "$1"
}

view_exists() {
  local name="$1"
  local cnt
  cnt=$(mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name='${name}';" \
    2>/dev/null || echo "ERR")
  [[ "$cnt" == "1" ]]
}

table_exists() {
  local name="$1"
  local cnt
  cnt=$(mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name='${name}';" \
    2>/dev/null || echo "ERR")
  [[ "$cnt" == "1" ]]
}
