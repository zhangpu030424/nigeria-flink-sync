#!/usr/bin/env bash
# 贷超老库 ng_loan_market（LM_MYSQL_*）
# 依赖: LM_MYSQL_HOST/PORT/USER/PASSWORD/DATABASE

mysql_lm_cmd() {
  local -a args=("$@")
  local connect_timeout="${LM_MYSQL_CONNECT_TIMEOUT:-15}"
  if command -v mysql >/dev/null 2>&1; then
    MYSQL_PWD="${LM_MYSQL_PASSWORD}" mysql --connect-timeout="${connect_timeout}" \
      -h "${LM_MYSQL_HOST}" -P "${LM_MYSQL_PORT:-3306}" \
      -u "${LM_MYSQL_USER}" "${LM_MYSQL_DATABASE}" "${args[@]}"
  else
    docker run --rm -i \
      -e MYSQL_PWD="${LM_MYSQL_PASSWORD}" \
      mysql:8.0 mysql --connect-timeout="${connect_timeout}" \
      -h "${LM_MYSQL_HOST}" -P "${LM_MYSQL_PORT:-3306}" \
      -u "${LM_MYSQL_USER}" "${LM_MYSQL_DATABASE}" "${args[@]}"
  fi
}

mysql_lm_query() {
  mysql_lm_cmd -N -e "$1"
}
