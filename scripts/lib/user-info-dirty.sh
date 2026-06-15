#!/usr/bin/env bash
# user_info 脏队列：增量启动前清空（全量已覆盖历史，避免 timestamp 重放积压）
# shellcheck shell=bash

truncate_user_info_dirty() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"

  if table_exists user_info_dirty; then
    local cnt
    cnt=$(mysql_source_query "SELECT COUNT(*) FROM user_info_dirty;" 2>/dev/null | tr -d '[:space:]')
    mysql_source_query "TRUNCATE TABLE user_info_dirty;"
    echo "  ✓ TRUNCATE user_info_dirty（清空前 ${cnt:-0} 行）"
  else
    echo "  user_info_dirty 不存在，跳过"
  fi
}
