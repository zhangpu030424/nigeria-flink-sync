#!/usr/bin/env bash
# user_info 脏队列：分片表运维（清空 / 迁移 / 分片号）
# shellcheck shell=bash

USER_INFO_DIRTY_SHARD_COUNT="${USER_INFO_DIRTY_SHARDS:-4}"

user_info_dirty_shard_tables() {
  local s
  for ((s = 0; s < USER_INFO_DIRTY_SHARD_COUNT; s++)); do
    echo "user_info_dirty_${s}"
  done
}

user_info_dirty_shard_for_id() {
  local uid="${1:?user_id}"
  echo $((uid % USER_INFO_DIRTY_SHARD_COUNT))
}

user_info_dirty_shard_table_for_id() {
  echo "user_info_dirty_$(user_info_dirty_shard_for_id "$1")"
}

user_info_dirty_legacy_table_type() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"
  mysql_source_query \
    "SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name='user_info_dirty' LIMIT 1;" \
    2>/dev/null | tr -d '[:space:]'
}

user_info_dirty_ensure_view() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"
  local ttype

  ttype="$(user_info_dirty_legacy_table_type)"
  if [[ "$ttype" == "BASE TABLE" ]]; then
    echo "ERR: user_info_dirty 仍是基表，无法 CREATE VIEW（请先完成分片迁移）" >&2
    return 1
  fi
  if [[ "$ttype" == "VIEW" ]]; then
    mysql_source_query "DROP VIEW user_info_dirty;" || return 1
  fi

  mysql_source_query "
CREATE VIEW user_info_dirty AS
SELECT user_id, updated_at FROM user_info_dirty_0
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_1
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_2
UNION ALL
SELECT user_id, updated_at FROM user_info_dirty_3;" || return 1
  return 0
}

user_info_dirty_drop_legacy_name() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"
  local ttype

  ttype="$(user_info_dirty_legacy_table_type)"
  case "$ttype" in
    "BASE TABLE")
      echo ">> DROP TABLE user_info_dirty（旧单表）"
      mysql_source_query "DROP TABLE user_info_dirty;" || return 1
      ;;
    VIEW)
      echo ">> DROP VIEW user_info_dirty（重建 UNION）"
      mysql_source_query "DROP VIEW user_info_dirty;" || return 1
      ;;
    "")
      return 0
      ;;
    *)
      echo ">> WARN: user_info_dirty 类型=${ttype}，跳过 DROP" >&2
      return 1
      ;;
  esac
}

migrate_user_info_dirty_to_shards() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"
  local legacy_type s moved=0

  for ((s = 0; s < USER_INFO_DIRTY_SHARD_COUNT; s++)); do
    if ! table_exists "user_info_dirty_${s}"; then
      echo "ERR: user_info_dirty_${s} 不存在，请先 deploy user_info_dirty.sql" >&2
      return 1
    fi
  done

  legacy_type="$(user_info_dirty_legacy_table_type)"
  if [[ "$legacy_type" == "BASE TABLE" ]]; then
    echo ">> 迁移旧 user_info_dirty 表 → 分片表（MOD ${USER_INFO_DIRTY_SHARD_COUNT}）"
    for ((s = 0; s < USER_INFO_DIRTY_SHARD_COUNT; s++)); do
      mysql_source_query "
        INSERT INTO user_info_dirty_${s} (user_id, updated_at)
        SELECT user_id, updated_at FROM user_info_dirty WHERE MOD(user_id, ${USER_INFO_DIRTY_SHARD_COUNT}) = ${s}
        ON DUPLICATE KEY UPDATE updated_at = GREATEST(user_info_dirty_${s}.updated_at, VALUES(updated_at));" \
        || return 1
      moved=1
    done
    user_info_dirty_drop_legacy_name || return 1
    echo "  ✓ 已迁移并 DROP 旧表 user_info_dirty"
  elif [[ "$legacy_type" == "VIEW" ]]; then
    echo ">> user_info_dirty 已是视图，跳过分片数据迁移"
  elif [[ -n "$legacy_type" ]]; then
    echo ">> WARN: user_info_dirty 类型=${legacy_type}，请人工检查" >&2
    return 1
  fi

  if ! user_info_dirty_ensure_view; then
    echo "ERR: 创建 user_info_dirty 视图失败" >&2
    return 1
  fi
  if [[ "$moved" -eq 1 ]]; then
    echo "  ✓ 已创建只读视图 user_info_dirty（UNION 分片）"
  elif [[ "$legacy_type" != "VIEW" ]]; then
    echo "  ✓ 已创建只读视图 user_info_dirty（UNION 分片）"
  fi
  return 0
}

truncate_user_info_dirty() {
  # shellcheck source=scripts/lib/mysql-source.sh
  source "$(dirname "${BASH_SOURCE[0]}")/mysql-source.sh"
  local tbl total=0 cnt

  for tbl in $(user_info_dirty_shard_tables); do
    if table_exists "$tbl"; then
      cnt=$(mysql_source_query "SELECT COUNT(*) FROM ${tbl};" 2>/dev/null | tr -d '[:space:]')
      mysql_source_query "TRUNCATE TABLE ${tbl};"
      echo "  ✓ TRUNCATE ${tbl}（清空前 ${cnt:-0} 行）"
      total=$((total + ${cnt:-0}))
    fi
  done

  if [[ "$total" -eq 0 ]] && ! table_exists user_info_dirty_0; then
    if table_exists user_info_dirty && [[ "$(user_info_dirty_legacy_table_type)" == "BASE TABLE" ]]; then
      cnt=$(mysql_source_query "SELECT COUNT(*) FROM user_info_dirty;" 2>/dev/null | tr -d '[:space:]')
      mysql_source_query "TRUNCATE TABLE user_info_dirty;"
      echo "  ✓ TRUNCATE user_info_dirty（旧单表，清空前 ${cnt:-0} 行）"
    else
      echo "  user_info_dirty 分片表不存在，跳过"
    fi
  fi
}
