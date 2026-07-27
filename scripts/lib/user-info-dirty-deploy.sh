#!/usr/bin/env bash
# user_info 脏队列：存储过程 + TRIGGER 部署与校验
# shellcheck shell=bash

USER_INFO_DIRTY_REQUIRED_PROCS=(
  sp_user_info_dirty_enqueue
  sp_user_info_dirty_enqueue_bvn
  sp_user_info_dirty_enqueue_adid
  sp_user_info_dirty_enqueue_emergency_mobile
)

_user_info_dirty_proc_count() {
  local name="$1"
  mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema='${SOURCE_MYSQL_DATABASE}' AND routine_name='${name}' AND routine_type='PROCEDURE';" \
    2>/dev/null | tr -d '[:space:]'
}

user_info_dirty_procs_ok() {
  local p
  for p in "${USER_INFO_DIRTY_REQUIRED_PROCS[@]}"; do
    [[ "$(_user_info_dirty_proc_count "$p")" -ge 1 ]] || return 1
  done
  return 0
}

user_info_dirty_trigger_count() {
  mysql_source_query \
    "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema='${SOURCE_MYSQL_DATABASE}' AND trigger_name LIKE 'trg_user_info_dirty_%';" \
    2>/dev/null | tr -d '[:space:]'
}

user_info_dirty_triggers_ok() {
  [[ "$(user_info_dirty_trigger_count)" -ge 14 ]]
}

_mysql_source_file_as() {
  local user="$1" pass="$2" file="$3"
  local saved_user="${SOURCE_MYSQL_USER}" saved_pass="${SOURCE_MYSQL_PASSWORD}"
  SOURCE_MYSQL_USER="$user" SOURCE_MYSQL_PASSWORD="$pass"
  mysql_source_file "$file"
  SOURCE_MYSQL_USER="$saved_user" SOURCE_MYSQL_PASSWORD="$saved_pass"
}

deploy_user_info_dirty_sql() {
  local user="${1:-$SOURCE_MYSQL_USER}" pass="${2:-$SOURCE_MYSQL_PASSWORD}"
  echo ">> user_info_dirty SQL（${user}@${SOURCE_MYSQL_HOST}）"
  _mysql_source_file_as "$user" "$pass" sql/ddl/user_info_dirty_enqueue.sql
  _mysql_source_file_as "$user" "$pass" sql/ddl/user_info_dirty.sql
}

_verify_user_info_dirty_objects() {
  local p failed=0
  echo ">> 校验 debounce 存储过程"
  for p in "${USER_INFO_DIRTY_REQUIRED_PROCS[@]}"; do
    if [[ "$(_user_info_dirty_proc_count "$p")" -ge 1 ]]; then
      echo "  ✓ ${p}"
    else
      echo "  ✗ ${p} 缺失"
      failed=1
    fi
  done
  local trg_cnt
  trg_cnt="$(user_info_dirty_trigger_count)"
  if [[ "${trg_cnt:-0}" -ge 14 ]]; then
    echo "  ✓ user_info_dirty TRIGGER 数量=${trg_cnt}"
  else
    echo "  ✗ user_info_dirty TRIGGER 不足（当前 ${trg_cnt:-0}，期望≥14）"
    failed=1
  fi
  return "$failed"
}

# 自动部署：user_info 增量已改多源 CDC，dirty TRIGGER 非必需。
# DEPLOY_REQUIRE_USER_INFO_DIRTY=1 时才硬失败；默认只告警。
ensure_user_info_dirty_deploy() {
  local require="${DEPLOY_REQUIRE_USER_INFO_DIRTY:-0}"

  if [[ "${DEPLOY_SKIP_USER_INFO_DIRTY:-0}" == "1" ]]; then
    echo ">> 跳过 user_info_dirty（DEPLOY_SKIP_USER_INFO_DIRTY=1；多源 CDC 不依赖 TRIGGER）"
    return 0
  fi

  if user_info_dirty_procs_ok && user_info_dirty_triggers_ok; then
    echo ">> user_info_dirty 存储过程 + TRIGGER 已就绪，跳过"
    _verify_user_info_dirty_objects
    return 0
  fi

  echo ">> user_info_dirty 未就绪，尝试部署存储过程 + TRIGGER（可选；增量 Job 已不 CDC dirty）"
  echo ">> 使用 ${SOURCE_MYSQL_USER} 部署"
  deploy_user_info_dirty_sql || true

  if ! _verify_user_info_dirty_objects; then
    if [[ -n "${SOURCE_MYSQL_ROOT_USER:-}" && -n "${SOURCE_MYSQL_ROOT_PASSWORD:-}" ]]; then
      echo ">> ${SOURCE_MYSQL_USER} 未成功，改用 SOURCE_MYSQL_ROOT_USER=${SOURCE_MYSQL_ROOT_USER}"
      deploy_user_info_dirty_sql "${SOURCE_MYSQL_ROOT_USER}" "${SOURCE_MYSQL_ROOT_PASSWORD}" || true
    fi
  fi

  if _verify_user_info_dirty_objects; then
    return 0
  fi

  echo ""
  echo "WARN: user_info_dirty TRIGGER/存储过程未完整（可忽略：02_sync_user_info_incr 已多源 CDC）。"
  echo "  若仍要部署: sql/ddl/user_info_dirty_enqueue.sql + user_info_dirty.sql"
  echo "  或 DEPLOY_SKIP_USER_INFO_DIRTY=1 彻底跳过；硬要求则设 DEPLOY_REQUIRE_USER_INFO_DIRTY=1"
  if [[ "$require" == "1" ]]; then
    return 1
  fi
  return 0
}
