#!/usr/bin/env bash
# 全量监控 SQL：占位符展开 + 从 sync-jobs.conf 按表名查计数 SQL
# 用法: source scripts/lib/sync-monitor-sql.sh

expand_monitor_sql() {
  local sql="${1:-}"
  local offset="${USER_ID_OFFSET:-100000000}"
  sql="${sql//__USER_ID_OFFSET__/$offset}"
  echo "$sql"
}

# 目标 SQL 带迁移切片（offset / ng0 前缀）→ 用「目标≈宽表」绝对对比，避免 UPSERT 与全表基线加法失真
monitor_tgt_uses_absolute() {
  local sql="${1:-}"
  [[ "$sql" == *"user_id > "* ]] && return 0
  [[ "$sql" == *"group_user_id > "* ]] && return 0
  [[ "$sql" == *"id > "* ]] && return 0
  [[ "$sql" == *"LIKE 'ng0%"* ]] && return 0
  [[ "$sql" == *"application_no REGEXP"* ]] && return 0
  return 1
}

resolve_monitor_count_mode() {
  local mode="${SYNC_MONITOR_COUNT_MODE:-auto}"
  local tgt_sql="${1:-}"
  case "$mode" in
    absolute|baseline_delta)
      echo "$mode"
      return 0
      ;;
    auto)
      if monitor_tgt_uses_absolute "$tgt_sql"; then
        echo "absolute"
      elif [[ "${SYNC_TARGET_BASELINE_AUTO:-1}" == "1" ]]; then
        echo "baseline_delta"
      else
        echo "absolute"
      fi
      ;;
    *)
      echo "absolute"
      ;;
  esac
}

# 输出两行：SRC_CNT_SQL、TGT_CNT_SQL（已 expand）；找不到则空
lookup_job_monitor_sql() {
  local table="${1:-}"
  local conf="${2:-config/sync-jobs.conf}"
  [[ -n "$table" && -f "$conf" ]] || return 1
  local row
  # shellcheck source=scripts/lib/sync-jobs.sh
  source "$(dirname "${BASH_SOURCE[0]}")/sync-jobs.sh"
  while IFS= read -r row || [[ -n "$row" ]]; do
    row="${row%%#*}"
    row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$row" ]] && continue
    sync_job_parse_line "$row"
    [[ "$SYNC_JOB_MONITOR_TABLE" == "$table" && "$SYNC_JOB_ENABLED" == "1" ]] || continue
    expand_monitor_sql "$SYNC_JOB_SRC_CNT_SQL"
    expand_monitor_sql "$SYNC_JOB_TGT_CNT_SQL"
    return 0
  done < "$conf"
  return 1
}
