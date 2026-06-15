#!/usr/bin/env bash
# 从 config/sync-jobs.conf 解析 ENABLED=1 的 Job 列表
# 用法: source scripts/lib/sync-jobs.sh && sync_jobs_load "user,user_info"

sync_jobs_load() {
  local filter="${1:-}"
  local conf="${SYNC_JOBS_CONF:-config/sync-jobs.conf}"
  SYNC_ENABLED_JOBS=()

  [[ -f "$conf" ]] || { echo "ERR: 缺少 $conf" >&2; return 1; }

  while IFS= read -r row || [[ -n "$row" ]]; do
    row="${row%%#*}"
    row="$(echo "$row" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$row" ]] && continue
    local key="${row%%|*}"
    local enabled="${row##*|}"
    [[ "$enabled" != "1" ]] && continue
    if [[ -n "$filter" ]]; then
      echo ",${filter}," | grep -q ",${key}," || continue
    fi
    SYNC_ENABLED_JOBS+=("$key")
  done < "$conf"

  if [[ ${#SYNC_ENABLED_JOBS[@]} -eq 0 ]]; then
    echo "ERR: 无 ENABLED=1 的 Job（filter=${filter:-all}）" >&2
    return 1
  fi
  return 0
}

sync_jobs_print_plan() {
  local phase="$1"
  echo ">> ${phase} Job 顺序 (${#SYNC_ENABLED_JOBS[@]}): ${SYNC_ENABLED_JOBS[*]}"
}
