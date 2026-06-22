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

# 按 Job 解析并行度（user_info 默认高于 FLINK_PARALLELISM_INCR）
# 用法: sync_job_parallelism user_info incr
sync_job_parallelism() {
  local job_key="$1"
  local mode="${2:-incr}"
  local incr_default="${FLINK_PARALLELISM_INCR:-4}"
  local bulk_default="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-8}}"

  if [[ "$job_key" == "user_info" ]]; then
    if [[ "$mode" == "bulk" ]]; then
      if [[ -n "${FLINK_PARALLELISM_USER_INFO_BULK:-}" ]]; then
        echo "${FLINK_PARALLELISM_USER_INFO_BULK}"
      elif [[ -n "${FLINK_PARALLELISM_USER_INFO:-}" ]]; then
        echo "${FLINK_PARALLELISM_USER_INFO}"
      else
        echo "$bulk_default"
      fi
      return 0
    fi
    if [[ -n "${FLINK_PARALLELISM_USER_INFO_INCR:-}" ]]; then
      echo "${FLINK_PARALLELISM_USER_INFO_INCR}"
    elif [[ -n "${FLINK_PARALLELISM_USER_INFO:-}" ]]; then
      echo "${FLINK_PARALLELISM_USER_INFO}"
    else
      # 默认 2× 通用增量并行（Lookup + VT UDF 较重）
      echo $(( incr_default * 2 ))
    fi
    return 0
  fi

  if [[ "$mode" == "bulk" ]]; then
    echo "$bulk_default"
  else
    echo "$incr_default"
  fi
}

# 估算多 Job 增量峰值 slot（user_info 按专用并行度计）
sync_jobs_peak_incr_slots() {
  local jobs=("$@")
  local total=0
  local job par
  for job in "${jobs[@]}"; do
    par=$(sync_job_parallelism "$job" incr)
    total=$((total + par))
  done
  echo "$total"
}
