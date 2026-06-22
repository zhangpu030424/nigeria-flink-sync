#!/usr/bin/env bash
# 诊断 Flink Job：各算子吞吐 + 反压，判断「读慢 / Join慢 / 写慢」
#
# 用法:
#   ./scripts/diagnose-job.sh <job_id>
#   ./scripts/diagnose-job.sh <job_id> 10    # 每 10 秒刷新
set -euo pipefail
cd "$(dirname "$0")/.."

JOB_ID="${1:-}"
INTERVAL="${2:-0}"

if [[ -z "$JOB_ID" ]]; then
  echo "用法: $0 <job_id> [刷新间隔秒数，0=只跑一次]"
  echo ""
  echo "当前 Job:"
  docker exec "${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}" ./bin/flink list 2>/dev/null || true
  exit 1
fi

# shellcheck disable=SC1091
[[ -f .env ]] && set -a && source .env && set +a
FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
BASE="http://127.0.0.1:${FLINK_WEB_PORT}"

fetch() {
  curl -sf "$1" 2>/dev/null || echo ""
}

metric_sum() {
  local vid=$1 metric=$2
  local json val sum=0
  json=$(fetch "${BASE}/jobs/${JOB_ID}/vertices/${vid}/subtasks/metrics?get=${metric}")
  [[ -z "$json" || "$json" == "[]" ]] && echo "n/a" && return
  while read -r val; do
    [[ "$val" =~ ^[0-9.]+$ ]] && sum=$(awk "BEGIN {print $sum + $val}")
  done < <(echo "$json" | grep -oE '"value":"[^"]+"' | sed 's/"value":"//;s/"//')
  awk "BEGIN {printf \"%.1f\", $sum}"
}

print_report() {
  local job_json vertices
  job_json=$(fetch "${BASE}/jobs/${JOB_ID}")
  if [[ -z "$job_json" ]]; then
    echo "无法访问 Flink REST API (${BASE})，Job 是否存在？"
    exit 1
  fi

  echo "========================================"
  echo "Job: ${JOB_ID}  时间: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "Web UI: ${BASE}/#/job/${JOB_ID}/overview"
  echo "----------------------------------------"

  vertices=$(echo "$job_json" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin)
    for v in d.get('vertices',[]):
        print(v['id']+'\t'+v.get('name',''))
except Exception as e:
    sys.exit(1)
" 2>/dev/null) || {
    echo "需要 python3 解析 Job 拓扑；或直接在 Web UI → Job → 各算子 Metrics 查看"
    echo "Raw: ${BASE}/jobs/${JOB_ID}"
    exit 1
  }

  printf "%-8s %-12s %-12s %-12s %-12s %s\n" "反压%" "读(条/s)" "写(条/s)" "Busy%" "Idle%" "算子"
  echo "------------------------------------------------------------------------------------------------"

  local max_bp=0 max_bp_name=""
  local sink_out=0 source_out=0

  while IFS=$'\t' read -r vid vname; do
    [[ -z "$vid" ]] && continue
    local in_r out_r busy idle bp
    in_r=$(metric_sum "$vid" "numRecordsInPerSecond")
    out_r=$(metric_sum "$vid" "numRecordsOutPerSecond")
    busy=$(metric_sum "$vid" "busyTimeMsPerSecond")
    idle=$(metric_sum "$vid" "idleTimeMsPerSecond")
    bp=$(metric_sum "$vid" "backPressuredTimeMsPerSecond")

    local bp_pct="n/a"
    if [[ "$bp" != "n/a" ]]; then
      bp_pct=$(awk "BEGIN {printf \"%.0f\", $bp / 10}")   # ms/s → 百分比
      if [[ "$bp_pct" =~ ^[0-9]+$ ]] && (( bp_pct > max_bp )); then
        max_bp=$bp_pct
        max_bp_name="$vname"
      fi
    fi

    [[ "$vname" == *"Source:"* || "$vname" == *"src_user"* ]] && source_out=$out_r
    [[ "$vname" == *"Sink:"* || "$vname" == *"sink_user"* ]] && sink_out=$out_r

    printf "%-8s %-12s %-12s %-12s %-12s %s\n" \
      "${bp_pct}" "${in_r}" "${out_r}" "${busy}" "${idle}" "${vname}"
  done <<< "$vertices"

  echo "----------------------------------------"
  echo "解读:"
  if [[ "$max_bp" -gt 50 ]]; then
    echo "  ● 反压最高 (${max_bp}%) 在: ${max_bp_name}"
    if [[ "$max_bp_name" == *"Sink"* || "$max_bp_name" == *"sink_user"* ]]; then
      echo "  → 瓶颈在【写入目标库】JDBC/MySQL 写入慢"
      echo "    调优: sink.buffer-flush.max-rows↑、parallelism↑、目标库 IOPS/innodb、同机房内网"
    elif [[ "$max_bp_name" == *"LookupJoin"* || "$max_bp_name" == *"Join"* ]]; then
      echo "  → 瓶颈在【维表 Lookup】app_config 查询慢"
      echo "    调优: lookup.cache↑、全量阶段去掉 Join、或 app_config 改广播维表"
    elif [[ "$max_bp_name" == *"Source"* || "$max_bp_name" == *"src_user"* ]]; then
      echo "  → 瓶颈在【CDC 读源库】"
      echo "    调优: SET parallelism.default↑、chunk.size↑、源库读性能/网络"
    else
      echo "  → 看算子名称判断；Web UI 点该算子 → Back Pressure 标签页"
    fi
  else
    echo "  ● 反压不高，整体吞吐受最慢环节限制"
  fi
  echo "  ● 源侧写出≈${source_out} 条/s，Sink 写出≈${sink_out} 条/s（Sink≈实际入库速率）"
  echo ""
  echo "对比实验（判断读/写）:"
  echo "  1) 只跑 01_cdc_smoke + print → 测 CDC 读上限"
  echo "  2) 02 去掉 LookupJoin（app_id 写 0）→ 若变快则 Join 是瓶颈"
  echo "  3) 02 提高 sink.buffer-flush.max-rows → 若变快则 JDBC 写是瓶颈"
  echo "========================================"
}

if [[ "$INTERVAL" -gt 0 ]]; then
  while true; do
    print_report
    sleep "$INTERVAL"
  done
else
  print_report
fi
