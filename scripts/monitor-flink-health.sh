#!/usr/bin/env bash
# Flink 增量健康监控：Job 失败/重启、checkpoint 大量失败、目标库连不上、长时间无吞吐 → 通知
#
# 用法:
#   ./scripts/monitor-flink-health.sh              # 循环监控（默认 60s）
#   ./scripts/monitor-flink-health.sh --once        # 只扫一轮
#   ./scripts/monitor-flink-health.sh --interval 30
#
# 通知（任选）:
#   ALERT_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxx
#   ALERT_WEBHOOK_TYPE=feishu   # feishu|slack|generic（默认 feishu）
#   未配置 webhook 时只写 logs/flink-health.log
#
# 阈值（可用环境变量覆盖）:
#   HEALTH_INTERVAL_SEC=60
#   HEALTH_CP_FAIL_WINDOW=5          # 最近 N 次 checkpoint 里失败数达到阈值则告警
#   HEALTH_CP_FAIL_THRESHOLD=3
#   HEALTH_IDLE_MINUTES=30           # sink 读入条数连续 N 分钟无增长则告警（可排除表）
#   HEALTH_IDLE_EXCLUDE=             # 逗号分隔 job 短名；id_mapping 已多源 CDC，默认不再排除
#   HEALTH_EXPECTED_SINKS=sink_user,sink_user_info,sink_user_bankcard,sink_user_product,sink_application,sink_loan,sink_id_mapping
#   HEALTH_ALERT_COOLDOWN_SEC=1800   # 同类告警冷却，避免刷屏
#
set -euo pipefail
cd "$(dirname "$0")/.."

ONCE=0
INTERVAL="${HEALTH_INTERVAL_SEC:-60}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --once) ONCE=1 ;;
    --interval=*) INTERVAL="${1#--interval=}" ;;
    --interval)
      shift
      INTERVAL="${1:-60}"
      ;;
    -h|--help)
      sed -n '2,28p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
  shift
done

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
FLINK_BASE="http://127.0.0.1:${FLINK_WEB_PORT}"
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/flink-health.log"
STATE_DIR="${LOG_DIR}/flink-health-state"
mkdir -p "$LOG_DIR" "$STATE_DIR"

CP_FAIL_WINDOW="${HEALTH_CP_FAIL_WINDOW:-5}"
CP_FAIL_THRESHOLD="${HEALTH_CP_FAIL_THRESHOLD:-3}"
IDLE_MINUTES="${HEALTH_IDLE_MINUTES:-30}"
IDLE_EXCLUDE="${HEALTH_IDLE_EXCLUDE:-}"
EXPECTED_SINKS="${HEALTH_EXPECTED_SINKS:-sink_user,sink_user_info,sink_user_bankcard,sink_user_product,sink_application,sink_loan,sink_id_mapping}"
ALERT_COOLDOWN_SEC="${HEALTH_ALERT_COOLDOWN_SEC:-1800}"
ALERT_WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_WEBHOOK_TYPE="${ALERT_WEBHOOK_TYPE:-feishu}"

log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

job_short() {
  local name="$1"
  echo "${name##*.}"
}

in_csv() {
  local needle="$1"
  local csv="$2"
  local IFS=,
  local x
  # shellcheck disable=SC2086
  for x in $csv; do
    [[ "$x" == "$needle" ]] && return 0
  done
  return 1
}

cooldown_ok() {
  local key="$1"
  local f="${STATE_DIR}/alert_${key//\//_}.ts"
  local now
  now=$(date +%s)
  if [[ -f "$f" ]]; then
    local last
    last=$(cat "$f" 2>/dev/null || echo 0)
    if (( now - last < ALERT_COOLDOWN_SEC )); then
      return 1
    fi
  fi
  echo "$now" >"$f"
  return 0
}

send_alert() {
  local title="$1"
  local body="$2"
  local key="${3:-generic}"
  if ! cooldown_ok "$key"; then
    log "ALERT(冷却跳过) [${key}] ${title}"
    return 0
  fi
  log "ALERT [${key}] ${title}"
  log "  ${body}"
  [[ -z "$ALERT_WEBHOOK_URL" ]] && return 0

  local payload
  case "$ALERT_WEBHOOK_TYPE" in
    slack)
      payload=$(python3 -c "import json,sys; print(json.dumps({'text': sys.argv[1]+'\\n'+sys.argv[2]}))" "$title" "$body")
      ;;
    generic)
      payload=$(python3 -c "import json,sys; print(json.dumps({'title':sys.argv[1],'text':sys.argv[2]}))" "$title" "$body")
      ;;
    *)
      # 飞书自定义机器人
      payload=$(python3 -c "import json,sys; print(json.dumps({'msg_type':'text','content':{'text': sys.argv[1]+'\\n'+sys.argv[2]}}))" "$title" "$body")
      ;;
  esac
  curl -sf -X POST -H 'Content-Type: application/json' \
    -d "$payload" "$ALERT_WEBHOOK_URL" >/dev/null 2>&1 \
    || log "WARN: webhook 发送失败（检查 ALERT_WEBHOOK_URL）"
}

mysql_ping() {
  local label="$1" host="$2" port="$3" user="$4" pass="$5" db="$6"
  local err
  if err=$(MYSQL_PWD="$pass" mysql -h "$host" -P "$port" -u "$user" "$db" -N -e 'SELECT 1' 2>&1); then
    echo OK
    return 0
  fi
  echo "$err"
  return 1
}

check_mysql() {
  local tgt src
  if ! tgt=$(mysql_ping target \
    "${TARGET_MYSQL_HOST}" "${TARGET_MYSQL_PORT:-3306}" \
    "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_PASSWORD}" "${TARGET_MYSQL_DATABASE}"); then
    send_alert "目标库 JDBC 连不上" \
      "host=${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT:-3306} db=${TARGET_MYSQL_DATABASE} err=${tgt}" \
      "target_mysql"
  fi
  if ! src=$(mysql_ping source \
    "${SOURCE_MYSQL_HOST}" "${SOURCE_MYSQL_PORT:-3306}" \
    "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_PASSWORD}" "${SOURCE_MYSQL_DATABASE}"); then
    send_alert "源库连不上（CDC/Lookup 会断）" \
      "host=${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT:-3306} db=${SOURCE_MYSQL_DATABASE} err=${src}" \
      "source_mysql"
  fi
}

# 返回: jid|state|short|sink_r|src_w|cp_completed|cp_failed|cp_in_progress|exceptions
fetch_jobs_json() {
  python3 - <<'PY'
import json, urllib.request, sys

base = "http://127.0.0.1:%s" % (__import__("os").environ.get("FLINK_WEB_PORT", "8089"))
try:
    ov = json.load(urllib.request.urlopen(base + "/jobs/overview", timeout=15))
except Exception as e:
    print("ERR|" + str(e), file=sys.stderr)
    sys.exit(2)

rows = []
for j in ov.get("jobs", []):
    if j.get("state") not in ("RUNNING", "RESTARTING", "FAILED", "FAILING"):
        continue
    jid = j["jid"]
    name = j.get("name") or ""
    short = name.rsplit(".", 1)[-1] if "." in name else name
    state = j["state"]
    sink_r = 0
    src_w = 0
    cp_ok = cp_fail = cp_in = 0
    exc = ""
    try:
        d = json.load(urllib.request.urlopen(base + "/jobs/" + jid, timeout=20))
        for v in d.get("vertices", []):
            m = v.get("metrics") or {}
            n = v.get("name") or ""
            wr = int(m.get("write-records") or 0)
            rr = int(m.get("read-records") or 0)
            if "Sink" in n:
                sink_r = max(sink_r, rr)
            if n.startswith("Source"):
                src_w += wr
    except Exception as e:
        exc = "metrics:" + str(e)
    try:
        cp = json.load(urllib.request.urlopen(base + "/jobs/" + jid + "/checkpoints", timeout=20))
        counts = cp.get("counts") or {}
        cp_ok = int(counts.get("completed") or 0)
        cp_fail = int(counts.get("failed") or 0)
        cp_in = int(counts.get("in_progress") or 0)
        hist = cp.get("history") or []
        recent = hist[: int(__import__("os").environ.get("HEALTH_CP_FAIL_WINDOW", "5"))]
        recent_fail = sum(1 for h in recent if (h.get("status") or "").upper() == "FAILED")
    except Exception:
        recent_fail = 0
    try:
        ex = json.load(urllib.request.urlopen(base + "/jobs/" + jid + "/exceptions", timeout=15))
        root = (ex.get("root-exception") or "")[:400]
        if root:
            exc = (exc + ";" if exc else "") + root.replace("\n", " ")[:400]
    except Exception:
        pass
    print("|".join([
        jid, state, short, str(sink_r), str(src_w),
        str(cp_ok), str(cp_fail), str(cp_in), str(recent_fail), exc.replace("|", "/")
    ]))
PY
}

check_jobs() {
  export FLINK_WEB_PORT HEALTH_CP_FAIL_WINDOW="$CP_FAIL_WINDOW"
  local line now_ts
  now_ts=$(date +%s)
  local seen=""
  local out
  if ! out=$(fetch_jobs_json 2>"${STATE_DIR}/fetch.err"); then
    send_alert "Flink UI 不可达" \
      "url=${FLINK_BASE} err=$(tr '\n' ' ' <"${STATE_DIR}/fetch.err" | head -c 300)" \
      "flink_ui"
    return 0
  fi

  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    IFS='|' read -r jid state short sink_r src_w cp_ok cp_fail cp_in recent_fail exc <<<"$line"
    seen="${seen},${short}"

    log "job=${short} state=${state} sink_r=${sink_r} src_w=${src_w} cp_ok=${cp_ok} cp_fail=${cp_fail} recent_fail=${recent_fail}"

    if [[ "$state" == "FAILED" || "$state" == "FAILING" ]]; then
      send_alert "Job FAILED: ${short}" \
        "jid=${jid} exc=${exc:-none}" \
        "failed_${short}"
    fi
    if [[ "$state" == "RESTARTING" ]]; then
      send_alert "Job RESTARTING: ${short}" \
        "jid=${jid} 可能目标库断连/反压/序列化错误 exc=${exc:-none}" \
        "restart_${short}"
    fi

    if [[ "${recent_fail:-0}" =~ ^[0-9]+$ ]] && (( recent_fail >= CP_FAIL_THRESHOLD )); then
      send_alert "Checkpoint 大量失败: ${short}" \
        "jid=${jid} 最近${CP_FAIL_WINDOW}次中失败=${recent_fail} 累计失败=${cp_fail} （常见：JDBC Sink 连不上 / 链路断开）" \
        "cpfail_${short}"
    fi

    # 异常文本含 Communications link failure / Connection refused
    if [[ -n "$exc" ]] && echo "$exc" | grep -qiE 'Communications link failure|Connection refused|Connection timed out|Too many connections|Access denied'; then
      send_alert "Sink/JDBC 链路异常: ${short}" \
        "jid=${jid} ${exc}" \
        "jdbc_${short}"
    fi

    # 空闲检测：对比上次 sink_r
    if in_csv "$short" "$IDLE_EXCLUDE"; then
      continue
    fi
    local stf="${STATE_DIR}/sink_${short}.tsv"
    if [[ -f "$stf" ]]; then
      local prev_r prev_ts
      prev_r=$(awk 'NR==1{print $1}' "$stf")
      prev_ts=$(awk 'NR==1{print $2}' "$stf")
      if [[ "$prev_r" =~ ^[0-9]+$ && "$sink_r" =~ ^[0-9]+$ && "$prev_ts" =~ ^[0-9]+$ ]]; then
        if (( sink_r == prev_r )); then
          local idle_sec=$((now_ts - prev_ts))
          if (( idle_sec >= IDLE_MINUTES * 60 )); then
            send_alert "长时间无数据进 Sink: ${short}" \
              "jid=${jid} sink_r=${sink_r} 已空闲约 $((idle_sec / 60)) 分钟（阈值 ${IDLE_MINUTES}m）。若源库确有变更，请查 Lookup/CDC/反压。" \
              "idle_${short}"
          fi
        else
          echo "${sink_r} ${now_ts}" >"$stf"
        fi
      else
        echo "${sink_r} ${now_ts}" >"$stf"
      fi
    else
      echo "${sink_r} ${now_ts}" >"$stf"
    fi
  done <<<"$out"

  # 期望 Job 是否缺失
  local IFS=,
  local expect
  for expect in $EXPECTED_SINKS; do
    if ! echo ",${seen}," | grep -q ",${expect},"; then
      # 只对仍在 EXPECTED 且当前没有 RUNNING 的告警
      send_alert "期望的增量 Job 未运行: ${expect}" \
        "当前 RUNNING/RESTARTING/FAILED 列表中找不到 ${expect}。若刚 cancel 可忽略。" \
        "missing_${expect}"
    fi
  done
}

log "========================================"
log "Flink 健康监控启动 interval=${INTERVAL}s idle=${IDLE_MINUTES}m cp_fail_threshold=${CP_FAIL_THRESHOLD}/${CP_FAIL_WINDOW}"
log "webhook=${ALERT_WEBHOOK_URL:-未配置（仅写日志）} exclude_idle=${IDLE_EXCLUDE}"
log "日志: ${LOG_FILE}"

while true; do
  check_mysql
  check_jobs
  [[ "$ONCE" -eq 1 ]] && break
  sleep "$INTERVAL"
done
