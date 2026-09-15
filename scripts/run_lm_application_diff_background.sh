#!/usr/bin/env bash
# 后台跑 LM vs 目标 application_no 全量 diff（SSH 断开也不停）
#
# Usage:
#   cd /opt/nigeria-flink-sync
#   ./scripts/run_lm_application_diff_background.sh
#   ./scripts/run_lm_application_diff_background.sh status
#   ./scripts/run_lm_application_diff_background.sh tail
#   ./scripts/run_lm_application_diff_background.sh stop
#
# 环境变量:
#   WORK_DIR=/tmp/lm_application_diff
#   ENV_FILE=./.env
#   PHASE=all          # 或 export / compare
#   SKIP_EXISTING=1    # 已有完整 all.keys 则跳过导出

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK_DIR="${WORK_DIR:-/tmp/lm_application_diff}"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
PHASE="${PHASE:-all}"
LOG="$WORK_DIR/run.log"
PID_FILE="$WORK_DIR/pid.txt"
SKIP_FLAG=()
if [[ "${SKIP_EXISTING:-1}" == "1" ]]; then
  SKIP_FLAG=(--skip-existing)
fi

mkdir -p "$WORK_DIR"

cmd_status() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "RUNNING pid=$(cat "$PID_FILE") log=$LOG"
  else
    echo "NOT running (no pid or process dead) log=$LOG"
  fi
  ls -lh "$WORK_DIR/lm/all.keys" "$WORK_DIR/target/all.keys" 2>/dev/null || true
}

cmd_tail() {
  tail -f "$LOG"
}

cmd_stop() {
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" 2>/dev/null && echo "sent TERM to $(cat "$PID_FILE")" || echo "already stopped"
  else
    echo "no pid file"
  fi
}

cmd_start() {
  if [[ -f "$PID_FILE" ]] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "already running pid=$(cat "$PID_FILE"), log=$LOG"
    exit 0
  fi
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "ERR: missing ENV_FILE=$ENV_FILE" >&2
    exit 1
  fi
  echo "start phase=$PHASE work_dir=$WORK_DIR log=$LOG"
  nohup python3 -u "$ROOT/scripts/diff_lm_application_keys.py" \
    --env "$ENV_FILE" \
    --phase "$PHASE" \
    --work-dir "$WORK_DIR" \
    --progress-every 500000 \
    "${SKIP_FLAG[@]}" \
    >> "$LOG" 2>&1 &
  echo $! > "$PID_FILE"
  echo "started pid=$(cat "$PID_FILE")"
  echo "  tail -f $LOG"
}

case "${1:-start}" in
  start) cmd_start ;;
  status) cmd_status ;;
  tail) cmd_tail ;;
  stop) cmd_stop ;;
  *)
    echo "usage: $0 [start|status|tail|stop]" >&2
    exit 1
    ;;
esac
