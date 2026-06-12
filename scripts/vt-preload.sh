#!/usr/bin/env bash
# VT 字典预加载（调用 vt-preload.py）
# 后台: ./scripts/vt-preload.sh --background --skip-count
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "需要 python3"
  exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
  echo "需要 mysql 客户端；或: docker run --rm -i mysql:8.0 mysql ..."
  exit 1
fi

chmod +x scripts/vt-preload.py

# 快捷子命令: ./scripts/vt-preload.sh bank_account → --vt-type bank_account --skip-count
if [[ $# -ge 1 ]]; then
  case "$1" in
    mobile)
      shift
      set -- --vt-type mobile --skip-count "$@"
      ;;
    bank_account|bank-account|bankcard)
      shift
      set -- --vt-type bank_account --skip-count "$@"
      ;;
    id_number|id-number|bvn)
      shift
      set -- --vt-type id_number --skip-count "$@"
      ;;
    gaid_idfa|gaid-idfa|gaid)
      shift
      set -- --vt-type gaid_idfa --skip-count "$@"
      ;;
    emergency_contact|emergency-contact|emergency)
      shift
      set -- --vt-type emergency_contact --skip-count "$@"
      ;;
    all)
      shift
      set -- --vt-type all --skip-count "$@"
      ;;
  esac
fi

BACKGROUND=0
PY_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --background|-d) BACKGROUND=1 ;;
    *) PY_ARGS+=("$arg") ;;
  esac
done

if [[ "$BACKGROUND" -eq 1 ]]; then
  LOG_DIR="logs"
  mkdir -p "$LOG_DIR"
  LOG_FILE="${LOG_DIR}/vt-preload-$(date +%Y%m%d-%H%M%S).log"
  PID_FILE="${LOG_DIR}/vt-preload.pid"

  if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "已有 VT 预加载在跑 pid=${old_pid}，日志见 logs/vt-preload-*.log"
      echo "停止: kill ${old_pid}"
      exit 1
    fi
  fi

  nohup python3 scripts/vt-preload.py "${PY_ARGS[@]}" >> "$LOG_FILE" 2>&1 &
  echo "$!" > "$PID_FILE"
  echo "VT 预加载已在后台启动 pid=$(cat "$PID_FILE")"
  echo "日志: ${LOG_FILE}"
  echo "跟踪: tail -f ${LOG_FILE}"
  echo "停止: kill \$(cat ${PID_FILE})"
  exit 0
fi

exec python3 scripts/vt-preload.py "${PY_ARGS[@]}"
