#!/usr/bin/env bash
# VT 对账（nigeria-flink-sync 新系统）：TINYINT vt_type，与 vt-preload.py 共用配置
# 后台: ./scripts/vt-reconcile.sh all --skip-count --background
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
  echo "需要 mysql 客户端"
  exit 1
fi

chmod +x scripts/vt-reconcile.py

if [[ $# -ge 1 ]]; then
  case "$1" in
    mobile)
      shift
      set -- --vt-type mobile "$@"
      ;;
    bank_account|bank-account|bankcard)
      shift
      set -- --vt-type bank_account "$@"
      ;;
    id_number|id-number|bvn)
      shift
      set -- --vt-type id_number "$@"
      ;;
    gaid_idfa|gaid-idfa|gaid)
      shift
      set -- --vt-type gaid_idfa "$@"
      ;;
    emergency_contact|emergency-contact|emergency)
      shift
      set -- --vt-type emergency_contact "$@"
      ;;
    id2)
      shift
      set -- --vt-type id2 "$@"
      ;;
    all)
      shift
      set -- --vt-type all "$@"
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
  LOG_FILE="${LOG_DIR}/vt-reconcile-$(date +%Y%m%d-%H%M%S).log"
  PID_FILE="${LOG_DIR}/vt-reconcile.pid"

  if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "已有 VT 对账在跑 pid=${old_pid}"
      exit 1
    fi
  fi

  nohup python3 scripts/vt-reconcile.py "${PY_ARGS[@]}" >> "$LOG_FILE" 2>&1 &
  echo "$!" > "$PID_FILE"
  echo "VT 对账已在后台启动 pid=$(cat "$PID_FILE")"
  echo "日志: ${LOG_FILE}"
  echo "跟踪: tail -f ${LOG_FILE}"
  exit 0
fi

exec python3 scripts/vt-reconcile.py "${PY_ARGS[@]}"
