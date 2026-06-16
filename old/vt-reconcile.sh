#!/usr/bin/env bash
# VT 对账（旧系统 ENUM vt_type）— 部署到 /data 与 vt-preload 共用 .env
#
# 推荐（大表）:
#   export VT_RECONCILE_INDEX=idx_reconcile
#   pip install pymysql   # 可选，长连接比 mysql-cli 更快
#   ./vt-reconcile.sh mobile --skip-count --db-shards 4
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if ! command -v python3 >/dev/null 2>&1; then
  echo "需要 python3"
  exit 1
fi

if ! command -v mysql >/dev/null 2>&1; then
  echo "需要 mysql 客户端"
  exit 1
fi

chmod +x "$SCRIPT_DIR/vt-reconcile.py"

if [[ $# -ge 1 ]]; then
  case "$1" in
    mobile) shift; set -- --vt-type mobile "$@" ;;
    bank_account|bank-account|bankcard) shift; set -- --vt-type bank_account "$@" ;;
    id_number|id-number|bvn) shift; set -- --vt-type id_number "$@" ;;
    gaid_idfa|gaid-idfa|gaid) shift; set -- --vt-type gaid_idfa "$@" ;;
    emergency_contact|emergency-contact|emergency) shift; set -- --vt-type emergency_contact "$@" ;;
    all) shift; set -- --vt-type all "$@" ;;
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
  LOG_DIR="$SCRIPT_DIR/logs"
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
  nohup python3 "$SCRIPT_DIR/vt-reconcile.py" "${PY_ARGS[@]}" >> "$LOG_FILE" 2>&1 &
  echo "$!" > "$PID_FILE"
  echo "VT 对账已在后台启动 pid=$(cat "$PID_FILE")"
  echo "日志: ${LOG_FILE}"
  exit 0
fi

exec python3 "$SCRIPT_DIR/vt-reconcile.py" "${PY_ARGS[@]}"
