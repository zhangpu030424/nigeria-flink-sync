#!/usr/bin/env bash
# 独立 VT 预加载包入口（解压后在同一目录执行: ./run.sh）
# 后台: ./run.sh --background --skip-count
# 单类型: ./run.sh bank_account | ./run.sh mobile | ./run.sh id_number
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

if [[ ! -f .env ]]; then
  if [[ -f .env.example ]]; then
    echo "请先: cp .env.example .env 并填写 SOURCE_MYSQL_* / VT_BASE_URL"
  else
    echo "缺少 .env（需 SOURCE_MYSQL_*、VT_BASE_URL 等）"
  fi
  exit 1
fi

PYTHON=python3
if [[ -x "${ROOT}/py/bin/python3" ]]; then
  PYTHON="${ROOT}/py/bin/python3"
elif [[ -x "${ROOT}/py/bin/python" ]]; then
  PYTHON="${ROOT}/py/bin/python"
fi

if [[ -d "${ROOT}/bin" ]]; then
  export PATH="${ROOT}/bin:${PATH}"
fi

chmod +x "${ROOT}/vt-preload.py"

# 快捷子命令（其余参数透传给 vt-preload.py）
if [[ $# -ge 1 ]]; then
  case "$1" in
    fix-mobile-only)
      shift
      set -- --fix-mobile-only "$@"
      ;;
    fix-mobile)
      shift
      set -- --fix-mobile-only "$@"
      ;;
    mobile)
      shift
      set -- --vt-type mobile --skip-count "$@"
      ;;
    mobile-only)
      shift
      set -- --vt-type mobile --no-fix-mobile-raw --skip-count "$@"
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
  LOG_DIR="${ROOT}/logs"
  mkdir -p "$LOG_DIR"
  LOG_FILE="${LOG_DIR}/vt-preload-$(date +%Y%m%d-%H%M%S).log"
  PID_FILE="${LOG_DIR}/vt-preload.pid"

  if [[ -f "$PID_FILE" ]]; then
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old_pid" ]] && kill -0 "$old_pid" 2>/dev/null; then
      echo "已有 VT 预加载在跑 pid=${old_pid}"
      echo "停止: kill ${old_pid}"
      exit 1
    fi
  fi

  nohup "$PYTHON" "${ROOT}/vt-preload.py" "${PY_ARGS[@]}" >> "$LOG_FILE" 2>&1 &
  echo "$!" > "$PID_FILE"
  echo "VT 预加载已在后台启动 pid=$(cat "$PID_FILE")"
  echo "日志: ${LOG_FILE}"
  echo "跟踪: tail -f ${LOG_FILE}"
  exit 0
fi

exec "$PYTHON" "${ROOT}/vt-preload.py" "${PY_ARGS[@]}"
