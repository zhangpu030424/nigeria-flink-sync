#!/usr/bin/env bash
# VT 字典预加载（调用 vt-preload.py）
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
exec python3 scripts/vt-preload.py "$@"
