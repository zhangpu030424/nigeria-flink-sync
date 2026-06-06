#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先复制配置: cp .env.example .env 并填写数据库连接"
  exit 1
fi

docker compose --env-file .env up -d --build
echo "Flink 已启动，Web UI: http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo localhost):${FLINK_WEB_PORT:-8089}"
