#!/usr/bin/env bash
# 同步前/无数据排查：容器、UDF、VT、Job、目标库条数
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
TM="${FLINK_TASKMANAGER_CONTAINER:-nigeria-flink-taskmanager}"
VT_URL="${VT_BASE_URL:-http://101.47.27.225}"

echo "========== 1. 容器状态 =========="
docker ps --filter "name=nigeria-flink" --format "table {{.Names}}\t{{.Status}}\t{{.Image}}" || true

echo ""
echo "========== 2. UDF jar 是否在镜像内 =========="
for c in "$JM" "$TM"; do
  if docker ps --format '{{.Names}}' | grep -qx "$c"; then
    echo "[$c]"
    docker exec "$c" ls -la /opt/flink/lib/flink-sync-udf.jar 2>&1 || echo "  ✗ 缺少 flink-sync-udf.jar，需 docker compose build --no-cache"
  else
    echo "[$c] 未运行"
  fi
done

echo ""
echo "========== 3. VT /v2t（宿主机） =========="
if curl -sf --connect-timeout 5 --max-time 10 -X POST \
  -H "Content-Type: application/json" \
  -d '["+2348123456789"]' \
  "${VT_URL}/v2t"; then
  echo ""
  echo "  ✓ 宿主机可达 ${VT_URL}/v2t"
else
  echo "  ✗ 宿主机不可达 ${VT_URL}/v2t"
fi

echo ""
echo "========== 4. VT /v2t（TaskManager 容器内，与 UDF 同网络） =========="
if docker ps --format '{{.Names}}' | grep -qx "$TM"; then
  VT_ENV=$(docker exec "$TM" printenv VT_BASE_URL 2>/dev/null || echo "(未设置，UDF 用默认 http://101.47.27.225)")
  echo "  TM 环境变量 VT_BASE_URL=${VT_ENV}"
  if docker exec "$TM" curl -sf --connect-timeout 5 --max-time 15 -X POST \
    -H "Content-Type: application/json" \
    -d '["+2348123456789"]' \
    "${VT_URL}/v2t"; then
    echo ""
    echo "  ✓ TaskManager 容器内可达 VT"
  else
    echo ""
    echo "  ✗ TaskManager 容器内不可达 VT — 这是「无数据」最常见原因"
    echo "    处理：放通安全组/防火墙，或 .env 改 VT_BASE_URL 为容器可达地址"
  fi
else
  echo "  TaskManager 未运行，跳过"
fi

echo ""
echo "========== 5. Flink Job =========="
if docker ps --format '{{.Names}}' | grep -qx "$JM"; then
  docker exec "$JM" ./bin/flink list 2>/dev/null || true
else
  echo "  JobManager 未运行"
fi

echo ""
echo "========== 6. 源库 adjust 视图（Lookup 依赖） =========="
if [[ -f .env ]]; then
  set -a
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%%#*}"
    line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
    export "$line"
  done < .env
  set +a
  if command -v mysql >/dev/null 2>&1; then
    for v in adjust_latest_by_adid v_adjust_latest_by_adid; do
      if [[ "$v" == "adjust_latest_by_adid" ]]; then
        cnt=$(MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
          -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -N -e \
          "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name='adjust_latest_by_adid';" 2>/dev/null || echo "ERR")
      else
        cnt=$(MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
          -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -N -e \
          "SELECT COUNT(*) FROM information_schema.views WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name='${v}';" 2>/dev/null || echo "ERR")
      fi
      if [[ "$cnt" == "1" ]]; then
        echo "  ✓ ${v}"
      else
        echo "  ✗ ${v} 不存在 — 请在源库执行: mysql ... < sql/ddl/source_views_adjust.sql"
      fi
    done
  else
    echo "  本机无 mysql 客户端，请手动检查源库视图"
  fi
else
  echo "  无 .env，跳过"
fi

echo ""
echo "========== 7. 目标库 user 条数 =========="
query_target_count() {
  MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT:-3306}" \
    -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" \
    -e "SELECT COUNT(*) AS user_cnt FROM \`user\`;" 2>&1
}
if [[ -f .env ]]; then
  if command -v mysql >/dev/null 2>&1; then
    query_target_count || echo "  目标库查询失败，检查 .env 与网络"
  else
    echo "  本机无 mysql 客户端，改用 docker 查询..."
    docker run --rm mysql:8.0 mysql \
      -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT:-3306}" \
      -u "${TARGET_MYSQL_USER}" -p"${TARGET_MYSQL_PASSWORD}" "${TARGET_MYSQL_DATABASE}" \
      -e "SELECT COUNT(*) AS user_cnt FROM \`user\`;" 2>&1 || \
      echo "  docker mysql 查询失败，检查 .env、安全组与目标库连通"
  fi
else
  echo "  无 .env，跳过"
fi

echo ""
echo "========== 8. 源库 vt_token_cache / 宽表 =========="
if [[ -f .env ]] && command -v mysql >/dev/null 2>&1; then
  MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
    -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -e \
    "SELECT vt_type, status, COUNT(*) AS cnt FROM vt_token_cache GROUP BY vt_type, status ORDER BY 1, 2;" 2>/dev/null || \
    echo "  vt_token_cache 查询失败"
  missing=$(MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
    -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -N -e \
    "SELECT COUNT(*) FROM user_sync_staging WHERE mobile_norm IS NOT NULL AND (mobile_token IS NULL OR mobile_token='');" 2>/dev/null || echo "ERR")
  echo "  missing_token_cnt=${missing}（应为 0）"
else
  echo "  跳过（无 .env 或 mysql 客户端）"
fi

echo ""
echo "========== 9. TaskManager 最近日志（JDBC / Job 状态） =========="
if docker ps --format '{{.Names}}' | grep -qx "$TM"; then
  docker logs "$TM" 2>&1 | tail -80 | grep -iE 'FAILED|ERROR|Exception|SQLException|UnsupportedOperation' | tail -15 || \
    echo "  最近 80 行无 ERROR（若全是 CANCELED → Job 已被取消，需重新提交）"
fi

echo ""
echo "完成。"
echo "  • Job 状态 CANCELED → ./scripts/sync-job-auto.sh user --bulk-only"
echo "  • missing_token_cnt>0 → ./scripts/rebuild-all-staging.sh"
