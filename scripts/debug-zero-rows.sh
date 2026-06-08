#!/usr/bin/env bash
# 目标库一直 0 时的分步排查
set -euo pipefail
cd "$(dirname "$0")/.."

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
TM="${FLINK_TASKMANAGER_CONTAINER:-nigeria-flink-taskmanager}"

echo "========== A. 加载 .env =========="
if [[ ! -f .env ]]; then
  echo "缺少 .env"; exit 1
fi
set -a
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  export "$line"
done < .env
set +a

echo "源: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"
echo "目标: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}"

echo ""
echo "========== B. 源库 user 行数（CDC 应有数据可读） =========="
MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
  -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -e "SELECT COUNT(*) AS src_user_cnt FROM \`user\`;" 2>&1 || \
  echo "✗ 源库连不通或 flink_cdc 无 SELECT 权限"

echo ""
echo "========== C. 目标库 user 行数 =========="
MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql -h "${TARGET_MYSQL_HOST}" -P "${TARGET_MYSQL_PORT:-3306}" \
  -u "${TARGET_MYSQL_USER}" "${TARGET_MYSQL_DATABASE}" -e "SELECT COUNT(*) AS tgt_user_cnt FROM \`user\`;" 2>&1 || \
  echo "✗ 目标库连不通"

echo ""
echo "========== D. Lookup 物化表是否存在 =========="
MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT:-3306}" \
  -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -N -e \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='${SOURCE_MYSQL_DATABASE}' AND table_name IN ('user_adjust_cache');" 2>&1 || true
echo "（无 adjust_latest_by_adid 时 LookupJoin 可能 hang → 先执行 sql/ddl/source_materialize_user_adjust.sql）"

echo ""
echo "========== E. Flink Job 列表 =========="
docker exec "$JM" ./bin/flink list 2>/dev/null || echo "JobManager 不可用"

echo ""
echo "========== F. 当前 Job Metrics（Records Out） =========="
JOB_ID=$(docker exec "$JM" ./bin/flink list 2>/dev/null | grep -oE '[a-f0-9]{32}' | head -1 || true)
if [[ -n "$JOB_ID" ]]; then
  FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
  curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${JOB_ID}" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print('Job state:', d.get('state'))
for v in d.get('vertices',[]):
    print(' -', v.get('name','')[:80])
" 2>/dev/null || echo "REST API 不可用"
  echo ""
  echo "若 Source Records Sent = 0 超过 5 分钟 → CDC 未出数（权限/binlog/网络）"
  echo "若 Source 有数、Sink 0 → JDBC 写入失败，看 TM 日志"
else
  echo "无 RUNNING Job"
fi

echo ""
echo "========== G. TaskManager 最近 ERROR =========="
docker logs "$TM" 2>&1 | tail -80 | grep -iE 'ERROR|Exception|SQLException|CDC|binlog|Lookup|denied|timeout' | tail -20 || \
  echo "（无匹配 ERROR，看全量: docker logs $TM | tail -300）"

echo ""
echo "========== 建议操作 =========="
echo "1. ./scripts/cancel-all-jobs.sh"
echo "2. 最小链路测试: ./scripts/run-sql.sh sql/03_sync_user_minimal.sql"
echo "3. 监控: ./scripts/monitor-sync.sh user 15"
echo "4. 最小链路有数后再跑: ./scripts/run-sql.sh sql/02_sync_user_no_vt.sql"
