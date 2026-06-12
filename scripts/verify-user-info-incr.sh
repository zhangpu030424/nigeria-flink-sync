#!/usr/bin/env bash
# user_info 增量不写数据时的排查（源库 user_id 为未加偏移 id）
# 用法: bash scripts/verify-user-info-incr.sh [user_id]
set -euo pipefail
cd "$(dirname "$0")/.."

SRC_UID="${1:-211038}"
TGT_UID=$((SRC_UID + 100000000))

[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
# shellcheck source=scripts/lib/load-env.sh
source "$(dirname "$0")/lib/load-env.sh"
set -a
load_env_file .env
set +a

echo "=== user_info 增量排查 user_id=${SRC_UID} → 目标 ${TGT_UID} ==="
echo

if [[ -f logs/bulk-start-ms.env ]]; then
  set -a
  load_env_file logs/bulk-start-ms.env
  set +a
  echo "[1] bulk-start-ms=${BULK_START_MS:-?} (${BULK_START_ISO_NG:-WAT 未知})"
  echo "    timestamp 模式只同步该时刻之后的 binlog"
else
  echo "[1] 无 logs/bulk-start-ms.env（incr-only 可能用提交时刻作为 CDC 起点）"
fi
echo

echo "[2] Flink Job（sink_user_info）"
JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$JM"; then
  docker exec "$JM" ./bin/flink list -r 2>/dev/null | grep -E 'sink_user_info|JobID' || echo "    无 RUNNING 的 sink_user_info Job"
else
  echo "    JobManager 容器未运行，跳过"
fi
echo

mysql_q() {
  MYSQL_PWD="$SOURCE_MYSQL_PASSWORD" mysql -h "$SOURCE_MYSQL_HOST" -P "$SOURCE_MYSQL_PORT" \
    -u "$SOURCE_MYSQL_USER" "$SOURCE_MYSQL_DATABASE" -N -e "$1" 2>/dev/null || echo "ERR"
}

echo "[3] 源库 user_personal_info + user 是否存在（INNER JOIN 必需）"
mysql_q "
SELECT CONCAT('personal_info 行数=', COUNT(*), ', user 存在=', MAX(CASE WHEN u.id IS NOT NULL THEN 1 ELSE 0 END))
FROM user_personal_info p
LEFT JOIN \`user\` u ON u.id = p.user_id
WHERE p.user_id = ${SRC_UID};
"
echo

echo "[4] Lookup 视图是否可读（多源 CDC 触发后均 Lookup 取最新）"
for t in user_info_user_lookup user_personal_latest_lookup user_id_by_bvn_lookup \
         device_uuid_user_lookup session_uuid_user_lookup \
         app_config_lookup vt_token_cache_lookup user_work_latest_lookup \
         user_credit_latest_lookup user_reg_ip_lookup user_emergency_contacts_lookup user_info_install_source_lookup; do
  cnt=$(mysql_q "SELECT COUNT(*) FROM ${t} LIMIT 1" | tr -d '[:space:]')
  echo "    ${t}: ${cnt}"
done
echo "    若 ERR：./scripts/deploy-source-ddl.sh 或 ./scripts/sync-all-auto.sh --incr-only"
echo

echo "[5] BVN / VT token（有 BVN 时必须有 token 才会写 sink，同全量宽表逻辑）"
mysql_q "
SELECT CONCAT(
  'bvn=', COALESCE(MAX(TRIM(p.bvn)), '(空)'),
  ', vt_token=', COALESCE(MAX(vt.token), '(无)'),
  ', full_name=', COALESCE(MAX(TRIM(CONCAT(COALESCE(p.first_name,''), ' ', COALESCE(p.sur_name,'')))), '')
)
FROM user_personal_info p
LEFT JOIN vt_token_cache vt ON vt.vt_type = 'id_number' AND vt.status = 1
  AND vt.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
WHERE p.user_id = ${SRC_UID};
"
echo

echo "[6] 目标库当前行"
MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" -e \
  "SELECT user_id, full_name, LEFT(id_number,8) AS id_num_prefix, updated_at FROM user_info WHERE user_id = ${TGT_UID};" \
  2>/dev/null || echo "    目标库查询失败"
echo

echo "=== 测试写入（须在 Job RUNNING 之后；任一 CDC 源表变更均可触发）==="
echo "-- personal_info:"
echo "UPDATE user_personal_info SET first_name = 'IncrTest' WHERE user_id = ${SRC_UID};"
echo "-- work:"
echo "UPDATE user_work_related SET company_name = 'IncrCo' WHERE user_id = ${SRC_UID};"
echo "-- emergency:"
echo "UPDATE user_emergency_contact SET contact_name = contact_name WHERE user_id = ${SRC_UID};"
echo "-- user（registration_time / install_source）:"
echo "UPDATE \`user\` SET adid = adid WHERE id = ${SRC_UID};"
echo "等待 5~10s 后查目标："
echo "SELECT user_id, full_name, updated_at FROM user_info WHERE user_id = ${TGT_UID};"
