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

echo "[3] user_info_dirty 脏队列（单路 CDC；源表变更须经 TRIGGER 写入）"
dirty_tbl=$(mysql_q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='user_info_dirty'" | tr -d '[:space:]')
trg_cnt=$(mysql_q "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name LIKE 'trg_user_info_dirty_%'" | tr -d '[:space:]')
echo "    表存在=${dirty_tbl:-ERR}, TRIGGER 数=${trg_cnt:-ERR}（期望≥14）"
if [[ "${dirty_tbl:-0}" == "1" ]]; then
  mysql_q "SELECT CONCAT('队列行数=', COUNT(*), ', 测试用户=', MAX(CASE WHEN user_id=${SRC_UID} THEN 1 ELSE 0 END)) FROM user_info_dirty;"
else
  echo "    若缺失：./scripts/deploy-source-ddl.sh（TRIGGER 须 DBA/root）"
fi
echo

echo "[4] 源库 user + personal_info（组装必需；仅以 user 注册也可同步）"
mysql_q "
SELECT CONCAT('personal_info 行数=', COUNT(*), ', user 存在=', MAX(CASE WHEN u.id IS NOT NULL THEN 1 ELSE 0 END))
FROM user_personal_info p
LEFT JOIN \`user\` u ON u.id = p.user_id
WHERE p.user_id = ${SRC_UID};
"
echo

echo "[5] Lookup 视图是否可读（脏队列触发后 Lookup 取最新）"
for t in user_info_incr_bundle_lookup user_info_user_lookup user_personal_latest_lookup user_id_by_bvn_lookup \
         device_uuid_user_lookup session_uuid_user_lookup \
         app_config_lookup vt_token_cache_lookup user_work_latest_lookup \
         user_credit_latest_lookup user_reg_ip_lookup user_emergency_contacts_lookup user_info_install_source_lookup; do
  cnt=$(mysql_q "SELECT COUNT(*) FROM ${t} LIMIT 1" | tr -d '[:space:]')
  echo "    ${t}: ${cnt}"
done
echo "    若 ERR：./scripts/deploy-source-ddl.sh 或 ./scripts/sync-all-auto.sh --incr-only"
echo

echo "[6] BVN / VT token（有 BVN 时必须有 token 才会写 sink，同全量宽表逻辑）"
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

echo "[7] 目标库当前行"
MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" -e \
  "SELECT user_id, full_name, LEFT(id_number,8) AS id_num_prefix, updated_at FROM user_info WHERE user_id = ${TGT_UID};" \
  2>/dev/null || echo "    目标库查询失败"
echo

echo "[8] 脏队列规模（过大时 timestamp 模式需追大量 binlog，见 docs/MULTI_JOB_SYNC.md）"
mysql_q "SELECT CONCAT('dirty 行数=', COUNT(*)) FROM user_info_dirty;"
echo

echo "=== 测试写入（须在 Job RUNNING 且 TRIGGER 已部署）==="
echo "-- 1) 改 personal_info（触发器写入 dirty）："
echo "UPDATE user_personal_info SET first_name = CONCAT('IncrTest', UNIX_TIMESTAMP()) WHERE user_id = ${SRC_UID};"
echo "-- 2) 确认脏队列："
echo "SELECT user_id, updated_at FROM user_info_dirty WHERE user_id = ${SRC_UID};"
echo "-- 3) 若 dirty 有行但目标仍不变，直接捅脏队列（绕过业务表，测 Flink CDC）："
echo "UPDATE user_info_dirty SET updated_at = CURRENT_TIMESTAMP(3) WHERE user_id = ${SRC_UID};"
echo "-- 4) Web UI 看 cdc_user_info_dirty Records Sent、sink_user_info Records Received 是否增加"
echo "等待 10~30s 后查目标："
echo "SELECT user_id, full_name, updated_at FROM user_info WHERE user_id = ${TGT_UID};"
echo ""
echo "仍无数据：./scripts/diagnose-job.sh <job_id>  或 Cancel 后重提："
echo "  ./scripts/sync-job-auto.sh user_info --incr-only --bulk-start-ms \$(grep BULK_START_MS logs/bulk-start-ms.env|cut -d= -f2) --keep-other-jobs"
echo "快速验证可试: CDC_STARTUP_MODE=latest-offset ./scripts/sync-job-auto.sh user_info --incr-only --keep-other-jobs"
