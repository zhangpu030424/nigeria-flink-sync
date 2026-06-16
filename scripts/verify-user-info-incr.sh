#!/usr/bin/env bash
# user_info 增量验证（源库 user_id 为未加偏移 id）
# 用法:
#   bash scripts/verify-user-info-incr.sh [user_id]           # 静态对账 + 结论
#   bash scripts/verify-user-info-incr.sh [user_id] --e2e     # 捅脏队列并轮询目标（需 Job RUNNING）
set -euo pipefail
cd "$(dirname "$0")/.."

SRC_UID="${1:-211038}"
E2E=false
if [[ "${2:-}" == "--e2e" ]]; then
  E2E=true
elif [[ "${1:-}" == "--e2e" ]]; then
  E2E=true
  SRC_UID="${2:-211038}"
fi

TGT_UID=$((SRC_UID + 100000000))
POLL_SECS="${VERIFY_E2E_POLL_SECS:-120}"
POLL_INTERVAL="${VERIFY_E2E_POLL_INTERVAL:-5}"

[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
# shellcheck source=scripts/lib/load-env.sh
source "$(dirname "$0")/lib/load-env.sh"
set -a
load_env_file .env
set +a

mysql_src() {
  MYSQL_PWD="$SOURCE_MYSQL_PASSWORD" mysql -h "$SOURCE_MYSQL_HOST" -P "$SOURCE_MYSQL_PORT" \
    -u "$SOURCE_MYSQL_USER" "$SOURCE_MYSQL_DATABASE" "$@"
}

mysql_tgt() {
  MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
    -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" "$@"
}

mysql_q() {
  mysql_src -N -e "$1" 2>/dev/null || echo "ERR"
}

echo "=== user_info 增量验证 user_id=${SRC_UID} → 目标 ${TGT_UID} ==="
echo

# ---------- 基础设施 ----------
if [[ -f logs/bulk-start-ms.env ]]; then
  set -a
  load_env_file logs/bulk-start-ms.env
  set +a
  echo "[1] CDC 起点 bulk-start-ms=${BULK_START_MS:-?}（timestamp 模式只消费该时刻之后 binlog）"
else
  echo "[1] 无 bulk-start-ms.env（incr-only 常用提交时刻或 latest-offset）"
fi

JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
job_line=""
if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$JM"; then
  job_line=$(docker exec "$JM" ./bin/flink list -r 2>/dev/null | grep -E 'sink_user_info' || true)
fi
echo "[2] Flink: ${job_line:-无 RUNNING 的 sink_user_info Job}"
echo

dirty_tbl=$(mysql_q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name LIKE 'user_info_dirty_%' AND table_name NOT LIKE '%enqueue%'" | tr -d '[:space:]')
trg_cnt=$(mysql_q "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name LIKE 'trg_user_info_dirty_%'" | tr -d '[:space:]')
dirty_cnt=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty" 2>/dev/null | tr -d '[:space:]')
dirty_cnt=${dirty_cnt:-$(mysql_q "SELECT SUM(c) FROM (SELECT COUNT(*) c FROM user_info_dirty_0 UNION ALL SELECT COUNT(*) FROM user_info_dirty_1 UNION ALL SELECT COUNT(*) FROM user_info_dirty_2 UNION ALL SELECT COUNT(*) FROM user_info_dirty_3) t" | tr -d '[:space:]')}
in_dirty=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty WHERE user_id=${SRC_UID}" 2>/dev/null | tr -d '[:space:]')
in_dirty=${in_dirty:-0}
shard_tbl="user_info_dirty_$((SRC_UID % 4))"
echo "[3] 脏队列: 分片表=${dirty_tbl:-ERR}/4, TRIGGER=${trg_cnt:-ERR}(≥14), 总行=${dirty_cnt:-?}, 本用户=${in_dirty:-?}（shard=${shard_tbl}）"
echo

# ---------- 静态对账：bundle 期望 vs 目标（多列输出，避免 CONCAT 内嵌 TAB 解析失败）----------
bundle_line=$(mysql_q "
SELECT COALESCE(TRIM(b.bvn), ''),
       COALESCE(TRIM(CONCAT(COALESCE(b.first_name,''), ' ', COALESCE(b.sur_name,''))), ''),
       COALESCE(b.vt_token, ''),
       COALESCE(CAST(b.vt_status AS CHAR), '')
FROM user_info_incr_bundle_lookup b WHERE b.user_id = ${SRC_UID} LIMIT 1;
")

if [[ "$bundle_line" == "ERR" || -z "$bundle_line" ]]; then
  echo "[4] bundle_lookup: 无行（user 不存在或视图未部署）"
  BVN_RAW=""
  EXPECT_FN=""
  VT_TOKEN=""
  VT_STATUS=""
  SINK_ELIGIBLE="no"
  SINK_REASON="bundle 无数据"
else
  IFS=$'\t' read -r BVN_RAW EXPECT_FN VT_TOKEN VT_STATUS <<< "$bundle_line"
  echo "[4] bundle_lookup: bvn=${BVN_RAW:-(空)}, full_name=${EXPECT_FN:-(空)}, vt_token=${VT_TOKEN:-(无)}, vt_status=${VT_STATUS:-?}"

  if [[ -z "${BVN_RAW}" ]]; then
    SINK_ELIGIBLE="yes"
    SINK_REASON="无 BVN，sink WHERE 放行"
  elif [[ -n "${VT_TOKEN}" ]]; then
    SINK_ELIGIBLE="yes"
    SINK_REASON="有 BVN 且 vt_token 非空"
  else
    SINK_ELIGIBLE="maybe"
    SINK_REASON="有 BVN 无 cache token，依赖 Flink vt_tokenize UDF（离线无法预判）"
  fi
fi
echo "    sink 可写预判: ${SINK_ELIGIBLE} — ${SINK_REASON}"
echo

staging_chk=$(mysql_q "
SELECT CASE
  WHEN s.user_id IS NULL THEN 'no_staging'
  WHEN TRIM(COALESCE(s.full_name,'')) = TRIM(CONCAT(COALESCE(b.first_name,''), ' ', COALESCE(b.sur_name,''))) THEN 'match'
  ELSE CONCAT('mismatch staging=', COALESCE(s.full_name,''), ' | bundle=',
              TRIM(CONCAT(COALESCE(b.first_name,''), ' ', COALESCE(b.sur_name,''))))
END
FROM user_info_incr_bundle_lookup b
LEFT JOIN user_info_sync_staging s ON s.user_id = b.user_id
WHERE b.user_id = ${SRC_UID} LIMIT 1;
" 2>/dev/null || echo "no_staging_table")
echo "[5] 全量宽表对照: ${staging_chk:-跳过}（match 说明 Lookup 与全量组装一致）"
echo

tgt_line=$(mysql_tgt -N -e \
  "SELECT COALESCE(full_name,''), COALESCE(updated_at,''), COALESCE(LEFT(id_number,12),'') FROM user_info WHERE user_id=${TGT_UID} LIMIT 1;" \
  2>/dev/null || echo "ERR")
if [[ "$tgt_line" == "ERR" ]]; then
  TGT_FN=""
  TGT_UPD=""
  TGT_ID_PREFIX=""
  echo "[6] 目标库: 无行或查询失败"
else
  IFS=$'\t' read -r TGT_FN TGT_UPD TGT_ID_PREFIX <<< "$tgt_line"
  echo "[6] 目标库: full_name=${TGT_FN:-(空)}, updated_at=${TGT_UPD:-?}, id_number前缀=${TGT_ID_PREFIX:-(空)}"
fi

if [[ -n "$EXPECT_FN" && -n "$TGT_FN" ]]; then
  if [[ "$(echo "$EXPECT_FN" | xargs)" == "$(echo "$TGT_FN" | xargs)" ]]; then
    echo "    数据对账: PASS（目标 full_name 与 bundle 一致）"
    DATA_MATCH=pass
  else
    echo "    数据对账: STALE（目标与 bundle 不一致 — 可能未消费到该用户或 sink 被过滤）"
    DATA_MATCH=stale
  fi
elif [[ -z "$TGT_FN" && "$SINK_ELIGIBLE" == "yes" ]]; then
  echo "    数据对账: MISSING（应可写但目标无行 — Job 未跑到或 CDC 起点在 dirty.updated_at 之后）"
  DATA_MATCH=missing
else
  DATA_MATCH=unknown
fi
echo

# ---------- 结论 ----------
echo "=== 验证结论 ==="
infra_blockers=()
realtime_blockers=()
[[ "${dirty_tbl:-0}" -lt 4 ]] && infra_blockers+=("user_info_dirty 分片表不足 4 张")
[[ "${trg_cnt:-0}" -lt 14 ]] && infra_blockers+=("TRIGGER 不足")
[[ "$SINK_ELIGIBLE" == "no" ]] && infra_blockers+=("bundle 无数据")
[[ -z "$job_line" ]] && realtime_blockers+=("无 RUNNING Job（无法保证后续实时同步）")
[[ "${dirty_cnt:-0}" -gt 500 ]] && realtime_blockers+=("脏队列积压 ${dirty_cnt} 行，单并行度消化慢")

if [[ ${#infra_blockers[@]} -gt 0 ]]; then
  echo "基础设施阻塞:"
  for b in "${infra_blockers[@]}"; do echo "  - $b"; done
else
  echo "基础设施: OK（脏表 + TRIGGER + bundle）"
fi

if [[ ${#realtime_blockers[@]} -gt 0 ]]; then
  echo "实时同步注意:"
  for b in "${realtime_blockers[@]}"; do echo "  - $b"; done
fi

if [[ "$DATA_MATCH" == "pass" ]]; then
  echo "静态正确性: PASS — 目标 full_name 与 bundle 一致（该用户历史同步结果可信）"
elif [[ "$DATA_MATCH" == "stale" ]]; then
  echo "静态正确性: STALE — 目标与 bundle 不一致，需 Job 消费或查 sink 过滤"
elif [[ "$DATA_MATCH" == "missing" ]]; then
  echo "静态正确性: MISSING — 应可写但目标无行"
elif [[ "$staging_chk" == "match" && -n "$TGT_FN" ]]; then
  echo "静态正确性: PASS — staging 与 bundle match，且目标有行（full_name 请人工核对大小写）"
else
  echo "静态正确性: 待确认 — 先 git pull 重跑本脚本（已修复 bundle 解析）"
fi
echo

# ---------- 可选 E2E ----------
if [[ "$E2E" == true ]]; then
  echo "=== E2E 探测（UPDATE dirty → 轮询目标 ${POLL_SECS}s）==="
  if [[ -z "$job_line" ]]; then
    echo "FAIL: 无 RUNNING Job，跳过 E2E"
    exit 1
  fi
  if [[ "${in_dirty:-0}" == "0" ]]; then
    echo "WARN: 用户不在 dirty，先 INSERT 到分片 ${shard_tbl}..."
    mysql_src -e "INSERT INTO ${shard_tbl} (user_id, updated_at) VALUES (${SRC_UID}, CURRENT_TIMESTAMP(3));" || true
  else
    mysql_src -e "UPDATE ${shard_tbl} SET updated_at = CURRENT_TIMESTAMP(3) WHERE user_id = ${SRC_UID};"
  fi
  dirty_ts=$(mysql_q "SELECT updated_at FROM ${shard_tbl} WHERE user_id=${SRC_UID} LIMIT 1")
  echo "脏队列 updated_at=${dirty_ts}"
  before_upd="$TGT_UPD"
  before_fn="$TGT_FN"
  elapsed=0
  e2e_pass=false
  while [[ $elapsed -lt $POLL_SECS ]]; do
    sleep "$POLL_INTERVAL"
    elapsed=$((elapsed + POLL_INTERVAL))
    cur=$(mysql_tgt -N -e \
      "SELECT COALESCE(full_name,''), COALESCE(updated_at,'') FROM user_info WHERE user_id=${TGT_UID} LIMIT 1;" 2>/dev/null || echo "")
    if [[ -n "$cur" ]]; then
      IFS=$'\t' read -r cur_fn cur_upd <<< "$cur"
      if [[ "$cur_fn" != "$before_fn" || "$cur_upd" != "$before_upd" ]]; then
        echo "PASS (${elapsed}s): 目标已变化 full_name=${cur_fn}, updated_at=${cur_upd}"
        if [[ -n "$EXPECT_FN" && "$(echo "$EXPECT_FN" | xargs)" == "$(echo "$cur_fn" | xargs)" ]]; then
          echo "PASS: 与 bundle 期望 full_name 一致"
        else
          echo "WARN: 与 bundle 期望不一致（期望=${EXPECT_FN}）— 查 vt_token / sink WHERE"
        fi
        e2e_pass=true
        break
      fi
    fi
    echo "  ... ${elapsed}s 目标未变"
  done
  if [[ "$e2e_pass" != true ]]; then
    echo "FAIL: ${POLL_SECS}s 内目标未更新"
    echo "  → Web UI 看 Records Sent 是否增加"
    echo "  → 脏队列 ${dirty_cnt} 行；分片 CDC 后 Web UI 应见 4 路 Source 或 parallelism=${FLINK_PARALLELISM_USER_INFO:-4}"
    exit 2
  fi
  exit 0
fi

echo "分片验证:   ./scripts/verify-user-info-dirty-shards.sh [--probe]"
echo "端到端探测: bash scripts/verify-user-info-incr.sh ${SRC_UID} --e2e"
echo "详细 SQL: mysql ... --init-command=\"SET @uid=${SRC_UID}\" < sql/verify/user_info_incr_expected.sql"
echo "批量抽样: mysql ... < sql/verify/user_info_incr_sample_check.sql"
