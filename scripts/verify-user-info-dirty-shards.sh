#!/usr/bin/env bash
# user_info 脏队列分片 + 多路 CDC 验证
#
# 用法:
#   ./scripts/verify-user-info-dirty-shards.sh              # 基础设施 + 数据完整性 + Flink
#   ./scripts/verify-user-info-dirty-shards.sh --probe      # 额外测试 sp_user_info_dirty_enqueue 路由
#   ./scripts/verify-user-info-dirty-shards.sh --sql-only   # 只跑 SQL 检查
#   ./scripts/verify-user-info-dirty-shards.sh --no-flink   # 跳过 Flink Job 检查
#
# 退出码: 0=全通过  1=基础设施失败  2=数据完整性失败  3=Flink/并行度失败
set -euo pipefail
cd "$(dirname "$0")/.."

PROBE=0
SQL_ONLY=0
CHECK_FLINK=1
SHARDS="${USER_INFO_DIRTY_SHARDS:-4}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --probe) PROBE=1; shift ;;
    --sql-only) SQL_ONLY=1; shift ;;
    --no-flink) CHECK_FLINK=0; shift ;;
    -h|--help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "未知参数: $1（--help 查看用法）"; exit 1 ;;
  esac
done

[[ -f .env ]] || { echo "缺少 .env"; exit 1; }
# shellcheck source=scripts/lib/load-env.sh
source scripts/lib/load-env.sh
set -a
load_env_file .env
set +a

# shellcheck source=scripts/lib/user-info-dirty.sh
source scripts/lib/user-info-dirty.sh

mysql_src() {
  MYSQL_PWD="$SOURCE_MYSQL_PASSWORD" mysql -h "$SOURCE_MYSQL_HOST" -P "$SOURCE_MYSQL_PORT" \
    -u "$SOURCE_MYSQL_USER" "$SOURCE_MYSQL_DATABASE" "$@"
}

mysql_q() {
  mysql_src -N -e "$1" 2>/dev/null || echo "ERR"
}

pass=0
fail_infra=0
fail_data=0
fail_flink=0

ok() { echo "  PASS: $*"; pass=$((pass + 1)); }
bad() { echo "  FAIL: $*"; }
bad_infra() { bad "$*"; fail_infra=1; }
bad_data() { bad "$*"; fail_data=1; }
bad_flink() { bad "$*"; fail_flink=1; }

echo "=== user_info 脏队列分片验证（shards=${SHARDS}）==="
echo "源库: ${SOURCE_MYSQL_USER}@${SOURCE_MYSQL_HOST}:${SOURCE_MYSQL_PORT}/${SOURCE_MYSQL_DATABASE}"
echo

# ---------- 1. 分片表 ----------
echo "[1] 分片表"
for ((s = 0; s < SHARDS; s++)); do
  tbl="user_info_dirty_${s}"
  exists=$(mysql_q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='${tbl}'" | tr -d '[:space:]')
  if [[ "${exists:-0}" == "1" ]]; then
    cnt=$(mysql_q "SELECT COUNT(*) FROM ${tbl}" | tr -d '[:space:]')
    ok "${tbl} 存在（${cnt:-0} 行）"
  else
    bad_infra "${tbl} 缺失"
  fi
done
echo

# ---------- 2. user_info_dirty 须为视图 ----------
echo "[2] user_info_dirty 对象类型"
dirty_type=$(mysql_q "SELECT TABLE_TYPE FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='user_info_dirty' LIMIT 1" | tr -d '[:space:]')
case "$dirty_type" in
  VIEW) ok "user_info_dirty 是 VIEW" ;;
  BASE\ TABLE) bad_infra "user_info_dirty 仍是单表，请 deploy-source-ddl.sh 或 migrate-user-info-dirty-shards.sh" ;;
  "") bad_infra "user_info_dirty 不存在（视图未建）" ;;
  *) bad_infra "user_info_dirty 类型异常: ${dirty_type}" ;;
esac
echo

# ---------- 3. 存储过程 / TRIGGER ----------
echo "[3] 存储过程 + TRIGGER"
for proc in sp_user_info_dirty_upsert_one sp_user_info_dirty_enqueue \
  sp_user_info_dirty_enqueue_bvn sp_user_info_dirty_enqueue_adid sp_user_info_dirty_enqueue_emergency_mobile; do
  n=$(mysql_q "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema=DATABASE() AND routine_name='${proc}' AND routine_type='PROCEDURE'" | tr -d '[:space:]')
  if [[ "${n:-0}" -ge 1 ]]; then
    ok "${proc}"
  else
    bad_infra "${proc} 缺失"
  fi
done
trg_cnt=$(mysql_q "SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=DATABASE() AND trigger_name LIKE 'trg_user_info_dirty_%'" | tr -d '[:space:]')
if [[ "${trg_cnt:-0}" -ge 14 ]]; then
  ok "TRIGGER 数量=${trg_cnt}"
else
  bad_infra "TRIGGER 不足（${trg_cnt:-0}，期望≥14）"
fi
echo

# ---------- 4. 数据完整性 ----------
echo "[4] 数据完整性"
shard_sum=0
for ((s = 0; s < SHARDS; s++)); do
  c=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty_${s}" | tr -d '[:space:]')
  shard_sum=$((shard_sum + ${c:-0}))
done
view_cnt=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty" | tr -d '[:space:]')
if [[ "$view_cnt" == "ERR" ]]; then
  bad_data "无法查询 user_info_dirty 视图"
elif [[ "${view_cnt:-0}" -eq "$shard_sum" ]]; then
  ok "视图行数=${view_cnt} = 分片合计=${shard_sum}"
else
  bad_data "视图行数=${view_cnt} ≠ 分片合计=${shard_sum}"
fi

wrong=0
for ((s = 0; s < SHARDS; s++)); do
  w=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty_${s} WHERE MOD(user_id, ${SHARDS}) <> ${s}" | tr -d '[:space:]')
  wrong=$((wrong + ${w:-0}))
done
if [[ "$wrong" -eq 0 ]]; then
  ok "无错分片行（MOD user_id 与表后缀一致）"
else
  bad_data "错分片行数=${wrong}（需从旧单表重新 migrate 或手工搬迁）"
fi

dup=$(mysql_q "
SELECT COUNT(*) FROM (
  SELECT user_id
  FROM (
    SELECT user_id FROM user_info_dirty_0
    UNION ALL SELECT user_id FROM user_info_dirty_1
    UNION ALL SELECT user_id FROM user_info_dirty_2
    UNION ALL SELECT user_id FROM user_info_dirty_3
  ) u
  GROUP BY user_id
  HAVING COUNT(*) > 1
) d
" | tr -d '[:space:]')
if [[ "${dup:-0}" == "0" ]]; then
  ok "无跨分片重复 user_id"
else
  bad_data "存在跨分片重复 user_id"
fi
echo

if [[ "$SQL_ONLY" -eq 1 ]]; then
  echo "[SQL] 详细报告:"
  mysql_src < sql/verify/user_info_dirty_shards_check.sql || true
  echo
  [[ "$fail_infra" -eq 0 && "$fail_data" -eq 0 ]] && echo "结论: PASS（sql-only）" && exit 0
  [[ "$fail_infra" -ne 0 ]] && echo "结论: FAIL 基础设施" && exit 1
  echo "结论: FAIL 数据完整性" && exit 2
fi

# ---------- 5. 入队路由探测（可选）----------
if [[ "$PROBE" -eq 1 ]]; then
  echo "[5] 入队路由探测（sp_user_info_dirty_enqueue）"
  probe_ids=(999999992 999999993 999999994 999999995)
  probe_cleanup=()
  for uid in "${probe_ids[@]}"; do
    shard=$(user_info_dirty_shard_for_id "$uid")
    tbl="user_info_dirty_${shard}"
    had=$(mysql_q "SELECT COUNT(*) FROM ${tbl} WHERE user_id=${uid}" | tr -d '[:space:]')
    mysql_src -e "CALL sp_user_info_dirty_enqueue(${uid}, 0);" >/dev/null
    in_shard=$(mysql_q "SELECT COUNT(*) FROM ${tbl} WHERE user_id=${uid}" | tr -d '[:space:]')
    wrong_shards=0
    for ((s = 0; s < SHARDS; s++)); do
      [[ "$s" -eq "$shard" ]] && continue
      o=$(mysql_q "SELECT COUNT(*) FROM user_info_dirty_${s} WHERE user_id=${uid}" | tr -d '[:space:]')
      wrong_shards=$((wrong_shards + ${o:-0}))
    done
    if [[ "${in_shard:-0}" -ge 1 && "$wrong_shards" -eq 0 ]]; then
      ok "user_id=${uid} → ${tbl}（shard=${shard}）"
    else
      bad_data "user_id=${uid} 路由失败（in_${tbl}=${in_shard} 其它分片=${wrong_shards}）"
    fi
    if [[ "${had:-0}" == "0" ]]; then
      probe_cleanup+=("$tbl:$uid")
    fi
  done
  for item in "${probe_cleanup[@]}"; do
    tbl="${item%%:*}"
    uid="${item##*:}"
    mysql_src -e "DELETE FROM ${tbl} WHERE user_id=${uid};" >/dev/null 2>&1 || true
  done
  [[ ${#probe_cleanup[@]} -gt 0 ]] && echo "  （已清理探测插入的 ${#probe_cleanup[@]} 行）"
  echo
fi

# ---------- 6. Flink Job / 并行度 ----------
if [[ "$CHECK_FLINK" -eq 1 ]]; then
  echo "[6] Flink sink_user_info Job"
  JM="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
  FLINK_WEB_PORT="${FLINK_WEB_PORT:-8089}"
  job_line=""
  job_id=""

  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$JM"; then
    job_line=$(docker exec "$JM" ./bin/flink list -r 2>/dev/null | grep -E 'sink_user_info' || true)
    job_id=$(echo "$job_line" | grep -oE '[a-f0-9]{32}' | head -1 || true)
  fi

  if [[ -z "$job_line" ]]; then
    bad_flink "无 RUNNING 的 sink_user_info Job"
  else
    ok "Job RUNNING: ${job_id}"
    # REST: 各 vertex parallelism
    if [[ -n "$job_id" ]] && command -v curl >/dev/null 2>&1; then
      verts_json=$(curl -sf "http://127.0.0.1:${FLINK_WEB_PORT}/jobs/${job_id}/vertices" 2>/dev/null || true)
      if [[ -n "$verts_json" ]]; then
        if command -v python3 >/dev/null 2>&1; then
          par_summary=$(printf '%s' "$verts_json" | python3 -c "
import json,sys
d=json.load(sys.stdin)
vs=d.get('vertices',[])
pars=sorted({v.get('parallelism',0) for v in vs})
names=[v.get('name','')[:60] for v in vs[:4]]
print('parallelism_set='+','.join(map(str,pars)))
print('vertex_count='+str(len(vs)))
for n in names:
    print('vertex:'+n)
" 2>/dev/null || true)
          IFS=$'\n' read -r par_line vc_line _rest <<< "$par_summary"
          max_par=$(echo "${par_line:-}" | sed -n 's/parallelism_set=//p' | tr ',' '\n' | sort -n | tail -1)
          if [[ -n "${max_par:-}" && "${max_par:-0}" -ge 2 ]]; then
            ok "Web UI 最大 vertex parallelism=${max_par}（期望≥2，分片 CDC 生效后通常≥${SHARDS}）"
          elif [[ -n "${max_par:-}" && "${max_par:-0}" -eq 1 ]]; then
            bad_flink "Web UI 全部 parallelism=1（可能仍是旧单路 Job，请 cancel 后 git pull 重提）"
          fi
          echo "    ${par_line:-}"
          echo "    ${vc_line:-}"
        fi
      else
        echo "  WARN: 无法访问 Flink REST :${FLINK_WEB_PORT}/jobs/${job_id}/vertices"
      fi
    fi
    # 提交日志中的并行度
    if [[ -f logs/sync-user_info-auto.log ]]; then
      inj=$(grep "注入并行度" logs/sync-user_info-auto.log | tail -1 || true)
      sw=$(grep "切换增量" logs/sync-user_info-auto.log | tail -1 || true)
      [[ -n "$sw" ]] && echo "    日志: ${sw}"
      [[ -n "$inj" ]] && echo "    日志: ${inj}"
    fi
  fi
  echo
fi

# ---------- 结论 ----------
echo "=== 验证结论 ==="
echo "通过项: ${pass}"
[[ "$fail_infra" -ne 0 ]] && echo "基础设施: FAIL"
[[ "$fail_data" -ne 0 ]] && echo "数据完整性: FAIL"
[[ "$fail_flink" -ne 0 ]] && echo "Flink: FAIL"

if [[ "$fail_infra" -eq 0 && "$fail_data" -eq 0 && "$fail_flink" -eq 0 ]]; then
  echo "总结: 全通过 — 分片 DDL/数据/Flink 就绪"
  echo
  echo "下一步:"
  echo "  单用户静态/E2E: ./scripts/verify-user-info-incr.sh <user_id> --e2e"
  echo "  批量对账:       ./scripts/verify-user-info-reconcile.sh --sample 200"
  exit 0
fi

if [[ "$fail_infra" -ne 0 ]]; then
  echo "总结: 基础设施未就绪 — ./scripts/deploy-source-ddl.sh"
  exit 1
fi
if [[ "$fail_data" -ne 0 ]]; then
  echo "总结: 数据完整性问题 — ./scripts/migrate-user-info-dirty-shards.sh"
  exit 2
fi
echo "总结: Flink 未就绪 — cancel 旧 Job 后 ./scripts/sync-incr-auto.sh --jobs user_info --keep-jobs"
exit 3
