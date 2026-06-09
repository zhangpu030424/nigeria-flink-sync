#!/usr/bin/env bash
# 一键：建字典表 → 灌 mobile → VT 预加载 → 重建宽表 → 检查
#
# 用法:
#   ./scripts/setup-vt-preload-full.sh              # 执行到检查为止，不提交 Flink
#   ./scripts/setup-vt-preload-full.sh --run-flink  # 额外提交预 VT Flink Job
set -euo pipefail
cd "$(dirname "$0")/.."

RUN_FLINK=0
for arg in "$@"; do
  [[ "$arg" == "--run-flink" ]] && RUN_FLINK=1
done

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env"
  exit 1
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

SOURCE_MYSQL_PORT="${SOURCE_MYSQL_PORT:-3306}"
run_sql_file() {
  local f=$1
  echo ""
  echo "========== $f =========="
  MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
    -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" < "$f"
}

echo ">> [1/5] 确保 adjust 视图/物化表存在（若已做过可跳过报错）"
run_sql_file sql/ddl/source_views_adjust.sql || true
run_sql_file sql/ddl/source_materialize_user_adjust.sql || true

echo ">> [2/5] 创建 vt_token_cache 并灌入全类型去重明文（mobile/gaid_idfa/bank_account/id_number）"
run_sql_file sql/ddl/vt_token_cache_init_all.sql

echo ">> [3/5] 脚本批量 /v2t（多线程，默认 4 workers）"
./scripts/vt-preload.sh --vt-type all \
  --workers "${VT_PRELOAD_WORKERS:-4}" \
  --batch-size "${VT_PRELOAD_BATCH_SIZE:-10000}" \
  --http-batch-size "${VT_PRELOAD_HTTP_BATCH:-2000}"

echo ">> [4/5] 重建 user_sync_staging（含 mobile_token）"
run_sql_file sql/ddl/source_user_sync_staging_vt.sql

echo ">> [5/5] 检查 missing_token"
MISSING=$(MYSQL_PWD="${SOURCE_MYSQL_PASSWORD}" mysql -h "${SOURCE_MYSQL_HOST}" -P "${SOURCE_MYSQL_PORT}" \
  -u "${SOURCE_MYSQL_USER}" "${SOURCE_MYSQL_DATABASE}" -N -e \
  "SELECT COUNT(*) FROM user_sync_staging WHERE mobile_norm IS NOT NULL AND (mobile_token IS NULL OR mobile_token='');")
echo "missing_token_cnt=${MISSING}"
if [[ "${MISSING}" != "0" ]]; then
  echo "WARN: 仍有未命中 token 的行，请再跑 ./scripts/vt-preload.sh --retry-failed 后重建宽表"
fi

if [[ "$RUN_FLINK" -eq 1 ]]; then
  echo ">> 提交 Flink 预 VT Job"
  ./scripts/cancel-all-jobs.sh || true
  ./scripts/run-user-fast-pre-vt.sh
fi

echo ""
echo "完成。监控: ./scripts/monitor-sync.sh"
