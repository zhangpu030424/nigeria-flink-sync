#!/usr/bin/env bash
# 贷超 DK/LD 宽表 loan_dk_ld_sync_staging → 目标 loan（独立 Flink batch，不接入 sync-migrate-auto）
#
# 前置:
#   1. 源库 ng_loan_market 已执行 sql/ddl/loan_dk_ld_application_staging.sql
#   2. .env 已填 LM_MYSQL_* 与 TARGET_MYSQL_*
#
# 用法:
#   bash scripts/run-lm-loan-dk-ld-sync.sh
#   LM_LOAN_SKIP_SHARD_PREP=1 bash scripts/run-lm-loan-dk-ld-sync.sh   # 已写过 sync_shard 时跳过
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
source scripts/lib/load-project-env.sh
load_project_env .

for v in LM_MYSQL_HOST LM_MYSQL_PORT LM_MYSQL_USER LM_MYSQL_PASSWORD LM_MYSQL_DATABASE \
         TARGET_MYSQL_HOST TARGET_MYSQL_PORT TARGET_MYSQL_USER TARGET_MYSQL_PASSWORD TARGET_MYSQL_DATABASE; do
  if [[ -z "${!v:-}" ]]; then
    echo ">> ERR: .env 缺少 ${v}"
    exit 1
  fi
done

# shellcheck disable=SC1091
source scripts/lib/mysql-lm.sh

STAGING_TABLE="${LM_LOAN_STAGING_TABLE:-loan_dk_ld_sync_staging}"
export FLINK_PARALLELISM="${FLINK_PARALLELISM_BULK:-${FLINK_PARALLELISM:-40}}"
export LM_LOAN_SYNC_SHARD_MAX="${LM_LOAN_SYNC_SHARD_MAX:-$((FLINK_PARALLELISM - 1))}"

echo ">> 贷超 DK/LD loan 同步（独立 Job）"
echo ">> 源: ${LM_MYSQL_USER}@${LM_MYSQL_HOST}:${LM_MYSQL_PORT}/${LM_MYSQL_DATABASE}.${STAGING_TABLE}"
echo ">> 目标: ${TARGET_MYSQL_USER}@${TARGET_MYSQL_HOST}:${TARGET_MYSQL_PORT}/${TARGET_MYSQL_DATABASE}.loan"
echo ">> 并行: FLINK_PARALLELISM=${FLINK_PARALLELISM}  sync_shard 0..${LM_LOAN_SYNC_SHARD_MAX}"

tbl_cnt=$(mysql_lm_query \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${STAGING_TABLE}';" \
  2>/dev/null || echo "ERR")
if [[ "$tbl_cnt" != "1" ]]; then
  echo ">> ERR: 源库不存在表 ${STAGING_TABLE}，请先执行 sql/ddl/loan_dk_ld_application_staging.sql"
  exit 1
fi

src_cnt=$(mysql_lm_query "SELECT COUNT(*) FROM \`${STAGING_TABLE}\`;" 2>/dev/null || echo "ERR")
echo ">> 源宽表行数: ${src_cnt}"
if [[ "$src_cnt" == "0" || "$src_cnt" == "ERR" ]]; then
  echo ">> ERR: 宽表无数据或无法连接 LM_MYSQL"
  exit 1
fi

if [[ "${LM_LOAN_SKIP_SHARD_PREP:-0}" != "1" ]]; then
  shard_col=$(mysql_lm_query \
    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='${LM_MYSQL_DATABASE}' AND table_name='${STAGING_TABLE}' AND column_name='sync_shard';" \
    2>/dev/null || echo "0")
  if [[ "$shard_col" != "1" ]]; then
    echo ">> 添加 sync_shard 列（Flink JDBC 分片读）..."
    mysql_lm_cmd -e \
      "ALTER TABLE \`${STAGING_TABLE}\` ADD COLUMN sync_shard INT UNSIGNED NOT NULL DEFAULT 0, ADD KEY idx_sync_shard (sync_shard);"
  fi

  distinct_shard=$(mysql_lm_query \
    "SELECT COUNT(DISTINCT sync_shard) FROM \`${STAGING_TABLE}\` WHERE sync_shard <= ${LM_LOAN_SYNC_SHARD_MAX};" \
    2>/dev/null || echo "0")
  if [[ "${distinct_shard:-0}" -lt "$((FLINK_PARALLELISM / 2))" ]]; then
    echo ">> 回填 sync_shard = CRC32(application_no) % ${FLINK_PARALLELISM}（约 ${src_cnt} 行，可能 1~3 分钟）..."
    t0=$SECONDS
    mysql_lm_cmd -e \
      "UPDATE \`${STAGING_TABLE}\` SET sync_shard = CRC32(application_no) % ${FLINK_PARALLELISM};"
    echo ">> sync_shard 回填完成（${SECONDS - t0}s）"
  else
    echo ">> sync_shard 已分布（distinct=${distinct_shard}），跳过回填"
  fi
fi

exec ./scripts/run-sql.sh sql/lm/02_sync_loan_dk_ld_lm_bulk.sql
