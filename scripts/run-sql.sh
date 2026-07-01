#!/usr/bin/env bash
# 读取 .env 替换占位符，在 JobManager 容器内执行 Flink SQL 文件
# 用法: bash scripts/run-sql.sh sql/02_sync_user_fast.sql
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "请先: cp .env.example .env 并填写"
  exit 1
fi

SQL_FILE="${1:-}"
if [[ -z "$SQL_FILE" || ! -f "$SQL_FILE" ]]; then
  echo "用法: $0 <sql文件路径>"
  echo "示例:"
  echo "  $0 sql/02_sync_user_fast.sql"
  exit 1
fi

# shellcheck disable=SC1091
set -a
# 只加载 KEY=VALUE 行；已在环境中的变量不覆盖（便于切增量前 export FLINK_PARALLELISM=1）
while IFS= read -r line || [[ -n "$line" ]]; do
  line="${line%%#*}"
  line="$(echo "$line" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$line" ]] && continue
  [[ "$line" =~ ^[A-Za-z_][A-Za-z0-9_]*= ]] || continue
  key="${line%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

# 老库 Batch bulk 用 BULK 并行（latest100 / gpt_one 由脚本传入低并行，不在此覆盖）
if [[ "$SQL_FILE" == *id_add_user_bulk* || "$SQL_FILE" == *_lm_bulk* || "$SQL_FILE" == *ng_migration_bulk* ]]; then
  _BULK="${FLINK_PARALLELISM_BULK:-${FLINK_TASK_SLOTS:-40}}"
  export FLINK_PARALLELISM="${_BULK}"
fi
export FLINK_PARALLELISM="${FLINK_PARALLELISM:-16}"
export USER_ID_OFFSET="${USER_ID_OFFSET:-100000000}"
export FLINK_MINI_BATCH_SIZE="${FLINK_MINI_BATCH_SIZE:-10000}"
export FLINK_SINK_BUFFER_ROWS="${FLINK_SINK_BUFFER_ROWS:-10000}"
export FLINK_CDC_CHUNK_SIZE="${FLINK_CDC_CHUNK_SIZE:-100000}"
export FLINK_CDC_FETCH_SIZE="${FLINK_CDC_FETCH_SIZE:-10000}"
export FLINK_CHECKPOINT_INTERVAL="${FLINK_CHECKPOINT_INTERVAL:-300s}"
export FLINK_CHECKPOINT_TIMEOUT="${FLINK_CHECKPOINT_TIMEOUT:-1800s}"
export USER_INFO_DIRTY_COALESCE_SEC="${USER_INFO_DIRTY_COALESCE_SEC:-5}"
export CDC_STARTUP_MODE="${CDC_STARTUP_MODE:-initial}"
export CDC_STARTUP_TIMESTAMP_MILLIS="${CDC_STARTUP_TIMESTAMP_MILLIS:-0}"
export LM_MIGRATION_LIMIT_CLAUSE="${LM_MIGRATION_LIMIT_CLAUSE:-}"
export LM_USER_ID_RANGE_CLAUSE="${LM_USER_ID_RANGE_CLAUSE:-}"
export LM_SRC_TABLE_READY="${LM_SRC_TABLE_READY:-flink_stg_user_info_ready}"
export LM_USER_REG_IP_TABLE="${LM_USER_REG_IP_TABLE:-user_reg_ip}"
export LM_MIGRATION_ID_UPPER="${LM_MIGRATION_ID_UPPER:-500000000}"
export LM_CDC_SERVER_ID_USER="${LM_CDC_SERVER_ID_USER:-5701-5704}"
export LM_CDC_SERVER_ID_USER_DATA="${LM_CDC_SERVER_ID_USER_DATA:-5711-5714}"
export LM_CDC_SERVER_ID_LUP="${LM_CDC_SERVER_ID_LUP:-5721-5724}"
export LM_CDC_SERVER_ID_DAC="${LM_CDC_SERVER_ID_DAC:-5731-5734}"
export LM_CDC_SERVER_ID_URI="${LM_CDC_SERVER_ID_URI:-5741-5744}"
# user_info 增量多 CDC（5400+）
_ui_base="${CDC_SERVER_ID_UI_BASE:-5400}"
_ui_span="${CDC_SERVER_ID_UI_SPAN:-4}"
# 脏队列表 CDC 关闭 incremental snapshot 时 server-id 须为单值（非 5401-5404 范围，否则 NumberFormatException）
export CDC_SERVER_ID_UI_DIRTY="${CDC_SERVER_ID_UI_DIRTY:-$((_ui_base + 1))}"
if [[ "$CDC_SERVER_ID_UI_DIRTY" == *-* ]]; then
  CDC_SERVER_ID_UI_DIRTY="${CDC_SERVER_ID_UI_DIRTY%%-*}"
  export CDC_SERVER_ID_UI_DIRTY
fi
export CDC_SERVER_ID_UI_USER="${CDC_SERVER_ID_UI_USER:-$((_ui_base + 1))-$((_ui_base + _ui_span))}"
export CDC_SERVER_ID_UI_PERSONAL="${CDC_SERVER_ID_UI_PERSONAL:-$((_ui_base + 11))-$((_ui_base + 11 + _ui_span))}"
export CDC_SERVER_ID_UI_WORK="${CDC_SERVER_ID_UI_WORK:-$((_ui_base + 21))-$((_ui_base + 21 + _ui_span))}"
export CDC_SERVER_ID_UI_EMERGENCY="${CDC_SERVER_ID_UI_EMERGENCY:-$((_ui_base + 31))-$((_ui_base + 31 + _ui_span))}"
export CDC_SERVER_ID_UI_CREDIT="${CDC_SERVER_ID_UI_CREDIT:-$((_ui_base + 41))-$((_ui_base + 41 + _ui_span))}"
export CDC_SERVER_ID_UI_VT="${CDC_SERVER_ID_UI_VT:-$((_ui_base + 51))-$((_ui_base + 51 + _ui_span))}"
export CDC_SERVER_ID_UI_DEVICE_IDS="${CDC_SERVER_ID_UI_DEVICE_IDS:-$((_ui_base + 61))-$((_ui_base + 61 + _ui_span))}"
export CDC_SERVER_ID_UI_DEVICE_NET="${CDC_SERVER_ID_UI_DEVICE_NET:-$((_ui_base + 71))-$((_ui_base + 71 + _ui_span))}"
# user / bankcard / product / application / loan 增量 CDC server-id
_sid="${CDC_SERVER_ID_SPAN:-4}"
export CDC_SERVER_ID_USER_MAIN="${CDC_SERVER_ID_USER_MAIN:-5501-$((5500 + _sid))}"
export CDC_SERVER_ID_USER_ADJUST="${CDC_SERVER_ID_USER_ADJUST:-5511-$((5510 + _sid))}"
export CDC_SERVER_ID_BANKCARD_INFO="${CDC_SERVER_ID_BANKCARD_INFO:-5521-$((5520 + _sid))}"
export CDC_SERVER_ID_BANKCARD_VT="${CDC_SERVER_ID_BANKCARD_VT:-5531-$((5530 + _sid))}"
export CDC_SERVER_ID_USER_PRODUCT="${CDC_SERVER_ID_USER_PRODUCT:-5541-$((5540 + _sid))}"
export CDC_SERVER_ID_APP_ORDER="${CDC_SERVER_ID_APP_ORDER:-5601-$((5600 + _sid))}"
export CDC_SERVER_ID_APP_USER="${CDC_SERVER_ID_APP_USER:-5611-$((5610 + _sid))}"
export CDC_SERVER_ID_APP_BANK="${CDC_SERVER_ID_APP_BANK:-5621-$((5620 + _sid))}"
export CDC_SERVER_ID_APP_PERSONAL="${CDC_SERVER_ID_APP_PERSONAL:-5631-$((5630 + _sid))}"
export CDC_SERVER_ID_APP_DEVICE="${CDC_SERVER_ID_APP_DEVICE:-5641-$((5640 + _sid))}"
export CDC_SERVER_ID_APP_REPAY="${CDC_SERVER_ID_APP_REPAY:-5651-$((5650 + _sid))}"
export CDC_SERVER_ID_APP_RISK="${CDC_SERVER_ID_APP_RISK:-5661-$((5660 + _sid))}"
export CDC_SERVER_ID_APP_INSTALLMENT="${CDC_SERVER_ID_APP_INSTALLMENT:-5671-$((5670 + _sid))}"
export CDC_SERVER_ID_LOAN_INSTALLMENT="${CDC_SERVER_ID_LOAN_INSTALLMENT:-5801-$((5800 + _sid))}"
export CDC_SERVER_ID_LOAN_ORDER="${CDC_SERVER_ID_LOAN_ORDER:-5811-$((5810 + _sid))}"
export CDC_SERVER_ID_LOAN_REPAY="${CDC_SERVER_ID_LOAN_REPAY:-5821-$((5820 + _sid))}"

export LM_LOAN_SYNC_SHARD_MAX="${LM_LOAN_SYNC_SHARD_MAX:-$((FLINK_PARALLELISM - 1))}"

VARS='${SOURCE_MYSQL_HOST} ${SOURCE_MYSQL_PORT} ${SOURCE_MYSQL_USER} ${SOURCE_MYSQL_PASSWORD} ${SOURCE_MYSQL_DATABASE} ${LM_MYSQL_HOST} ${LM_MYSQL_PORT} ${LM_MYSQL_USER} ${LM_MYSQL_PASSWORD} ${LM_MYSQL_DATABASE} ${LM_LOAN_SYNC_SHARD_MAX} ${LM_CORE_MYSQL_HOST} ${LM_CORE_MYSQL_PORT} ${LM_CORE_MYSQL_USER} ${LM_CORE_MYSQL_PASSWORD} ${LM_CORE_MYSQL_DATABASE} ${LM_MIGRATION_LIMIT} ${LM_MIGRATION_LIMIT_CLAUSE} ${LM_MIGRATION_ID_UPPER} ${LM_USER_ID_RANGE_CLAUSE} ${LM_SRC_TABLE_READY} ${LM_USER_REG_IP_TABLE} ${LM_CDC_SERVER_ID_USER} ${LM_CDC_SERVER_ID_USER_DATA} ${LM_CDC_SERVER_ID_LUP} ${LM_CDC_SERVER_ID_DAC} ${LM_CDC_SERVER_ID_URI} ${CDC_SERVER_ID_UI_DIRTY} ${CDC_SERVER_ID_UI_USER} ${CDC_SERVER_ID_UI_PERSONAL} ${CDC_SERVER_ID_UI_WORK} ${CDC_SERVER_ID_UI_EMERGENCY} ${CDC_SERVER_ID_UI_CREDIT} ${CDC_SERVER_ID_UI_VT} ${CDC_SERVER_ID_UI_DEVICE_IDS} ${CDC_SERVER_ID_UI_DEVICE_NET} ${CDC_SERVER_ID_USER_MAIN} ${CDC_SERVER_ID_USER_ADJUST} ${CDC_SERVER_ID_BANKCARD_INFO} ${CDC_SERVER_ID_BANKCARD_VT} ${CDC_SERVER_ID_USER_PRODUCT} ${CDC_SERVER_ID_APP_ORDER} ${CDC_SERVER_ID_APP_USER} ${CDC_SERVER_ID_APP_BANK} ${CDC_SERVER_ID_APP_PERSONAL} ${CDC_SERVER_ID_APP_DEVICE} ${CDC_SERVER_ID_APP_REPAY} ${CDC_SERVER_ID_APP_RISK} ${CDC_SERVER_ID_APP_INSTALLMENT} ${CDC_SERVER_ID_LOAN_INSTALLMENT} ${CDC_SERVER_ID_LOAN_ORDER} ${CDC_SERVER_ID_LOAN_REPAY} ${TARGET_MYSQL_HOST} ${TARGET_MYSQL_PORT} ${TARGET_MYSQL_USER} ${TARGET_MYSQL_PASSWORD} ${TARGET_MYSQL_DATABASE} ${FLINK_PARALLELISM} ${FLINK_MINI_BATCH_SIZE} ${FLINK_SINK_BUFFER_ROWS} ${FLINK_CDC_CHUNK_SIZE} ${FLINK_CDC_FETCH_SIZE} ${FLINK_CHECKPOINT_INTERVAL} ${FLINK_CHECKPOINT_TIMEOUT} ${CDC_STARTUP_MODE} ${CDC_STARTUP_TIMESTAMP_MILLIS} ${USER_INFO_DIRTY_COALESCE_SEC}'

PREPARED="/tmp/nigeria-flink-run-$$.sql"
envsubst "$VARS" < "$SQL_FILE" > "$PREPARED"

CONTAINER="${FLINK_JOBMANAGER_CONTAINER:-nigeria-flink-jobmanager}"
REMOTE="/tmp/nigeria-flink-run.sql"

if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo ">> ERR: JobManager 容器 [$CONTAINER] 未运行（可能 OOM 或崩溃退出）"
  echo ">> 恢复: ./scripts/up.sh"
  echo ">> 诊断: docker logs --tail 80 $CONTAINER"
  docker ps -a --filter "name=nigeria-flink" --format "table {{.Names}}\t{{.Status}}" 2>/dev/null || true
  rm -f "$PREPARED"
  exit 1
fi

echo ">> 执行: $SQL_FILE"
echo ">> 注入并行度: FLINK_PARALLELISM=${FLINK_PARALLELISM}  fetch=${FLINK_CDC_FETCH_SIZE}  sink_buffer=${FLINK_SINK_BUFFER_ROWS}"
if [[ "$SQL_FILE" == *id_add_user_bulk* || "$SQL_FILE" == *_lm_bulk* ]]; then
  if [[ "${FLINK_PARALLELISM:-1}" -lt 4 ]]; then
    echo ">> ERR: 全量 FLINK_PARALLELISM=${FLINK_PARALLELISM}（应≥4）"
    echo ">> 请在 .env 设 FLINK_PARALLELISM_BULK 与 FLINK_TASK_SLOTS 一致"
    rm -f "$PREPARED"
    exit 1
  fi
  par=$(grep -oE "scan\.partition\.num' = '[0-9]+'" "$PREPARED" | head -1 | grep -oE '[0-9]+' || echo "0")
  if [[ "${par:-0}" -lt 4 ]]; then
    echo ">> ERR: scan.partition.num=${par}，并行度未注入"
    rm -f "$PREPARED"
    exit 1
  fi
fi
grep -E "^SET 'parallelism|scan.partition.num|'table-name'" "$PREPARED" 2>/dev/null | head -10 || true
docker cp "$PREPARED" "${CONTAINER}:${REMOTE}"
SQL_LOG="$(mktemp)"
trap 'rm -f "$PREPARED" "$SQL_LOG"' EXIT
docker exec "$CONTAINER" ./bin/sql-client.sh -D "parallelism.default=${FLINK_PARALLELISM}" -f "$REMOTE" \
  2>&1 | tee "$SQL_LOG"
rm -f "$PREPARED"
PREPARED=""

# batch Job 常在 sql-client 返回前已 FINISHED，flink list 捕不到；从输出解析 Job ID
FLINK_JOB_ID="$(sed -n 's/.*Job ID: \([a-f0-9]\{32\}\).*/\1/p' "$SQL_LOG" | tail -1)"
LAST_JOB_FILE="${FLINK_LAST_JOB_ID_FILE:-logs/last-flink-job-id}"
mkdir -p "$(dirname "$LAST_JOB_FILE")"
if [[ -n "$FLINK_JOB_ID" ]]; then
  echo "$FLINK_JOB_ID" > "$LAST_JOB_FILE"
  echo ">> FLINK_JOB_ID=${FLINK_JOB_ID}"
else
  : > "$LAST_JOB_FILE"
fi
rm -f "$SQL_LOG"
SQL_LOG=""
trap - EXIT

if [[ -n "$FLINK_JOB_ID" ]]; then
  echo ">> 完成。Job 已提交（batch 可能已结束，见 FLINK_JOB_ID）。"
else
  echo ">> 完成。INSERT 类语句会提交长期 Job，请到 Web UI 查看 Running Jobs。"
fi
