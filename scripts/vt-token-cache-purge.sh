#!/usr/bin/env bash
# 分批删除 vt_token_cache（或大表），避免 DROP 长时间锁表
#
# 用法:
#   ./scripts/vt-token-cache-purge.sh
#   ./scripts/vt-token-cache-purge.sh --table vt_token_cache_legacy
#   ./scripts/vt-token-cache-purge.sh --batch 20000 --sleep 0.5
#   ./scripts/vt-token-cache-purge.sh --dry-run
#   ./scripts/vt-token-cache-purge.sh --drop-after   # 删空后 DROP TABLE
#
# 建议先停 Flink / vt-preload，再执行。
#
set -euo pipefail
cd "$(dirname "$0")/.."

TABLE="vt_token_cache"
BATCH=10000
SLEEP_SEC="0.2"
DRY_RUN=0
DROP_AFTER=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --table=*) TABLE="${1#--table=}" ;;
    --table)
      shift
      TABLE="${1:-vt_token_cache}"
      ;;
    --batch=*) BATCH="${1#--batch=}" ;;
    --batch)
      shift
      BATCH="${1:-10000}"
      ;;
    --sleep=*) SLEEP_SEC="${1#--sleep=}" ;;
    --sleep)
      shift
      SLEEP_SEC="${1:-0.2}"
      ;;
    --dry-run) DRY_RUN=1 ;;
    --drop-after) DROP_AFTER=1 ;;
    -h|--help)
      sed -n '2,14p' "$0"
      exit 0
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
  shift
done

[[ -f .env ]] || { echo "ERR: 请先 cp .env.example .env"; exit 1; }

# shellcheck source=scripts/lib/load-project-env.sh
source scripts/lib/load-project-env.sh
load_project_env "$(pwd)"

# shellcheck source=scripts/lib/mysql-source.sh
source scripts/lib/mysql-source.sh

if ! table_exists "$TABLE"; then
  echo "ERR: 表 ${SOURCE_MYSQL_DATABASE}.${TABLE} 不存在"
  exit 1
fi

total=$(mysql_source_query "SELECT COUNT(*) FROM \`${TABLE}\`;" 2>/dev/null || echo "ERR")
min_id=$(mysql_source_query "SELECT COALESCE(MIN(id),0) FROM \`${TABLE}\`;" 2>/dev/null || echo "0")
max_id=$(mysql_source_query "SELECT COALESCE(MAX(id),0) FROM \`${TABLE}\`;" 2>/dev/null || echo "0")

echo "=========================================="
echo "分批删除 ${SOURCE_MYSQL_DATABASE}.${TABLE}"
echo "  行数≈${total}  id范围 ${min_id}~${max_id}"
echo "  batch=${BATCH}  sleep=${SLEEP_SEC}s"
echo "=========================================="

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo ">> dry-run，不执行 DELETE"
  exit 0
fi

if [[ "$total" == "0" || "$max_id" == "0" ]]; then
  echo ">> 表已空"
  if [[ "$DROP_AFTER" -eq 1 ]]; then
    echo ">> DROP TABLE \`${TABLE}\`"
    mysql_source_query "DROP TABLE \`${TABLE}\`;"
    echo ">> 完成"
  fi
  exit 0
fi

round=0
deleted=0
cursor="$min_id"

while [[ "$cursor" -le "$max_id" ]]; do
  round=$((round + 1))
  end_id=$((cursor + BATCH - 1))

  # 按主键区间删，比 ORDER BY id LIMIT 更稳（少扫索引）
  affected=$(mysql_source_cmd -N -e "
    DELETE FROM \`${TABLE}\` WHERE id >= ${cursor} AND id <= ${end_id};
    SELECT ROW_COUNT();
  " | tail -1 | tr -d '[:space:]')

  [[ "$affected" =~ ^[0-9]+$ ]] || affected=0
  deleted=$((deleted + affected))
  cursor=$((end_id + 1))

  remain=$(mysql_source_query "SELECT COUNT(*) FROM \`${TABLE}\`;" 2>/dev/null || echo "?")
  echo ">> 第${round}批 id<=${end_id} 本批=${affected} 累计≈${deleted} 剩余≈${remain}"

  if [[ "$affected" -eq 0 && "$remain" != "0" ]]; then
    # 稀疏 id：跳过大段空区间
    next_id=$(mysql_source_query "SELECT COALESCE(MIN(id),0) FROM \`${TABLE}\` WHERE id > ${end_id};" 2>/dev/null || echo "0")
    if [[ "$next_id" == "0" ]]; then
      break
    fi
    cursor="$next_id"
  fi

  if [[ "$remain" == "0" ]]; then
    break
  fi

  sleep "$SLEEP_SEC"
done

echo ""
echo ">> 删除完成，剩余行数: $(mysql_source_query "SELECT COUNT(*) FROM \`${TABLE}\`;" 2>/dev/null || echo ERR)"

if [[ "$DROP_AFTER" -eq 1 ]]; then
  echo ">> DROP TABLE \`${TABLE}\`"
  mysql_source_query "DROP TABLE \`${TABLE}\`;"
  echo ">> 表已 DROP"
fi
