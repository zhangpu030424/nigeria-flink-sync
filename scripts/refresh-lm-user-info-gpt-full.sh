#!/usr/bin/env bash
# 全量 GPT JSON 落地 flink_stg_user_info_ready（依赖 flink_stg_* 实体表）
# 用法: bash scripts/refresh-lm-user-info-gpt-full.sh
set -euo pipefail
cd "$(dirname "$0")/.."

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
  key="${line%%=*}"
  [[ -n "${!key:-}" ]] && continue
  export "$line"
done < .env
set +a

: "${LM_MYSQL_HOST:?}"
: "${LM_MYSQL_USER:?}"
: "${LM_MYSQL_PASSWORD:?}"
LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"

base_table_exists() {
  local tbl=$1
  local cnt
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM information_schema.tables
           WHERE table_schema='${LM_MYSQL_DATABASE}'
             AND table_name='${tbl}' AND table_type='BASE TABLE';" 2>/dev/null || echo 0)
  [[ "$cnt" == "1" ]]
}

stg_cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_stg_mkt_user;" 2>/dev/null || echo 0)

if [[ ! "$stg_cnt" =~ ^[0-9]+$ ]] || [[ "$stg_cnt" -lt 1000 ]]; then
  echo ">> flink_stg_mkt_user 为空或不存在，先落地基础实体表..."
  bash scripts/refresh-lm-user-info-staging.sh
fi

PREP="/tmp/lm_user_info_gpt_staging_full-$$.sql"
cp sql/ddl/lm_user_info_gpt_staging_full.sql "$PREP"

patch_sql() {
  python3 - "$PREP" <<'PY'
import re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()

def strip_uri(sql):
    sql = sql.replace("'registration_ip', uri.ip", "'registration_ip', CAST(NULL AS CHAR)")
    sql = re.sub(
        r"LEFT JOIN \(\s*SELECT CAST\(r1\.`userId`.*?LEFT JOIN \(\s*SELECT CAST\(a\.id",
        "LEFT JOIN (\n    SELECT CAST(a.id",
        sql,
        count=1,
        flags=re.S,
    )
    return sql

def strip_app(sql):
    sql = sql.replace("'name', app.`name`", "'name', CAST(NULL AS CHAR)")
    sql = re.sub(
        r"LEFT JOIN \(\s*SELECT CAST\(a\.id AS CHAR\) AS id, CAST\(a\.`name` AS CHAR\) AS `name`\s*FROM `app` a\s*\) app ON app\.id = u\.`appId`;\s*",
        "",
        sql,
        count=1,
        flags=re.S,
    )
    return sql

import os
if os.environ.get("STRIP_URI") == "1":
    text = strip_uri(text)
if os.environ.get("STRIP_APP") == "1":
    text = strip_app(text)
open(path, "w", encoding="utf-8").write(text)
PY
}

if ! base_table_exists "user_registration_ip"; then
  echo ">> WARN: 无 user_registration_ip 表，registration_ip 写 NULL"
  export STRIP_URI=1
else
  export STRIP_URI=0
fi

if ! base_table_exists "app"; then
  echo ">> WARN: 无 app 表，app.name 写 NULL"
  export STRIP_APP=1
else
  export STRIP_APP=0
fi

if [[ "$STRIP_URI" == "1" || "$STRIP_APP" == "1" ]]; then
  patch_sql
fi

# Flink 05 多表路径仍可能需要 VIEW；有权限则创建，失败不阻断本脚本
if [[ -f sql/ddl/lm_user_info_gpt_views.sql ]]; then
  echo ">> 可选: 刷新 v_flink_uri_latest / v_flink_mkt_app（Flink 多表 Job 用）"
  if base_table_exists "user_registration_ip" && base_table_exists "app"; then
    MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=30 \
      -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
      < sql/ddl/lm_user_info_gpt_views.sql && echo ">> VIEW 已刷新" \
      || echo ">> WARN: VIEW 创建失败（无权限可忽略，全量走 flink_stg_user_info_ready）"
  else
    echo ">> 跳过 VIEW（缺少 user_registration_ip 或 app 基表）"
  fi
fi

echo ">> 全量拼 GPT JSON → flink_stg_user_info_ready"
echo ">> ${LM_MYSQL_DATABASE} @ ${LM_MYSQL_HOST}:${LM_MYSQL_PORT}"
echo ">> 开始: $(date '+%F %T')"
echo ">> 【说明】此步在 MySQL 跑大 SQL（约 900 万行 × JSON），Flink Job 在下一步 Step 4 才提交"
echo ">>        此处长时间无新日志是正常的，可用下面命令另开终端看进度："
echo ">>        mysql ... -e \"SHOW PROCESSLIST\\G\""
echo ">>        mysql ... -e \"SELECT COUNT(*) FROM flink_stg_user_info_ready;\""

POLL_SEC="${LM_GPT_FULL_POLL_SEC:-60}"
MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  < "$PREP" &
MYSQL_PID=$!

prev_cnt=""
while kill -0 "$MYSQL_PID" 2>/dev/null; do
  sleep "$POLL_SEC"
  proc=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=5 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT ID, TIME, STATE, LEFT(INFO,120) FROM information_schema.processlist
           WHERE USER=USER() AND COMMAND='Query' AND INFO LIKE '%flink_stg_user_info_ready%'
           ORDER BY TIME DESC LIMIT 1;" 2>/dev/null || true)
  cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=5 \
    -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
    -N -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;" 2>/dev/null || echo "?")
  delta=""
  if [[ "$cnt" =~ ^[0-9]+$ && "$prev_cnt" =~ ^[0-9]+$ ]]; then
    delta=$((cnt - prev_cnt))
  fi
  prev_cnt="$cnt"
  echo "[$(date '+%F %T')] MySQL 仍在跑… ready表=${cnt} 本段+${delta:-?}  process=${proc:-(查不到)}"
done

wait "$MYSQL_PID" || { echo "ERR: MySQL 全量 INSERT 失败 exit=$?"; exit 1; }

rm -f "$PREP"

cnt=$(MYSQL_PWD="$LM_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;" 2>/dev/null || echo "?")
echo ">> 完成: $(date '+%F %T')  flink_stg_user_info_ready=${cnt} 行"
