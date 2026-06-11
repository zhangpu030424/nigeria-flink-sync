#!/usr/bin/env bash
# user_info 纯 MySQL 迁移（无 Flink）：老库 Join 落地 staging → 目标库 UPSERT
# 用法: LM_PICK_N=20 bash lm/scripts/run-ng-user-info-mysql-only.sh
# 全量: LM_PICK_N=2147483647 bash lm/scripts/run-ng-user-info-mysql-only.sh  （按老库 user 量，耗时会很长）
set -euo pipefail
cd "$(dirname "$0")/../.."

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
: "${TARGET_MYSQL_HOST:?}"
: "${TARGET_MYSQL_USER:?}"
: "${TARGET_MYSQL_PASSWORD:?}"

LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
TARGET_MYSQL_DATABASE="${TARGET_MYSQL_DATABASE:?}"
export LM_PICK_N="${LM_PICK_N:-20}"

LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/sync-ng-user-info-mysql-only.log"
mkdir -p "$LOG_DIR"

log() {
  echo "[$(date '+%F %T')] $*" | tee -a "$LOG_FILE"
}

log "user_info 纯 MySQL 同步 LM_PICK_N=${LM_PICK_N}"
log "  1) 老库 ${LM_MYSQL_DATABASE}@${LM_MYSQL_HOST} 落地 flink_stg_user_info_ready"
log "  2) 目标 ${TARGET_MYSQL_DATABASE}@${TARGET_MYSQL_HOST} UPSERT user_info"

export LM_PICK_N
bash lm/scripts/refresh-lm-user-info-latest100.sh 2>&1 | tee -a "$LOG_FILE"

# 目标库须能连老库（同 VPC / 白名单）。若不通，改用下方「两步 dump」注释里的方案。
STAGING_REF="\`${LM_MYSQL_DATABASE}\`.flink_stg_user_info_ready"

LOAD_SQL="/tmp/load-user-info-target-$$.sql"
cat > "$LOAD_SQL" <<EOF
INSERT INTO user_info (
    user_id, id_number, full_name, password, live_image, id_card, info, created_at, updated_at
)
SELECT
    s.user_id_part,
    LEFT(s.id_number, 28),
    LEFT(s.full_name, 255),
    LEFT(COALESCE(s.password, ''), 191),
    LEFT(COALESCE(s.live_image, ''), 191),
    LEFT(COALESCE(s.id_card, ''), 191),
    s.info,
    NOW(),
    NOW()
FROM ${STAGING_REF} s
ON DUPLICATE KEY UPDATE
    id_number   = VALUES(id_number),
    full_name   = VALUES(full_name),
    password    = VALUES(password),
    live_image  = VALUES(live_image),
    id_card     = VALUES(id_card),
    info        = VALUES(info),
    updated_at  = NOW();
EOF

log "写入目标库..."
if ! MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql \
  --connect-timeout=60 \
  -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" \
  < "$LOAD_SQL" 2>>"$LOG_FILE"; then
  rm -f "$LOAD_SQL"
  log "ERR: 目标库 INSERT 失败。常见原因: 目标库无法访问老库 ${LM_MYSQL_HOST}"
  log "     若两库网络隔离，请用: bash lm/scripts/run-ng-user-info-mysql-dump.sh（本地中转）"
  exit 1
fi
rm -f "$LOAD_SQL"

tgt_cnt=$(MYSQL_PWD="$TARGET_MYSQL_PASSWORD" mysql --connect-timeout=10 \
  -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" \
  -N -e "SELECT COUNT(*) FROM user_info;" 2>/dev/null || echo "?")

log "完成。目标库 user_info 总行数=${tgt_cnt}"
log "校验: SELECT user_id, id_number, full_name FROM user_info ORDER BY user_id DESC LIMIT ${LM_PICK_N};"
