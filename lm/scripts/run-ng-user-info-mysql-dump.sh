#!/usr/bin/env bash
# user_info 纯 MySQL（两库网络隔离版）：staging → mysqldump → 目标库
# 用法: LM_PICK_N=20 bash lm/scripts/run-ng-user-info-mysql-dump.sh
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
: "${LM_MYSQL_PASSWORD:?}"
: "${LM_MYSQL_USER:?}"
: "${TARGET_MYSQL_HOST:?}"
: "${TARGET_MYSQL_PASSWORD:?}"
: "${TARGET_MYSQL_USER:?}"

LM_MYSQL_PORT="${LM_MYSQL_PORT:-3306}"
LM_MYSQL_DATABASE="${LM_MYSQL_DATABASE:-ng_loan_market}"
TARGET_MYSQL_PORT="${TARGET_MYSQL_PORT:-3306}"
TARGET_MYSQL_DATABASE="${TARGET_MYSQL_DATABASE:?}"
export LM_PICK_N="${LM_PICK_N:-20}"

LOG_DIR="logs"
DUMP_FILE="${LOG_DIR}/flink_stg_user_info_ready-${LM_PICK_N}.sql"
UPSERT_FILE="${LOG_DIR}/user_info_upsert-${LM_PICK_N}.sql"
mkdir -p "$LOG_DIR"

echo "[$(date '+%F %T')] 1) 老库落地 staging"
bash lm/scripts/refresh-lm-user-info-latest100.sh

echo "[$(date '+%F %T')] 2) mysqldump → ${DUMP_FILE}"
MYSQL_PWD="${LM_MYSQL_PASSWORD}" mysqldump \
  --connect-timeout=30 \
  -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" \
  -u "$LM_MYSQL_USER" "$LM_MYSQL_DATABASE" \
  flink_stg_user_info_ready \
  --no-create-info --complete-insert --skip-triggers \
  > "$DUMP_FILE"

echo "[$(date '+%F %T')] 3) 生成目标 UPSERT → ${UPSERT_FILE}"
cat > "$UPSERT_FILE" <<'HDR'
DROP TABLE IF EXISTS _stg_user_info_import;
CREATE TABLE _stg_user_info_import (
    user_id_part BIGINT NOT NULL,
    user_id      VARCHAR(32) NOT NULL,
    id_number    VARCHAR(64) NOT NULL DEFAULT '',
    full_name    VARCHAR(512) NOT NULL DEFAULT '',
    password     VARCHAR(256) NOT NULL DEFAULT '',
    live_image   VARCHAR(256) DEFAULT NULL,
    id_card      VARCHAR(256) DEFAULT NULL,
    info         JSON NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
HDR

sed 's/`flink_stg_user_info_ready`/`_stg_user_info_import`/g' "$DUMP_FILE" >> "$UPSERT_FILE"

cat >> "$UPSERT_FILE" <<'TAIL'
INSERT INTO user_info (user_id, id_number, full_name, password, live_image, id_card, info, created_at, updated_at)
SELECT user_id_part,
       LEFT(id_number, 28), LEFT(full_name, 255),
       LEFT(COALESCE(password, ''), 191), LEFT(COALESCE(live_image, ''), 191),
       LEFT(COALESCE(id_card, ''), 191), info, NOW(), NOW()
FROM _stg_user_info_import
ON DUPLICATE KEY UPDATE
    id_number = VALUES(id_number), full_name = VALUES(full_name),
    password = VALUES(password), live_image = VALUES(live_image),
    id_card = VALUES(id_card), info = VALUES(info), updated_at = NOW();
DROP TABLE IF EXISTS _stg_user_info_import;
TAIL

echo "[$(date '+%F %T')] 4) 导入目标库"
MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql \
  --connect-timeout=120 \
  -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" \
  < "$UPSERT_FILE"

cnt=$(MYSQL_PWD="${TARGET_MYSQL_PASSWORD}" mysql -h "$TARGET_MYSQL_HOST" -P "$TARGET_MYSQL_PORT" \
  -u "$TARGET_MYSQL_USER" "$TARGET_MYSQL_DATABASE" -N -e "SELECT COUNT(*) FROM user_info;")
echo "[$(date '+%F %T')] 完成。目标 user_info=${cnt} 行"
