#!/usr/bin/env bash
# 流水线起始时间戳：增量 CDC 从该时刻起补 binlog（与宽表重建耗时无关）
# 由 sync-pipeline-auto.sh 在第一步调用；写入 logs/bulk-start-ms.env

bulk_start_ms_now() {
  python3 -c 'import time; print(int(time.time()*1000))' 2>/dev/null || echo "$(($(date +%s)*1000))"
}

bulk_start_iso_ng() {
  # 源库 / Flink 业务时区（与 SQL 中 server-time-zone=Africa/Lagos 一致）
  TZ=Africa/Lagos date '+%Y-%m-%d %H:%M:%S %Z' 2>/dev/null \
    || python3 -c "from datetime import datetime, timezone, timedelta; print(datetime.fromtimestamp(${BULK_START_MS}/1000, timezone(timedelta(hours=1))).strftime('%Y-%m-%d %H:%M:%S WAT'))"
}

record_bulk_start_ms() {
  local log_dir="${1:-logs}"
  mkdir -p "$log_dir"
  local f="${log_dir}/bulk-start-ms.env"
  export BULK_START_MS
  BULK_START_MS="$(bulk_start_ms_now)"
  BULK_START_ISO_HOST="$(date '+%Y-%m-%d %H:%M:%S %Z' 2>/dev/null || date -u '+%Y-%m-%dT%H:%M:%SZ')"
  BULK_START_ISO_NG="$(bulk_start_iso_ng)"
  BULK_START_ISO_UTC="$(TZ=UTC date '+%Y-%m-%d %H:%M:%S UTC' 2>/dev/null || date -u '+%Y-%m-%d %H:%M:%SZ')"
  cat > "$f" <<EOF
# 流水线锁定时刻（毫秒 Unix epoch）。CDC scan.startup.timestamp-millis 用此值，与时区显示无关。
# 宽表重建 / 全量耗时期间产生的 binlog，在切增量时从此时间点补读，不会丢。
# 源库/Flink 业务时区: Africa/Lagos (WAT, UTC+1)
BULK_START_MS=${BULK_START_MS}
BULK_START_ISO_NG=${BULK_START_ISO_NG}
BULK_START_ISO_UTC=${BULK_START_ISO_UTC}
BULK_START_ISO_HOST=${BULK_START_ISO_HOST}
EOF
  echo ">> 锁定 bulk-start-ms=${BULK_START_MS}"
  echo ">>   尼日利亚(WAT): ${BULK_START_ISO_NG}"
  echo ">>   UTC:           ${BULK_START_ISO_UTC}"
  echo ">>   本机:          ${BULK_START_ISO_HOST}"
  echo ">> 已写入 ${f}"
}

load_bulk_start_ms() {
  local f="${1:-logs/bulk-start-ms.env}"
  if [[ -f "$f" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$f"
    set +a
    if [[ -n "${BULK_START_MS:-}" ]]; then
      echo ">> 读取 bulk-start-ms=${BULK_START_MS} (WAT ${BULK_START_ISO_NG:-${BULK_START_ISO:-}}) ← ${f}"
      return 0
    fi
  fi
  return 1
}

resolve_bulk_start_ms() {
  local explicit="${1:-}"
  if [[ -n "$explicit" ]]; then
    export BULK_START_MS="$explicit"
    return 0
  fi
  if load_bulk_start_ms; then
    return 0
  fi
  export BULK_START_MS
  BULK_START_MS="$(bulk_start_ms_now)"
  echo ">> WARN: 无 logs/bulk-start-ms.env，临时使用 bulk-start-ms=${BULK_START_MS}"
}
