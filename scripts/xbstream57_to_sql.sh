#!/usr/bin/env bash
# MySQL 5.7 xbstream(quicklz) 物理备份 -> SQL
#
# 工具镜像 ng-xb57-tools:local 只构建一次，后续 extract/decompress/prepare 不再 apt install
# dump 用官方 mysql:5.7 镜像
#
# 用法:
#   ./scripts/xbstream57_to_sql.sh build-image   # 首次或升级工具时执行一次
#   ./scripts/xbstream57_to_sql.sh prepare
#   ./scripts/xbstream57_to_sql.sh dump

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# macOS 重新挂载后 /Volumes/新加卷 与 /Volumes/新加卷 1 会互换，按内容定位而非写死盘符
locate_backup_dir() {
  local name="${XB_BASENAME:-backup_235g.xbstream}" v
  for v in /Volumes/*/Ng_old; do
    [[ -e "$v/$name" || -d "$v/xb-extract" ]] && { echo "$v"; return 0; }
  done
  return 1
}

if [[ -n "${XB_INPUT:-}" ]]; then
  BACKUP_DIR="$(cd "$(dirname "$XB_INPUT")" && pwd)"
else
  BACKUP_DIR="$(locate_backup_dir)" || {
    echo "ERR: 找不到备份目录（已扫描 /Volumes/*/Ng_old）"
    echo "     外置盘是否已挂载？ls /Volumes"
    echo "     或显式指定: XB_INPUT=/path/to/backup_235g.xbstream $0 $*"
    exit 1
  }
  XB_INPUT="${BACKUP_DIR}/backup_235g.xbstream"
fi
XB_EXTRACT="${XB_EXTRACT:-${BACKUP_DIR}/xb-extract}"
SQL_OUT="${SQL_OUT:-${BACKUP_DIR}/sql-out}"
LOG_DIR="${LOG_DIR:-${BACKUP_DIR}/logs}"
DOCKER_PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
XB_IMAGE="${XB_IMAGE:-ng-xb57-tools:local}"
XB_CONTAINER="ng-xb-tools-57"
MYSQL_CONTAINER="ng-xb-mysql57"
DOCKER_VOLUME="${DOCKER_VOLUME:-ng-xb57-datadir}"
OVERLAY_VOLUME="${OVERLAY_VOLUME:-ng-xb57-overlay}"
MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:5.7}"
# overlay 需要 mount 命令；官方 mysql:5.7 是 Oracle Linux slim，不带 util-linux
MYSQL_OVERLAY_IMAGE="${MYSQL_OVERLAY_IMAGE:-mysql:5.7-debian}"
# stage 目标：默认外置 APFS 分区路径；内置盘不够时不要 STAGE_TO_VOLUME=1
STAGE_DIR="${STAGE_DIR:-${BACKUP_DIR}/../xb-staging}"
# dump datadir：stage 完成后设 MYSQL_DATADIR=$STAGE_DIR；或 EXFAT_DIRECT=1 直连 exFAT（碰运气）
MYSQL_DATADIR="${MYSQL_DATADIR:-${XB_EXTRACT}}"

log() { echo "[$(date '+%F %T')] $*"; }

count_dot_underscore() {
  find "${1:-$XB_EXTRACT}" -name '._*' 2>/dev/null | wc -l | tr -d ' '
}

fs_personality() {
  local p="$1"
  [[ -e "$p" ]] || p="$(dirname "$p")"
  diskutil info "$p" 2>/dev/null | awk -F': ' '/File System Personality/{print $2; exit}'
}

is_exfat_path() {
  [[ "$(fs_personality "$1")" == *exFAT* ]]
}

# exFAT bind mount + chown 会重建 ._ 文件；APFS/HFS+/ext4 无此问题
is_risky_exfat_bind() {
  local p="$1"
  [[ "$p" == docker:* ]] && return 1
  is_exfat_path "$p"
}

disable_usb_metadata_hint() {
  echo "提示: 可禁止 Mac 往 U 盘写 ._ 元数据（执行后注销再登录）:"
  echo "  defaults write com.apple.desktopservices DSDontWriteUSBStores -bool true"
}

require_staged_datadir() {
  echo "ERR: exFAT 上的 xb-extract 不能直接给 Docker mysqld（._ 文件导致 InnoDB 崩溃）"
  echo ""
  echo "  方案 A（推荐，不占内置盘）: 在同一块 6T 外置盘上划 ~800G APFS 分区，然后:"
  echo "    STAGE_DIR=/Volumes/你的APFS分区/xb-staging ./scripts/xbstream57_to_sql.sh stage-datadir"
  echo "    MYSQL_DATADIR=/Volumes/你的APFS分区/xb-staging ./scripts/xbstream57_to_sql.sh dump"
  echo ""
  echo "  方案 B（碰运气，不拷贝）: 禁止 Mac 写 ._ 后直连 exFAT:"
  echo "    EXFAT_DIRECT=1 ./scripts/xbstream57_to_sql.sh dump"
  echo ""
  echo "  方案 C（内置盘够大）: STAGE_TO_VOLUME=1 stage-datadir && DUMP_USE_VOLUME=1 dump"
  disable_usb_metadata_hint
  exit 1
}

# Mac 外置 exFAT 会产生 ._ 文件；Linux mysqld 无法容忍，必须在宿主机删掉
clean_mac_metadata() {
  local dir="${1:-$XB_EXTRACT}"
  [[ -d "$dir" ]] || { echo "ERR: not found: $dir"; exit 1; }

  local before after
  before=$(count_dot_underscore "$dir")
  log "clean Mac metadata in $dir (._* count=$before)"

  if [[ "$before" == "0" ]]; then
    log "no ._ files, skip"
    return 0
  fi

  if [[ "$(uname -s)" == "Darwin" ]] && command -v dot_clean >/dev/null; then
    log "dot_clean -m ..."
    dot_clean -m "$dir" 2>&1 | tail -5 || true
  fi

  log "rm ._ files on host ..."
  find "$dir" -name '._*' -print0 2>/dev/null | xargs -0 rm -f 2>/dev/null || true
  find "$dir" -name '.DS_Store' -print0 2>/dev/null | xargs -0 rm -f 2>/dev/null || true

  after=$(count_dot_underscore "$dir")
  log "._* remaining=$after"
  if [[ "$after" != "0" ]]; then
    echo "ERR: 仍有 $after 个 ._ 文件，exFAT 上 Docker mysqld 会失败"
    echo "     请改用: ./scripts/xbstream57_to_sql.sh stage-datadir && DUMP_USE_VOLUME=1 ./scripts/xbstream57_to_sql.sh dump"
    return 1
  fi
}

# rsync 到 STAGE_DIR（推荐 APFS 外置分区）或 Docker volume（占内置盘）
stage_datadir() {
  require_docker
  clean_mac_metadata || log "WARN: ._ 未清干净，stage 会 exclude 掉它们"
  mkdir -p "$LOG_DIR"

  if [[ "${STAGE_TO_VOLUME:-0}" == "1" ]]; then
    log "stage target: docker volume $DOCKER_VOLUME (占 Mac 内置 Docker 磁盘 ≥800G)"
    docker volume create "$DOCKER_VOLUME" >/dev/null
    docker run --rm --platform "$DOCKER_PLATFORM" \
      -v "$XB_EXTRACT:/src:ro" \
      -v "$DOCKER_VOLUME:/dst" \
      alpine:3.20 sh -lc '
        set -euo pipefail
        apk add --no-cache rsync
        rsync -a --info=progress2 \
          --exclude="._*" --exclude=".DS_Store" --exclude=".Trashes" \
          /src/ /dst/
        du -sh /dst
        find /dst -name "._*" | wc -l
      ' 2>&1 | tee -a "${LOG_DIR}/stage-datadir.log"
    log "stage done; run: DUMP_USE_VOLUME=1 ./scripts/xbstream57_to_sql.sh dump"
    return 0
  fi

  local dst="$STAGE_DIR"
  mkdir -p "$dst"
  if is_exfat_path "$dst"; then
    echo "ERR: STAGE_DIR 不能是 exFAT: $dst"
    echo "     请在 6T 外置盘上划 APFS 分区，例如 STAGE_DIR=/Volumes/xb-apfs/xb-staging"
    exit 1
  fi
  log "stage target: $dst ($(fs_personality "$dst"))"
  log "rsync xb-extract -> STAGE_DIR (~711G, 同盘拷贝，数小时)..."
  if command -v rsync >/dev/null; then
    COPYFILE_DISABLE=1 rsync -a --info=progress2 \
      --exclude='._*' --exclude='.DS_Store' --exclude='.Trashes' \
      "${XB_EXTRACT}/" "${dst}/" 2>&1 | tee -a "${LOG_DIR}/stage-datadir.log"
  else
    docker run --rm --platform "$DOCKER_PLATFORM" \
      -v "$XB_EXTRACT:/src:ro" \
      -v "$dst:/dst" \
      alpine:3.20 sh -lc '
        apk add --no-cache rsync
        rsync -a --info=progress2 \
          --exclude="._*" --exclude=".DS_Store" --exclude=".Trashes" \
          /src/ /dst/
      ' 2>&1 | tee -a "${LOG_DIR}/stage-datadir.log"
  fi
  du -sh "$dst"
  log "._* in stage: $(count_dot_underscore "$dst")"
  log "stage done; run: MYSQL_DATADIR=$dst ./scripts/xbstream57_to_sql.sh dump"
}

require_docker() {
  command -v docker >/dev/null || { echo "ERR: 需要 Docker Desktop"; exit 1; }
  docker info >/dev/null 2>&1 || {
    echo "ERR: Docker 未启动，请先打开 Docker Desktop"
    exit 1
  }
}

build_image() {
  require_docker
  log "building $XB_IMAGE (one-time, ~5-10 min)..."
  docker build --platform "$DOCKER_PLATFORM" \
    -t "$XB_IMAGE" \
    -f "$SCRIPT_DIR/xbstream57/Dockerfile" \
    "$SCRIPT_DIR/xbstream57"
  log "done: $XB_IMAGE"
  docker run --rm --platform "$DOCKER_PLATFORM" "$XB_IMAGE" xtrabackup --version
}

ensure_xb_image() {
  if docker image inspect "$XB_IMAGE" >/dev/null 2>&1; then
    log "use cached image $XB_IMAGE"
    return 0
  fi
  log "image $XB_IMAGE not found, building..."
  build_image
}

run_xb_container() {
  local phase="$1"
  local script="$2"
  require_docker
  ensure_xb_image
  mkdir -p "$XB_EXTRACT" "$SQL_OUT" "$LOG_DIR"

  log "phase=$phase image=$XB_IMAGE platform=$DOCKER_PLATFORM"
  log "extract_dir=$XB_EXTRACT"
  docker rm -f "$XB_CONTAINER" >/dev/null 2>&1 || true

  docker run --name "$XB_CONTAINER" --platform "$DOCKER_PLATFORM" \
    --privileged \
    -v "$XB_INPUT:/backup/input.xbstream:ro" \
    -v "$XB_EXTRACT:/data/xb-extract" \
    -v "$SQL_OUT:/data/sql-out" \
    -v "$LOG_DIR:/data/logs" \
    "$XB_IMAGE" bash -lc "$script" \
    2>&1 | tee -a "${LOG_DIR}/${phase}.log"
}

progress_watch() {
  cat <<'EOS'
watch_progress() {
  local dir="$1"
  local label="$2"
  while true; do
    if [[ -d "$dir" ]]; then
      local sz files
      sz=$(du -sh "$dir" 2>/dev/null | awk '{print $1}')
      files=$(find "$dir" -type f 2>/dev/null | wc -l | tr -d ' ')
      echo "[$(date '+%F %T')] $label: size=$sz files=$files"
    fi
    sleep 30
  done
}
EOS
}

phase_extract() {
  run_xb_container extract "$(progress_watch)
$(cat <<'EOS'
set -euo pipefail
EX=/data/xb-extract
INPUT=/backup/input.xbstream

if [[ -f "$EX/xtrabackup_info" || -f "$EX/xtrabackup_info.qp" ]]; then
  echo "skip extract: xtrabackup_info already exists"
  ls -lh "$EX" | head
  exit 0
fi

echo "===== xbstream extract start ====="
echo "input=$INPUT size=$(ls -lh "$INPUT" | awk '{print $5}')"
mkdir -p "$EX"

watch_progress "$EX" "extract" &
WPID=$!
trap 'kill $WPID 2>/dev/null || true' EXIT

xbstream -x -C "$EX" < "$INPUT"

kill $WPID 2>/dev/null || true
trap - EXIT

echo "===== extract done $(date) ====="
du -sh "$EX"
find "$EX" -type f | wc -l

if [[ -f "$EX/xtrabackup_info" ]]; then
  grep -E 'server_version|tool_version|compressed' "$EX/xtrabackup_info" || true
elif [[ -f "$EX/xtrabackup_info.qp" ]]; then
  echo "xtrabackup_info.qp present (need decompress next)"
fi
QP=$(find "$EX" -name '*.qp' 2>/dev/null | wc -l | tr -d ' ')
echo "qp_files=$QP"
EOS
)"
}

phase_decompress() {
  run_xb_container decompress "$(cat <<'EOS'
set -euo pipefail
EX=/data/xb-extract
QP=$(find "$EX" -name '*.qp' 2>/dev/null | wc -l | tr -d ' ')
echo "qp_count=$QP"
if [[ "$QP" == "0" ]]; then
  echo "skip decompress: no .qp files"
  exit 0
fi
echo "===== decompress start $(date) ====="
xtrabackup --decompress --remove-original --parallel=4 --target-dir="$EX"
echo "===== decompress done $(date) ====="
du -sh "$EX"
EOS
)"
}

phase_prepare() {
  clean_mac_metadata
  run_xb_container prepare "$(cat <<'EOS'
set -euo pipefail
EX=/data/xb-extract
echo "===== prepare start $(date) ====="
xtrabackup --prepare --target-dir="$EX"
echo "===== prepare done $(date) ====="
grep -E 'server_version|tool_version' "$EX/xtrabackup_info" || true
EOS
)"
}

phase_dump() {
  require_docker
  mkdir -p "$SQL_OUT" "$LOG_DIR"

  # OVERLAY=1：exFAT 作只读 lowerdir，写入落到内置盘的小 upperdir，不拷 711G
  if [[ "${OVERLAY:-0}" == "1" ]]; then
    phase_dump_overlay
    return $?
  fi

  local datadir_spec="$MYSQL_DATADIR"
  if [[ "${DUMP_USE_VOLUME:-0}" == "1" ]]; then
    datadir_spec="docker:${DOCKER_VOLUME}"
  fi

  if is_risky_exfat_bind "$datadir_spec" && [[ "${EXFAT_DIRECT:-0}" != "1" ]]; then
    require_staged_datadir
  fi

  if [[ "${EXFAT_DIRECT:-0}" == "1" ]]; then
    log "EXFAT_DIRECT=1: 不 chown + InnoDB 只读，避免 Docker 写 ._ 元数据"
    clean_mac_metadata || true
    disable_usb_metadata_hint
  fi

  if [[ "$datadir_spec" == docker:* ]]; then
    local vol="${datadir_spec#docker:}"
    if ! docker volume inspect "$vol" >/dev/null 2>&1; then
      echo "ERR: Docker volume '$vol' 不存在，请先: ./scripts/xbstream57_to_sql.sh stage-datadir"
      exit 1
    fi
    if ! docker run --rm -v "${vol}:/d:ro" alpine:3.20 test -d /d/ng_loan_core; then
      echo "ERR: volume '$vol' 里没有 ng_loan_core（空卷或未 stage）"
      echo "     直接用 xb-extract: unset MYSQL_DATADIR DUMP_USE_VOLUME && ./scripts/xbstream57_to_sql.sh dump"
      echo "     或先 stage: ./scripts/xbstream57_to_sql.sh stage-datadir && DUMP_USE_VOLUME=1 ./scripts/xbstream57_to_sql.sh dump"
      exit 1
    fi
  elif [[ ! -d "$datadir_spec/ng_loan_core" ]]; then
    echo "ERR: datadir 缺少 ng_loan_core: $datadir_spec"
    echo "     确认 prepare 已完成: ls $datadir_spec"
    exit 1
  fi

  # 挂载源可能含空格（/Volumes/新加卷 1/...），必须整体引用，不能靠 $var 拆词
  local datadir_src="$datadir_spec"
  [[ "$datadir_spec" == docker:* ]] && datadir_src="${datadir_spec#docker:}"

  log "phase=dump datadir=$datadir_spec image=$MYSQL_IMAGE"
  docker rm -f "$MYSQL_CONTAINER" >/dev/null 2>&1 || true

  local skip_chown=0 read_only=0
  if [[ "${EXFAT_DIRECT:-0}" == "1" ]]; then
    skip_chown=1
    read_only=1
  fi

  docker run --name "$MYSQL_CONTAINER" --platform "$DOCKER_PLATFORM" \
    -e "SKIP_CHOWN=${skip_chown}" \
    -e "INNODB_READ_ONLY=${read_only}" \
    -v "${datadir_src}:/var/lib/mysql" \
    -v "$SQL_OUT:/sql-out" \
    -v "$LOG_DIR:/logs" \
    "$MYSQL_IMAGE" bash -lc "$(dump_inner_script)" \
    2>&1 | tee -a "${LOG_DIR}/dump.log"
}

# exFAT 只读 lower + 内置盘 upper：InnoDB 的写全落 upperdir，exFAT 一个字节都不改
phase_dump_overlay() {
  require_docker
  mkdir -p "$SQL_OUT" "$LOG_DIR"

  [[ -d "$XB_EXTRACT/ng_loan_core" ]] || {
    echo "ERR: $XB_EXTRACT 里没有 ng_loan_core，prepare 未完成？"
    exit 1
  }

  clean_mac_metadata || log "WARN: 宿主机 ._ 未清净，overlay 会用 whiteout 屏蔽"
  docker volume create "$OVERLAY_VOLUME" >/dev/null

  # 默认只读：InnoDB 不写任何数据文件，upperdir 只放 ._ whiteout（几 KB）
  # OVERLAY_RW=1 才允许写（ibdata1/ib_logfile copy-up 约 6G，仍远小于 711G）
  local read_only=1
  [[ "${OVERLAY_RW:-0}" == "1" ]] && read_only=0

  log "phase=dump mode=overlay read_only=$read_only lower=$XB_EXTRACT upper=docker:$OVERLAY_VOLUME"
  docker rm -f "$MYSQL_CONTAINER" >/dev/null 2>&1 || true

  docker run --name "$MYSQL_CONTAINER" --platform "$DOCKER_PLATFORM" \
    --privileged \
    -e "SKIP_CHOWN=1" \
    -e "INNODB_READ_ONLY=${read_only}" \
    -e "USE_OVERLAY=1" \
    -v "$XB_EXTRACT:/lower:ro" \
    -v "$OVERLAY_VOLUME:/ovl" \
    -v "$SQL_OUT:/sql-out" \
    -v "$LOG_DIR:/logs" \
    "$MYSQL_OVERLAY_IMAGE" bash -lc "$(dump_inner_script)" \
    2>&1 | tee -a "${LOG_DIR}/dump.log"
}

dump_inner_script() {
  cat <<'INNER'
set -euo pipefail
SOCK=/tmp/mysql_restore.sock
LOG=/logs/mysqld.log
MYSQLD_USER=mysql

step() { echo ""; echo "===== $(date "+%F %T") $* ====="; }

if [[ "${USE_OVERLAY:-0}" == "1" ]]; then
  step "mount overlayfs (lower=exFAT ro, upper=docker volume)"
  if ! command -v mount >/dev/null 2>&1; then
    echo "mount 不存在，尝试安装 util-linux ..."
    (apt-get update -qq && apt-get install -y -qq util-linux) >/dev/null 2>&1 \
      || (microdnf install -y util-linux) >/dev/null 2>&1 \
      || (yum install -y util-linux) >/dev/null 2>&1 || true
  fi
  command -v mount >/dev/null 2>&1 || {
    echo "ERR: 容器内没有 mount 命令，且安装失败"
    echo "     该镜像不支持 overlay，请改用: EXFAT_DIRECT=1 ./scripts/xbstream57_to_sql.sh dump"
    exit 1
  }

  mkdir -p /ovl/upper /ovl/work /var/lib/mysql
  mount -t overlay overlay \
    -o lowerdir=/lower,upperdir=/ovl/upper,workdir=/ovl/work \
    /var/lib/mysql || {
    echo "ERR: overlayfs 挂载失败（内核不支持 fuse/virtiofs 作为 lowerdir？）"
    echo "     改用: EXFAT_DIRECT=1 ./scripts/xbstream57_to_sql.sh dump"
    exit 1
  }
  echo "overlay mounted; lower files=$(ls -1 /var/lib/mysql | wc -l)"

  # 后台盯 upperdir，超过 UPPER_LIMIT_GB 就杀 mysqld，避免撑爆内置盘
  UPPER_LIMIT_GB="${UPPER_LIMIT_GB:-60}"
  (
    while sleep 60; do
      used_kb=$(du -sk /ovl/upper 2>/dev/null | awk '{print $1}')
      used_gb=$(( ${used_kb:-0} / 1048576 ))
      echo "[upperdir] ${used_gb}G / ${UPPER_LIMIT_GB}G (内置盘占用)"
      if (( used_gb >= UPPER_LIMIT_GB )); then
        echo "ERR: upperdir 超过 ${UPPER_LIMIT_GB}G，停止以保护内置盘"
        pkill -9 mysqld 2>/dev/null || true
        break
      fi
    done
  ) &
  UPPER_WATCH=$!
  trap 'kill $UPPER_WATCH 2>/dev/null || true' EXIT
fi

if [[ "${SKIP_CHOWN:-0}" == "1" ]]; then
  step "skip chown (避免 Docker 在 exFAT 写 ._ 元数据)"
  MYSQLD_USER=root
else
  step "fix permissions"
  chown -R mysql:mysql /var/lib/mysql
fi

step "purge AppleDouble (._*) before mysqld"
find /var/lib/mysql -name '._*' -delete 2>/dev/null || true
dot_count=$(find /var/lib/mysql -name '._*' 2>/dev/null | wc -l | tr -d ' ')
echo "._* in datadir=$dot_count"
if [[ "$dot_count" != "0" ]]; then
  echo "ERR: datadir 仍有 ._ 文件，mysqld 无法启动"
  find /var/lib/mysql -maxdepth 2 -name '._*' 2>/dev/null | head -20
  exit 1
fi

is_system_db() {
  case "$1" in
    information_schema|mysql|performance_schema|sys) return 0 ;;
    __*) return 0 ;;
    '#'*) return 0 ;;
    *) return 1 ;;
  esac
}

step "build restore.cnf from backup-my.cnf"
CNF=/tmp/restore.cnf
BACKUP_CNF=/var/lib/mysql/backup-my.cnf

{
  echo "[mysqld]"
  # 必须沿用备份的 redo/表空间几何，否则 InnoDB 会尝试 resize（只读模式下直接失败）
  if [[ -f "$BACKUP_CNF" ]]; then
    grep -E '^[[:space:]]*(innodb_data_file_path|innodb_log_files_in_group|innodb_log_file_size|innodb_page_size|innodb_checksum_algorithm|innodb_undo_directory|innodb_undo_tablespaces)[[:space:]]*=' \
      "$BACKUP_CNF" || true
  fi
  echo "datadir=/var/lib/mysql"
  echo "socket=${SOCK}"
  echo "tmpdir=/tmp"
  echo "user=${MYSQLD_USER}"
  echo "skip-grant-tables"
  echo "skip-slave-start"
  echo "skip-log-bin"
  echo "performance_schema=0"
  echo "innodb_use_native_aio=0"
  echo "innodb_flush_method=fsync"
  echo "lower_case_table_names=1"
  echo "innodb_buffer_pool_size=2G"
  [[ "${INNODB_READ_ONLY:-0}" == "1" ]] && echo "innodb_read_only=1"
} > "$CNF"

echo "--- restore.cnf ---"
cat "$CNF"

step "start mysqld 5.7 (user=$MYSQLD_USER read_only=${INNODB_READ_ONLY:-0})"
mysqld --defaults-file="$CNF" > "$LOG" 2>&1 &
MPID=$!
echo "mysqld pid=$MPID"

for i in $(seq 1 180); do
  if mysqladmin --socket="$SOCK" ping >/dev/null 2>&1; then
    echo "mysqld_ready after ${i} checks"
    break
  fi
  echo "wait_mysql_$i"
  sleep 2
done
mysqladmin --socket="$SOCK" ping >/dev/null 2>&1 || {
  echo "ERR: mysqld 未就绪"
  tail -80 "$LOG"
  exit 1
}

mysql --socket="$SOCK" -e "SELECT VERSION(); SHOW DATABASES;"
echo "datadir dirs (top 30):"
ls -1 /var/lib/mysql | head -30

dump_count=0
while IFS= read -r db; do
  [[ -z "$db" ]] && continue
  is_system_db "$db" && continue
  out="/sql-out/${db}.sql"
  step "dump $db"
  mysqldump --socket="$SOCK" -uroot \
    --single-transaction --quick --hex-blob --routines --triggers --events \
    --set-gtid-purged=OFF \
    --default-character-set=utf8mb4 \
    "$db" > "$out"
  ls -lh "$out"
  dump_count=$((dump_count + 1))
done < <(mysql --socket="$SOCK" -N -e "SHOW DATABASES")

if [[ "$dump_count" == "0" ]]; then
  echo "ERR: 未发现业务库（SHOW DATABASES 只有系统库）"
  echo "     检查 datadir 是否 prepare 完成、stage-datadir 是否完整"
  echo "     期望看到 ng_loan_core / ng_loan_market 等目录"
  ls -1 /var/lib/mysql
  exit 1
fi
echo "dumped_databases=$dump_count"

step "ALL DONE"
ls -lh /sql-out
if [[ "${USE_OVERLAY:-0}" == "1" ]]; then
  echo "upperdir 最终占用（内置盘）: $(du -sh /ovl/upper 2>/dev/null | awk '{print $1}')"
fi
kill "$MPID" 2>/dev/null || true
INNER
}

main() {
  local cmd="${1:-all}"
  case "$cmd" in
    build-image)     build_image ;;
    clean-metadata)  clean_mac_metadata ;;
    stage-datadir)   stage_datadir ;;
    extract)         phase_extract ;;
    decompress)      phase_decompress ;;
    prepare)         phase_prepare ;;
    dump)            phase_dump ;;
    all)         phase_extract; phase_decompress; phase_prepare; phase_dump ;;
    -h|--help)
      sed -n '1,12p' "$0"
      echo "  build-image     构建本地工具镜像（首次一次）"
      echo "  clean-metadata  删除 Mac ._ 元数据文件"
      echo "  stage-datadir   rsync 到 STAGE_DIR（推荐外置 APFS 分区，不占内置盘）"
      echo "  dump            导出 SQL"
      echo ""
      echo "exFAT 上不拷贝 711G 的两种 dump 方式（优先试）:"
      echo "  OVERLAY=1     ./scripts/xbstream57_to_sql.sh dump   # 写入落内置盘小 upperdir"
      echo "  EXFAT_DIRECT=1 ./scripts/xbstream57_to_sql.sh dump  # InnoDB 只读，零写入"
      echo ""
      echo "要拷贝时（外置 APFS 分区，不占内置盘）:"
      echo "  STAGE_DIR=/Volumes/APFS名/xb-staging ./scripts/xbstream57_to_sql.sh stage-datadir"
      echo "  MYSQL_DATADIR=/Volumes/APFS名/xb-staging ./scripts/xbstream57_to_sql.sh dump"
      ;;
    *) echo "unknown: $cmd"; exit 1 ;;
  esac
}

main "$@"
