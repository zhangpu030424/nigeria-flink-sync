#!/bin/bash
# 从已 prepare 的 xb-extract 起 mysqld 并 dump（跳过解包/解压/prepare）
set -euo pipefail
LOG=/data/restore_dump.log
exec > >(tee -a "$LOG") 2>&1
echo "===== $(date) dump-only start ====="

DATADIR=/data/xb-extract
SQL_OUT=/data/sql-out
ERRLOG=/tmp/mysqld_restore.err
SOCK=/tmp/mysql_restore.sock
PID=/tmp/mysql_restore.pid
# 只 dump 业务库；__recycle_bin__ 可手动加
DUMP_DBS=(nigeria_backend nigeria_admin nigeria_event spug)

mkdir -p "$SQL_OUT"
service mysql stop 2>/dev/null || true
pkill mysqld 2>/dev/null || true
sleep 2
rm -f "$SOCK" "$PID" "$ERRLOG"

dump_complete() {
  local f="$1"
  [[ -f "$f" ]] || return 1
  tail -c 512 "$f" | grep -q 'Dump completed on'
}

cat >/tmp/restore.cnf <<'EOF'
[mysqld]
datadir=/data/xb-extract
port=3307
bind-address=127.0.0.1
socket=/tmp/mysql_restore.sock
pid-file=/tmp/mysql_restore.pid
user=mysql
skip-grant-tables
skip-log-bin
skip-slave-start
innodb_use_native_aio=0
innodb_flush_method=fsync
innodb_buffer_pool_size=1G
innodb_log_buffer_size=64M
innodb_doublewrite=0
lower_case_table_names=1
character-set-server=utf8mb4
log-error=/tmp/mysqld_restore.err
EOF

chown -R mysql:mysql "$DATADIR" 2>/dev/null || true

echo "===== $(date) starting mysqld (socket=$SOCK) ====="
mysqld --defaults-file=/tmp/restore.cnf &
MYSQLD_PID=$!
echo "mysqld pid=$MYSQLD_PID"

ready=0
for i in $(seq 1 120); do
  if mysql --socket="$SOCK" -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo mysqld_ready_socket
    ready=1
    break
  fi
  if ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    echo "ERR: mysqld 进程已退出"
    tail -n 120 "$ERRLOG" || true
    exit 1
  fi
  echo "wait_mysql_$i"
  sleep 3
done

if [ "$ready" != 1 ]; then
  echo "ERR: mysqld 未就绪"
  tail -n 120 "$ERRLOG" || true
  exit 1
fi

mysql --socket="$SOCK" -u root -e "SHOW DATABASES;"

for db in "${DUMP_DBS[@]}"; do
  out="${SQL_OUT}/${db}.sql"
  if dump_complete "$out"; then
    echo "skip complete dump: $out ($(ls -lh "$out" | awk '{print $5}'))"
    continue
  fi
  if [[ -f "$out" ]]; then
    echo "redo incomplete dump: $out (was $(ls -lh "$out" | awk '{print $5}'))"
    rm -f "$out"
  fi
  echo "===== $(date) dump $db ====="
  mysqldump --socket="$SOCK" -u root \
    --single-transaction --quick --routines --triggers --events \
    --default-character-set=utf8mb4 \
    -r "$out" "$db"
  ls -lh "$out"
done

echo "===== $(date) ALL DONE ====="
ls -lh "$SQL_OUT"

# 保持容器不退出，便于 docker logs 观察
tail -f /dev/null
