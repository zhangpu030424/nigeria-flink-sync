#!/bin/bash
set -euo pipefail
LOG=/data/restore.log
touch /data/.write_test 2>/dev/null && WRITABLE=1 || WRITABLE=0
if [ "$WRITABLE" = 1 ]; then rm -f /data/.write_test; else LOG=/tmp/restore.log; fi
exec > >(tee -a "$LOG") 2>&1
echo "===== $(date) start writable=$WRITABLE ====="
export DEBIAN_FRONTEND=noninteractive

install_pkgs() {
  apt-get update
  apt-get install -y wget gnupg lsb-release ca-certificates curl mysql-server mysql-client g++ make unzip
  apt-get install -y qpress || true
  if ! command -v xtrabackup >/dev/null 2>&1; then
    wget -q https://repo.percona.com/apt/percona-release_latest.generic_all.deb -O /tmp/percona-release.deb
    dpkg -i /tmp/percona-release.deb || apt-get install -y -f
    percona-release enable-only pxb-80 release || percona-release setup pxb80 || true
    apt-get update
    apt-get install -y percona-xtrabackup-80
  fi
}

install_qpress() {
  if command -v qpress >/dev/null 2>&1; then
    echo "qpress: $(command -v qpress)"
    return 0
  fi
  echo "===== $(date) compile qpress (aarch64 无现成二进制) ====="
  cd /tmp
  rm -rf qpress-src
  if wget -qO /tmp/qpress.tgz "https://github.com/PierreLvx/qpress/archive/refs/heads/master.tar.gz"; then
    mkdir -p qpress-src && tar -xzf /tmp/qpress.tgz -C qpress-src --strip-components=1
    make -C qpress-src
    install -m 0755 qpress-src/qpress /usr/local/bin/qpress
  else
    echo "ERR: 无法下载 qpress 源码"
    exit 1
  fi
  command -v qpress
}

install_pkgs
install_qpress
echo "xtrabackup: $(xtrabackup --version 2>&1 | head -1)"
echo "qpress: $(command -v qpress || echo missing)"
if [ "$WRITABLE" != 1 ]; then echo "ERR: /data 只读，无法解包"; exit 1; fi
mkdir -p /data/xb-extract /data/sql-out

XB=/data/hins100942635_data_20260816045105_qp.xb
if [ -f /data/xb-extract/xtrabackup_checkpoints ]; then
  echo "===== $(date) skip xbstream (already extracted) ====="
else
  echo "===== $(date) xbstream extract ====="
  xbstream -x < "$XB" -C /data/xb-extract
  echo "===== $(date) xbstream done ====="
fi

qp_count=$(find /data/xb-extract -name '*.qp' | wc -l | tr -d ' ')
echo "qp_count=$qp_count"
if [ "$qp_count" -gt 0 ]; then
  echo "===== $(date) decompress qp ====="
  xtrabackup --decompress --remove-original --parallel=4 --target-dir=/data/xb-extract
  leftover=$(find /data/xb-extract -name '*.qp' | wc -l | tr -d ' ')
  if [ "$leftover" -gt 0 ]; then
    echo "xtrabackup --decompress leftover=$leftover, fallback qpress"
    find /data/xb-extract -name '*.qp' -print0 | xargs -0 -P 4 -I{} bash -c 'qpress -do "$1" && rm -f "$1"' _ {}
  fi
  leftover=$(find /data/xb-extract -name '*.qp' | wc -l | tr -d ' ')
  echo "qp leftover after decompress: $leftover"
  ls -l /data/xb-extract/xtrabackup_info /data/xb-extract/xtrabackup_checkpoints
fi

echo "===== $(date) prepare ====="
xtrabackup --prepare --target-dir=/data/xb-extract
echo "===== $(date) prepare done ====="
service mysql stop || true
pkill mysqld || true
sleep 2
chown -R mysql:mysql /data/xb-extract || true
mysqld --user=mysql --datadir=/data/xb-extract --port=3307 \
  --bind-address=127.0.0.1 --skip-grant-tables --skip-log-bin \
  --innodb-use-native-aio=0 --innodb-flush-method=fsync \
  --socket=/tmp/mysql_restore.sock --pid-file=/tmp/mysql_restore.pid &
ready=0
for i in $(seq 1 90); do
  if mysql -h127.0.0.1 -P3307 -u root -e "SELECT 1" >/dev/null 2>&1; then
    echo mysqld_ready
    ready=1
    break
  fi
  echo wait_mysql_$i
  sleep 5
done
if [ "$ready" != 1 ]; then
  echo "ERR: mysqld 未起来，见 error log"
  tail -n 80 /data/xb-extract/*.err 2>/dev/null || true
  exit 1
fi
mysql -h127.0.0.1 -P3307 -u root -e "SHOW DATABASES;"
mysql -h127.0.0.1 -P3307 -u root -N -e "SHOW DATABASES;" | grep -Ev '^(information_schema|performance_schema|mysql|sys)$' | while read -r db; do
  echo "dump $db"
  mysqldump -h127.0.0.1 -P3307 -u root --single-transaction --quick --default-character-set=utf8mb4 -r "/data/sql-out/${db}.sql" "$db"
  ls -lh "/data/sql-out/${db}.sql"
done
echo "===== $(date) ALL DONE ====="
ls -lh /data/sql-out
