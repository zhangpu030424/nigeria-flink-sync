FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y wget curl ca-certificates gnupg lsb-release \
    g++ make rsync psmisc procps file git \
 && wget -O /tmp/percona-release.deb https://repo.percona.com/apt/percona-release_latest.generic_all.deb \
 && dpkg -i /tmp/percona-release.deb \
 && percona-release disable all \
 && percona-release enable pxb-24 release \
 && apt-get update \
 && apt-get install -y percona-xtrabackup-24 \
 && rm -rf /tmp/qpress-src \
 && git clone --depth 1 https://github.com/PierreLvx/qpress.git /tmp/qpress-src \
 && make -C /tmp/qpress-src \
 && install -m 0755 /tmp/qpress-src/qpress /usr/local/bin/qpress \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* /tmp/qpress-src /tmp/percona-release.deb

RUN xtrabackup --version && qpress 2>&1 | head -1 || true
