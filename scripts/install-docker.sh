#!/usr/bin/env bash
# CentOS / RHEL / Aliyun ECS 安装 Docker CE + Compose 插件
# 用法: curl -fsSL ... | bash   或  ./scripts/install-docker.sh
set -euo pipefail

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "Docker 已安装: $(docker --version)"
  docker compose version
  exit 0
fi

if [[ $EUID -ne 0 ]]; then
  echo "请用 root 执行"
  exit 1
fi

echo ">> 卸载旧版 docker（如有）..."
yum remove -y docker docker-client docker-client-latest docker-common \
  docker-latest docker-latest-logrotate docker-logrotate docker-engine 2>/dev/null || true

echo ">> 安装依赖..."
yum install -y yum-utils device-mapper-persistent-data lvm2

echo ">> 添加 Docker CE 源（阿里云镜像）..."
yum-config-manager --add-repo https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo 2>/dev/null \
  || yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo

echo ">> 安装 Docker..."
yum makecache fast || yum makecache
yum install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

echo ">> 启动 Docker..."
systemctl enable --now docker

echo ">> 验证..."
docker --version
docker compose version

echo ""
echo "安装完成。若 pull 镜像慢，可配置 /etc/docker/daemon.json 镜像加速后 systemctl restart docker"
