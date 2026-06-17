#!/bin/bash
set -e

# 进入发布目录
cd ${SPUG_DST_DIR}

echo "=========================================="
echo "发布后操作（nigeria-backend-api）"
echo "=========================================="
echo "当前目录: $(pwd)"
echo "部署路径: ${SPUG_DST_DIR}"

# 验证部署路径
if [ ! -d "${SPUG_DST_DIR}" ]; then
    echo "❌ 错误: 部署路径不存在: ${SPUG_DST_DIR}"
    exit 1
fi

# 调试：显示目录内容

echo ""
echo "=========================================="
echo "调试信息"
echo "=========================================="
echo "  当前目录: $(pwd)"
echo "  部署路径: ${SPUG_DST_DIR}"
echo "  目录内容:"
ls -la | head -20
echo ""

# 检查 Git 仓库状态
echo "Git 仓库信息："
if [ -d ".git" ]; then
    echo "  ✅ 是 Git 仓库"
    echo "  当前分支: $(git branch --show-current 2>/dev/null || echo 'unknown')"
    echo "  最新提交: $(git log -1 --oneline 2>/dev/null || echo 'no commits')"
    echo "  Dockerfile 在 Git 中: $(git ls-files Dockerfile 2>/dev/null | head -1 || echo 'not found')"
else
    echo "  ⚠️  不是 Git 仓库或 .git 目录不存在"
fi
echo ""

# 处理符号链接：切换到实际路径
if [ -L "${SPUG_DST_DIR}" ]; then
    REAL_DIR=$(readlink -f "${SPUG_DST_DIR}")
    echo ""
    echo "检测到发布路径是符号链接"
    echo "  符号链接: ${SPUG_DST_DIR}"
    echo "  实际路径: ${REAL_DIR}"
    cd "${REAL_DIR}"
    echo "✅ 已切换到实际路径: $(pwd)"
    echo ""
fi

# 如果当前目录不是 Git 仓库，但 Spug 可能已经拉取了代码，尝试初始化 Git 并获取 Dockerfile
if [ ! -d ".git" ] && [ ! -f "Dockerfile" ]; then
    echo ""
    echo "检测到不是 Git 仓库，尝试手动获取 Dockerfile..."
    GIT_REPO_URL="https://github.com/EstNgTeam/nigeria-backend-api.git"
    GIT_BRANCH="${SPUG_GIT_BRANCH:-main}"

    # 方法1: 尝试使用 git archive（如果 git 可用）
    if command -v git &> /dev/null; then
        echo "  尝试使用 git archive 获取 Dockerfile..."
        TEMP_DIR=$(mktemp -d)
        if git archive --remote="${GIT_REPO_URL}" "${GIT_BRANCH}" Dockerfile 2>/dev/null | tar -x -C "${TEMP_DIR}" 2>/dev/null; then
            if [ -f "${TEMP_DIR}/Dockerfile" ]; then
                cp "${TEMP_DIR}/Dockerfile" ./Dockerfile
                rm -rf "${TEMP_DIR}"
                echo "✅ 使用 git archive 成功获取 Dockerfile"
            fi
        else
            rm -rf "${TEMP_DIR}"
            echo "  ⚠️  git archive 失败（可能需要认证）"
        fi
    fi
fi

# 查找 Dockerfile
if [ ! -f "Dockerfile" ]; then
    echo ""
    echo "⚠️  当前目录未找到 Dockerfile，尝试查找..."

    # 方法1: 在当前目录及子目录中查找
    DOCKERFILE_PATH=$(find . -maxdepth 3 -name "Dockerfile" -type f 2>/dev/null | head -1)
    if [ -n "$DOCKERFILE_PATH" ]; then
        DOCKERFILE_DIR=$(dirname "$DOCKERFILE_PATH")
        cd "$DOCKERFILE_DIR"
        echo "✅ 找到 Dockerfile，切换到目录: $(pwd)"
    else
        # 方法2: 如果是 Git 仓库，尝试从 Git 中检出
        if [ -d ".git" ]; then
            echo "  方法2: 尝试从 Git 仓库检出 Dockerfile..."
            if git checkout HEAD -- Dockerfile 2>/dev/null; then
                echo "✅ 从 Git 仓库成功检出 Dockerfile"
            else
                echo "❌ 无法从 Git 仓库检出 Dockerfile，尝试其他方法..."
            fi
        fi

        # 方法3: 尝试从 GitHub 直接下载（无论是否有 Git 仓库都尝试）
        if [ ! -f "Dockerfile" ]; then
            echo "  方法3: 尝试从 GitHub 直接下载 Dockerfile..."
            GIT_REPO_URL="https://github.com/EstNgTeam/nigeria-backend-api.git"
            GIT_BRANCH="${SPUG_GIT_BRANCH:-main}"

            # 尝试使用 curl 从 GitHub 下载 Dockerfile
            DOCKERFILE_URL="https://raw.githubusercontent.com/EstNgTeam/nigeria-backend-api/${GIT_BRANCH}/Dockerfile"
            echo "    下载地址: ${DOCKERFILE_URL}"

            # 检查 curl 是否可用
            if ! command -v curl &> /dev/null; then
                echo "    ⚠️  curl 命令不可用，尝试使用 wget..."
                if command -v wget &> /dev/null; then
                    if wget -q -O Dockerfile "${DOCKERFILE_URL}" 2>/dev/null; then
                        echo "✅ 使用 wget 从 GitHub 成功下载 Dockerfile"
                    else
                        echo "❌ 使用 wget 无法从 GitHub 下载 Dockerfile"
                    fi
                else
                    echo "❌ curl 和 wget 都不可用，无法下载 Dockerfile"
                fi
            else
                if curl -f -s -o Dockerfile "${DOCKERFILE_URL}" 2>/dev/null; then
                    echo "✅ 使用 curl 从 GitHub 成功下载 Dockerfile"
                else
                    echo "❌ 使用 curl 无法从 GitHub 下载 Dockerfile"
                    echo "    可能的原因："
                    echo "    1. GitHub 仓库是私有的（需要认证）"
                    echo "    2. 网络连接问题"
                    echo "    3. 分支名称不正确（当前: ${GIT_BRANCH}）"
                fi
            fi
        fi

        # 再次检查
        if [ ! -f "Dockerfile" ]; then
            echo ""
            echo "❌ 错误: 未找到 Dockerfile"
            echo "   当前目录: $(pwd)"
            echo "   目录内容:"
            ls -la
            echo ""
            echo "   实际路径内容（如果不同）:"
            if [ -L "${SPUG_DST_DIR}" ]; then
                REAL_DIR=$(readlink -f "${SPUG_DST_DIR}")
                echo "   实际路径: ${REAL_DIR}"
                if [ -d "${REAL_DIR}" ]; then
                    ls -la "${REAL_DIR}" | head -20
                fi
            fi
            echo ""
            echo "   Git 状态:"
            if [ -d ".git" ]; then
                git status --short 2>/dev/null || echo "  无法获取 Git 状态"
            else
                echo "  不是 Git 仓库"
            fi
            echo ""
            echo "   请检查："
            echo "   1. Git 仓库是否包含 Dockerfile"
            echo "   2. Spug 是否正确拉取了代码"
            echo "   3. 发布路径配置是否正确"
            echo "   4. 是否有文件过滤规则"
            echo "   5. Spug 的 Git 拉取配置是否正确"
            exit 1
        fi
    fi
fi

echo "✅ 找到 Dockerfile: $(pwd)/Dockerfile"

# ==========================================
# 构建 Docker 镜像
# ==========================================
echo ""
echo "=========================================="
echo "构建 Docker 镜像"
echo "=========================================="

IMAGE_NAME="nigeria-backend-api"
SPUG_ENV="${SPUG_ENV:-cas}"

# 生成镜像标签：使用环境 + 时间戳，便于版本管理和回滚
TIMESTAMP=$(date +%Y%m%d%H%M%S)
IMAGE_TAG="${SPUG_ENV}-${TIMESTAMP}"
IMAGE_TAG_LATEST="${SPUG_ENV}-latest"

echo "构建镜像标签: ${IMAGE_NAME}:${IMAGE_TAG}"
echo "最新标签: ${IMAGE_NAME}:${IMAGE_TAG_LATEST}"

# ==========================================
# 复制统一的 Common 模块到项目目录
# ==========================================
echo ""
echo "=========================================="
echo "复制统一的 Common 模块"
echo "=========================================="

# 获取当前工作目录（可能是实际路径）
CURRENT_DIR=$(pwd)
echo "当前工作目录: ${CURRENT_DIR}"

# 确定 Common 模块的路径（按优先级查找，使用绝对路径）
COMMON_MODULE_PATH=""

# 优先级1: 固定路径（Spug 部署时使用，最可靠）
if [ -d "/www/nigeria-common" ]; then
    COMMON_MODULE_PATH="/www/nigeria-common"
    echo "✅ 优先级1: 找到 Common 模块在固定路径: ${COMMON_MODULE_PATH}"
# 优先级2: 项目目录的上一级（使用绝对路径）
elif [ -d "${CURRENT_DIR}/../nigeria-common" ]; then
    COMMON_MODULE_PATH=$(readlink -f "${CURRENT_DIR}/../nigeria-common")
    echo "✅ 优先级2: 找到 Common 模块在上一级: ${COMMON_MODULE_PATH}"
# 优先级3: 项目目录的上两级（使用绝对路径）
elif [ -d "${CURRENT_DIR}/../../nigeria-common" ]; then
    COMMON_MODULE_PATH=$(readlink -f "${CURRENT_DIR}/../../nigeria-common")
    echo "✅ 优先级3: 找到 Common 模块在上两级: ${COMMON_MODULE_PATH}"
# 优先级4: 使用 SPUG_DST_DIR 的上一级（如果 SPUG_DST_DIR 是符号链接，使用其实际路径的上一级）
elif [ -n "${SPUG_DST_DIR}" ]; then
    if [ -L "${SPUG_DST_DIR}" ]; then
        REAL_DST_DIR=$(readlink -f "${SPUG_DST_DIR}")
        if [ -d "${REAL_DST_DIR}/../nigeria-common" ]; then
            COMMON_MODULE_PATH=$(readlink -f "${REAL_DST_DIR}/../nigeria-common")
            echo "✅ 优先级4: 找到 Common 模块在发布路径的上一级: ${COMMON_MODULE_PATH}"
        fi
elif [ -d "${SPUG_DST_DIR}/../nigeria-common" ]; then
        COMMON_MODULE_PATH=$(readlink -f "${SPUG_DST_DIR}/../nigeria-common")
        echo "✅ 优先级4: 找到 Common 模块在发布路径的上一级: ${COMMON_MODULE_PATH}"
    fi
fi

# 如果还没找到，尝试更多可能的路径
if [ -z "$COMMON_MODULE_PATH" ]; then
    echo ""
    echo "⚠️  未在常见路径找到 Common 模块，尝试其他位置..."
    echo "   检查的路径："
    echo "   - /www/nigeria-common"
    echo "   - ${CURRENT_DIR}/../nigeria-common"
    echo "   - ${CURRENT_DIR}/../../nigeria-common"
    if [ -n "${SPUG_DST_DIR}" ]; then
        echo "   - ${SPUG_DST_DIR}/../nigeria-common"
    fi

    # 尝试查找项目根目录（工作区根目录）
    WORKSPACE_ROOT="/www"
    if [ -d "${WORKSPACE_ROOT}/nigeria-common" ]; then
        COMMON_MODULE_PATH="${WORKSPACE_ROOT}/nigeria-common"
        echo "✅ 在工作区根目录找到: ${COMMON_MODULE_PATH}"
    fi
fi

# 验证找到的 Common 模块是否有效
if [ -n "$COMMON_MODULE_PATH" ] && [ -d "$COMMON_MODULE_PATH" ]; then
    # 检查 Common 模块是否包含必要的文件
    if [ ! -f "${COMMON_MODULE_PATH}/pom.xml" ]; then
        echo "⚠️  警告: ${COMMON_MODULE_PATH} 存在但不是有效的 Common 模块（缺少 pom.xml）"
    COMMON_MODULE_PATH=""
    else
        echo ""
        echo "✅ 找到统一的 Common 模块: ${COMMON_MODULE_PATH}"
        echo "   验证 Common 模块结构..."
        if [ -d "${COMMON_MODULE_PATH}/common-core" ] && [ -d "${COMMON_MODULE_PATH}/common-web" ]; then
            echo "   ✅ Common 模块结构正确"
        else
            echo "   ⚠️  警告: Common 模块结构可能不完整"
        fi
    fi
fi

# 复制或使用 Common 模块
if [ -n "$COMMON_MODULE_PATH" ] && [ -d "$COMMON_MODULE_PATH" ]; then
    # 删除项目内旧的 Common 模块（如果存在）
    if [ -d "nigeria-common" ]; then
        echo ""
        echo "删除项目内旧的 Common 模块..."
        rm -rf nigeria-common
    fi

    # 复制统一的 Common 模块到项目目录
    echo ""
    echo "复制统一的 Common 模块到项目目录..."
    echo "   源路径: ${COMMON_MODULE_PATH}"
    echo "   目标路径: ${CURRENT_DIR}/nigeria-common"

    # 使用 rsync 复制（排除 target 目录和构建产物，确保所有源文件被复制）
    if command -v rsync &> /dev/null; then
        echo "   使用 rsync 复制（排除构建产物）..."
        if rsync -av --exclude='target' --exclude='.git' --exclude='*.iml' --exclude='.idea' \
            "${COMMON_MODULE_PATH}/" ./nigeria-common/; then
            echo "✅ Common 模块复制完成"
        else
            echo "❌ 错误: rsync 复制失败，尝试使用 cp..."
    if cp -r "${COMMON_MODULE_PATH}" ./nigeria-common; then
                # 删除 target 目录以节省空间
                find ./nigeria-common -type d -name "target" -exec rm -rf {} + 2>/dev/null || true
                echo "✅ Common 模块复制完成（使用 cp）"
            else
                echo "❌ 错误: Common 模块复制失败"
                exit 1
            fi
        fi
    else
        echo "   使用 cp 复制..."
        if cp -r "${COMMON_MODULE_PATH}" ./nigeria-common; then
            # 删除 target 目录以节省空间
            find ./nigeria-common -type d -name "target" -exec rm -rf {} + 2>/dev/null || true
    echo "✅ Common 模块复制完成"
        else
            echo "❌ 错误: Common 模块复制失败"
            exit 1
        fi
    fi

    # 验证复制是否成功（检查所有必需的 pom.xml 文件）
    echo ""
    echo "验证 Common 模块复制结果..."
        if [ -d "nigeria-common" ] && [ -f "nigeria-common/pom.xml" ]; then
        echo "✅ 根 pom.xml 存在"

        # 检查所有必需的子模块
        REQUIRED_MODULES=("common-core" "common-web" "common-security" "common-database" "common-oss" "common-sms")
        MISSING_MODULES=()

        for module in "${REQUIRED_MODULES[@]}"; do
            if [ -f "nigeria-common/${module}/pom.xml" ]; then
                echo "✅ ${module}/pom.xml 存在"
            else
                echo "❌ ${module}/pom.xml 缺失"
                MISSING_MODULES+=("${module}")
            fi
        done

        if [ ${#MISSING_MODULES[@]} -eq 0 ]; then
            echo "✅ 验证通过: 所有必需的 pom.xml 文件都已复制"
            echo "   Common 模块内容:"
            ls -la nigeria-common/ | head -10
else
            echo "❌ 错误: 以下模块的 pom.xml 文件缺失: ${MISSING_MODULES[*]}"
            echo "   请检查源路径 ${COMMON_MODULE_PATH} 是否包含完整的 Common 模块"
        exit 1
    fi
    else
        echo "❌ 错误: Common 模块复制后验证失败"
        exit 1
    fi
else
    echo ""
    echo "⚠️  未找到统一的 Common 模块，尝试使用项目内的 Common 模块（如果存在）"
    if [ -d "nigeria-common" ] && [ -f "nigeria-common/pom.xml" ]; then
        echo "✅ 使用项目内的 Common 模块"
    else
        echo ""
        echo "⚠️  未找到 Common 模块，尝试自动拉取..."

        # 尝试自动从 Git 拉取 Common 模块到固定路径
        COMMON_GIT_REPO="${NIGERIA_COMMON_GIT_REPO:-}"
        COMMON_GIT_BRANCH="${NIGERIA_COMMON_GIT_BRANCH:-main}"

        # 如果未配置 Git 仓库，尝试从项目仓库推断
        if [ -z "$COMMON_GIT_REPO" ]; then
            # 尝试从当前项目的 Git 仓库推断 Common 模块的位置
            # 如果 Common 模块在同一个仓库中，可能在仓库根目录
            if [ -d ".git" ]; then
                GIT_REMOTE_URL=$(git remote get-url origin 2>/dev/null || echo "")
                if [ -n "$GIT_REMOTE_URL" ]; then
                    # 尝试推断 Common 模块的仓库地址
                    # 例如：从 nigeria-backend-api 推断 nigeria-common
                    if echo "$GIT_REMOTE_URL" | grep -q "nigeria-backend-api"; then
                        COMMON_GIT_REPO=$(echo "$GIT_REMOTE_URL" | sed 's/nigeria-backend-api/nigeria-common/g')
                    elif echo "$GIT_REMOTE_URL" | grep -q "EstNgTeam"; then
                        COMMON_GIT_REPO="https://github.com/EstNgTeam/nigeria-common.git"
                    fi
                fi
            fi
        fi

        # 如果找到了 Git 仓库地址，尝试克隆
        if [ -n "$COMMON_GIT_REPO" ]; then
            echo ""
            echo "尝试从 Git 仓库自动拉取 Common 模块..."
            echo "  仓库地址: ${COMMON_GIT_REPO}"
            echo "  分支: ${COMMON_GIT_BRANCH}"
            echo "  目标路径: /www/nigeria-common"

            # 创建目录
            mkdir -p /www

            # 如果目录已存在但不是 Git 仓库，先备份
            if [ -d "/www/nigeria-common" ] && [ ! -d "/www/nigeria-common/.git" ]; then
                echo "  警告: /www/nigeria-common 已存在但不是 Git 仓库，备份为 nigeria-common.backup"
                mv /www/nigeria-common /www/nigeria-common.backup.$(date +%Y%m%d%H%M%S) 2>/dev/null || true
            fi

            # 如果目录不存在或是空的，尝试克隆
            if [ ! -d "/www/nigeria-common" ] || [ ! -d "/www/nigeria-common/.git" ]; then
                if command -v git &> /dev/null; then
                    echo "  执行 git clone..."
                    if git clone -b "${COMMON_GIT_BRANCH}" "${COMMON_GIT_REPO}" /www/nigeria-common 2>&1; then
                        echo "✅ 成功从 Git 仓库拉取 Common 模块"
                        COMMON_MODULE_PATH="/www/nigeria-common"
                    else
                        echo "❌ 从 Git 仓库拉取失败（可能需要认证或仓库不存在）"
                        # 如果克隆失败，恢复备份（如果有）
                        if [ -d "/www/nigeria-common.backup"* ]; then
                            BACKUP_DIR=$(ls -td /www/nigeria-common.backup.* 2>/dev/null | head -1)
                            if [ -n "$BACKUP_DIR" ] && [ -d "$BACKUP_DIR" ]; then
                                rm -rf /www/nigeria-common 2>/dev/null || true
                                mv "$BACKUP_DIR" /www/nigeria-common 2>/dev/null || true
                                echo "  已恢复备份的 Common 模块"
                            fi
                        fi
                    fi
                else
                    echo "❌ git 命令不可用，无法自动拉取"
                fi
            else
                # 如果已经是 Git 仓库，尝试更新
                echo "  /www/nigeria-common 已是 Git 仓库，尝试更新..."
                cd /www/nigeria-common
                if git pull origin "${COMMON_GIT_BRANCH}" 2>&1; then
                    echo "✅ 成功更新 Common 模块"
                    COMMON_MODULE_PATH="/www/nigeria-common"
                else
                    echo "⚠️  更新失败，使用现有版本"
                    COMMON_MODULE_PATH="/www/nigeria-common"
                fi
                cd "${CURRENT_DIR}"
            fi

            # 如果成功获取到 Common 模块，重新验证
            if [ -n "$COMMON_MODULE_PATH" ] && [ -d "$COMMON_MODULE_PATH" ] && [ -f "${COMMON_MODULE_PATH}/pom.xml" ]; then
                echo ""
                echo "✅ 已获取 Common 模块，继续部署流程..."
                # 删除项目内旧的 Common 模块（如果存在）
                if [ -d "nigeria-common" ]; then
                    rm -rf nigeria-common
                fi
                # 复制到项目目录
                if cp -r "${COMMON_MODULE_PATH}" ./nigeria-common; then
                    echo "✅ Common 模块已复制到项目目录"
                else
                    echo "❌ 错误: Common 模块复制失败"
                    exit 1
                fi
            else
                echo ""
                echo "❌ 错误: 无法获取 Common 模块"
                echo ""
                echo "请手动部署 Common 模块到以下位置之一："
                echo "  1. /www/nigeria-common (推荐)"
                echo "  2. ${CURRENT_DIR}/../nigeria-common"
                echo ""
                echo "部署方法："
                echo "  mkdir -p /www"
                echo "  cd /www"
                echo "  git clone https://github.com/EstNgTeam/nigeria-common.git nigeria-common"
                echo "  # 或者如果 Common 模块在同一个仓库中："
                echo "  # 从项目根目录复制 nigeria-common 到 /www/nigeria-common"
                echo ""
                echo "当前目录内容:"
                ls -la | head -20
                exit 1
            fi
        else
            echo ""
            echo "❌ 错误: 未找到 Common 模块，且无法自动拉取（未配置 Git 仓库地址）"
            echo ""
            echo "请手动部署 Common 模块到以下位置之一："
            echo "  1. /www/nigeria-common (推荐)"
            echo "  2. ${CURRENT_DIR}/../nigeria-common"
            echo ""
            echo "部署方法："
            echo "  mkdir -p /www"
            echo "  cd /www"
            echo "  git clone https://github.com/EstNgTeam/nigeria-common.git nigeria-common"
            echo ""
            echo "或者在 Spug 环境变量中配置："
            echo "  NIGERIA_COMMON_GIT_REPO=https://github.com/EstNgTeam/nigeria-common.git"
            echo "  NIGERIA_COMMON_GIT_BRANCH=main"
            echo ""
            echo "当前目录内容:"
            ls -la | head -20
            exit 1
        fi
    fi
fi

# 构建 Docker 镜像
# 如果 Common 模块更新了，建议使用 --no-cache 强制重新构建 common-builder 阶段
# 可以通过环境变量 FORCE_REBUILD_COMMON=true 来启用
FORCE_REBUILD=${FORCE_REBUILD_COMMON:-false}

if [ "$FORCE_REBUILD" = "true" ]; then
    echo "⚠️  强制重新构建 Common 模块（不使用缓存）..."
    docker build --no-cache --target common-builder -t ${IMAGE_NAME}:common-builder-temp . || true
fi

# 构建完整镜像
# 注意：由于类加载兼容性问题，强制在容器内重新构建 common 模块，不使用宿主机的版本
# 这样可以确保编译环境一致，避免类文件不兼容的问题
echo "ℹ️  强制在容器内构建 Common 模块（确保编译环境一致性）"
mkdir -p ./host-maven-repo/com/nigeria
touch ./host-maven-repo/com/nigeria/.placeholder
USE_HOST_MAVEN_REPO="false"

echo "开始构建 Docker 镜像..."
# 在容器内构建 common 模块（确保编译环境一致性）
if docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .; then
    echo "✅ Docker 镜像构建成功（在容器内构建了 Common 模块）"
    docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:${IMAGE_TAG_LATEST}
    docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${IMAGE_NAME}:latest
else
    echo "❌ Docker 镜像构建失败"
    rm -rf ./host-maven-repo 2>/dev/null || true
    exit 1
fi

# 清理临时镜像
if [ "$FORCE_REBUILD" = "true" ]; then
    docker rmi ${IMAGE_NAME}:common-builder-temp 2>/dev/null || true
fi

# 清理构建上下文中复制的 host-maven-repo（无论构建成功或失败）
rm -rf ./host-maven-repo 2>/dev/null || true

echo "✅ Docker 镜像构建完成: ${IMAGE_NAME}:${IMAGE_TAG}"

# ==========================================
# 创建网络（如果不存在）
# ==========================================
echo ""
echo "=========================================="
echo "创建 Docker 网络"
echo "=========================================="

if ! docker network ls | grep -q nigeria-network; then
    echo "创建网络: nigeria-network..."
    if docker network create nigeria-network 2>&1; then
        echo "✅ 网络创建成功: nigeria-network"
    else
        echo "❌ 网络创建失败，尝试继续..."
        # 再次检查网络是否存在（可能其他进程已创建）
        if docker network ls | grep -q nigeria-network; then
            echo "✅ 网络已存在（可能由其他进程创建）: nigeria-network"
        else
            echo "❌ 错误: 无法创建网络 nigeria-network"
            echo "   请检查 Docker 权限和网络配置"
            exit 1
        fi
    fi
else
    echo "✅ 网络已存在: nigeria-network"
fi

# 验证网络确实存在（使用更可靠的方法）
if ! docker network inspect nigeria-network >/dev/null 2>&1; then
    echo "❌ 错误: 网络 nigeria-network 不存在，尝试创建..."
    if docker network create nigeria-network >/dev/null 2>&1; then
        echo "✅ 网络创建成功: nigeria-network"
    else
        echo "❌ 错误: 无法创建网络 nigeria-network"
        echo "   请检查 Docker 权限和网络配置"
        echo "   尝试手动创建: docker network create nigeria-network"
        exit 1
    fi
fi

# 再次验证网络确实存在
if docker network inspect nigeria-network >/dev/null 2>&1; then
    echo "✅ 网络验证通过: nigeria-network"
    docker network inspect nigeria-network --format '{{.Name}} ({{.Driver}})' 2>/dev/null || true
else
    echo "❌ 错误: 网络 nigeria-network 验证失败"
    exit 1
fi

# ==========================================
# 清理旧镜像（可选，保留最近5个版本）
# ==========================================
echo ""
echo "=========================================="
echo "清理旧镜像"
echo "=========================================="

# 保留最近5个版本的镜像，删除更旧的
KEEP_IMAGES=5
# 获取所有镜像（排除 latest 和 ${SPUG_ENV}-latest 标签），按创建时间排序
OLD_IMAGES=$(docker images ${IMAGE_NAME} --format "{{.ID}} {{.Tag}} {{.CreatedAt}}" | \
    grep -vE "latest|${SPUG_ENV}-latest" | \
    sort -k3 -r | \
    tail -n +$((KEEP_IMAGES + 1)) | \
    awk '{print $1}' | \
    sort -u)

if [ -n "$OLD_IMAGES" ]; then
    echo "发现旧镜像，保留最近 ${KEEP_IMAGES} 个版本..."
    for IMAGE_ID in $OLD_IMAGES; do
        # 检查镜像是否被使用
        if ! docker ps -a --filter ancestor=${IMAGE_ID} --format "{{.ID}}" | grep -q .; then
            echo "  删除未使用的旧镜像: ${IMAGE_ID}"
            docker rmi ${IMAGE_ID} 2>/dev/null || echo "    ⚠️  无法删除镜像 ${IMAGE_ID}（可能正在使用）"
        else
            echo "  保留镜像 ${IMAGE_ID}（正在使用）"
        fi
    done
    echo "✅ 旧镜像清理完成"
else
    echo "✅ 没有需要清理的旧镜像（保留最近 ${KEEP_IMAGES} 个版本）"
fi

# ==========================================
# 停止并删除旧容器
# ==========================================
echo ""
echo "=========================================="
echo "停止旧容器"
echo "=========================================="

if docker ps -a | grep -q nigeria-backend-api; then
    docker stop nigeria-backend-api || true
    docker rm nigeria-backend-api || true
    echo "✅ 旧容器已停止并删除"
else
    echo "✅ 没有运行中的容器"
fi

# ==========================================
# 启动新容器
# ==========================================
echo ""
echo "=========================================="
echo "启动新容器"
echo "=========================================="

# 确定环境
SPRING_PROFILES_ACTIVE="${SPUG_ENV:-cas}"

# 创建配置目录
CONFIG_DIR="/www/config/nigeria-backend-api"
mkdir -p ${CONFIG_DIR}

# ==========================================
# 自动收集 Spug 环境变量（动态从项目配置中提取 key）
# ==========================================
echo ""
echo "=========================================="
echo "收集 Spug 环境变量"
echo "=========================================="

# 策略：动态从项目配置文件中提取环境变量 key，然后在 Spug 环境变量中查找以这些 key 结尾的变量
# 1. 从 application-{profile}.yml 中提取所有 ${KEY} 格式的环境变量名
# 2. 在 Spug 环境变量中查找以这些 key 结尾的变量（例如：SPUG_SPRING_DATASOURCE_URL）
# 3. 将匹配的 key=value 传递给容器
# 例如：项目配置中有 ${SPRING_DATASOURCE_URL}
#      -> 查找 Spug 环境变量中以 SPRING_DATASOURCE_URL 结尾的变量
#      -> 找到：SPUG_SPRING_DATASOURCE_URL=jdbc:mysql://...
#      -> 传递给容器：SPRING_DATASOURCE_URL=jdbc:mysql://...

# 查找配置文件路径（按 SPUG_ENV 选择 application-{profile}.yml）
CONFIG_FILE=""
PROFILE_CONFIG="application-${SPRING_PROFILES_ACTIVE}.yml"
for candidate in \
    "nigeria-api/src/main/resources/${PROFILE_CONFIG}" \
    "src/main/resources/${PROFILE_CONFIG}" \
    "${PROFILE_CONFIG}"; do
    if [ -f "$candidate" ]; then
        CONFIG_FILE="$candidate"
        break
    fi
done

if [ -z "$CONFIG_FILE" ]; then
    echo "⚠️  未找到 ${PROFILE_CONFIG} 配置文件"
    echo "  尝试查找配置文件..."
    find . -name "application-*.yml" -type f 2>/dev/null | head -10
else
    echo "✅ 找到配置文件: ${CONFIG_FILE}"
fi

# 禁止注入容器系统变量（覆盖 PATH 会导致 java: executable file not found）
is_forbidden_env_key() {
    case "$1" in
        PATH|HOME|HOSTNAME|LANG|PWD|SHLVL|TERM|SHELL|USER|LOGNAME|JAVA_HOME|LD_LIBRARY_PATH|LD_PRELOAD)
            return 0
            ;;
        LC_*|OLDPWD)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# 从配置文件中提取所有环境变量 key（格式：${KEY} 或 ${KEY:default}）
NEEDED_KEYS=()
if [ -n "$CONFIG_FILE" ] && [ -f "$CONFIG_FILE" ]; then
    echo ""
    echo "从配置文件中提取环境变量 key..."
    # 提取所有 ${KEY} 或 ${KEY:default} 格式的变量名
    while IFS= read -r key; do
        if [ -n "$key" ]; then
            NEEDED_KEYS+=("$key")
            echo "  📝 需要配置: ${key}"
        fi
    done < <(grep -oE '\$\{[A-Za-z0-9_]+(:[^}]*)?\}' "$CONFIG_FILE" 2>/dev/null | sed 's/\${\([^:}]*\).*/\1/' | sort -u)

    if [ ${#NEEDED_KEYS[@]} -eq 0 ]; then
        echo "  ⚠️  未从配置文件中提取到环境变量 key"
    else
        echo "✅ 共提取到 ${#NEEDED_KEYS[@]} 个环境变量 key"
    fi
else
    echo "⚠️  无法读取配置文件，将尝试匹配所有 Spug 环境变量"
fi

# 调试：显示所有环境变量（用于诊断）
echo ""
echo "调试：所有环境变量（前100个）:"
env | head -100

# 使用临时文件存储环境变量（避免子shell问题）
ENV_FILE=$(mktemp)

# 收集匹配的环境变量
echo ""
echo "在 Spug 环境变量中查找匹配的配置..."

if [ ${#NEEDED_KEYS[@]} -gt 0 ]; then
    # 如果从配置文件中提取到了 key，则精确匹配
    # 使用进程替换避免子shell问题
    while IFS='=' read -r line || [ -n "$line" ]; do
        # 跳过空行
        [ -z "$line" ] && continue

        # 提取 key 和 value
        key="${line%%=*}"
        value="${line#*=}"

        # 跳过空 key
        [ -z "$key" ] && continue

        # 跳过 Spug 系统变量
        if echo "$key" | grep -qE "^(SPUG_DST_DIR|SPUG_APP_NAME|SPUG_APP_KEY|SPUG_ENV_KEY)$"; then
            continue
        fi

        # 检查是否以任何一个 needed_key 结尾
        matched_key=""
        for needed_key in "${NEEDED_KEYS[@]}"; do
            if echo "$key" | grep -qE "${needed_key}$"; then
                matched_key="$needed_key"
                break
            fi
        done

        # 如果没有匹配，跳过
        if [ -z "$matched_key" ]; then
            continue
        fi

        # 禁止覆盖容器系统变量
        if is_forbidden_env_key "$matched_key"; then
            echo "  ⚠️  跳过系统变量: ${matched_key}（来源: ${key}）"
            continue
        fi

        # 跳过包含换行符、Java 堆栈跟踪特征的值（可能是错误日志）
        if echo "$value" | grep -qE "(at |Caused by|Exception|\.java:|~\[|jar!)"; then
            continue
        fi

        # 跳过过长的值（可能是错误日志）
        if [ ${#value} -gt 10000 ]; then
            continue
        fi

        # 处理值中的引号
        value=$(echo "$value" | sed 's/^"\(.*\)"$/\1/')

        # 安全地写入文件（使用 matched_key，不是完整的 Spug key）
        printf '%s=%s\n' "$matched_key" "$value" >> "$ENV_FILE" 2>/dev/null || true
        echo "  ✅ 匹配: ${key} -> ${matched_key}=${value:0:30}..."
    done < <(env)
else
    # 如果没有提取到 key，尝试匹配所有可能的 Spug 环境变量
    echo "  未找到配置文件中的 key，尝试匹配所有 Spug 环境变量..."
    env | while IFS='=' read -r line || [ -n "$line" ]; do
        # 跳过空行
        [ -z "$line" ] && continue

        # 提取 key 和 value
        key="${line%%=*}"
        value="${line#*=}"

        # 跳过空 key
        [ -z "$key" ] && continue

        # 跳过 Spug 系统变量
        if echo "$key" | grep -qE "^(SPUG_DST_DIR|SPUG_APP_NAME|SPUG_APP_KEY|SPUG_ENV_KEY)$"; then
            continue
        fi

        # 跳过包含换行符、Java 堆栈跟踪特征的值（可能是错误日志）
        if echo "$value" | grep -qE "(at |Caused by|Exception|\.java:|~\[|jar!)"; then
            continue
        fi

        # 跳过过长的值（可能是错误日志）
        if [ ${#value} -gt 10000 ]; then
            continue
        fi

        # 处理值中的引号
        value=$(echo "$value" | sed 's/^"\(.*\)"$/\1/')

        # 如果 key 包含下划线，尝试提取最后一部分作为实际 key
        if echo "$key" | grep -qE "_"; then
            # 尝试提取最后一个下划线后的部分
            actual_key="${key##*_}"
            if [ -n "$actual_key" ] && [ "$actual_key" != "$key" ]; then
                if is_forbidden_env_key "$actual_key"; then
                    echo "  ⚠️  跳过系统变量: ${actual_key}（来源: ${key}）"
                    continue
                fi
                printf '%s=%s\n' "$actual_key" "$value" >> "$ENV_FILE" 2>/dev/null || true
                echo "  ✅ 匹配: ${key} -> ${actual_key}=${value:0:30}..."
            fi
        fi
    done
fi

# 读取环境变量文件并构建 docker run 命令参数
ENV_ARGS=()
if [ -f "$ENV_FILE" ] && [ -s "$ENV_FILE" ]; then
    ENV_COUNT=0
    while IFS='=' read -r env_key env_value || [ -n "$env_key" ]; do
        # 跳过空行或无效行
        [ -z "$env_key" ] && continue

        # 二次校验，防止系统变量进入容器
        if is_forbidden_env_key "$env_key"; then
            echo "  ⚠️  忽略系统变量: ${env_key}"
            continue
        fi

        # 安全地添加环境变量
        ENV_ARGS+=("-e")
        ENV_ARGS+=("${env_key}=${env_value}")
        ENV_COUNT=$((ENV_COUNT + 1))
    done < "$ENV_FILE"
    rm -f "$ENV_FILE"
    echo "✅ 已收集 ${ENV_COUNT} 个环境变量（从 Spug 环境变量动态匹配）"
else
    echo "⚠️  未找到匹配的 Spug 环境变量"
    echo ""
    echo "提示："
    echo "  1. 脚本会从项目配置文件中提取环境变量 key（例如：\${SPRING_DATASOURCE_URL}）"
    echo "  2. 然后在 Spug 环境变量中查找以这些 key 结尾的变量"
    echo "  3. 例如：项目配置中有 \${SPRING_DATASOURCE_URL}，查找 SPUG_SPRING_DATASOURCE_URL 或 XXX_SPRING_DATASOURCE_URL"
    echo "  4. 请确认 Spug 中已配置环境变量，且变量名以项目配置中的 key 结尾"
    echo "  5. 检查 Spug 的发布配置，确认环境变量已正确设置"
    rm -f "$ENV_FILE"
fi

# 添加必需的环境变量（如果还没有从环境变量中收集到）
if ! echo "${ENV_ARGS[@]}" | grep -qE "(SPRING_PROFILES_ACTIVE|SPUG_ENV)="; then
    ENV_ARGS+=("-e")
    ENV_ARGS+=("SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE}")
    echo "  添加默认 SPRING_PROFILES_ACTIVE=${SPRING_PROFILES_ACTIVE}"
fi

# 调试：显示关键环境变量
echo ""
echo "关键环境变量预览:"
for arg in "${ENV_ARGS[@]}"; do
    if [[ "$arg" =~ ^-e$ ]]; then
        continue
    fi
    if [[ "$arg" =~ ^(SPRING_DATASOURCE_|SPRING_DATA_REDIS_|SPRING_PROFILES_ACTIVE|NIGERIA_TEST_) ]]; then
        # 隐藏密码
        if [[ "$arg" =~ PASSWORD ]]; then
            echo "  ${arg%%=*}=***"
        else
            echo "  ${arg:0:80}..."
        fi
    fi
done

# 启动容器前再次验证网络
echo ""
echo "启动容器前验证网络..."
if ! docker network inspect nigeria-network >/dev/null 2>&1; then
    echo "❌ 错误: 网络 nigeria-network 在启动容器前验证失败"
    echo "   尝试重新创建网络..."
    docker network create nigeria-network 2>&1 || {
        echo "❌ 无法创建网络，请检查 Docker 状态"
        echo "   手动创建网络: docker network create nigeria-network"
        exit 1
    }
    echo "✅ 网络已重新创建"
fi
echo "✅ 网络验证通过，准备启动容器"

# 启动容器前测试数据库连接（从宿主机）
echo ""
echo "启动容器前测试数据库连接..."
DB_HOST=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed -E 's/.*:\/\/([^:]+):([0-9]+).*/\1/' | head -1 || echo "nigeria-test-mysql")
DB_PORT=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed -E 's/.*:\/\/[^:]+:([0-9]+).*/\1/' | head -1 || echo "3306")
echo "  数据库地址: ${DB_HOST}:${DB_PORT}"

if command -v nc &> /dev/null; then
    if nc -zv -w 3 "${DB_HOST}" "${DB_PORT}" 2>&1; then
        echo "  ✅ 从宿主机可以访问数据库端口"
    else
        echo "  ⚠️  从宿主机无法访问数据库端口（可能使用容器名，这是正常的）"
        echo "  提示：如果使用容器名（如 nigeria-test-mysql），从宿主机无法直接访问是正常的"
    fi
else
    echo "  ⚠️  nc 命令不可用，跳过连接测试"
fi

# 启动容器
echo ""
echo "启动容器..."
docker run -d \
    --name nigeria-backend-api \
    --restart unless-stopped \
    --network nigeria-network \
    -p 8080:8080 \
    "${ENV_ARGS[@]}" \
    -v ${CONFIG_DIR}:/app/config \
    -v /www/logs/nigeria-backend-api:/app/logs \
    ${IMAGE_NAME}:${IMAGE_TAG_LATEST}

echo "✅ 容器已启动"

# 在容器内测试数据库连接
echo ""
echo "在容器内测试数据库连接..."
sleep 3

# 从环境变量中提取数据库信息（如果之前没有提取）
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ]; then
    DB_HOST=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed -E 's/.*:\/\/([^:]+):([0-9]+).*/\1/' | head -1 || echo "nigeria-test-mysql")
    DB_PORT=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed -E 's/.*:\/\/[^:]+:([0-9]+).*/\1/' | head -1 || echo "3306")
fi

if docker exec nigeria-backend-api sh -c "command -v nc >/dev/null 2>&1" 2>/dev/null; then
    if docker exec nigeria-backend-api nc -zv -w 3 "${DB_HOST}" "${DB_PORT}" 2>&1; then
        echo "  ✅ 容器内可以访问数据库端口 ${DB_HOST}:${DB_PORT}"
    else
        echo "  ⚠️  容器内无法访问数据库端口 ${DB_HOST}:${DB_PORT}"
        echo "  可能的原因："
        echo "    1. Docker 网络配置问题（容器不在同一网络）"
        echo "    2. 数据库容器未运行或端口未开放"
        echo "    3. 数据库主机名解析失败"
        echo ""
        echo "  建议检查："
        echo "    1. 确认数据库容器运行: docker ps | grep nigeria-test-mysql"
        echo "    2. 确认容器在同一网络: docker network inspect nigeria-network"
        echo "    3. 尝试使用容器 IP 而不是容器名"
    fi
else
    echo "  ⚠️  容器内没有 nc 命令，使用 ping 测试网络连通性:"
    if docker exec nigeria-backend-api ping -c 2 "${DB_HOST}" 2>&1 | head -5; then
        echo "  ✅ 容器内可以 ping 通数据库主机 ${DB_HOST}"
    else
        echo "  ❌ 容器内无法 ping 通数据库主机 ${DB_HOST}"
    fi
fi

# ==========================================
# 检查容器状态
# ==========================================
echo ""
echo "=========================================="
echo "检查容器状态"
echo "=========================================="

sleep 5

if docker ps | grep -q nigeria-backend-api; then
    echo "✅ 容器运行正常"
    docker ps | grep nigeria-backend-api
else
    echo "❌ 容器启动失败"
    docker logs nigeria-backend-api --tail 50
    exit 1
fi

# ==========================================
# 健康检查验证
# ==========================================
echo ""
echo "=========================================="
echo "健康检查验证"
echo "=========================================="

HEALTH_CHECK_URL="http://localhost:8080/actuator/health"
MAX_RETRIES=30
RETRY_INTERVAL=2
TIMEOUT=60

echo "健康检查地址: ${HEALTH_CHECK_URL}"
echo "最大重试次数: ${MAX_RETRIES}"
echo "重试间隔: ${RETRY_INTERVAL} 秒"
echo "超时时间: ${TIMEOUT} 秒"
echo ""

# 等待服务启动并检查健康状态
HEALTH_CHECK_PASSED=false
for i in $(seq 1 ${MAX_RETRIES}); do
    echo -n "尝试 ${i}/${MAX_RETRIES}: "

    # 检查容器是否还在运行
    if ! docker ps | grep -q nigeria-backend-api; then
        echo "❌ 容器已停止"
        echo "容器日志："
        docker logs nigeria-backend-api --tail 50
        exit 1
    fi

    # 检查健康检查接口
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${HEALTH_CHECK_URL}" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" = "200" ]; then
        # 获取健康检查响应
        HEALTH_RESPONSE=$(curl -s --max-time 5 "${HEALTH_CHECK_URL}" 2>/dev/null || echo "")

        if echo "$HEALTH_RESPONSE" | grep -q '"status":"UP"'; then
            echo "✅ 健康检查通过"
            echo ""
            echo "健康检查响应:"
            if command -v jq &> /dev/null; then
                echo "$HEALTH_RESPONSE" | jq . 2>/dev/null || echo "$HEALTH_RESPONSE"
            else
                echo "$HEALTH_RESPONSE"
            fi
            HEALTH_CHECK_PASSED=true
            break
        else
            echo "⚠️  服务响应但状态不是 UP"
            echo "响应: ${HEALTH_RESPONSE:0:100}..."
        fi
    elif [ "$HTTP_CODE" = "000" ]; then
        echo "⏳ 服务未就绪（连接失败）"
    else
        echo "⚠️  HTTP ${HTTP_CODE}（服务可能正在启动）"
    fi

    if [ $i -lt ${MAX_RETRIES} ]; then
        sleep ${RETRY_INTERVAL}
    fi
done

# 验证健康检查结果
if [ "$HEALTH_CHECK_PASSED" = false ]; then
    echo ""
    echo "❌ 健康检查失败：服务在 ${TIMEOUT} 秒内未就绪"
    echo ""
    echo "容器状态:"
    docker ps -a | grep nigeria-backend-api || echo "容器不存在"
    echo ""
    echo "容器日志（最后 50 行）:"
    docker logs nigeria-backend-api --tail 50
    echo ""
    echo "健康检查接口测试:"
    curl -v "${HEALTH_CHECK_URL}" 2>&1 | head -20
    echo ""
    echo "数据库连接测试:"
    # 从环境变量中提取数据库信息
    DB_HOST=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed 's/.*:\/\/\([^:]*\):.*/\1/' || echo "152.32.140.140")
    DB_PORT=$(echo "${ENV_ARGS[@]}" | grep -oE 'SPRING_DATASOURCE_URL=[^ ]*' | sed 's/.*:\/\/[^:]*:\([0-9]*\).*/\1/' || echo "3306")
    echo "  数据库地址: ${DB_HOST}:${DB_PORT}"
    if command -v nc &> /dev/null; then
        if nc -zv -w 3 "${DB_HOST}" "${DB_PORT}" 2>&1; then
            echo "  ✅ 数据库端口可访问"
        else
            echo "  ❌ 数据库端口不可访问"
        fi
    else
        echo "  ⚠️  nc 命令不可用，无法测试数据库连接"
    fi
    echo ""
    echo "请检查："
    echo "  1. 应用是否正常启动"
    echo "  2. 端口 8080 是否被占用"
    echo "  3. 数据库和 Redis 连接是否正常"
    echo "  4. 环境变量是否正确传递（检查上面的'关键环境变量预览'）"
    echo "  5. 查看完整日志: docker logs -f nigeria-backend-api"
    exit 1
fi

echo ""
echo "✅ 健康检查验证通过"

echo ""
echo "=========================================="
echo "✅ 发布完成！"
echo "=========================================="
echo "容器管理命令："
echo "  查看日志: docker logs -f nigeria-backend-api"
echo "  停止容器: docker stop nigeria-backend-api"
echo "  重启容器: docker restart nigeria-backend-api"
echo "  进入容器: docker exec -it nigeria-backend-api sh"
echo ""