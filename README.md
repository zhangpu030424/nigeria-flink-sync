# Nigeria Flink Sync

API 源库（`nigeria_backend`）→ 中台目标库的 Flink CDC 同步项目。与 `nigeria-backend-api` 解耦，整目录上传到服务器即可运行。

## 目录结构

```text
nigeria-flink-sync/
├── docker-compose.yml      # JobManager + TaskManager
├── Dockerfile              # Flink 1.18 + MySQL CDC + JDBC
├── .env.example            # 复制为 .env 后填写
├── conf/flink-conf.yaml    # 非 Docker 部署参考
├── sql/
│   ├── 01_cdc_smoke.sql    # 阶段 A：CDC 冒烟
│   ├── 02_sync_user_test.sql  # 阶段 B：user 全字段同步（含 UTM + VT）
│   └── ddl/source_views_adjust.sql  # 源库 adjust 维表视图（执行一次）
├── docs/
│   ├── DEPLOY.md           # 服务器部署与测试步骤
│   ├── FIELD_MAPPING.md    # 字段映射要点
│   └── schema/Target.sql   # 目标库 DDL
└── scripts/
    ├── up.sh               # 启动集群
    ├── down.sh             # 停止集群
    ├── sql-client.sh       # 进入 SQL Client 交互模式
    ├── run-sql.sh          # 直接执行 sql/*.sql（读 .env）
    └── monitor-sync.sh     # 每分钟统计同步条数/速率
```

## 快速开始（服务器）

```bash
# 1. 上传到 ECS（示例）
rsync -avz --exclude '.env' --exclude '.git' \
  ./nigeria-flink-sync/ root@10.52.81.161:/opt/nigeria-flink-sync/

# 2. 在服务器上
cd /opt/nigeria-flink-sync
cp .env.example .env
# 编辑 .env 填写 SOURCE_* / TARGET_*

chmod +x scripts/*.sh

# 源库执行一次（adjust + 宽表）
mysql -h ... -u ... -p nigeria_backend < sql/ddl/source_views_adjust.sql
mysql -h ... -u ... -p nigeria_backend < sql/ddl/source_materialize_user_adjust.sql
mysql -h ... -u ... -p nigeria_backend < sql/ddl/source_user_sync_staging.sql

./scripts/up.sh

# 推荐：全量自动切增量（一条命令）
./scripts/sync-user-auto.sh

# 或手动分步：
# ./scripts/run-sql.sh sql/02_sync_user_fast.sql      # 全量
# ./scripts/run-sql.sh sql/02_sync_user_incr.sql      # 全量完成后增量
```

详细步骤见 [docs/DEPLOY.md](docs/DEPLOY.md)。

## 前置条件

| 项 | 说明 |
|----|------|
| 源库 binlog | `ROW` 格式，账号需 `REPLICATION SLAVE` + `SELECT` |
| 目标库 | 先执行 `docs/schema/Target.sql` |
| Docker | 20.10+，含 compose v2 |
| 端口 | 默认 `8089`（宿主机与容器内均为 8089） |

## 测试阶段

1. **A — CDC 冒烟**：`sql/01_cdc_smoke.sql`，TaskManager 日志看 `+I`
2. **B — user 同步**：`sql/02_sync_user_test.sql`，验证全量 + 增量
3. **C — 增量验证**：源库 UPDATE 一条 user，观察目标库
4. **D — 扩展**：按 `docs/FIELD_MAPPING.md` 补全 7 张表 + VT/UTM

## 字段映射

完整映射见 `nigeria-backend-api` 仓库的 `docs/字段映射.md`。本项目 `docs/FIELD_MAPPING.md` 为关键口径摘要。

- `user_id = group_user_id = 源 id + 100000000`
