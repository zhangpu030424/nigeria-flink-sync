# Nigeria Flink Sync

API 源库（`nigeria_backend`）→ 中台目标库的 Flink CDC 同步。整目录上传服务器即可运行。

架构说明见 [docs/FLINK_IMPLEMENTATION.md](docs/FLINK_IMPLEMENTATION.md)。

## 目录结构

```text
nigeria-flink-sync/
├── docker-compose.yml / Dockerfile
├── .env.example
├── config/sync-jobs.conf          # 6 表 Job 编排
├── sql/
│   ├── 02_sync_*_fast.sql         # 全量（JDBC 读宽表）
│   ├── 02_sync_*_fast_vt_miss.sql # 全量阶段 2（无 VT token）
│   ├── 02_sync_*_incr.sql         # 增量（CDC + Lookup）
│   ├── ddl/                       # 源库宽表、视图、脏队列、VT
│   └── verify/                    # 对账 SQL
├── scripts/
│   ├── sync-migrate-auto.sh       # ★ 完整迁移（全量 → 增量）
│   ├── sync-bulk-auto.sh          # 仅全量
│   ├── sync-incr-auto.sh          # 仅增量
│   ├── full-rerun.sh              # 从零重跑
│   ├── rebuild-all-staging.sh     # VT + 重建宽表
│   ├── deploy-source-ddl.sh       # Lookup 视图 + 脏队列
│   ├── vt-preload.py / vt-preload.sh
│   └── sync-job-auto.sh           # 单表全量→增量
├── udf/                           # VtTokenizeFunction（VT 兜底）
└── docs/
```

## 快速开始

```bash
cp .env.example .env   # 填写 SOURCE_* / TARGET_* / VT_BASE_URL
chmod +x scripts/*.sh
./scripts/up.sh

# 完整迁移（推荐）
./scripts/sync-migrate-auto.sh

# 或分步
./scripts/sync-bulk-auto.sh
./scripts/sync-incr-auto.sh
```

从零重跑（含 VT 表重建）：`./scripts/full-rerun.sh`

运维手册：[docs/MULTI_JOB_SYNC.md](docs/MULTI_JOB_SYNC.md) · VT：[docs/VT_PRELOAD_GUIDE.md](docs/VT_PRELOAD_GUIDE.md)

## 前置条件

| 项 | 说明 |
|----|------|
| 源库 binlog | `ROW` 格式，`REPLICATION SLAVE` + `SELECT` |
| 目标库 | 先执行 `docs/schema/Target.sql` |
| VT | `rebuild-all-staging.sh` 自动 seed + preload |
| Docker | 20.10+，compose v2 |

## Job 顺序

`user` → `user_info` → `user_bankcard` → `user_product` → `application` → `loan`
