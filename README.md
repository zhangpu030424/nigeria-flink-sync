# Nigeria Flink Sync

API 源库（`nigeria_backend`）→ 中台目标库的 Flink CDC 同步项目。与 `nigeria-backend-api` 解耦，整目录上传到服务器即可运行。

## 目录结构

```text
nigeria-flink-sync/
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── sql/
│   ├── 02_sync_user_fast.sql      # 全量（预 VT 宽表 CDC）
│   ├── 02_sync_user_incr.sql      # 增量（vt_token_cache Lookup）
│   └── ddl/
│       ├── source_user_sync_staging.sql  # 宽表含 mobile_token
│       └── vt_token_cache.sql
├── docs/
│   ├── VT_PRELOAD_GUIDE.md        # VT 预加载操作手册
│   └── FIELD_MAPPING.md
└── scripts/
    ├── setup-vt-preload-full.sh   # 一键 VT + 宽表 + 可选 Flink
    ├── run-user-fast.sh           # 提交全量 Job
    ├── sync-user-auto.sh          # 全量 → 自动切增量
    └── vt-preload.sh              # 批量 /v2t 写字典表
```

## 快速开始（服务器）

```bash
cd /opt/nigeria-flink-sync
cp .env.example .env
# 编辑 .env 填写 SOURCE_* / TARGET_* / VT_BASE_URL

chmod +x scripts/*.sh
./scripts/up.sh

# 推荐：VT 预加载 + 全量 + 增量（一条命令）
./scripts/setup-vt-preload-full.sh --run-flink
./scripts/sync-user-auto.sh --incr-only   # 全量完成后

# 或全自动：全量监控达标后切增量
./scripts/sync-user-auto.sh
```

**前置（源库 DBA 一次）**：`source_views_adjust.sql` → `source_materialize_user_adjust.sql` → `vt_token_cache` 灌满 → `source_user_sync_staging.sql`。

详见 [docs/VT_PRELOAD_GUIDE.md](docs/VT_PRELOAD_GUIDE.md)。

## 前置条件

| 项 | 说明 |
|----|------|
| 源库 binlog | `ROW` 格式，账号需 `REPLICATION SLAVE` + `SELECT` |
| 目标库 | 先执行 `docs/schema/Target.sql` |
| VT 字典 | `vt_token_cache` 已预加载（`status=1`） |
| Docker | 20.10+，含 compose v2 |

## 字段映射

完整映射见 `nigeria-backend-api/docs/字段映射.md`。本项目 `docs/FIELD_MAPPING.md` 为关键口径摘要。
