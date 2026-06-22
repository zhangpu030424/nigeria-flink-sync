# VT 预加载操作手册

VT 已从 Flink 拆出：**字典表 + 脚本批量 /v2t + 宽表 JOIN**，Flink 只做 CDC 搬运。

## 架构

```text
源表明文 → vt_seed_all.sql → vt-preload.py (/v2t)
         → vt_token_cache (status=1)
         → source_all_sync_staging.sql（宽表含 *_token）
         → Flink 02_sync_*_fast.sql（不调 HTTP）
```

增量：`02_sync_*_incr.sql` 通过 Lookup 取 token，miss 时 UDF 兜底。

## 一键（推荐）

```bash
./scripts/rebuild-all-staging.sh          # VT seed + preload + 全部宽表
./scripts/sync-migrate-auto.sh            # 全量 → 增量
```

或嵌入完整流水线（`sync-bulk-auto.sh` 会自动调用 rebuild）。

## 分步

```bash
mysql ... < sql/ddl/vt_token_cache.sql
mysql ... < sql/ddl/vt_seed_all.sql
./scripts/vt-preload.sh --mode fast --vt-type all --workers 4 --http-batch-size 50000
mysql ... < sql/ddl/source_all_sync_staging.sql
```

首次空库可用一步初始化：`sql/ddl/vt_token_cache_init_all.sql`（建表 + 灌明文）。

## 写库模式（慢多半是用了 update_id）

| 模式 | 说明 |
|------|------|
| **upsert**（默认） | 5 万行一条 `INSERT ... ON DUPLICATE KEY UPDATE` | 快 |
| delete_insert | DELETE 旧 status=0 行 + 批量 INSERT 新行 | 需 DELETE；**必须** `VT_PRELOAD_ASYNC_WRITE=0` |
| update_id | 每 3000 行一条 `CASE id` UPDATE | 极慢，仅无 INSERT 时 |

`.env` 确认：

```bash
VT_PRELOAD_WRITE_MODE=upsert
VT_PRELOAD_WRITE_WORKERS=4
VT_PRELOAD_ASYNC_WRITE=1
```

### 使用 DELETE + INSERT

```bash
# 1. DBA 授权（flink_cdc 或跑 preload 的账号）
#    GRANT DELETE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'%';

# 2. .env
VT_PRELOAD_WRITE_MODE=delete_insert
VT_PRELOAD_ASYNC_WRITE=0          # 必须关异步，否则删完未插会丢行
VT_PRELOAD_WRITE_WORKERS=1        # 同步写时无意义，保持 1 即可

# 3. 跑 preload（会先 SELECT id，再 DELETE + INSERT）
./scripts/vt-preload.sh --mode fast --vt-type all --workers 4 --http-batch-size 50000
```

日志应出现 `SELECT id,raw ... (delete_insert)` 和 `DEL+INS`，而不是 `UPSERT`。

## 推荐配置（.env）

```bash
VT_PRELOAD_MODE=fast
VT_PRELOAD_WRITE_MODE=upsert   # upsert 快；无 INSERT 权限时用 update_id
VT_PRELOAD_WRITE_WORKERS=4
VT_PRELOAD_WORKERS=4
VT_PRELOAD_HTTP_BATCH=50000
```

## vt_type 编码

见 `sql/ddl/vt_type_codes.sql`：1=mobile, 2=gaid, 3=bank, 4=id_number, 5=emergency, 6=id2

## 大表重建 vt_token_cache

```bash
# RENAME 换表（推荐）
mysql ... < sql/ddl/vt_token_cache_rebuild_swap.sql
mysql ... < sql/ddl/vt_token_cache_vt_triggers.sql

# 或分批删旧表
./scripts/vt-token-cache-purge.sh --table vt_token_cache_legacy --drop-after
```

## 故障

| 现象 | 处理 |
|------|------|
| missing_token_cnt > 0 | 再跑 vt-preload，重建宽表 |
| vt-preload 权限不足 | DBA 执行 `sql/ddl/vt_token_cache_grants.sql` |
| DROP 视图/表卡住 | Cancel Flink Job → `SHOW PROCESSLIST` → KILL |
