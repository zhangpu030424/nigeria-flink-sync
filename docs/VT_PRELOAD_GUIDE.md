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

## 推荐配置（.env）

```bash
VT_PRELOAD_MODE=fast
VT_PRELOAD_WRITE_MODE=update_id
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
