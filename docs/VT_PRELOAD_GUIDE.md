# VT 预加载方案 — 操作手册

VT 已从 Flink 拆出：**去重字典表 + 脚本批量 /v2t + 宽表 JOIN**，Flink 只做 CDC 搬运。

---

## 架构

```
user 源表
  → vt_token_cache_init_all.sql（灌明文 status=0）
  → vt-preload.sh（批量 /v2t）
  → vt_token_cache（status=1, token）
  → source_user_sync_staging.sql（宽表含 mobile_norm + mobile_token）
  → Flink 02_sync_user_fast.sql（纯 CDC+JDBC，无 VT HTTP）
  → 目标库 user
```

增量：`02_sync_user_incr.sql` 通过 `vt_token_cache` Lookup 取 token，Flink 同样不调 `/v2t`。

---

## 一键全流程

```bash
./scripts/setup-vt-preload-full.sh --run-flink
```

或分步：

```bash
# 1. adjust 视图/物化表
mysql ... < sql/ddl/source_views_adjust.sql
mysql ... < sql/ddl/source_materialize_user_adjust.sql

# 2. 灌明文 + VT（若已做过可跳过 init）
mysql ... < sql/ddl/vt_token_cache_init_all.sql
./scripts/vt-preload.sh --mode fast --vt-type all --skip-count --workers 4 --http-batch-size 50000

# 3. 重建宽表（missing_token_cnt 应为 0）
mysql ... < sql/ddl/source_user_sync_staging.sql

# 4. 提交 Flink
./scripts/cancel-all-jobs.sh
./scripts/run-user-fast.sh

# 5. 全量完成后切增量
./scripts/sync-user-auto.sh --incr-only
```

---

## 推荐配置（.env）

```bash
VT_PRELOAD_MODE=fast
VT_PRELOAD_WRITE_MODE=update_id   # 勿用 delete_insert
VT_PRELOAD_WORKERS=4
VT_PRELOAD_HTTP_BATCH=50000
```

---

## 增量补新手机号（cron 示例）

```bash
mysql ... < sql/ddl/vt_seed_mobile.sql
./scripts/vt-preload.sh --mode fast --vt-type mobile --skip-count --workers 2 --http-batch-size 5000
mysql ... < sql/ddl/vt_refresh_staging_mobile_token.sql   # 可选，刷新宽表
```

增量 Flink Job 通过 Lookup `vt_token_cache` 取 token；新号在 preload 完成前不会写入目标库。

---

## 权限

`flink_cdc` 需：`vt_token_cache` 的 SELECT（Flink Lookup）；跑 preload 的机器另需 UPDATE。

见 `sql/ddl/vt_token_cache_grants.sql`。

---

## 验收

```sql
-- 字典表
SELECT vt_type, status, COUNT(*) FROM vt_token_cache GROUP BY 1, 2;

-- 宽表
SELECT COUNT(*) FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND (mobile_token IS NULL OR mobile_token = '');
-- 应为 0
```

Flink TaskManager 日志**不应**出现 `VT /v2t batch`。

---

## 扩展到其他 VT 字段

`vt_token_cache` 已支持 `gaid_idfa`、`bank_account`、`id_number`。后续 Job 在宽表 JOIN 对应 `*_token` 列即可。
