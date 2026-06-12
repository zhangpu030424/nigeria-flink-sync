# 多 Job 流水线同步

基于 `docs/中台数据迁移 — 源库 → 目标库字段映射 (3).md`。

## 一键执行（推荐）

```bash
./scripts/sync-pipeline-auto.sh
# 或同义入口
./scripts/sync-all-auto.sh
```

**全自动顺序（勿拆开手动执行）：**

| 步骤 | 动作 |
|------|------|
| 0 | **锁定** `bulk-start-ms` → `logs/bulk-start-ms.env`（增量 CDC 从此刻起补 binlog） |
| 1 | 重建 6 张宽表 `source_all_sync_staging.sql` |
| 2 | 按 `config/sync-jobs.conf` 顺序全量 → 切增量 |

宽表重建可能耗时很久；期间源库 binlog **不会丢**，因增量 Job 使用步骤 0 的时间戳（`scan.startup.mode=timestamp`），而非「切增量那一刻」。

各 Job 顺序：`user` → `user_info` → `user_bankcard` → `user_product` → `application` → `loan`

VT 表全量两阶段：先有 token → 无 token 运行时 `/v2t` → 增量 Lookup+UDF。

仅宽表已建好、且 **沿用同一次** `bulk-start-ms`：

```bash
./scripts/sync-pipeline-auto.sh --skip-staging
```

全量已完成、只补提交增量：

```bash
./scripts/sync-pipeline-auto.sh --incr-only
```

只跑部分表：

```bash
./scripts/sync-pipeline-auto.sh --skip-staging --jobs=user,user_bankcard
```

## 单独重建宽表

```bash
mysql -h <源库> -u ... -p nigeria_backend < sql/ddl/source_all_sync_staging.sql
# 或
./scripts/rebuild-all-staging.sh
```

## Slot 规划

峰值 ≈ `FLINK_PARALLELISM_BULK + N个增量Job × FLINK_PARALLELISM_INCR`

| 配置 | 示例 |
|------|------|
| `FLINK_TASK_SLOTS` | 30 |
| `FLINK_PARALLELISM_BULK` | 20 |
| `FLINK_PARALLELISM_INCR` | 1 |
| 6 表全跑完 | 20 + 6×1 = 26 slots |

## 宽表一览

| 宽表 | VT 字段 | 源表 |
|------|---------|------|
| `user_sync_staging` | mobile_token | user + adjust |
| `user_info_sync_staging` | id_number_token | user_personal_info + work + app_config |
| `user_bankcard_sync_staging` | bank_account_token | user_bank_info |
| `user_product_sync_staging` | — | user_order 最新 product 聚合 |
| `application_sync_staging` | mobile/id_number/gaid/bank VT | user_order 多表 JOIN |
| `loan_sync_staging` | — | user_order_installment + user_order |
| `id_mapping_sync_staging` | mobile/gaid/bank/id_number VT；device 原始 UUID | application + user + bankcard + device_ids 双向边 |

## 增量模式（统一）

| Job | CDC 源表 | VT |
|-----|---------|-----|
| user | `user` | Lookup `vt_token_cache` → miss `vt_tokenize` |
| user_info | `user_personal_info` | 同上（id_number） |
| user_bankcard | `user_bank_info` | 同上（bank_account） |
| user_product | `user_order` | 无 |
| application | `user_order` + Lookup 维表 | mobile/gaid/bank/id_number |
| loan | `user_order_installment` + Lookup | 无 |

增量 Job 启动前由 **`./scripts/deploy-source-ddl.sh`** 自动部署（`sync-all-auto.sh` / `sync-pipeline-auto.sh` 已内置，无需 DMS 手动执行）。

全量仍走宽表；增量不再依赖定时刷新 `*_sync_staging`。

## 未纳入流水线

- **id_mapping**：SQL 宽表已就绪；增量仍 CDC 宽表（待改多源 CDC + 双写）

## 金额

映射 v3 要求 **×100 存分/kobo**，宽表内字段后缀 `_minor`，Flink 直写目标 bigint。
