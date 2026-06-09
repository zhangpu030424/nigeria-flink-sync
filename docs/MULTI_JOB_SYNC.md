# 多 Job 流水线同步

基于 `docs/中台数据迁移 — 源库 → 目标库字段映射 (3).md`。

## 一键执行（推荐）

```bash
# VT 已灌满后：
./scripts/sync-pipeline-auto.sh
```

流程：
1. `sql/ddl/source_all_sync_staging.sql` — **一个文件**重建 6 张宽表
2. 按 `config/sync-jobs.conf` **顺序**跑 6 个 Job：
   - `user` → `user_info` → `user_bankcard` → `user_product` → `application` → `loan`
3. 每表：**全量打满核** → 达标后 **只 cancel 本表全量** → 提交 **增量（低并行）**
4. 下一表全量时，**已完成的增量 Job 不停止**（`--keep-other-jobs`）

宽表已建好：

```bash
./scripts/sync-pipeline-auto.sh --skip-staging
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

## 未纳入流水线

- **id_mapping**：需 ProcessFunction 双写，`ENABLED=0`
- **application/loan 增量**：CDC 宽表；新订单需 cron 刷新 `source_all_sync_staging.sql` 相关段或重跑全量段

## 金额

映射 v3 要求 **×100 存分/kobo**，宽表内字段后缀 `_minor`，Flink 直写目标 bigint。
