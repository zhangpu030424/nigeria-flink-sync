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
| user | `user` + `adjust_callback_record` | 运行时 vt_tokenize |
| user_info | **`user_info_dirty` 单路 CDC** + Lookup（源表 TRIGGER 入队） | id_number |
| user_bankcard | `user_bank_info` + `vt_token_cache` | bank_account |
| user_product | `user_order` → Lookup 最新 product | 无 |
| application | 多源 CDC（见下）+ Lookup | mobile/gaid/bank/id_number |
| loan | 多源 CDC（见下）+ Lookup | 无 |

增量 Job 启动前由 **`./scripts/deploy-source-ddl.sh`** 自动部署（`sync-all-auto.sh` / `sync-pipeline-auto.sh` 已内置，无需 DMS 手动执行）。

**user_info 增量（脏队列方案 B）**

Flink **只 CDC 一张表** `user_info_dirty`；下列源表变更时由 **MySQL TRIGGER** 写入 `user_id`，Flink 收到后 Lookup 组装整行：

| TRIGGER 源表 | 影响字段 |
|-------------|----------|
| `user` | registration_time、app、adid |
| `user_personal_info` | 姓名、BVN、地址、education 等 |
| `user_work_related` | job_type、salary、company、profession |
| `user_emergency_contact` | emergency_contacts |
| `risk_user_credit_callback` | credit_limit |
| `vt_token_cache`（id_number） | id_number |
| `adjust_callback_record` | install_source |

DDL：`sql/ddl/user_info_dirty.sql`（`deploy-source-ddl.sh` 自动执行；**TRIGGER 须 root/DBA 权限**）。

未入队（仅 Lookup）：`app_config`、`device_ids` / `device_network`（`registration_ip`）。

**user 增量 CDC**：`user`、`adjust_callback_record`（UTM 变更，经 adid 关联用户）

**user_bankcard 增量 CDC**：`user_bank_info`、`vt_token_cache(bank_account)`

**user_product 增量**：`user_order` 任意变更 → Lookup `user_product_latest_lookup` 取最新一单

**application 增量 CDC**（任一变更 → 按 order_id 重算整单）：

| CDC 表 | 影响字段 |
|--------|----------|
| `user_order` | 订单主字段、金额、状态 |
| `user` | mobile、device、gaid |
| `user_bank_info` | 银行卡 |
| `user_personal_info` | BVN / id_number |
| `device_ids` | session_id、gaid |
| `user_repay` | last_paid_time |
| `risk_user_approval_callback` | reviewed_time |
| `user_order_installment` | 逾期状态 |

**loan 增量 CDC**（任一变更 → 按 installment_id 重算）：

| CDC 表 | 影响字段 |
|--------|----------|
| `user_order_installment` | 分期本金/利息/还款 |
| `user_order` | 订单状态、settled_time |
| `user_repay` | paid_time（按期次） |

全量仍走宽表；增量不再依赖定时刷新 `*_sync_staging`。

## 增量故障排查

### user_info 增量无数据

1. `deploy-source-ddl.sh` 校验 `user_info_dirty` 表 + TRIGGER≥14。
2. 源表 UPDATE 后查 `SELECT * FROM user_info_dirty WHERE user_id=?` 是否有行。
3. `bash scripts/verify-user-info-incr.sh <user_id>`。

### Checkpoint expired / tolerable failure threshold

日志特征：`Checkpoint expired before completing`、`checkpoint request time in queue: 655935`（队列等约 11 分钟）、`Exceeded checkpoint tolerable failure threshold`。

原因：多 CDC + Lookup 快照期 state 大，默认 **10 分钟 checkpoint 超时**不够；120s 间隔又触发新 checkpoint 排队。

处理：`git pull` 后重建 JobManager 并重提增量 Job：

```bash
docker compose up -d --force-recreate jobmanager
./scripts/sync-job-auto.sh user_info --incr-only --bulk-start-ms <MS> --keep-other-jobs
```

已调：`interval=300s`、`timeout=1800s`、`unaligned=true`、`tolerable-failed-checkpoints=10`（见 `docker-compose.yml` 与各 `*_incr.sql`）。

## 未纳入流水线

- **id_mapping**：SQL 宽表已就绪；增量仍 CDC 宽表（待改多源 CDC + 双写）

## 金额

映射 v3 要求 **×100 存分/kobo**，宽表内字段后缀 `_minor`，Flink 直写目标 bigint。
