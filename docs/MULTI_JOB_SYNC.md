# 多 Job 流水线同步

基于 `docs/中台数据迁移 — 源库 → 目标库字段映射 (3).md`。架构原理见 [FLINK_IMPLEMENTATION.md](FLINK_IMPLEMENTATION.md)。

## 脚本总览

| 脚本 | 用途 |
|------|------|
| **`sync-bulk-auto.sh`** | **全量迁移**：DDL → 锁定 bulk-start-ms → VT+宽表 → 各表全量（不切增量） |
| **`sync-incr-auto.sh`** | **增量迁移**：读 bulk-start-ms → DDL → 提交各表增量 Job |
| **`sync-migrate-auto.sh`** | **完整迁移**：先 `sync-bulk-auto` 再 `sync-incr-auto`（**推荐**） |
| `sync-pipeline-auto.sh` | 兼容：每表「全量→增量」串行（旧行为） |
| `full-rerun.sh` | 从零重跑：Cancel → 重建 VT → 清 dirty → bulk → incr |

## 一键执行（推荐）

```bash
# 两阶段：先全部全量，再全部增量（便于中间对账）
./scripts/sync-migrate-auto.sh
```

### 分步执行

```bash
# 1. 全量（会写入 logs/bulk-start-ms.env，增量必用）
./scripts/sync-bulk-auto.sh

# 2. 可选：全量后对账
bash scripts/verify-user-info-reconcile.sh --sample 500

# 3. 增量（默认 timestamp，正确性优先）
./scripts/sync-incr-auto.sh

# 全量已覆盖缺口、要提速 user_info 时：
./scripts/sync-incr-auto.sh --user-info-latest-offset --truncate-user-info-dirty
```

### 从零重跑（全量 + 增量全部重来）

```bash
git pull
# 确认 .env：FLINK_PARALLELISM_INCR=4、CDC_SERVER_ID_UI_DIRTY=5401、FLINK_TASK_SLOTS 足够

chmod +x scripts/full-rerun.sh
./scripts/full-rerun.sh
```

脚本会自动：**Cancel Job → DROP 重建 vt_token_cache(TINYINT) → TRUNCATE dirty → sync-bulk-auto → sync-incr-auto**

TRIGGER 须 root 时，流水线前执行：

```bash
mysql -u root ... nigeria_backend < sql/ddl/user_info_dirty_enqueue.sql
mysql -u root ... nigeria_backend < sql/ddl/user_info_dirty.sql
```

### vt_token_cache 大表无法 DROP

先 **Cancel Flink Job + 停 vt-preload**，再选一种：

| 方案 | 命令 | 说明 |
|------|------|------|
| **A RENAME 换表（推荐）** | `./scripts/full-rerun.sh` 或 `--rebuild-vt-swap` | 秒级完成，旧表改名 `vt_token_cache_legacy` |
| **B 分批删除** | `./scripts/vt-token-cache-purge.sh --drop-after` | 按 id 区间每批 1 万行删，删空再 DROP |
| **C 直接 DROP** | `--rebuild-vt-drop` | 仅小表或已停服时 |

换表后后台清旧表：

```bash
./scripts/vt-token-cache-purge.sh --table vt_token_cache_legacy --batch 20000 --drop-after
```

跑完检查：

```bash
docker exec nigeria-flink-jobmanager ./bin/flink list -r   # 应有 6 个 RUNNING 增量 Job
bash scripts/verify-user-info-incr.sh 211038
```

**全自动顺序（`sync-bulk-auto` + `sync-incr-auto`）：**

| 阶段 | 步骤 | 动作 |
|------|------|------|
| 全量 | 0 | **锁定** `bulk-start-ms` → `logs/bulk-start-ms.env` |
| 全量 | 1 | VT 补灌 + 重建 6 张宽表 |
| 全量 | 2 | 按 `config/sync-jobs.conf` 顺序跑各表全量（`--bulk-only`） |
| 增量 | 3 | 按同顺序提交各表增量 Job（默认 `timestamp`） |

**兼容模式**（`sync-pipeline-auto.sh`）：每表全量完成后立刻切增量，再跑下一表。

宽表重建可能耗时很久；期间源库 binlog **不会丢**，因增量 Job 使用步骤 0 的时间戳（`scan.startup.mode=timestamp`），而非「切增量那一刻」。

各 Job 顺序：`user` → `user_info` → `user_bankcard` → `user_product` → `application` → `loan`

VT 表全量两阶段：先有 token → 无 token 运行时 `/v2t` → 增量 Lookup+UDF。

仅宽表已建好、且 **沿用同一次** `bulk-start-ms`：

```bash
./scripts/sync-bulk-auto.sh --skip-staging
./scripts/sync-incr-auto.sh
```

全量已完成、只提交增量：

```bash
./scripts/sync-incr-auto.sh
# 或兼容入口
./scripts/sync-pipeline-auto.sh --incr-only
```

只跑部分表：

```bash
./scripts/sync-migrate-auto.sh --jobs=user,user_bankcard
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
| `vt_token_cache`（emergency_contact） | info.emergency_contacts[].mobile |
| `adjust_callback_record` | install_source |

DDL：`sql/ddl/user_info_dirty.sql`（`deploy-source-ddl.sh` 自动执行；**TRIGGER 须 root/DBA 权限**）。

组装：`user_info_incr_bundle_lookup` **单次 JDBC Lookup**（勿 9 路串行 Lookup）。建议 `FLINK_PARALLELISM_INCR=4`。

`CDC_SERVER_ID_UI_DIRTY` 须为**单值**（如 `5401`）；脏队列 CDC 关闭 incremental snapshot 时写 `5401-5404` 会报 `NumberFormatException`。

### `RELOAD or FLUSH_TABLES privilege`（CDC 启动失败）

日志：`Access denied; you need RELOAD or FLUSH_TABLES`。云 RDS 默认不给 `flink_cdc` 锁表权限。

处理：`git pull` 后重提 Job（`02_sync_user_info_incr.sql` 已加 `debezium.snapshot.locking.mode=none`）。勿反复重启——日志里 `attempt #12` 说明已在失败循环。

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
3. **dirty 有行但目标不变**：
   - Web UI：`cdc_user_info_dirty` **Records Sent**、sink **Records Received** 是否增加。
   - 直接 `UPDATE user_info_dirty SET updated_at=NOW(3) WHERE user_id=?` 测 CDC 链路。
   - 脏表行数 `SELECT COUNT(*) FROM user_info_dirty` 极大时，timestamp 模式要从 bulk-start 重放大量 binlog，需等待或改用 `CDC_STARTUP_MODE=latest-offset` 重提 Job（全量已覆盖缺口时）。
   - 确认 SQL 已关 `scan.incremental.snapshot.enabled=false`（脏队列表勿做全表快照）。
4. `bash scripts/verify-user-info-incr.sh <user_id>`；端到端：`bash scripts/verify-user-info-incr.sh <user_id> --e2e`。

### 如何验证方案正确性（分层）

| 层 | 验证什么 | 命令 / 方法 | 通过标准 |
|----|----------|-------------|----------|
| L1 基础设施 | 脏表、TRIGGER、Job | `deploy-source-ddl.sh` + Flink UI RUNNING | TRIGGER≥14，Job 不 RESTARTING |
| L2 触发器 | 源表变更 → dirty | `UPDATE user_personal_info ...` 后查 dirty | 对应 `user_id` 有行且 `updated_at` 更新 |
| L3 组装逻辑 | Lookup 与全量一致 | `verify-user-info-incr.sh` 的 staging_vs_bundle | 显示 `match` |
| L4 管道 | CDC → Sink | `--e2e` 或 UI Records +1 | 目标 `updated_at` / `full_name` 变化 |
| L5 数据 | 与 bundle 一致 | 脚本「数据对账」或抽样 SQL | `expected_full_name` = 目标 `full_name` |

**无法一次证明整库正确**：脏队列有积压时，单用户可能要等很久才轮到。验证应用 `--e2e`（只测管道）+ 抽样对比（测组装逻辑），不要只靠改一条业务数据立刻看目标。

**常见「看起来不对」**：有 BVN 无 `vt_token_cache` → sink WHERE 过滤（与全量相同）；`timestamp` 起点在 dirty 行之前 → 需 `latest-offset` 或等 backlog 消化完。

### 脏队列生产快、消费跟不上

现象：`user_info_dirty` 行数不多但 binlog 极多（同 user 反复 bump `updated_at`）；Job Records 很慢、积压越来越大。

**生产侧（源库，须 DBA `git pull` 后 `deploy-source-ddl.sh`）**

- TRIGGER 改为 `sp_user_info_dirty_enqueue*`：**10s** debounce（用户资料）、**30s**（授信回调）、**60s**（VT / adjust）
- `adjust_callback_record` UPDATE 仅在 `tracker_name` 变化时入队；`user` UPDATE 仅在 app_code/device/adid 等变化时入队

**消费侧（Flink）**

- `USER_INFO_DIRTY_COALESCE_SEC=5`：窗口内同 user 只 Lookup 一次
- `FLINK_PARALLELISM_INCR=4`（或更高，受 slot 限制）
- Lookup cache TTL 120s

**清积压（全量已覆盖缺口时）**

```sql
TRUNCATE TABLE user_info_dirty;
```

```bash
CDC_STARTUP_MODE=latest-offset FLINK_PARALLELISM_INCR=4 \
  ./scripts/sync-job-auto.sh user_info --incr-only --keep-other-jobs
```

debounce 窗口内最后一次业务变更仍会被合并后的 Lookup 读到最新宽表；极端情况（变更后永不再入队且 Job 未消费）可手动 `UPDATE user_info_dirty` 补一枪。

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
