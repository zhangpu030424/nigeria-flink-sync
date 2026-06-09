# VT 预加载方案 — 分步操作手册

将 VT 从 Flink 实时链路拆出：**去重字典表 + 脚本批量 /v2t + 宽表 JOIN**，Flink 只做 CDC 搬运。

当前 **user 全量 Job** 仅对 `mobile` 做 VT（与 `VtBatchRowProcessFunction` 一致）。

---

## 架构

```
user 源表
  → vt_seed_mobile.sql（DISTINCT 规范化手机号）
  → vt_token_cache（status=0）
  → vt-preload.py（批量 /v2t，可多终端并行）
  → vt_token_cache（status=1, token）
  → source_user_sync_staging_vt.sql（宽表含 mobile_token）
  → Flink 02_sync_user_fast_pre_vt.sql（纯 JDBC，无 VT）
  → 目标库 user
```

---

## 第 0 步：环境确认

```bash
cd /path/to/nigeria-flink-sync
cp .env.example .env   # 填好 SOURCE_* / TARGET_* / VT_BASE_URL
```

确认本机或服务器有 `mysql` 客户端、`python3`。

---

## 第 1 步：源库建字典表

```bash
mysql -h $SOURCE_MYSQL_HOST -u$SOURCE_MYSQL_USER -p $SOURCE_MYSQL_DATABASE \ 
  < sql/ddl/vt_token_cache.sql
```

---

## 第 2 步：灌入待 VT 的 mobile（去重）

```bash
mysql -h $SOURCE_MYSQL_HOST -u $SOURCE_MYSQL_USER -p $SOURCE_MYSQL_DATABASE \
  < sql/ddl/vt_seed_mobile.sql
```

输出应看到 `status=0` 的数量（约十几万以内，**去重后通常远小于 user 行数**）。

---

## 第 3 步：脚本批量 VT

```bash
chmod +x scripts/vt-preload.sh scripts/vt-preload.py

# 默认：4 线程，每轮认领 10000，单次 HTTP 2000
./scripts/vt-preload.sh

# 调参示例（8 线程 × 2000/请求 ≈ 每轮 16000 条并行 VT）
./scripts/vt-preload.sh --workers 8 --batch-size 16000 --http-batch-size 2000

# 可选：更小单次 HTTP 避免 504
VT_PRELOAD_HTTP_BATCH=1500 ./scripts/vt-preload.sh --workers 4

# 失败重试
./scripts/vt-preload.sh --retry-failed

# 上次中断后 status=9 卡住
./scripts/vt-preload.sh --reset-processing
```

**加速**：优先调 `--workers` / `VT_PRELOAD_WORKERS`（单进程多线程，推荐 4~8）。  
若仍不够，可按 `vt_type` 拆多终端（如 `--vt-type gaid_idfa`），避免同类型重复认领。

成功标志：`status=1` 覆盖全部，`status=2` 为 0 或可忽略。

---

## 第 4 步：重建宽表（含 mobile_token）

```bash
# 确保 adjust 视图已存在
mysql ... < sql/ddl/source_views_adjust.sql
mysql ... < sql/ddl/source_materialize_user_adjust.sql

mysql ... < sql/ddl/source_user_sync_staging_vt.sql
```

检查 `missing_token_cnt` **必须为 0**。若非 0，回到第 3 步补 VT 后重建。

---

## 第 5 步：停旧 Job，提交预 VT Flink

```bash
./scripts/cancel-all-jobs.sh
./scripts/run-user-fast-pre-vt.sh
```

或一键（第 1~5 步，含可选 Flink）：

```bash
./scripts/setup-vt-preload-full.sh --run-flink
```

监控：

```bash
./scripts/monitor-sync.sh
docker exec nigeria-flink-jobmanager ./bin/flink list   # 应为 RUNNING，非 RESTARTING
```

**不应再出现** `VT /v2t batch` 日志（VT 已在表结构层完成）。

---

## 第 6 步：增量（上线后）

定时任务（示例每 3 分钟）：

```bash
mysql ... < sql/ddl/vt_seed_mobile.sql
./scripts/vt-preload.sh --batch-size 1000
mysql ... < sql/ddl/vt_refresh_staging_mobile_token.sql
```

Flink 继续 CDC `user_sync_staging`（`02_sync_user_fast_pre_vt.sql`），新用户写入宽表后由 refresh SQL 补上 `mobile_token`。

---

## 扩展到其他 VT 字段

`vt_token_cache.vt_type` 已预留 `gaid_idfa`、`bank_account`、`id_number`、`id2`。

后续为 `user_info` / `application` 等 Job 增加：

1. `vt_seed_*.sql` — 从 `user_personal_info.bvn`、`user_bank_info.bank_account` 等 DISTINCT 灌入
2. 宽表 JOIN 对应 `*_token` 列
3. 新建 `02_sync_*_pre_vt.sql`，sink 用 token 列

---

## 与旧方案对比

| 项 | Flink 内 VT | 预加载 VT |
|----|-------------|-----------|
| 瓶颈 | 单 keyBy(0) + HTTP 504 | 脚本可多进程 |
| 重复手机号 | 每行调 VT | 只调一次 |
| Flink Job | UserSyncFastJob | SQL 纯搬运 |
| 全量 18 万 | ~10–15 分钟 | Flink 段约 1–3 分钟 + 脚本 VT 时间 |

---

## 回滚

若需回到 Flink 内 VT：

```bash
mysql ... < sql/ddl/source_user_sync_staging.sql   # 旧宽表无 mobile_token
./scripts/run-user-fast-vt.sh
```
