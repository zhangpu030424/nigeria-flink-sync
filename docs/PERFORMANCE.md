# 同步性能调优

4 核 ECS 上，默认 `parallelism=1` + JDBC 小批量刷盘，**1～2 万条/分钟**很常见。  
要到 **10 万+/分钟**，需要下面几项一起调；**100 万/分钟** 对 JDBC 写 MySQL 很难，通常要「批量导全量 + CDC 只做增量」。

## 瓶颈在哪

```text
源库 CDC 读  →  Lookup app_config  →  JDBC 写目标库
     ↑                    ↑                  ↑
  可并行加速           全量时拖慢          最常见瓶颈
```

## 怎么判断「读慢」还是「写慢」

### 方法一：诊断脚本（推荐）

```bash
chmod +x scripts/diagnose-job.sh
./scripts/diagnose-job.sh <job_id>        # 看一次
./scripts/diagnose-job.sh <job_id> 15     # 每 15 秒刷新
```

看 **反压%** 最高的算子：

| 算子名含 | 瓶颈 |
|----------|------|
| `Source` / `src_user` | CDC 读源库慢 |
| `LookupJoin` | app_config 维表查询慢 |
| `Sink` / `sink_user` | JDBC 写目标库慢 |

### 方法二：Web UI

`http://<IP>:8089` → Job → **Overview** → 各算子颜色（红=反压）→ 点算子 **Metrics** → `numRecordsOutPerSecond`

### 方法三：对比实验（隔离瓶颈）

先 cancel 当前 Job，分别跑（测完 cancel，避免多 Job 抢 slot）：

| SQL | 测什么 |
|-----|--------|
| `03_bench_cdc_read.sql` | 只 CDC 读 + print，**读上限** |
| `04_bench_sink_no_join.sql` | CDC 读 + 写目标，**无 Lookup** |
| `02_sync_user_test.sql` | 完整链路 |

对比 monitor-sync 速率：

- 03 快、04 慢 → **写慢** → 调 JDBC / 目标 MySQL
- 03 快、04 快、02 慢 → **Lookup Join 慢** → 加大 cache 或全量去掉 Join
- 03 就慢 → **CDC/源库/网络慢** → 调 snapshot parallelism、chunk、源库

## 32C64G 默认配置（`.env`）

| 变量 | 32C64G 默认 | 4C8G 可改为 |
|------|-------------|-------------|
| `FLINK_TASK_SLOTS` | 16 | 4 |
| `FLINK_TM_MEMORY` | 40960m | 4096m |
| `FLINK_PARALLELISM` | 16 | 4 |
| `FLINK_MINI_BATCH_SIZE` | 10000 | 5000 |
| `FLINK_SINK_BUFFER_ROWS` | 10000 | 5000 |
| `FLINK_CDC_CHUNK_SIZE` | 100000 | 50000 |
| `FLINK_CDC_FETCH_SIZE` | 10000 | 5000 |

内存分配：JobManager 2G + TaskManager 40G + 系统预留 ~22G。

## 已在 `02_sync_user_test.sql` 里的优化

以上 `FLINK_*` 由 `run-sql.sh` 注入 SQL；与 `docker-compose` 中 slot/内存一致。

| 项 | 32C64G 默认 | 作用 |
|----|-------------|------|
| `parallelism.default` | 16 | 与 slot 对齐 |
| CDC chunk / fetch | 100000 / 10000 | 快照读批次 |
| JDBC sink buffer | 10000 / 1s | 写目标库批量 |
| mini-batch | 10000 / 3s | SQL 微批 |
| lookup cache | 50000 / 2h | 维表缓存 |

## 应用步骤

```bash
cd /opt/nigeria-flink-sync

# 1. .env 含 FLINK_TASK_SLOTS=16 等（见 .env.example）
docker compose --env-file .env up -d --build

# 2. 取消旧 Job
docker exec nigeria-flink-jobmanager ./bin/flink cancel <job_id>

# 3. DROP TABLE 后重新提交
./scripts/run-sql.sh sql/02_sync_user_test.sql

# 4. 监控
./scripts/monitor-sync.sh user 60 <job_id>
```

## 服务器 / MySQL 侧

**ECS（4 核）**

- 源、目标 MySQL 尽量与 Flink **同地域、走内网**
- 避免 Flink、MySQL 和监控脚本跨公网高延迟

**源库（读）**

- CDC 账号只需 SELECT + REPLICATION
- 全量快照会加大源库读 IO，低峰跑全量

**目标库（写）—— 往往决定上限**

- `innodb_buffer_pool_size` 尽量大（物理内存 50～70%）
- 全量前空表、无二级索引冲突；`user` 仅 PK 时写入最快
- 目标实例规格要够（CPU/磁盘 IOPS）
- 确认无触发器、外键拖慢 INSERT

## 还能怎么榨性能

### 1. 全量阶段去掉 Lookup（app_config 很小且稳定时）

若 `app_id` 映射可接受后置，全量 Job 可先不 JOIN `app_config`，写入 `app_id=0`，全量完成后再跑修复 Job。  
当前每条 user 都打一次维表 Lookup，全量 17 万+ 行会明显拖慢。

### 2. 全量与增量分离（冲百万级时）

| 阶段 | 方式 |
|------|------|
| 全量 | `mysqldump` / `INSERT SELECT` / 数据集成工具 bulk load |
| 增量 | Flink CDC Job 从快照结束位点开始，只追 binlog |

Flink 官方也推荐：**大批量历史用离线导入，CDC 负责增量**。

### 3. 加 TaskManager（8 slot）

4 核可试 `parallelism=4` 先打满；若 CPU 仍有余量，compose 里扩第二个 TaskManager（需去掉固定 `container_name` 或用 scale）。

### 4. Checkpoint

多 CDC 增量 Job（尤其 `user_info` 6 路源 + 多 Lookup）快照期 checkpoint 耗时长，默认 10min 超时会杀 Job。

| 参数 | 推荐值 |
|------|--------|
| `execution.checkpointing.interval` | `300s` |
| `execution.checkpointing.timeout` | `1800s`（30min） |
| `execution.checkpointing.unaligned` | `true` |
| `execution.checkpointing.tolerable-failed-checkpoints` | `10` |

`.env` 可覆盖：`FLINK_CHECKPOINT_INTERVAL=300s`、`FLINK_CHECKPOINT_TIMEOUT=1800s`。改 `docker-compose.yml` 后须 `docker compose up -d --force-recreate jobmanager`。

## 合理预期（4 核 + JDBC MySQL）

| 配置 | 大致速率 |
|------|----------|
| 4C8G parallelism=4 | 1～3 万/分钟 |
| **32C64G parallelism=16（当前默认）** | **5～15 万/分钟** |
| 同机房 + 目标库高配 + 无 Lookup | 10 万～30 万/分钟 |
| JDBC 单表写 MySQL | 100 万/分钟 通常达不到 |

以 **Web UI → Metrics → numRecordsOutPerSecond** 和 `monitor-sync.sh` 为准，调完对比前后速率。

## 排查「还是慢」

1. Web UI 看哪个算子 **反压（backpressure）** 最严重  
2. 反压在 **Sink** → 目标 MySQL 或 JDBC 批量  
3. 反压在 **LookupJoin** → 维表或缓存  
4. 反压在 **Source** → 源库读或网络  

```bash
docker logs nigeria-flink-taskmanager 2>&1 | grep -i backpressure | tail
```
