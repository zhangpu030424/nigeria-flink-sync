# 老库从库只读时的 user_info 迁移

## 原则

| 操作 | 在哪执行 |
|------|---------|
| `CREATE TABLE` / `CREATE VIEW` / `INSERT` 灌 staging | **主库**（有写权限） |
| Flink JDBC 全量读 | **从库**（`.env` 的 `LM_MYSQL_HOST`） |

从库不能建表时，不要用 `refresh-lm-user-info-staging.sh` 连从库。

## 方案 A：只要 VIEW（推荐，改动最小）

1. 把下面文件交给 DBA，在**主库** `ng_loan_market` 执行：

   `sql/ddl/lm_user_info_flink_views.sql`

2. 等主从同步（通常几分钟内），在**从库**验证：

   ```sql
   SELECT column_name FROM information_schema.columns
   WHERE table_schema='ng_loan_market' AND table_name='v_flink_dac_latest';
   -- 必须有 id_part

   SELECT 1 FROM v_flink_lup_latest WHERE id_part>=1 AND id_part<1000000 LIMIT 1;
   SELECT 1 FROM v_flink_dac_latest WHERE id_part>=1 AND id_part<1000000 LIMIT 1;
   ```

3. Flink 服务器 `.env`：

   ```bash
   LM_MYSQL_HOST=<从库地址>
   LM_MYSQL_READ_REPLICA=1
   ```

4. 提交：

   ```bash
   LM_SKIP_VIEW_PROBE=1 bash scripts/run-ng-user-info-bulk-max.sh
   ```

## 方案 B：主库建 staging 实体表（更快）

1. DBA 在**主库**执行：`sql/ddl/lm_user_info_flink_staging_tables.sql`（耗时长，只跑一次）
2. 再执行：`sql/ddl/lm_user_info_flink_views_staging.sql`
3. 等 4 张 `flink_stg_*` 同步到从库后，Flink 可连从库读实体表（不设 `LM_MYSQL_READ_REPLICA=1` 且 4 表齐全时自动走 staging）

## 方案 C：Flink 直连 VIEW（推荐，无物化、无 JDBC 分区）

不建 `flink_stg_*`、不跑 Step3 大 INSERT，Flink 直接读 VIEW 写目标库：

```bash
# .env: LM_MYSQL_HOST=从库；VIEW 须在主库建好并同步
bash scripts/run-ng-user-info-gpt-bulk-max.sh
# 或
bash scripts/run-ng-user-info-gpt-direct.sh
```

VIEW 缺失时脚本会尝试在**主库**执行 `lm_user_info_flink_views.sql`（需 `LM_MYSQL_WRITE_HOST`）。

## 方案 D：`.env` 主从分离 + 物化表（旧路径，易卡住）

旧路径（MySQL 物化 `flink_stg_user_info_ready`）:

```bash
LM_USER_INFO_MATERIALIZE=1 bash scripts/run-ng-user-info-gpt-bulk-max.sh
```

## 常见错误

- `ERROR 1290 ... read-only`：DDL/INSERT 连到了从库 → 配置 `LM_MYSQL_WRITE_HOST` 为主库，或让 DBA 在主库手动执行对应 SQL
- 从库有 2 张 `flink_stg_*`、缺另外 2 张：脚本会自动回退 VIEW 模式，不要手动只建一半
