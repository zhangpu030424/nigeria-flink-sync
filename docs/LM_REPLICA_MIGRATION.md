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

## 方案 C：`.env` 主从分离（GPT 全量推荐）

`.env` 同时配置读从库 + 写主库，脚本自动走可写库做 DDL/INSERT：

```bash
# Flink JDBC 读（从库）
LM_MYSQL_HOST=udbha-dmtlrne5.mysql.afr-nigeria.internet.ucloudcs.com
LM_MYSQL_PORT=34057

# Step 3 落地 staging / flink_stg_user_info_ready（主库，必填若 HOST 为从库）
LM_MYSQL_WRITE_HOST=<主库地址>
LM_MYSQL_WRITE_PORT=34057
# LM_MYSQL_WRITE_USER=   # 默认同 LM_MYSQL_USER
# LM_MYSQL_WRITE_PASSWORD=
```

流程：

```bash
# 1. 主库落地 flink_stg_*（若还没有）
bash scripts/refresh-lm-user-info-staging.sh

# 2. 主库拼 GPT JSON → flink_stg_user_info_ready
bash scripts/refresh-lm-user-info-gpt-full.sh

# 3. 等主从同步后，在从库确认行数
mysql -h "$LM_MYSQL_HOST" -P "$LM_MYSQL_PORT" -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;"

# 4. Flink 提交
bash scripts/run-ng-user-info-gpt-bulk-ready.sh
```

进度查询请连**主库**（写入端），不要连从库查 INSERT 进度：

```bash
mysql -h "$LM_MYSQL_WRITE_HOST" -P "$LM_MYSQL_WRITE_PORT" -e "SELECT COUNT(*) FROM flink_stg_user_info_ready;"
```

## 常见错误

- `ERROR 1290 ... read-only`：DDL/INSERT 连到了从库 → 配置 `LM_MYSQL_WRITE_HOST` 为主库，或让 DBA 在主库手动执行对应 SQL
- 从库有 2 张 `flink_stg_*`、缺另外 2 张：脚本会自动回退 VIEW 模式，不要手动只建一半
