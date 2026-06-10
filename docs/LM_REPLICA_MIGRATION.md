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

## 常见错误

- `Unknown column 'id_part' in 'where clause'`：从库 VIEW 未同步或仍是旧版 → 主库重跑 `lm_user_info_flink_views.sql`
- 从库有 2 张 `flink_stg_*`、缺另外 2 张：脚本会自动回退 VIEW 模式，不要手动只建一半
