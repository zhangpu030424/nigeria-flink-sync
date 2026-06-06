# 服务器部署与 CDC 测试手册

## 1. 清理旧目录（若曾散落在 /opt）

```bash
# 可选：停掉旧容器
docker stop nigeria-flink-jobmanager nigeria-flink-taskmanager 2>/dev/null || true
docker rm nigeria-flink-jobmanager nigeria-flink-taskmanager 2>/dev/null || true

# 删除 Mac 上传产生的垃圾文件
find /opt -name '._*' -delete 2>/dev/null || true
```

建议统一使用 **`/opt/nigeria-flink-sync`** 作为部署根目录。

## 2. 上传项目

本机（Mac）：

```bash
cd /Users/zhangpu/Documents/Java
rsync -avz --progress \
  --exclude '.env' \
  --exclude '.git' \
  --exclude '.DS_Store' \
  --exclude '._*' \
  nigeria-flink-sync/ root@<ECS公网IP>:/opt/nigeria-flink-sync/
```

上传后修正属主（避免 501:games）：

```bash
chown -R root:root /opt/nigeria-flink-sync
chmod +x /opt/nigeria-flink-sync/scripts/*.sh
```

## 3. 配置

```bash
cd /opt/nigeria-flink-sync
cp .env.example .env
vi .env
```

必填项：

- `SOURCE_MYSQL_*` — API 源库，账号需 CDC 权限
- `TARGET_MYSQL_*` — 中台目标库
- `USER_ID_OFFSET=100000000`

## 4. 源库 binlog 检查

源库 MySQL 需开启 ROW binlog。在源库执行：

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
-- log_bin=ON, binlog_format=ROW

-- CDC 账号示例
CREATE USER 'flink_cdc'@'%' IDENTIFIED BY 'xxx';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';
FLUSH PRIVILEGES;
```

## 5. 目标库建表

在目标库执行：

```bash
mysql -h <TARGET_HOST> -u <user> -p <TARGET_DB> < docs/schema/Target.sql
```

## 6. 启动 Flink

```bash
./scripts/up.sh
docker compose ps
curl -s http://127.0.0.1:8081/overview | head
```

Web UI：`http://<ECS公网IP>:8081`（安全组需放行 8081）。

## 7. 阶段 A — CDC 冒烟

```bash
./scripts/sql-client.sh
```

在 SQL Client 中：

1. 打开 `sql/01_cdc_smoke.sql`，将 `SOURCE_MYSQL_HOST` 等占位符替换为 `.env` 中的值
2. 逐段执行 CREATE TABLE
3. 取消最后一行 INSERT 注释并执行

另开终端查看增量输出：

```bash
docker logs -f nigeria-flink-taskmanager 2>&1 | grep -E '\+I|print_user'
```

源库插入或更新一条 user 记录，应看到 `+I` 或 `+U` 行。

## 8. 阶段 B — user 最小同步

1. 确认目标库 `user` 表已存在且为空（或测试库）
2. 编辑 `sql/02_sync_user_test.sql` 替换所有连接占位符
3. 在 SQL Client 中执行（含 INSERT INTO sink_user）

验证：

```sql
-- 目标库
SELECT COUNT(*), MAX(user_id) FROM user;
-- user_id 应为 源 id + 100000000
```

## 9. 阶段 C — 增量验证

源库：

```sql
UPDATE user SET mobile = CONCAT(mobile, '') WHERE id = <测试id> LIMIT 1;
-- 或改 update_time 触发行变更
```

观察目标库对应 `user_id` 的 `updated_at` 是否变化；Flink Web UI 中 Job 状态为 RUNNING。

## 10. 常见问题

| 现象 | 处理 |
|------|------|
| CDC 无数据 | 检查 binlog、账号权限、防火墙、时区 `Africa/Lagos` |
| 连接超时 | ECS 出网/入网安全组、MySQL 白名单是否含 ECS IP |
| 容器反复重启 | `docker logs nigeria-flink-jobmanager` 看 OOM 或端口冲突 |
| Mac 上传后权限乱 | `chown -R root:root` 并删 `._*` 文件 |

## 11. 停止与重建

```bash
./scripts/down.sh
docker compose --env-file .env up -d --build   # 改 Dockerfile 后重建
```
