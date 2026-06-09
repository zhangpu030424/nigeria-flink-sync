-- vt-preload.py 所需权限（在源库用 root/DBA 执行）
-- 错误示例: ERROR 1142 UPDATE command denied to user 'flink_cdc'@'...' for table 'vt_token_cache'
--
-- 将 <flink_host> 改为跑脚本的机器 IP（如 101.47.31.184），或改为 '%'
-- mysql -h <SOURCE_MYSQL_HOST> -u root -p

USE nigeria_backend;

-- 仅放开 vt_token_cache 的读+写（不影响其他表）
GRANT SELECT, UPDATE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'<flink_host>';

-- 若 init/seed 也用 flink_cdc 执行，还需 INSERT：
-- GRANT INSERT ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'<flink_host>';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'flink_cdc'@'<flink_host>';
