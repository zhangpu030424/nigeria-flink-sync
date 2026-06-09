-- vt-preload.py stream 模式所需权限（源库 DBA 执行）
-- 将 <flink_host> 改为跑脚本的机器 IP（如 101.47.31.184）

USE nigeria_backend;

GRANT SELECT, INSERT, UPDATE ON nigeria_backend.vt_token_cache TO 'flink_cdc'@'<flink_host>';

-- stream 模式还需读源表（CDC 用户通常已有 SELECT）
GRANT SELECT ON nigeria_backend.`user` TO 'flink_cdc'@'<flink_host>';
GRANT SELECT ON nigeria_backend.device_ids TO 'flink_cdc'@'<flink_host>';
GRANT SELECT ON nigeria_backend.user_bank_info TO 'flink_cdc'@'<flink_host>';
GRANT SELECT ON nigeria_backend.user_personal_info TO 'flink_cdc'@'<flink_host>';

FLUSH PRIVILEGES;

SHOW GRANTS FOR 'flink_cdc'@'<flink_host>';
