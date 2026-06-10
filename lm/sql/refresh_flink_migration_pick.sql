-- 刷新 flink_migration_user_pick / user_keys（envsubst ${LM_MIGRATION_LIMIT}）
TRUNCATE TABLE flink_migration_user_pick;
INSERT INTO flink_migration_user_pick (id)
SELECT id FROM `user` ORDER BY id DESC LIMIT ${LM_MIGRATION_LIMIT};

TRUNCATE TABLE flink_migration_user_keys;
INSERT INTO flink_migration_user_keys (id, `appId`, mobile, `deviceId`)
SELECT u.id, u.`appId`, u.mobile, u.`deviceId`
FROM `user` u
INNER JOIN flink_migration_user_pick p ON p.id = u.id;
