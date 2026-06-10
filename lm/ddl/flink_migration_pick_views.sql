-- Flink 多表 Join 试跑：MySQL 侧先圈定 user 范围，VIEW 内完成 MAX(id) 聚合
-- 一次性建表/视图: mysql ... ng_loan_market < lm/ddl/flink_migration_pick_views.sql
-- 每次试跑前刷新 pick: bash lm/scripts/refresh-flink-migration-pick.sh

CREATE TABLE IF NOT EXISTS flink_migration_user_pick (
    id BIGINT NOT NULL PRIMARY KEY
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
  COMMENT 'Flink join 试跑：选中的 user.id，由 refresh 脚本写入';

CREATE TABLE IF NOT EXISTS flink_migration_user_keys (
    id         BIGINT       NOT NULL PRIMARY KEY,
    `appId`    BIGINT       NOT NULL,
    mobile     VARCHAR(32)  NOT NULL,
    `deviceId` BIGINT       DEFAULT NULL,
    KEY idx_app_mobile (`appId`, mobile),
    KEY idx_device (`deviceId`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4
  COMMENT 'pick 用户的 appId/mobile/deviceId，供子表范围过滤';

CREATE OR REPLACE VIEW v_flink_pick_user AS
SELECT u.id, u.`appId`, u.mobile, u.`deviceId`, u.created
FROM `user` u
INNER JOIN flink_migration_user_pick p ON p.id = u.id;

CREATE OR REPLACE VIEW v_flink_pick_ud_latest AS
SELECT ud1.*
FROM `user_data` ud1
INNER JOIN (
    SELECT ud.`userId`, MAX(ud.id) AS max_id
    FROM `user_data` ud
    INNER JOIN flink_migration_user_pick pick ON pick.id = ud.`userId`
    GROUP BY ud.`userId`
) t ON t.max_id = ud1.id;

CREATE OR REPLACE VIEW v_flink_pick_lup_latest AS
SELECT l1.`appId`, l1.mobile, l1.password
FROM `log_user_password` l1
INNER JOIN (
    SELECT l.`appId`, l.mobile, MAX(l.id) AS max_id
    FROM `log_user_password` l
    INNER JOIN flink_migration_user_keys uk
        ON uk.`appId` = l.`appId` AND uk.mobile = l.mobile
    GROUP BY l.`appId`, l.mobile
) t ON t.max_id = l1.id;

CREATE OR REPLACE VIEW v_flink_pick_dac_latest AS
SELECT dac1.`deviceId`, dac1.channel
FROM `device_ad_channel` dac1
INNER JOIN (
    SELECT dac.`deviceId`, MAX(dac.id) AS max_id
    FROM `device_ad_channel` dac
    INNER JOIN flink_migration_user_keys uk
        ON uk.`deviceId` = dac.`deviceId`
       AND uk.`deviceId` IS NOT NULL
       AND uk.`deviceId` <> 0
    GROUP BY dac.`deviceId`
) t ON t.max_id = dac1.id;

-- registration_ip 视图由 refresh-flink-migration-pick.sh 按实际表名生成（user_reg_ip / user_registration_ip）
