-- 老库 ng_loan_market：user_info 全量迁移用实体表（聚合只跑一次，Flink 30 路 JDBC 分区读）
-- 比 VIEW 每次查询都 GROUP BY 快一个数量级
-- 执行: bash scripts/refresh-lm-user-info-staging.sh

-- mkt_user：user 表直拷 + 分区列
DROP TABLE IF EXISTS flink_stg_mkt_user;
CREATE TABLE flink_stg_mkt_user (
    id_part       DECIMAL(20, 0) NOT NULL,
    id            VARCHAR(32)    NOT NULL,
    appId         VARCHAR(64)    DEFAULT NULL,
    mobile        VARCHAR(32)    DEFAULT NULL,
    deviceId      VARCHAR(128)   DEFAULT NULL,
    created       DATETIME       DEFAULT NULL,
    updated       DATETIME       DEFAULT NULL,
    KEY idx_id_part (id_part)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flink_stg_mkt_user (id_part, id, appId, mobile, deviceId, created, updated)
SELECT
    CAST(id AS DECIMAL(20, 0)),
    CAST(id AS CHAR),
    CAST(`appId` AS CHAR),
    CAST(mobile AS CHAR),
    CAST(`deviceId` AS CHAR),
    created,
    updated
FROM `user`;

-- ud_latest：user_data 按 userId 取最新一条
DROP TABLE IF EXISTS flink_stg_ud_latest;
CREATE TABLE flink_stg_ud_latest (
    user_id_part      DECIMAL(20, 0) NOT NULL,
    id                VARCHAR(32)    NOT NULL,
    userId            VARCHAR(32)    NOT NULL,
    bvn               VARCHAR(64)    DEFAULT NULL,
    firstName         VARCHAR(128)   DEFAULT NULL,
    middleName        VARCHAR(128)   DEFAULT NULL,
    lastName          VARCHAR(128)   DEFAULT NULL,
    email             VARCHAR(256)   DEFAULT NULL,
    gender            VARCHAR(8)     DEFAULT NULL,
    birthday          VARCHAR(32)    DEFAULT NULL,
    marital           VARCHAR(16)    DEFAULT NULL,
    profession        VARCHAR(128)   DEFAULT NULL,
    education         VARCHAR(16)    DEFAULT NULL,
    salary            VARCHAR(32)    DEFAULT NULL,
    addressState      VARCHAR(64)    DEFAULT NULL,
    addressDistrict   VARCHAR(64)    DEFAULT NULL,
    address           VARCHAR(512)   DEFAULT NULL,
    emergencyContact  TEXT,
    numberOfChildren  VARCHAR(8)     DEFAULT NULL,
    payCycle          VARCHAR(16)    DEFAULT NULL,
    company           VARCHAR(256)   DEFAULT NULL,
    salaryDay         VARCHAR(16)    DEFAULT NULL,
    KEY idx_user_id_part (user_id_part),
    KEY idx_userId (userId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flink_stg_ud_latest (
    user_id_part, id, userId, bvn, firstName, middleName, lastName, email, gender,
    birthday, marital, profession, education, salary, addressState, addressDistrict,
    address, emergencyContact, numberOfChildren, payCycle, company, salaryDay
)
SELECT
    CAST(ud.`userId` AS DECIMAL(20, 0)),
    CAST(ud.id AS CHAR),
    CAST(ud.`userId` AS CHAR),
    CAST(ud.bvn AS CHAR),
    CAST(ud.`firstName` AS CHAR),
    CAST(ud.`middleName` AS CHAR),
    CAST(ud.`lastName` AS CHAR),
    CAST(ud.email AS CHAR),
    CAST(ud.gender AS CHAR),
    CAST(ud.birthday AS CHAR),
    CAST(ud.marital AS CHAR),
    CAST(ud.profession AS CHAR),
    CAST(ud.education AS CHAR),
    CAST(ud.salary AS CHAR),
    CAST(ud.`addressState` AS CHAR),
    CAST(ud.`addressDistrict` AS CHAR),
    CAST(ud.address AS CHAR),
    CAST(ud.`emergencyContact` AS CHAR),
    CAST(ud.`numberOfChildren` AS CHAR),
    CAST(ud.`payCycle` AS CHAR),
    CAST(ud.company AS CHAR),
    CAST(ud.`salaryDay` AS CHAR)
FROM user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM user_data
    GROUP BY `userId`
) x ON x.max_id = ud.id;

-- lup_latest
DROP TABLE IF EXISTS flink_stg_lup_latest;
CREATE TABLE flink_stg_lup_latest (
    id_part   DECIMAL(20, 0) NOT NULL,
    appId     VARCHAR(64)    NOT NULL,
    mobile    VARCHAR(32)    NOT NULL,
    password  VARCHAR(256)   DEFAULT NULL,
    KEY idx_id_part (id_part),
    KEY idx_app_mobile (appId, mobile)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flink_stg_lup_latest (id_part, appId, mobile, password)
SELECT
    CAST(l1.id AS DECIMAL(20, 0)),
    CAST(l1.`appId` AS CHAR),
    CAST(l1.mobile AS CHAR),
    CAST(l1.password AS CHAR)
FROM log_user_password l1
INNER JOIN (
    SELECT `appId`, mobile, MAX(id) AS max_id
    FROM log_user_password
    GROUP BY `appId`, mobile
) x ON x.max_id = l1.id;

-- dac_latest
DROP TABLE IF EXISTS flink_stg_dac_latest;
CREATE TABLE flink_stg_dac_latest (
    id_part    DECIMAL(20, 0) NOT NULL,
    deviceId   VARCHAR(128)   NOT NULL,
    channel    VARCHAR(128)   DEFAULT NULL,
    KEY idx_id_part (id_part),
    KEY idx_deviceId (deviceId)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO flink_stg_dac_latest (id_part, deviceId, channel)
SELECT
    CAST(d1.id AS DECIMAL(20, 0)),
    CAST(d1.`deviceId` AS CHAR),
    CAST(d1.channel AS CHAR)
FROM device_ad_channel d1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id
    FROM device_ad_channel
    GROUP BY `deviceId`
) x ON x.max_id = d1.id;
