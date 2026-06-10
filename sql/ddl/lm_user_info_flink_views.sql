-- 老库 ng_loan_market：user_info Flink 同步用 VIEW（类型 CAST + MySQL 侧预聚合，避免 Flink HashAggregate 扫全表）
-- 执行: mysql -h... -u... -p ng_loan_market < sql/ddl/lm_user_info_flink_views.sql

CREATE OR REPLACE VIEW v_flink_mkt_user AS
SELECT
    CAST(id AS DECIMAL(20, 0)) AS id_part,
    CAST(id AS CHAR)           AS id,
    CAST(`appId` AS CHAR)      AS `appId`,
    CAST(mobile AS CHAR)         AS mobile,
    CAST(`deviceId` AS CHAR)     AS `deviceId`,
    created,
    updated
FROM `user`;

CREATE OR REPLACE VIEW v_flink_ud_latest AS
SELECT
    CAST(ud.`userId` AS DECIMAL(20, 0)) AS user_id_part,
    CAST(ud.id AS CHAR)                 AS id,
    CAST(ud.`userId` AS CHAR)           AS `userId`,
    CAST(ud.bvn AS CHAR)                AS bvn,
    CAST(ud.`firstName` AS CHAR)        AS `firstName`,
    CAST(ud.`middleName` AS CHAR)       AS `middleName`,
    CAST(ud.`lastName` AS CHAR)         AS `lastName`,
    CAST(ud.email AS CHAR)              AS email,
    CAST(ud.gender AS CHAR)             AS gender,
    CAST(ud.birthday AS CHAR)           AS birthday,
    CAST(ud.marital AS CHAR)            AS marital,
    CAST(ud.profession AS CHAR)         AS profession,
    CAST(ud.education AS CHAR)          AS education,
    CAST(ud.salary AS CHAR)             AS salary,
    CAST(ud.`addressState` AS CHAR)     AS `addressState`,
    CAST(ud.`addressDistrict` AS CHAR)    AS `addressDistrict`,
    CAST(ud.address AS CHAR)            AS address,
    CAST(ud.`emergencyContact` AS CHAR) AS `emergencyContact`,
    CAST(ud.`numberOfChildren` AS CHAR) AS `numberOfChildren`,
    CAST(ud.`payCycle` AS CHAR)         AS `payCycle`,
    CAST(ud.company AS CHAR)            AS company,
    CAST(ud.`salaryDay` AS CHAR)        AS `salaryDay`
FROM user_data ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM user_data
    GROUP BY `userId`
) x ON x.max_id = ud.id;

CREATE OR REPLACE VIEW v_flink_lup_latest AS
SELECT
    CAST(l1.id AS DECIMAL(20, 0)) AS id_part,
    CAST(l1.`appId` AS CHAR)      AS `appId`,
    CAST(l1.mobile AS CHAR)       AS mobile,
    CAST(l1.password AS CHAR)     AS password
FROM log_user_password l1
INNER JOIN (
    SELECT `appId`, mobile, MAX(id) AS max_id
    FROM log_user_password
    GROUP BY `appId`, mobile
) x ON x.max_id = l1.id;

CREATE OR REPLACE VIEW v_flink_dac_latest AS
SELECT
    CAST(d1.id AS DECIMAL(20, 0)) AS id_part,
    CAST(d1.`deviceId` AS CHAR)     AS `deviceId`,
    CAST(d1.channel AS CHAR)        AS channel
FROM device_ad_channel d1
INNER JOIN (
    SELECT `deviceId`, MAX(id) AS max_id
    FROM device_ad_channel
    GROUP BY `deviceId`
) x ON x.max_id = d1.id;
