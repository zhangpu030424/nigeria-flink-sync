-- 在老库 ng_loan_market 执行一次（Flink JDBC 读 BIGINT 会返回 BigInteger，与 HashAggregate getLong 冲突）
-- 执行: mysql -h... -u... -p ng_loan_market < sql/ddl/lm_user_info_flink_views.sql
-- 或由 scripts/run-ng-user-info-bulk.sh 自动创建

CREATE OR REPLACE VIEW v_flink_mkt_user AS
SELECT
    CAST(id AS CHAR)         AS id,
    CAST(`appId` AS CHAR)    AS `appId`,
    CAST(mobile AS CHAR)     AS mobile,
    CAST(`deviceId` AS CHAR) AS `deviceId`,
    created,
    updated
FROM `user`;

CREATE OR REPLACE VIEW v_flink_mkt_user_data AS
SELECT
    CAST(id AS CHAR)               AS id,
    CAST(`userId` AS CHAR)         AS `userId`,
    CAST(bvn AS CHAR)              AS bvn,
    CAST(`firstName` AS CHAR)      AS `firstName`,
    CAST(`middleName` AS CHAR)     AS `middleName`,
    CAST(`lastName` AS CHAR)       AS `lastName`,
    CAST(email AS CHAR)            AS email,
    CAST(gender AS CHAR)           AS gender,
    CAST(birthday AS CHAR)         AS birthday,
    CAST(marital AS CHAR)          AS marital,
    CAST(profession AS CHAR)       AS profession,
    CAST(education AS CHAR)        AS education,
    CAST(salary AS CHAR)           AS salary,
    CAST(`addressState` AS CHAR)   AS `addressState`,
    CAST(`addressDistrict` AS CHAR) AS `addressDistrict`,
    CAST(address AS CHAR)          AS address,
    CAST(`emergencyContact` AS CHAR) AS `emergencyContact`,
    CAST(`numberOfChildren` AS CHAR) AS `numberOfChildren`,
    CAST(`payCycle` AS CHAR)       AS `payCycle`,
    CAST(company AS CHAR)          AS company,
    CAST(`salaryDay` AS CHAR)      AS `salaryDay`
FROM user_data;

CREATE OR REPLACE VIEW v_flink_mkt_log_user_password AS
SELECT
    CAST(id AS CHAR)      AS id,
    CAST(`appId` AS CHAR) AS `appId`,
    CAST(mobile AS CHAR)  AS mobile,
    CAST(password AS CHAR) AS password
FROM log_user_password;

CREATE OR REPLACE VIEW v_flink_mkt_device_ad_channel AS
SELECT
    CAST(id AS CHAR)         AS id,
    CAST(`deviceId` AS CHAR) AS `deviceId`,
    CAST(channel AS CHAR)    AS channel
FROM device_ad_channel;
