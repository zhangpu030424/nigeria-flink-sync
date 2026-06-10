-- VIEW 薄封装：Flink JDBC 仍读 v_flink_* 表名，底层走实体表 + 索引
-- 前置: sql/ddl/lm_user_info_flink_staging_tables.sql

CREATE OR REPLACE VIEW v_flink_mkt_user AS
SELECT id_part, id, `appId`, mobile, `deviceId`, created, updated
FROM flink_stg_mkt_user;

CREATE OR REPLACE VIEW v_flink_ud_latest AS
SELECT
    user_id_part, id, `userId`, bvn, `firstName`, `middleName`, `lastName`,
    email, gender, birthday, marital, profession, education, salary,
    `addressState`, `addressDistrict`, address, `emergencyContact`,
    `numberOfChildren`, `payCycle`, company, `salaryDay`
FROM flink_stg_ud_latest;

CREATE OR REPLACE VIEW v_flink_lup_latest AS
SELECT id_part, `appId`, mobile, password
FROM flink_stg_lup_latest;

CREATE OR REPLACE VIEW v_flink_dac_latest AS
SELECT id_part, `deviceId`, channel
FROM flink_stg_dac_latest;
