-- GPT 版 user_info 补充 VIEW：reg_ip + app（挂到既有 v_flink_mkt_* / ud / lup / dac）
-- 执行: mysql -h... -u... -p ng_loan_market < sql/ddl/lm_user_info_gpt_views.sql

CREATE OR REPLACE VIEW v_flink_uri_latest AS
SELECT
    CAST(r.`userId` AS DECIMAL(20, 0)) AS user_id_part,
    CAST(r.`userId` AS CHAR)           AS `userId`,
    CAST(r.`ip` AS CHAR)               AS ip
FROM user_registration_ip r
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM user_registration_ip
    GROUP BY `userId`
) x ON x.max_id = r.id;

CREATE OR REPLACE VIEW v_flink_mkt_app AS
SELECT
    CAST(id AS CHAR)   AS id,
    CAST(`name` AS CHAR) AS `name`
FROM `app`;
