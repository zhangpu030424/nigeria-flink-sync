-- 在老库 ng_loan_market 执行一次（Flink JDBC 读 unsigned/bigint/tinyint 会类型冲突，VIEW 侧全部 CAST 为 CHAR）

-- 执行: mysql -h... -u... -p ng_loan_market < lm/ddl/lm_id_add_user_flink_view.sql

CREATE OR REPLACE VIEW v_id_add_user_flink AS
SELECT
    CAST(user_id AS SIGNED) AS user_id_part,
    CAST(user_id AS CHAR)            AS user_id,
    CAST(app_id AS CHAR)          AS app_id,
    CAST(group_user_id AS CHAR)   AS group_user_id,
    CAST(info_user_id AS CHAR)    AS info_user_id,
    CAST(mobile AS CHAR)          AS mobile,
    CAST(closed_time AS CHAR)     AS closed_time,
    CAST(reg_device_uuid AS CHAR) AS reg_device_uuid,
    CAST(reg_time AS CHAR)        AS reg_time,
    CAST(test_flag AS CHAR)       AS test_flag,
    CAST(utm_source AS CHAR)      AS utm_source,
    CAST(utm_medium AS CHAR)      AS utm_medium,
    CAST(utm_campaign AS CHAR)     AS utm_campaign,
    CAST(utm_content AS CHAR)     AS utm_content,
    CAST(utm_term AS CHAR)        AS utm_term,
    CAST(campaign_id AS CHAR)     AS campaign_id,
    CAST(ad_group_id AS CHAR)     AS ad_group_id,
    CAST(advertiser_id AS CHAR)   AS advertiser_id
FROM id_add_user;
