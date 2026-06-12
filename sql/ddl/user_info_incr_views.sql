-- user_info 增量所需 Lookup 视图（DMS 可跑，无 GRANT）
-- mysql ... < sql/ddl/user_info_incr_views.sql

USE nigeria_backend;

CREATE OR REPLACE VIEW user_info_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

CREATE OR REPLACE VIEW app_config_lookup AS
SELECT CAST(app_code AS SIGNED) AS app_code,
       CAST(app_name AS CHAR) AS app_name,
       CAST(version AS CHAR) AS version
FROM app_config;

CREATE OR REPLACE VIEW vt_token_cache_lookup AS
SELECT CAST(vt_type AS CHAR) AS vt_type,
       CAST(raw_value AS CHAR) AS raw_value,
       CAST(token AS CHAR) AS token,
       CAST(status AS SIGNED) AS status
FROM vt_token_cache;

CREATE OR REPLACE VIEW user_work_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(work_type AS CHAR) AS work_type,
       CAST(occupation AS CHAR) AS occupation,
       CAST(company_name AS CHAR) AS company_name,
       CAST(monthly_income AS CHAR) AS monthly_income
FROM (
         SELECT user_id,
                work_type,
                occupation,
                company_name,
                monthly_income,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_work_related
     ) t
WHERE rn = 1;
