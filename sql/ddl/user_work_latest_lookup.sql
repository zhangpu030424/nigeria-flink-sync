-- user_info 增量唯一必需视图（无 GRANT，云 DMS 控制台可只跑本文件）
-- 前置: flink_cdc 对 user_work_related 已有 SELECT（CDC 通常已具备）

USE nigeria_backend;

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
