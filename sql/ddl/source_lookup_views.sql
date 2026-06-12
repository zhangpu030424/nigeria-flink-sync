-- 增量 Job JDBC Lookup 用维表视图（user_order / user_order_installment CDC 点查）
-- mysql ... < sql/ddl/source_lookup_views.sql

CREATE OR REPLACE VIEW user_bank_default_lookup AS
SELECT user_id, bank_code, bank_holder, bank_account
FROM (
         SELECT user_id,
                bank_code,
                bank_holder,
                bank_account,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_bank_info
         WHERE deleted = 0
           AND is_default = 1
           AND bank_account IS NOT NULL
           AND TRIM(bank_account) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_bvn_lookup AS
SELECT user_id, bvn
FROM (
         SELECT user_id,
                bvn,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_personal_info
         WHERE bvn IS NOT NULL AND TRIM(bvn) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW device_ids_latest_lookup AS
SELECT device_uuid, session_uuid, aaid, idfa
FROM (
         SELECT device_uuid,
                session_uuid,
                aaid,
                idfa,
                ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS rn
         FROM device_ids
         WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW risk_approval_latest_by_order AS
SELECT order_no, MAX(callback_time) AS callback_time
FROM risk_user_approval_callback
WHERE callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_repay_paid_latest_by_order AS
SELECT order_no, MAX(callback_time) AS callback_time
FROM user_repay
WHERE status = 2
  AND callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_order_installment_overdue AS
SELECT user_order_id, MAX(COALESCE(is_overdue, 0)) AS is_overdue
FROM user_order_installment
GROUP BY user_order_id;

CREATE OR REPLACE VIEW user_repay_paid_by_order_period AS
SELECT order_no,
       CAST(current_period AS SIGNED) AS current_period,
       MAX(callback_time) AS callback_time
FROM user_repay
WHERE status = 2
  AND callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no, current_period;

-- loan/application 增量 Lookup：避免 UNSIGNED → BigInteger 导致 ClassCastException
CREATE OR REPLACE VIEW user_order_loan_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       order_no,
       order_time,
       disburse_time,
       settled_time,
       CAST(risk_order_status AS SIGNED) AS risk_order_status
FROM user_order;

-- user_info 增量 Lookup：user.id / user_work_related.user_id 为 UNSIGNED 时需 CAST
CREATE OR REPLACE VIEW user_info_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       app_code,
       create_time
FROM user;

CREATE OR REPLACE VIEW user_work_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       work_type,
       occupation,
       company_name,
       monthly_income
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
