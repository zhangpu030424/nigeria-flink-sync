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
SELECT order_no, current_period, MAX(callback_time) AS callback_time
FROM user_repay
WHERE status = 2
  AND callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no, current_period;
