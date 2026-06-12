-- 增量 Job JDBC Lookup 用维表视图（application/loan/user_info 等）
-- 部署: ./scripts/deploy-source-ddl.sh

CREATE OR REPLACE VIEW user_bank_default_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bank_code AS CHAR) AS bank_code,
       CAST(bank_holder AS CHAR) AS bank_holder,
       CAST(bank_account AS CHAR) AS bank_account
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
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bvn AS CHAR) AS bvn
FROM (
         SELECT user_id,
                bvn,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_personal_info
         WHERE bvn IS NOT NULL AND TRIM(bvn) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW device_ids_latest_lookup AS
SELECT CAST(device_uuid AS CHAR) AS device_uuid,
       CAST(session_uuid AS CHAR) AS session_uuid,
       CAST(aaid AS CHAR) AS aaid,
       CAST(idfa AS CHAR) AS idfa
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
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(MAX(callback_time) AS DATETIME(3)) AS callback_time
FROM risk_user_approval_callback
WHERE callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_repay_paid_latest_by_order AS
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(MAX(callback_time) AS DATETIME(3)) AS callback_time
FROM user_repay
WHERE status = 2
  AND callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_order_installment_overdue AS
SELECT CAST(user_order_id AS SIGNED) AS user_order_id,
       CAST(MAX(COALESCE(is_overdue, 0)) AS SIGNED) AS is_overdue
FROM user_order_installment
GROUP BY user_order_id;

-- application 增量 Lookup：user.id 为 UNSIGNED；device 字段统一 CHAR
CREATE OR REPLACE VIEW application_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(mobile AS CHAR) AS mobile,
       CAST(device_id AS CHAR) AS device_id,
       CAST(gps_adid AS CHAR) AS gps_adid,
       CAST(idfa AS CHAR) AS idfa
FROM user;

CREATE OR REPLACE VIEW user_repay_paid_by_order_period AS
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(current_period AS SIGNED) AS current_period,
       CAST(MAX(callback_time) AS DATETIME(3)) AS callback_time
FROM user_repay
WHERE status = 2
  AND callback_time IS NOT NULL
  AND order_no IS NOT NULL
  AND TRIM(order_no) <> ''
GROUP BY order_no, current_period;

-- loan/application 增量 Lookup：避免 UNSIGNED / DATETIME / CHAR 类型导致 ClassCastException
CREATE OR REPLACE VIEW user_order_loan_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(order_no AS CHAR) AS order_no,
       CAST(order_time AS DATETIME(3)) AS order_time,
       CAST(disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(settled_time AS DATETIME(3)) AS settled_time,
       CAST(risk_order_status AS SIGNED) AS risk_order_status
FROM user_order;

CREATE OR REPLACE VIEW vt_id_number_lookup AS
SELECT CAST(raw_value AS CHAR) AS raw_value,
       CAST(token AS CHAR) AS token
FROM vt_token_cache
WHERE vt_type = 'id_number'
  AND status = 1
  AND token IS NOT NULL
  AND TRIM(token) <> '';

-- user_info 增量：vt_token_cache 全列 CAST（ENUM/TINYINT 直查会 ClassCastException）
CREATE OR REPLACE VIEW vt_token_cache_lookup AS
SELECT CAST(vt_type AS CHAR) AS vt_type,
       CAST(raw_value AS CHAR) AS raw_value,
       CAST(token AS CHAR) AS token,
       CAST(status AS SIGNED) AS status
FROM vt_token_cache;

-- user_info 增量 Lookup：user.id / user_work_related.user_id 为 UNSIGNED 时需 CAST
CREATE OR REPLACE VIEW user_info_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

-- user_info 增量：每人最新一条 personal_info（非 CDC 触发源也走 Lookup 取最新）
CREATE OR REPLACE VIEW user_personal_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bvn AS CHAR) AS bvn,
       CAST(first_name AS CHAR) AS first_name,
       CAST(sur_name AS CHAR) AS sur_name,
       CAST(date_of_birth AS DATE) AS date_of_birth,
       CAST(education_level AS SIGNED) AS education_level,
       CAST(gender AS SIGNED) AS gender,
       CAST(living_address_state AS CHAR) AS living_address_state,
       CAST(living_address_city AS CHAR) AS living_address_city,
       CAST(living_address_first_line AS CHAR) AS living_address_first_line,
       CAST(living_address_second_line AS CHAR) AS living_address_second_line,
       CAST(number_of_children AS SIGNED) AS number_of_children,
       CAST(marriage AS SIGNED) AS marriage
FROM (
         SELECT user_id,
                bvn,
                first_name,
                sur_name,
                date_of_birth,
                education_level,
                gender,
                living_address_state,
                living_address_city,
                living_address_first_line,
                living_address_second_line,
                number_of_children,
                marriage,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_personal_info
     ) t
WHERE rn = 1;

-- vt_token_cache CDC 触发：bvn → user_id
CREATE OR REPLACE VIEW user_id_by_bvn_lookup AS
SELECT CAST(TRIM(bvn) AS CHAR) AS bvn,
       CAST(user_id AS SIGNED) AS user_id
FROM (
         SELECT user_id,
                bvn,
                ROW_NUMBER() OVER (PARTITION BY TRIM(bvn) ORDER BY id DESC) AS rn
         FROM user_personal_info
         WHERE bvn IS NOT NULL AND TRIM(bvn) <> ''
     ) t
WHERE rn = 1;

-- device_ids / device_network CDC 触发：解析到 user_id
CREATE OR REPLACE VIEW device_uuid_user_lookup AS
SELECT CAST(TRIM(device_id) AS CHAR) AS device_uuid,
       CAST(MAX(id) AS SIGNED) AS user_id
FROM user
WHERE device_id IS NOT NULL AND TRIM(device_id) <> ''
GROUP BY TRIM(device_id);

CREATE OR REPLACE VIEW session_uuid_user_lookup AS
SELECT CAST(di.session_uuid AS CHAR) AS session_uuid,
       CAST(MAX(u.id) AS SIGNED) AS user_id
FROM user u
         INNER JOIN (
    SELECT device_uuid,
           session_uuid,
           ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS rn
    FROM device_ids
    WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
      AND session_uuid IS NOT NULL AND TRIM(session_uuid) <> ''
) di ON di.device_uuid = u.device_id AND di.rn = 1
WHERE u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
GROUP BY di.session_uuid;

CREATE OR REPLACE VIEW app_config_lookup AS
SELECT CAST(app_code AS SIGNED) AS app_code,
       CAST(app_name AS CHAR) AS app_name,
       CAST(version AS CHAR) AS version
FROM app_config;

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

-- user_info 增量：与 user_info_sync_staging 同源的 Lookup 维表
CREATE OR REPLACE VIEW user_credit_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(credit_limit AS CHAR) AS credit_limit
FROM (
         SELECT user_id,
                credit_limit,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
         FROM risk_user_credit_callback
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_reg_ip_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(ip AS CHAR) AS ip
FROM (
         SELECT u2.id AS user_id,
                dn.ip,
                ROW_NUMBER() OVER (PARTITION BY u2.id ORDER BY dn.create_time DESC) AS rn
         FROM user u2
                  LEFT JOIN (
             SELECT device_uuid, session_uuid
             FROM (
                      SELECT device_uuid, session_uuid,
                             ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS di_rn
                      FROM device_ids
                      WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
                  ) di0
             WHERE di_rn = 1
         ) di ON di.device_uuid = u2.device_id
                  INNER JOIN device_network dn
                             ON dn.ip IS NOT NULL AND TRIM(dn.ip) <> ''
                                 AND (
                                    (u2.device_id IS NOT NULL AND TRIM(u2.device_id) <> '' AND dn.device_uuid = u2.device_id)
                                        OR (di.session_uuid IS NOT NULL AND TRIM(di.session_uuid) <> ''
                                        AND dn.session_uuid = di.session_uuid)
                                    )
     ) rip
WHERE rn = 1;

CREATE OR REPLACE VIEW user_emergency_contacts_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(
               COALESCE(
                       JSON_ARRAYAGG(
                               JSON_OBJECT(
                                       'name', NULLIF(TRIM(contact_name), ''),
                                       'mobile', CASE
                                                     WHEN contact_number IS NULL OR TRIM(contact_number) = '' THEN NULL
                                                     WHEN TRIM(contact_number) LIKE '+234%' THEN SUBSTRING(TRIM(contact_number), 5)
                                                     WHEN TRIM(contact_number) LIKE '234%' AND CHAR_LENGTH(TRIM(contact_number)) > 3
                                                         THEN SUBSTRING(TRIM(contact_number), 4)
                                                     WHEN TRIM(contact_number) LIKE '0%' AND CHAR_LENGTH(TRIM(contact_number)) = 11
                                                         THEN SUBSTRING(TRIM(contact_number), 2)
                                                     ELSE TRIM(contact_number)
                                           END,
                                       'relation', contact_relationship
                               )
                       ),
                       JSON_ARRAY()
               ) AS CHAR
       ) AS emergency_contacts
FROM user_emergency_contact
GROUP BY user_id;

CREATE OR REPLACE VIEW user_info_install_source_lookup AS
SELECT CAST(u.id AS SIGNED) AS user_id,
       CAST(
               CASE
                   WHEN adj.tracker_name IS NULL OR TRIM(adj.tracker_name) = '' THEN NULL
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%unattributed%' THEN NULL
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%organic%' THEN 'ORGANIC'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%google%' THEN 'GG'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%apple%' THEN 'ASA'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%tiktok%' THEN 'TT'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%facebook%'
                       OR LOWER(TRIM(adj.tracker_name)) LIKE '%instagram%'
                       OR LOWER(TRIM(adj.tracker_name)) LIKE '%messenger%' THEN 'FB'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%sms%' THEN 'SMS'
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%kuai%' THEN 'KW'
                   ELSE TRIM(adj.tracker_name)
               END AS CHAR
       ) AS install_source
FROM user u
         LEFT JOIN v_adjust_latest_by_adid adj
                   ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid;

CREATE OR REPLACE VIEW users_by_adid_lookup AS
SELECT CAST(adid AS CHAR) AS adid,
       CAST(MAX(id) AS SIGNED) AS user_id
FROM user
WHERE adid IS NOT NULL AND TRIM(adid) <> ''
GROUP BY adid;

-- user 增量 Lookup
CREATE OR REPLACE VIEW user_incr_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(mobile AS CHAR) AS mobile,
       CAST(device_id AS CHAR) AS device_id,
       CAST(adid AS CHAR) AS adid,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

-- user_bankcard 增量：bank_account → bank_info.id
CREATE OR REPLACE VIEW user_bankcard_id_by_account_lookup AS
SELECT CAST(TRIM(bank_account) AS CHAR) AS bank_account,
       CAST(id AS SIGNED) AS bank_id
FROM (
         SELECT id, bank_account,
                ROW_NUMBER() OVER (PARTITION BY TRIM(bank_account) ORDER BY id DESC) AS rn
         FROM user_bank_info
         WHERE deleted = 0
           AND bank_account IS NOT NULL
           AND TRIM(bank_account) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_bankcard_incr_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(user_id AS SIGNED) AS user_id,
       CAST(bank_code AS CHAR) AS bank_code,
       CAST(bank_account AS CHAR) AS bank_account,
       CAST(is_default AS SIGNED) AS is_default,
       CAST(deleted AS SIGNED) AS deleted
FROM user_bank_info;

-- user_product 增量：user+product 最新一单
CREATE OR REPLACE VIEW user_product_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(product_id AS CHAR) AS product_id,
       CAST(amount_max AS CHAR) AS amount_max
FROM (
         SELECT user_id,
                product_id,
                amount_max,
                ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
         FROM user_order
         WHERE user_id IS NOT NULL
           AND product_id IS NOT NULL
           AND TRIM(product_id) <> ''
     ) t
WHERE rn = 1;

-- application 增量：订单全字段 Lookup
CREATE OR REPLACE VIEW application_order_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(order_no AS CHAR) AS order_no,
       CAST(user_id AS SIGNED) AS user_id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(product_id AS CHAR) AS product_id,
       CAST(period_days AS SIGNED) AS period_days,
       CAST(period_count AS SIGNED) AS period_count,
       CAST(re_loan AS SIGNED) AS re_loan,
       CAST(amount_max AS CHAR) AS amount_max,
       CAST(received AS CHAR) AS received,
       CAST(repayment AS CHAR) AS repayment,
       CAST(poundage AS CHAR) AS poundage,
       CAST(order_time AS DATETIME(3)) AS order_time,
       CAST(disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(settled_time AS DATETIME(3)) AS settled_time,
       CAST(last_repayment_time AS DATETIME(3)) AS last_repayment_time,
       CAST(risk_order_status AS SIGNED) AS risk_order_status
FROM user_order;

CREATE OR REPLACE VIEW application_order_id_by_order_no_lookup AS
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(id AS SIGNED) AS order_id
FROM user_order
WHERE order_no IS NOT NULL AND TRIM(order_no) <> '';

-- loan 增量：分期全字段 Lookup
CREATE OR REPLACE VIEW user_order_installment_loan_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(user_order_id AS SIGNED) AS user_order_id,
       CAST(installment_order_no AS CHAR) AS installment_order_no,
       CAST(current_period AS SIGNED) AS current_period,
       CAST(received AS CHAR) AS received,
       CAST(interests AS CHAR) AS interests,
       CAST(poundage_fees AS CHAR) AS poundage_fees,
       CAST(penalty_amount AS CHAR) AS penalty_amount,
       CAST(amt_due AS CHAR) AS amt_due,
       CAST(repaid_amount AS CHAR) AS repaid_amount,
       CAST(repayment_time AS DATETIME(3)) AS repayment_time,
       CAST(is_overdue AS SIGNED) AS is_overdue,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user_order_installment;
