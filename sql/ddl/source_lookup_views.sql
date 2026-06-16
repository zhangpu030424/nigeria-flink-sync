-- 增量 Job JDBC Lookup 视图（唯一入口，由 deploy-source-ddl.sh 部署）
-- 废弃视图清理: sql/ddl/drop_legacy_views.sql

-- ========== application / loan ==========

CREATE OR REPLACE VIEW user_bank_default_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bank_code AS CHAR) AS bank_code,
       CAST(bank_holder AS CHAR) AS bank_holder,
       CAST(bank_account AS CHAR) AS bank_account
FROM (
         SELECT user_id, bank_code, bank_holder, bank_account,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_bank_info
         WHERE deleted = 0 AND is_default = 1
           AND bank_account IS NOT NULL AND TRIM(bank_account) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_bvn_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bvn AS CHAR) AS bvn
FROM (
         SELECT user_id, bvn,
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
         SELECT device_uuid, session_uuid, aaid, idfa,
                ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS rn
         FROM device_ids
         WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW risk_approval_latest_by_order AS
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(MAX(callback_time) AS DATETIME(3)) AS callback_time
FROM risk_user_approval_callback
WHERE callback_time IS NOT NULL AND order_no IS NOT NULL AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_repay_paid_latest_by_order AS
SELECT CAST(order_no AS CHAR) AS order_no,
       CAST(MAX(callback_time) AS DATETIME(3)) AS callback_time
FROM user_repay
WHERE status = 2 AND callback_time IS NOT NULL
  AND order_no IS NOT NULL AND TRIM(order_no) <> ''
GROUP BY order_no;

CREATE OR REPLACE VIEW user_order_installment_overdue AS
SELECT CAST(user_order_id AS SIGNED) AS user_order_id,
       CAST(MAX(COALESCE(is_overdue, 0)) AS SIGNED) AS is_overdue
FROM user_order_installment
GROUP BY user_order_id;

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
WHERE status = 2 AND callback_time IS NOT NULL
  AND order_no IS NOT NULL AND TRIM(order_no) <> ''
GROUP BY order_no, current_period;

CREATE OR REPLACE VIEW user_order_loan_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(order_no AS CHAR) AS order_no,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(order_time AS DATETIME(3)) AS order_time,
       CAST(disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(settled_time AS DATETIME(3)) AS settled_time,
       CAST(risk_order_status AS SIGNED) AS risk_order_status
FROM user_order;

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

-- ========== user_info（子视图 → bundle）==========

CREATE OR REPLACE VIEW vt_token_cache_lookup AS
SELECT CAST(CASE vt_type
                WHEN 1 THEN 'mobile'
                WHEN 2 THEN 'gaid_idfa'
                WHEN 3 THEN 'bank_account'
                WHEN 4 THEN 'id_number'
                WHEN 5 THEN 'emergency_contact'
                WHEN 6 THEN 'id2'
                ELSE CAST(vt_type AS CHAR)
            END AS CHAR) AS vt_type,
       CAST(raw_value AS CHAR) AS raw_value,
       CAST(token AS CHAR) AS token,
       CAST(status AS SIGNED) AS status
FROM vt_token_cache;

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
         SELECT user_id, bvn, first_name, sur_name, date_of_birth,
                education_level, gender, living_address_state, living_address_city,
                living_address_first_line, living_address_second_line,
                number_of_children, marriage,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_personal_info
     ) t
WHERE rn = 1;

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
         SELECT user_id, work_type, occupation, company_name, monthly_income,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_work_related
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_credit_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(credit_limit AS CHAR) AS credit_limit
FROM (
         SELECT user_id, credit_limit,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
         FROM risk_user_credit_callback
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_reg_ip_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(ip AS CHAR) AS ip
FROM (
         SELECT u2.id AS user_id, dn.ip,
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
SELECT CAST(ec.user_id AS SIGNED) AS user_id,
       CAST(
               COALESCE(
                       JSON_ARRAYAGG(
                               JSON_OBJECT(
                                       'name', NULLIF(TRIM(ec.contact_name), ''),
                                       'mobile', CASE
                                                     WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = ''
                                                         THEN CAST(NULL AS JSON)
                                                     WHEN vt.token IS NOT NULL AND TRIM(vt.token) <> ''
                                                         THEN vt.token
                                                     ELSE (
                                                         CASE
                                                             WHEN TRIM(ec.contact_number) LIKE '+%'
                                                                 THEN TRIM(ec.contact_number)
                                                             WHEN TRIM(ec.contact_number) LIKE '234%'
                                                                 THEN CONCAT('+', TRIM(ec.contact_number))
                                                             WHEN TRIM(ec.contact_number) LIKE '0%'
                                                                 THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                                                             ELSE CONCAT('+234', TRIM(ec.contact_number))
                                                         END
                                                     )
                                           END,
                                       'relation', ec.contact_relationship
                               )
                       ),
                       JSON_ARRAY()
               ) AS CHAR
       ) AS emergency_contacts
FROM user_emergency_contact ec
         LEFT JOIN vt_token_cache vt
                   ON vt.vt_type = 5 AND vt.status = 1
                       AND vt.token IS NOT NULL AND TRIM(vt.token) <> ''
                       AND vt.raw_value COLLATE utf8mb4_bin = (
                           CASE
                               WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = '' THEN NULL
                               WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                               WHEN TRIM(ec.contact_number) LIKE '234%'
                                   THEN CONCAT('+', TRIM(ec.contact_number))
                               WHEN TRIM(ec.contact_number) LIKE '0%'
                                   THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                               ELSE CONCAT('+234', TRIM(ec.contact_number))
                               END
                           ) COLLATE utf8mb4_bin
GROUP BY ec.user_id;

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

-- Flink user_info 增量唯一 Lookup 入口
CREATE OR REPLACE VIEW user_info_incr_bundle_lookup AS
SELECT CAST(u.id AS SIGNED) AS user_id,
       CAST(u.app_code AS SIGNED) AS app_code,
       CAST(u.create_time AS DATETIME(3)) AS create_time,
       CAST(p.bvn AS CHAR) AS bvn,
       CAST(p.first_name AS CHAR) AS first_name,
       CAST(p.sur_name AS CHAR) AS sur_name,
       CAST(p.date_of_birth AS DATE) AS date_of_birth,
       CAST(p.education_level AS SIGNED) AS education_level,
       CAST(p.gender AS SIGNED) AS gender,
       CAST(p.living_address_state AS CHAR) AS living_address_state,
       CAST(p.living_address_city AS CHAR) AS living_address_city,
       CAST(p.living_address_first_line AS CHAR) AS living_address_first_line,
       CAST(p.living_address_second_line AS CHAR) AS living_address_second_line,
       CAST(p.number_of_children AS SIGNED) AS number_of_children,
       CAST(p.marriage AS SIGNED) AS marriage,
       CAST(vt.token AS CHAR) AS vt_token,
       CAST(vt.status AS SIGNED) AS vt_status,
       CAST(wr.work_type AS CHAR) AS work_type,
       CAST(wr.occupation AS CHAR) AS occupation,
       CAST(wr.company_name AS CHAR) AS company_name,
       CAST(wr.monthly_income AS CHAR) AS monthly_income,
       CAST(ac.app_name AS CHAR) AS app_name,
       CAST(ac.version AS CHAR) AS app_version,
       CAST(cc.credit_limit AS CHAR) AS credit_limit,
       CAST(rip.ip AS CHAR) AS reg_ip,
       CAST(ec.emergency_contacts AS CHAR) AS emergency_contacts,
       CAST(isrc.install_source AS CHAR) AS install_source,
       -- 与 user_info_sync_staging.info_json 同结构：固定全部 key，无值写 null；emergency_contacts 无数据为 []
       CAST(JSON_OBJECT(
               'birthday', DATE_FORMAT(p.date_of_birth, '%Y-%m-%d'),
               'job_type', wr.work_type,
               'education', p.education_level,
               'gender', p.gender,
               'registration_ip', rip.ip,
               'salary', CASE
                             WHEN wr.monthly_income IS NULL OR TRIM(wr.monthly_income) = '' THEN NULL
                             WHEN LENGTH(REPLACE(TRIM(wr.monthly_income), ',', '')) BETWEEN 1 AND 19
                                 AND REPLACE(TRIM(wr.monthly_income), ',', '') REGEXP '^[0-9]+$'
                                 THEN CAST(REPLACE(TRIM(wr.monthly_income), ',', '') AS UNSIGNED)
                             ELSE NULL
                   END,
               'loan_purpose', NULL,
               'face_similarity', NULL,
               'pay_cycle', NULL,
               'salary_yearly', NULL,
               'credit_limit', CASE
                                   WHEN cc.credit_limit IS NULL OR TRIM(cc.credit_limit) = '' THEN NULL
                                   WHEN CAST(cc.credit_limit AS CHAR) REGEXP '^[0-9]{1,19}$'
                                       THEN CAST(cc.credit_limit AS UNSIGNED)
                                   ELSE NULL
                   END,
               'company', NULLIF(TRIM(wr.company_name), ''),
               'install_source', isrc.install_source,
               'registration_time', UNIX_TIMESTAMP(u.create_time),
               'email', NULL,
               'ocr', NULL,
               'profession', wr.occupation,
               'app', JSON_OBJECT(
                       'name', ac.app_name,
                       'version', ac.version,
                       'app_id', u.app_code
                      ),
               'emergency_contacts', COALESCE(CAST(ec.emergency_contacts AS JSON), CAST('[]' AS JSON)),
               'salary_day', NULL,
               'address', JSON_OBJECT(
                       'province', p.living_address_state,
                       'city', p.living_address_city,
                       'district', NULL,
                       'detail', NULLIF(TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ',
                                                    COALESCE(p.living_address_second_line, ''))), ''),
                       'village', NULL
                      ),
               'salary_fortnightly', NULL,
               'salary_daily', NULL,
               'salary_monthly', 1,
               'children_num', p.number_of_children,
               'religion', NULL,
               'marital', p.marriage,
               'full_name', NULLIF(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))), ''),
               'salary_weekly', NULL,
               'survey', NULL,
               'salary_type', NULL
       ) AS CHAR) AS info_json
FROM `user` u
         LEFT JOIN user_personal_latest_lookup p ON p.user_id = u.id
         LEFT JOIN vt_token_cache_lookup vt
                   ON vt.vt_type = 'id_number'
                       AND p.bvn IS NOT NULL AND TRIM(p.bvn) <> ''
                       AND vt.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
         LEFT JOIN user_work_latest_lookup wr ON wr.user_id = u.id
         LEFT JOIN app_config_lookup ac ON ac.app_code = u.app_code
         LEFT JOIN user_credit_latest_lookup cc ON cc.user_id = u.id
         LEFT JOIN user_reg_ip_lookup rip ON rip.user_id = u.id
         LEFT JOIN user_emergency_contacts_lookup ec ON ec.user_id = u.id
         LEFT JOIN user_info_install_source_lookup isrc ON isrc.user_id = u.id;

-- ========== user / user_bankcard / user_product ==========

CREATE OR REPLACE VIEW users_by_adid_lookup AS
SELECT CAST(adid AS CHAR) AS adid,
       CAST(MAX(id) AS SIGNED) AS user_id
FROM user
WHERE adid IS NOT NULL AND TRIM(adid) <> ''
GROUP BY adid;

CREATE OR REPLACE VIEW user_incr_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(mobile AS CHAR) AS mobile,
       CAST(device_id AS CHAR) AS device_id,
       CAST(adid AS CHAR) AS adid,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

CREATE OR REPLACE VIEW user_bankcard_id_by_account_lookup AS
SELECT CAST(TRIM(bank_account) AS CHAR) AS bank_account,
       CAST(id AS SIGNED) AS bank_id
FROM (
         SELECT id, bank_account,
                ROW_NUMBER() OVER (PARTITION BY TRIM(bank_account) ORDER BY id DESC) AS rn
         FROM user_bank_info
         WHERE deleted = 0 AND bank_account IS NOT NULL AND TRIM(bank_account) <> ''
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

CREATE OR REPLACE VIEW user_product_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(product_id AS CHAR) AS product_id,
       CAST(amount_max AS CHAR) AS amount_max
FROM (
         SELECT user_id, product_id, amount_max,
                ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
         FROM user_order
         WHERE user_id IS NOT NULL AND product_id IS NOT NULL AND TRIM(product_id) <> ''
     ) t
WHERE rn = 1;
