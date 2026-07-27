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
SELECT id AS id,
       CAST(order_no AS CHAR) AS order_no,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(order_time AS DATETIME(3)) AS order_time,
       CAST(disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(settled_time AS DATETIME(3)) AS settled_time,
       CAST(risk_order_status AS SIGNED) AS risk_order_status
FROM user_order;

CREATE OR REPLACE VIEW application_order_lookup AS
SELECT CAST(o.id AS SIGNED) AS id,
       CAST(o.order_no AS CHAR) AS order_no,
       CAST(o.user_id AS SIGNED) AS user_id,
       CAST(o.app_code AS SIGNED) AS app_code,
       CAST(COALESCE(pm.dst, TRIM(o.product_id)) AS CHAR) AS product_id,
       CAST(o.period_days AS SIGNED) AS period_days,
       CAST(o.period_count AS SIGNED) AS period_count,
       CAST(o.re_loan AS SIGNED) AS re_loan,
       CAST(o.amount_max AS CHAR) AS amount_max,
       CAST(o.received AS CHAR) AS received,
       CAST(o.repayment AS CHAR) AS repayment,
       CAST(o.poundage AS CHAR) AS poundage,
       CAST(o.order_time AS DATETIME(3)) AS order_time,
       CAST(o.disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(o.settled_time AS DATETIME(3)) AS settled_time,
       CAST(o.last_repayment_time AS DATETIME(3)) AS last_repayment_time,
       CAST(o.risk_order_status AS SIGNED) AS risk_order_status
FROM user_order o
         LEFT JOIN product_id_map pm ON pm.src = TRIM(o.product_id);

CREATE OR REPLACE VIEW user_order_installment_loan_lookup AS
SELECT id AS id,
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
-- user_id 必须裸列 u.id：CAST(... AS SIGNED) 会导致 WHERE user_id=? 无法走 PRIMARY（全表扫）
SELECT u.id AS user_id,
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
SELECT adid AS adid,
       MAX(id) AS user_id
FROM user
WHERE adid IS NOT NULL AND TRIM(adid) <> ''
GROUP BY adid;

CREATE OR REPLACE VIEW user_incr_lookup AS
SELECT u.id AS id,
       CAST(u.app_code AS SIGNED) AS app_code,
       CAST(u.mobile AS CHAR) AS mobile,
       CAST((CASE
                 WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                 WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                 WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                 WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                 ELSE CONCAT('+234', TRIM(u.mobile))
           END) AS CHAR) AS mobile_norm,
       CAST(vt_m.token AS CHAR) AS mobile_token,
       CAST(u.device_id AS CHAR) AS device_id,
       CAST(u.adid AS CHAR) AS adid,
       CAST(u.create_time AS DATETIME(3)) AS create_time
FROM user u
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 1 AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin;

CREATE OR REPLACE VIEW user_bankcard_id_by_account_lookup AS
SELECT TRIM(bank_account) AS bank_account,
       id AS bank_id
FROM (
         SELECT id, bank_account,
                ROW_NUMBER() OVER (PARTITION BY TRIM(bank_account) ORDER BY id DESC) AS rn
         FROM user_bank_info
         WHERE deleted = 0 AND bank_account IS NOT NULL AND TRIM(bank_account) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_bankcard_incr_lookup AS
SELECT b.id AS id,
       CAST(b.user_id AS SIGNED) AS user_id,
       CAST(b.bank_code AS CHAR) AS bank_code,
       CAST(b.bank_account AS CHAR) AS bank_account,
       CAST(vt.token AS CHAR) AS bank_account_token,
       CAST(b.is_default AS SIGNED) AS is_default,
       CAST(b.deleted AS SIGNED) AS deleted
FROM user_bank_info b
         LEFT JOIN vt_token_cache vt
                   ON vt.vt_type = 3 AND vt.status = 1
                       AND vt.raw_value COLLATE utf8mb4_bin = TRIM(b.bank_account) COLLATE utf8mb4_bin;


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

-- ========== application 增量（30s SLA：单次 bundle Lookup + 无双流 Join）==========
-- 注意：Flink JDBC Lookup 是 WHERE pk=? 点查。禁止 JOIN 带 ROW_NUMBER/全表聚合的子视图，
-- 否则优化器会先物化几十万行再过滤，表现为 LookupJoin 100% 反压。

-- order_id 必须 SIGNED：MAX(id) 在 MySQL 对 bigint unsigned 会变成 unsigned，
-- Flink JDBC Lookup 读成 BigInteger → ClassCastException（不能转 Long）
CREATE OR REPLACE VIEW application_order_id_by_order_no_lookup AS
SELECT order_no AS order_no,
       CAST(MAX(id) AS SIGNED) AS order_id
FROM user_order
WHERE order_no IS NOT NULL AND TRIM(order_no) <> ''
  AND app_code IN (567, 568, 569, 571, 572, 573)
GROUP BY order_no;

CREATE OR REPLACE VIEW application_latest_order_by_user_lookup AS
SELECT user_id AS user_id,
       CAST(MAX(id) AS SIGNED) AS order_id
FROM user_order
WHERE user_id IS NOT NULL
  AND app_code IN (567, 568, 569, 571, 572, 573)
GROUP BY user_id;

CREATE OR REPLACE VIEW application_latest_order_by_device_lookup AS
SELECT u.device_id AS device_uuid,
       CAST(MAX(o.id) AS SIGNED) AS order_id
FROM `user` u
         INNER JOIN user_order o ON o.user_id = u.id
WHERE u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
  AND o.app_code IN (567, 568, 569, 571, 572, 573)
GROUP BY u.device_id;

-- 按 order.id 点查友好：禁止 SELECT 列表子查询（会强制 TEMPTABLE 全表物化）
CREATE OR REPLACE VIEW application_incr_bundle_lookup AS
SELECT o.id AS id,
       CAST(CONCAT('ng0', CAST(o.app_code AS CHAR), '-', o.order_no) AS CHAR) AS application_no,
       CAST(o.order_no AS CHAR) AS sn,
       CAST(o.user_id AS SIGNED) AS user_id,
       CAST(o.app_code AS SIGNED) AS app_code,
       CAST(COALESCE(u.device_id, '') AS CHAR) AS device_uuid,
       CAST(di.session_uuid AS CHAR) AS session_id,
       CAST((CASE
                 WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                 WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                 WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                 WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                 ELSE CONCAT('+234', TRIM(u.mobile))
           END) AS CHAR) AS mobile_norm,
       CAST(TRIM(p.bvn) AS CHAR) AS bvn_raw,
       CAST(TRIM(ub.bank_account) AS CHAR) AS bank_account_raw,
       CAST(TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''), NULLIF(TRIM(u.idfa), ''), NULLIF(TRIM(di.aaid), ''))) AS CHAR) AS gaid_idfa_raw,
       CAST(vt_m.token AS CHAR) AS mobile_token,
       CAST(vt_id.token AS CHAR) AS id_number_token,
       CAST(vt_g.token AS CHAR) AS gaid_idfa_token,
       CAST(COALESCE(ub.bank_code, '') AS CHAR) AS bank_code,
       CAST(COALESCE(ub.bank_holder, '') AS CHAR) AS bank_account_name,
       CAST(vt_ba.token AS CHAR) AS bank_account_token,
       CAST(COALESCE(pm.dst, TRIM(o.product_id)) AS CHAR) AS product_id,
       CAST(COALESCE(o.period_days, 7) AS SIGNED) AS period_days,
       CAST(COALESCE(o.period_count, 1) AS SIGNED) AS period_count,
       CAST(COALESCE(o.re_loan, 0) AS SIGNED) AS re_loan,
       CAST(o.order_time AS DATETIME(3)) AS order_time,
       CAST(ruac.callback_time AS DATETIME(3)) AS reviewed_time,
       CAST(o.disburse_time AS DATETIME(3)) AS disburse_time,
       CAST(o.settled_time AS DATETIME(3)) AS settled_time,
       CAST(ur_lp.callback_time AS DATETIME(3)) AS last_paid_time,
       CAST(o.last_repayment_time AS DATETIME(3)) AS last_repayment_time,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS credit_limit_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS loan_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS principal_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.repayment), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS total_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS disbursed_amount_minor,
       CAST(
               CASE CAST(o.risk_order_status AS SIGNED)
                   WHEN 2 THEN 3
                   WHEN 4 THEN 5
                   WHEN 6 THEN 13
                   WHEN 8 THEN 15
                   WHEN 10 THEN CASE WHEN ov.id IS NOT NULL THEN 23 ELSE 20 END
                   WHEN 11 THEN 23
                   WHEN 40 THEN 25
                   WHEN 20 THEN 27
                   WHEN 30 THEN 27
                   WHEN 50 THEN 27
                   ELSE 1
                   END AS SIGNED
       ) AS risk_status,
       CAST(JSON_OBJECT(
               'roll_sequence', 0,
               'period', 1,
               'principal', CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED),
               'disbursed_amount', CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED),
               'interest', 0,
               'admin_fee', CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.poundage), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED),
               'service_fee', 0,
               'tax_fee', 0,
               'reduction_amount', 0,
               'total_amount', CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.repayment), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED),
               'term', COALESCE(o.period_days, 7),
               'start_date', DATE_FORMAT(o.order_time, '%Y-%m-%d'),
               'due_date', DATE_FORMAT(o.last_repayment_time, '%Y-%m-%d'),
               'roll_allowed', 0
            ) AS CHAR) AS repayment_plan_json
FROM user_order o
         INNER JOIN `user` u ON u.id = o.user_id
         LEFT JOIN product_id_map pm ON pm.src = TRIM(o.product_id)
         LEFT JOIN user_personal_info p
                   ON p.id = (
                       SELECT MAX(p2.id)
                       FROM user_personal_info p2
                       WHERE p2.user_id = o.user_id
                         AND p2.bvn IS NOT NULL AND TRIM(p2.bvn) <> ''
                   )
         LEFT JOIN user_bank_info ub
                   ON ub.id = (
                       SELECT MAX(b2.id)
                       FROM user_bank_info b2
                       WHERE b2.user_id = o.user_id
                         AND b2.deleted = 0 AND b2.is_default = 1
                         AND b2.bank_account IS NOT NULL AND TRIM(b2.bank_account) <> ''
                   )
         LEFT JOIN device_ids di
                   ON di.id = (
                       SELECT MAX(d2.id)
                       FROM device_ids d2
                       WHERE u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
                         AND d2.device_uuid = u.device_id
                   )
         LEFT JOIN risk_user_approval_callback ruac
                   ON ruac.order_no = o.order_no
                       AND ruac.callback_time = (
                           SELECT MAX(ra2.callback_time)
                           FROM risk_user_approval_callback ra2
                           WHERE ra2.order_no = o.order_no
                             AND ra2.callback_time IS NOT NULL
                       )
         LEFT JOIN user_repay ur_lp
                   ON ur_lp.order_no = o.order_no
                       AND ur_lp.status = 2
                       AND ur_lp.callback_time = (
                           SELECT MAX(ur2.callback_time)
                           FROM user_repay ur2
                           WHERE ur2.order_no = o.order_no
                             AND ur2.status = 2
                             AND ur2.callback_time IS NOT NULL
                       )
         LEFT JOIN user_order_installment ov
                   ON ov.id = (
                       SELECT MAX(i2.id)
                       FROM user_order_installment i2
                       WHERE i2.user_order_id = o.id
                         AND COALESCE(i2.is_overdue, 0) = 1
                   )
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 1 AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 4 AND vt_id.status = 1
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_g
                   ON vt_g.vt_type = 2 AND vt_g.status = 1
                       AND vt_g.raw_value COLLATE utf8mb4_bin = TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''),
                                                                              NULLIF(TRIM(u.idfa), ''),
                                                                              NULLIF(TRIM(di.aaid), ''))) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_ba
                   ON vt_ba.vt_type = 3 AND vt_ba.status = 1
                       AND vt_ba.raw_value COLLATE utf8mb4_bin = TRIM(ub.bank_account) COLLATE utf8mb4_bin
WHERE o.app_code IN (567, 568, 569, 571, 572, 573)
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> '';

-- ========== loan 增量：去掉 CDC 双流 Join ==========
CREATE OR REPLACE VIEW loan_installment_ids_by_user_order_lookup AS
SELECT user_order_id AS user_order_id,
       id AS installment_id
FROM user_order_installment
WHERE user_order_id IS NOT NULL;

CREATE OR REPLACE VIEW loan_installment_id_by_order_no_period_lookup AS
SELECT o.order_no AS order_no,
       i.current_period AS current_period,
       i.id AS installment_id
FROM user_order_installment i
         INNER JOIN user_order o ON o.id = i.user_order_id
WHERE o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND i.current_period IS NOT NULL;
