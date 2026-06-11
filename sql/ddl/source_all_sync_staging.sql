-- =============================================================================
-- 一步重建全部宽表（VT 已预加载 vt_token_cache status=1 后执行）
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/source_all_sync_staging.sql
--
-- 前置（若未做过）:
--   sql/ddl/source_views_adjust.sql
--   sql/ddl/source_materialize_user_adjust.sql
-- =============================================================================

-- ---------- 1. user_sync_staging ----------
DROP TABLE IF EXISTS user_sync_staging;

CREATE TABLE user_sync_staging AS
SELECT u.id,
       u.app_code,
       u.mobile,
       u.device_id,
       u.adid,
       u.create_time,
       u.update_time,
       a.network_name,
       a.tracker_name,
       a.campaign_tracker,
       a.campaign_name,
       a.creative_name,
       a.adgroup_tracker,
       a.creative_tracker,
       a.adgroup_name,
       CASE
           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
           ELSE CONCAT('+234', TRIM(u.mobile))
       END AS mobile_norm,
       vt_m.token AS mobile_token
FROM `user` u
         LEFT JOIN adjust_latest_by_adid a
                   ON u.adid IS NOT NULL AND u.adid <> '' AND a.adid = u.adid
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 'mobile' AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin;

ALTER TABLE user_sync_staging ADD PRIMARY KEY (id);

-- ---------- 2. user_bankcard_sync_staging ----------
DROP TABLE IF EXISTS user_bankcard_sync_staging;

CREATE TABLE user_bankcard_sync_staging AS
SELECT b.id,
       b.user_id,
       b.bank_code,
       TRIM(b.bank_account) AS bank_account_raw,
       vt_b.token AS bank_account_token,
       b.is_default
FROM user_bank_info b
         LEFT JOIN vt_token_cache vt_b
                   ON vt_b.vt_type = 'bank_account' AND vt_b.status = 1
                       AND vt_b.raw_value COLLATE utf8mb4_bin = TRIM(b.bank_account) COLLATE utf8mb4_bin
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL AND TRIM(b.bank_account) <> '';

ALTER TABLE user_bankcard_sync_staging ADD PRIMARY KEY (id);

-- ---------- 3. user_info_sync_staging ----------
DROP TABLE IF EXISTS user_info_sync_staging;

CREATE TABLE user_info_sync_staging AS
SELECT p.user_id,
       TRIM(p.bvn) AS bvn_raw,
       vt_id.token AS id_number_token,
       TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))) AS full_name,
       JSON_MERGE_PRESERVE(
               CAST('{
                 "birthday": null,
                 "job_type": null,
                 "education": null,
                 "gender": null,
                 "registration_ip": null,
                 "salary": null,
                 "loan_purpose": null,
                 "face_similarity": null,
                 "pay_cycle": null,
                 "salary_yearly": null,
                 "credit_limit": null,
                 "company": null,
                 "install_source": null,
                 "registration_time": null,
                 "email": null,
                 "ocr": null,
                 "profession": null,
                 "app": {"name": null, "version": null, "app_id": null},
                 "emergency_contacts": null,
                 "salary_day": null,
                 "address": {"province": null, "city": null, "district": null, "detail": null, "village": null},
                 "salary_fortnightly": null,
                 "salary_daily": null,
                 "salary_monthly": 1,
                 "children_num": null,
                 "religion": null,
                 "marital": null,
                 "full_name": null,
                 "salary_weekly": null,
                 "survey": null,
                 "salary_type": null
               }' AS JSON),
               JSON_OBJECT(
                       'birthday', DATE_FORMAT(p.date_of_birth, '%Y-%m-%d'),
                       'job_type', wr.work_type,
                       'education', p.education_level,
                       'gender', p.gender,
                       'registration_ip', reg_ip.ip,
                       'salary', CASE
                                     WHEN wr.monthly_income IS NULL OR TRIM(wr.monthly_income) = '' THEN CAST(NULL AS JSON)
                                     ELSE CAST(REPLACE(TRIM(wr.monthly_income), ',', '') AS UNSIGNED)
                           END,
                       'credit_limit', cc.credit_limit,
                       'company', NULLIF(TRIM(wr.company_name), ''),
                       'install_source', CASE
                                             WHEN COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), '')) IS NULL
                                                 THEN CAST(NULL AS JSON)
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%unattributed%'
                                                 THEN CAST(NULL AS JSON)
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%organic%'
                                                 THEN 'ORGANIC'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%google%'
                                                 THEN 'GG'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%apple%'
                                                 THEN 'ASA'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%tiktok%'
                                                 THEN 'TT'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%facebook%'
                                                 OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%instagram%'
                                                 OR LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%messenger%'
                                                 THEN 'FB'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%sms%'
                                                 THEN 'SMS'
                                             WHEN LOWER(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), ''))) LIKE '%kuai%'
                                                 THEN 'KW'
                                             ELSE TRIM(COALESCE(NULLIF(TRIM(adj.network_name), ''), NULLIF(TRIM(adj.tracker_name), '')))
                           END,
                       'registration_time', UNIX_TIMESTAMP(u.create_time),
                       'profession', wr.occupation,
                       'app', JSON_MERGE_PRESERVE(
                               CAST('{"name": null, "version": null, "app_id": null}' AS JSON),
                               JSON_OBJECT(
                                       'name', ac.app_name,
                                       'version', ac.version,
                                       'app_id', u.app_code
                               )
                              ),
                       'emergency_contacts', ec.emergency_contacts,
                       'address', JSON_MERGE_PRESERVE(
                               CAST('{"province": null, "city": null, "district": null, "detail": null, "village": null}' AS JSON),
                               JSON_OBJECT(
                                       'province', p.living_address_state,
                                       'city', p.living_address_city,
                                       'detail', NULLIF(TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ',
                                                                    COALESCE(p.living_address_second_line, ''))), '')
                               )
                              ),
                       'salary_monthly', 1,
                       'children_num', p.number_of_children,
                       'marital', p.marriage,
                       'full_name', NULLIF(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))), '')
               )
       ) AS info_json
FROM user_personal_info p
         INNER JOIN `user` u ON u.id = p.user_id
         LEFT JOIN user_work_related wr ON wr.user_id = p.user_id
         LEFT JOIN app_config ac ON ac.app_code = u.app_code
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 'id_number' AND vt_id.status = 1
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
         LEFT JOIN (
    SELECT user_id, credit_limit
    FROM (
             SELECT user_id, credit_limit,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
             FROM risk_user_credit_callback
         ) t
    WHERE rn = 1
) cc ON cc.user_id = p.user_id
         LEFT JOIN adjust_latest_by_adid adj
                   ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid
         LEFT JOIN (
    SELECT user_id, ip
    FROM (
             SELECT u.id AS user_id,
                    dn.ip,
                    ROW_NUMBER() OVER (PARTITION BY u.id ORDER BY dn.create_time DESC) AS rn
             FROM `user` u
                      LEFT JOIN (
                 SELECT device_uuid, session_uuid
                 FROM (
                          SELECT device_uuid, session_uuid,
                                 ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS di_rn
                          FROM device_ids
                          WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
                      ) di0
                 WHERE di_rn = 1
             ) di ON di.device_uuid = u.device_id
                      INNER JOIN device_network dn
                                 ON dn.ip IS NOT NULL AND TRIM(dn.ip) <> ''
                                     AND (
                                        (u.device_id IS NOT NULL AND TRIM(u.device_id) <> '' AND dn.device_uuid = u.device_id)
                                            OR (di.session_uuid IS NOT NULL AND TRIM(di.session_uuid) <> ''
                                            AND dn.session_uuid = di.session_uuid)
                                        )
         ) rip
    WHERE rn = 1
) reg_ip ON reg_ip.user_id = p.user_id
         LEFT JOIN (
    SELECT user_id,
           JSON_ARRAYAGG(
                   JSON_MERGE_PRESERVE(
                           CAST('{"name": null, "mobile": null, "relation": null}' AS JSON),
                           JSON_OBJECT(
                                   'name', NULLIF(TRIM(contact_name), ''),
                                   'mobile', CASE
                                                 WHEN contact_number IS NULL OR TRIM(contact_number) = '' THEN CAST(NULL AS JSON)
                                                 WHEN TRIM(contact_number) LIKE '+234%' THEN SUBSTRING(TRIM(contact_number), 5)
                                                 WHEN TRIM(contact_number) LIKE '234%' AND CHAR_LENGTH(TRIM(contact_number)) > 3
                                                     THEN SUBSTRING(TRIM(contact_number), 4)
                                                 WHEN TRIM(contact_number) LIKE '0%' AND CHAR_LENGTH(TRIM(contact_number)) = 11
                                                     THEN SUBSTRING(TRIM(contact_number), 2)
                                                 ELSE TRIM(contact_number)
                                       END,
                                   'relation', contact_relationship
                           )
                   )
           ) AS emergency_contacts
    FROM user_emergency_contact
    GROUP BY user_id
) ec ON ec.user_id = p.user_id;

ALTER TABLE user_info_sync_staging ADD PRIMARY KEY (user_id);

-- ---------- 4. user_product_sync_staging（按映射 v3：user_order 取最新 product）----------
DROP TABLE IF EXISTS user_product_sync_staging;

CREATE TABLE user_product_sync_staging AS
SELECT t.user_id,
       t.product_id,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS credit_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS unpaid_amount_minor
FROM (
         SELECT o.user_id,
                o.product_id,
                o.amount_max,
                ROW_NUMBER() OVER (PARTITION BY o.user_id, o.product_id ORDER BY o.order_time DESC) AS rn
         FROM user_order o
     ) t
WHERE t.rn = 1;

ALTER TABLE user_product_sync_staging ADD PRIMARY KEY (user_id, product_id);

-- ---------- 5. application_sync_staging ----------
-- device_ids / user_bank_info 可能一对多，子查询去重保证每单 o.id 一行
DROP TABLE IF EXISTS application_sync_staging;

CREATE TABLE application_sync_staging AS
SELECT base.id,
       base.application_no,
       base.sn,
       base.user_id,
       base.app_code,
       base.app_id_num,
       base.device_uuid,
       base.session_id,
       base.mobile_norm,
       base.bvn_raw,
       base.bank_account_raw,
       base.gaid_idfa_raw,
       base.mobile_token,
       base.id_number_token,
       base.gaid_idfa_token,
       base.bank_code,
       base.bank_account_name,
       base.bank_account_token,
       base.product_id,
       base.period_days,
       base.period_count,
       base.re_loan,
       base.amount_max,
       base.received,
       base.repayment,
       base.amt_due,
       base.order_time,
       base.disburse_time,
       base.settled_time,
       base.last_repayment_time,
       base.approval_result,
       base.disburse_status,
       base.risk_order_status,
       base.settled_status,
       base.credit_limit_minor,
       base.loan_amount_minor,
       base.principal_minor,
       base.total_amount_minor,
       base.disbursed_amount_minor,
       CASE base.risk_order_status
           WHEN 2 THEN 3
           WHEN 4 THEN 5
           WHEN 6 THEN 13
           WHEN 8 THEN 15
           WHEN 10 THEN 20
           WHEN 11 THEN 23
           WHEN 40 THEN 25
           WHEN 20 THEN 27
           WHEN 30 THEN 27
           WHEN 50 THEN 27
           ELSE 1
           END AS risk_status,
       base.repayment_plan_json
FROM (
         SELECT o.id,
                o.order_no AS application_no,
                o.order_no AS sn,
                o.user_id,
                o.app_code,
                o.order_status,
                ac.id AS app_id_num,
                u.device_id AS device_uuid,
                di.session_uuid AS session_id,
                (CASE
                     WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                     WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                     WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                     WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                     ELSE CONCAT('+234', TRIM(u.mobile))
                 END) AS mobile_norm,
                TRIM(p.bvn) AS bvn_raw,
                TRIM(ub.bank_account) AS bank_account_raw,
                TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''),
                              NULLIF(TRIM(u.idfa), ''),
                              NULLIF(TRIM(di.aaid), ''))) AS gaid_idfa_raw,
                vt_m.token AS mobile_token,
                vt_id.token AS id_number_token,
                vt_g.token AS gaid_idfa_token,
                ub.bank_code,
                ub.bank_holder AS bank_account_name,
                vt_ba.token AS bank_account_token,
                o.product_id,
                o.period_days,
                o.period_count,
                o.re_loan,
                o.amount_max,
                o.received,
                o.repayment,
                o.amt_due,
                o.order_time,
                o.disburse_time,
                o.settled_time,
                o.last_repayment_time,
                o.approval_result,
                o.disburse_status,
                o.risk_order_status,
                o.settled_status,
                CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS credit_limit_minor,
                CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS loan_amount_minor,
                CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS principal_minor,
                CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.repayment), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS total_amount_minor,
                CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS disbursed_amount_minor,
                JSON_OBJECT(
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
       ) AS repayment_plan_json
         FROM user_order o
         INNER JOIN `user` u ON u.id = o.user_id
         LEFT JOIN app_config ac ON ac.app_code = o.app_code
         LEFT JOIN (
    SELECT user_id, bvn
    FROM (
             SELECT user_id, bvn,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
             FROM user_personal_info
             WHERE bvn IS NOT NULL AND TRIM(bvn) <> ''
         ) t
    WHERE rn = 1
) p ON p.user_id = o.user_id
         LEFT JOIN (
    SELECT user_id, bank_code, bank_holder, bank_account
    FROM (
             SELECT user_id, bank_code, bank_holder, bank_account,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
             FROM user_bank_info
             WHERE deleted = 0 AND is_default = 1
               AND bank_account IS NOT NULL AND TRIM(bank_account) <> ''
         ) t
    WHERE rn = 1
) ub ON ub.user_id = o.user_id
         LEFT JOIN (
    SELECT device_uuid, session_uuid, aaid
    FROM (
             SELECT device_uuid, session_uuid, aaid,
                    ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS rn
             FROM device_ids
             WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
         ) t
    WHERE rn = 1
) di ON di.device_uuid = u.device_id
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 'mobile' AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 'id_number' AND vt_id.status = 1
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_g
                   ON vt_g.vt_type = 'gaid_idfa' AND vt_g.status = 1
                       AND vt_g.raw_value COLLATE utf8mb4_bin = TRIM(COALESCE(NULLIF(TRIM(u.gps_adid), ''),
                                                                              NULLIF(TRIM(u.idfa), ''),
                                                                              NULLIF(TRIM(di.aaid), ''))) COLLATE utf8mb4_bin
         LEFT JOIN vt_token_cache vt_ba
                   ON vt_ba.vt_type = 'bank_account' AND vt_ba.status = 1
                       AND vt_ba.raw_value COLLATE utf8mb4_bin = TRIM(ub.bank_account) COLLATE utf8mb4_bin
         WHERE o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
     ) base;

ALTER TABLE application_sync_staging ADD PRIMARY KEY (id);

-- ---------- 6. loan_sync_staging ----------
DROP TABLE IF EXISTS loan_sync_staging;

CREATE TABLE loan_sync_staging AS
SELECT i.id,
       i.installment_order_no AS loan_no,
       o.order_no AS application_no,
       CAST(COALESCE(i.current_period, 1) AS UNSIGNED) AS period,
       CAST(0 AS UNSIGNED) AS roll_sequence,
       COALESCE(DATE(o.disburse_time), DATE(o.order_time), DATE(i.create_time)) AS start_date,
       DATE(i.repayment_time) AS due_date,
       DATE(i.repayment_time) AS due_date_final,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.received), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS principal_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.interests), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS interest_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.poundage_fees), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS admin_fee_minor,
       CAST(0 AS SIGNED) AS roll_fee_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.penalty_amount), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS penalty_amount_minor,
       CAST(0 AS SIGNED) AS reduction_amount_minor,
       CAST(COALESCE(ROUND((CAST(NULLIF(TRIM(i.amt_due), '') AS DECIMAL(20, 2))
           + CAST(NULLIF(TRIM(i.penalty_amount), '') AS DECIMAL(20, 2))), 0), 0) AS SIGNED) AS total_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(i.repaid_amount), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS paid_amount_minor,
       CAST(0 AS SIGNED) AS roll_paid_amount_minor,
       CAST(
           CASE
               WHEN ur_cb.callback_time IS NOT NULL
                   AND UNIX_TIMESTAMP(ur_cb.callback_time) > 0
                   THEN UNIX_TIMESTAMP(ur_cb.callback_time) * 1000
               ELSE NULL
               END AS SIGNED
       ) AS paid_time_ms,
       DATE(o.settled_time) AS paid_off_date,
       CASE
           WHEN o.risk_order_status = 10
               AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) = 0 THEN 20
           WHEN o.risk_order_status = 10
               AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) <> 0 THEN 24
           WHEN o.risk_order_status = 11 THEN 23
           WHEN o.risk_order_status = 40 THEN 25
           WHEN o.risk_order_status IN (20, 30, 50) THEN 27
           ELSE 20
           END AS risk_status
FROM user_order_installment i
         INNER JOIN user_order o ON o.id = i.user_order_id
         LEFT JOIN (
    SELECT order_no,
           current_period,
           MAX(callback_time) AS callback_time
    FROM user_repay
    WHERE status = 2
      AND callback_time IS NOT NULL
      AND order_no IS NOT NULL
      AND TRIM(order_no) <> ''
    GROUP BY order_no, current_period
) ur_cb ON ur_cb.order_no = o.order_no
    AND ur_cb.current_period = i.current_period
WHERE i.installment_order_no IS NOT NULL
  AND TRIM(i.installment_order_no) <> ''
  AND (o.risk_order_status IS NULL OR o.risk_order_status NOT IN (2, 4, 6, 8));

ALTER TABLE loan_sync_staging ADD PRIMARY KEY (id);

-- ---------- 校验 ----------
SELECT 'user' AS tbl, COUNT(*) AS missing_token
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL AND (mobile_token IS NULL OR mobile_token = '')
UNION ALL
SELECT 'user_bankcard', COUNT(*)
FROM user_bankcard_sync_staging
WHERE bank_account_raw IS NOT NULL AND (bank_account_token IS NULL OR bank_account_token = '')
UNION ALL
SELECT 'user_info', COUNT(*)
FROM user_info_sync_staging
WHERE bvn_raw IS NOT NULL AND TRIM(bvn_raw) <> ''
  AND (id_number_token IS NULL OR id_number_token = '')
UNION ALL
SELECT 'application_mobile', COUNT(*)
FROM application_sync_staging
WHERE mobile_token IS NULL OR mobile_token = ''
UNION ALL
SELECT 'application_id_number', COUNT(*)
FROM application_sync_staging
WHERE id_number_token IS NULL OR id_number_token = ''
UNION ALL
SELECT 'application_bank', COUNT(*)
FROM application_sync_staging
WHERE bank_account_token IS NULL OR bank_account_token = '';

SELECT 'staging_row_counts' AS label,
       (SELECT COUNT(*) FROM user_sync_staging) AS user_cnt,
       (SELECT COUNT(*) FROM user_bankcard_sync_staging) AS bankcard_cnt,
       (SELECT COUNT(*) FROM user_info_sync_staging) AS user_info_cnt,
       (SELECT COUNT(*) FROM user_product_sync_staging) AS user_product_cnt,
       (SELECT COUNT(*) FROM application_sync_staging) AS application_cnt,
       (SELECT COUNT(*) FROM loan_sync_staging) AS loan_cnt;
