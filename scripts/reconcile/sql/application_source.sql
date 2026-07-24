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
       base.reviewed_time,
       base.disburse_time,
       base.settled_time,
       base.last_paid_time,
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
           WHEN 10 THEN CASE WHEN base.is_overdue = 1 THEN 23 ELSE 20 END
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
                CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no) AS application_no,
                o.order_no AS sn,
                o.user_id,
                o.app_code,
                o.order_status,
                ac.id AS app_id_num,  -- 备查；目标 application.app_id 同步用 o.app_code
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
                COALESCE(pm.dst, TRIM(o.product_id)) AS product_id,
                o.period_days,
                o.period_count,
                o.re_loan,
                o.amount_max,
                o.received,
                o.repayment,
                o.amt_due,
                o.order_time,
                ruac.callback_time AS reviewed_time,
                o.disburse_time,
                o.settled_time,
                ur_lp.callback_time AS last_paid_time,
                o.last_repayment_time,
                o.approval_result,
                o.disburse_status,
                o.risk_order_status,
                o.settled_status,
                COALESCE(inst.is_overdue, 0) AS is_overdue,
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
         LEFT JOIN product_id_map pm ON pm.src = TRIM(o.product_id)
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
         LEFT JOIN (
    SELECT order_no,
           MAX(callback_time) AS callback_time
    FROM risk_user_approval_callback
    WHERE callback_time IS NOT NULL
      AND order_no IS NOT NULL
      AND TRIM(order_no) <> ''
    GROUP BY order_no
) ruac ON ruac.order_no = o.order_no
         LEFT JOIN (
    SELECT order_no,
           MAX(callback_time) AS callback_time
    FROM user_repay
    WHERE status = 2
      AND callback_time IS NOT NULL
      AND order_no IS NOT NULL
      AND TRIM(order_no) <> ''
    GROUP BY order_no
) ur_lp ON ur_lp.order_no = o.order_no
         LEFT JOIN (
    SELECT user_order_id,
           MAX(COALESCE(is_overdue, 0)) AS is_overdue
    FROM user_order_installment
    GROUP BY user_order_id
) inst ON inst.user_order_id = o.id
         WHERE o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
     ) base
