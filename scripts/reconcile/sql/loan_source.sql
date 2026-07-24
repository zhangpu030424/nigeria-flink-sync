SELECT CAST(i.id AS SIGNED),
       CONCAT(
               'ng-', o.order_no, '-',
               LPAD(CAST(COALESCE(i.current_period, 1) AS CHAR), 2, '0'),
               LPAD(CAST(0 AS CHAR), 3, '0')
       ) AS loan_no,
       CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no) AS application_no,
       CAST(COALESCE(i.current_period, 1) AS SIGNED) AS period,
       CAST(0 AS SIGNED) AS roll_sequence,
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
       CAST(GREATEST(
           COALESCE(UNIX_TIMESTAMP(o.disburse_time), UNIX_TIMESTAMP(o.order_time), UNIX_TIMESTAMP(i.create_time), 0) * 1000,
           0
       ) AS SIGNED) AS created_time_ms,
       CAST(CASE
           WHEN o.risk_order_status = 10
               AND COALESCE(i.is_overdue, 0) = 1 THEN 23
           WHEN o.risk_order_status = 10
               AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) = 0 THEN 20
           WHEN o.risk_order_status = 10
               AND CAST(COALESCE(NULLIF(TRIM(i.repaid_amount), ''), '0') AS DECIMAL(20, 2)) <> 0 THEN 24
           WHEN o.risk_order_status = 11 THEN 23
           WHEN o.risk_order_status = 40 THEN 25
           WHEN o.risk_order_status IN (20, 30, 50) THEN 27
           ELSE 20
           END AS SIGNED) AS risk_status
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
WHERE o.order_no IS NOT NULL
  AND TRIM(o.order_no) <> ''
  AND o.risk_order_status IS NOT NULL
  AND o.risk_order_status NOT IN (0, 2, 4, 6, 8)
