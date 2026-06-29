-- 源库诊断：已放款（disburse_time 非空）但不在宽表 staging 的订单
-- 用法: mysql -h $SOURCE_MYSQL_HOST -u ... nigeria_backend < sql/verify/loan_disbursed_missing_staging.sql
--
-- 宽表入仓条件回顾：
--   application_sync_staging: user_order INNER JOIN user，且 order_no 非空（无 risk 过滤）
--   loan_sync_staging: user_order_installment INNER JOIN user_order，且 risk_order_status NOT IN (0,2,4,6,8)

-- ========== 0. 概览 ==========
SELECT 'disbursed_orders' AS metric, COUNT(*) AS cnt
FROM user_order o
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
UNION ALL
SELECT 'in_application_staging', COUNT(*)
FROM user_order o
INNER JOIN application_sync_staging a ON a.sn = o.order_no
WHERE o.disburse_time IS NOT NULL
UNION ALL
SELECT 'in_loan_staging', COUNT(*)
FROM user_order o
INNER JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
UNION ALL
SELECT 'disbursed_missing_application_staging', COUNT(*)
FROM user_order o
LEFT JOIN application_sync_staging a ON a.sn = o.order_no
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND a.sn IS NULL
UNION ALL
SELECT 'disbursed_missing_loan_staging', COUNT(*)
FROM user_order o
LEFT JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND l.application_no IS NULL;

-- ========== 1. 已放款但不在 application 宽表（明细 + 原因）==========
SELECT o.id,
       o.order_no,
       o.app_code,
       o.user_id,
       o.disburse_time,
       o.risk_order_status,
       CASE
           WHEN o.order_no IS NULL OR TRIM(o.order_no) = '' THEN 'empty_order_no'
           WHEN u.id IS NULL THEN 'user_missing'
           ELSE 'staging_stale_or_unknown'
           END AS likely_reason
FROM user_order o
LEFT JOIN application_sync_staging a ON a.sn = o.order_no
LEFT JOIN `user` u ON u.id = o.user_id
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND a.sn IS NULL
ORDER BY o.disburse_time DESC
LIMIT 50;

-- ========== 2. 已放款但不在 loan 宽表（明细 + 原因）==========
SELECT o.id,
       o.order_no,
       o.app_code,
       CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no) AS expected_application_no,
       o.disburse_time,
       o.risk_order_status,
       COUNT(i.id) AS installment_cnt,
       CASE
           WHEN o.risk_order_status IS NULL THEN 'risk_status_null'
           WHEN o.risk_order_status IN (0, 2, 4, 6, 8) THEN 'risk_status_filtered'
           WHEN COUNT(i.id) = 0 THEN 'no_installment'
           WHEN a.sn IS NULL THEN 'not_in_application_staging'
           ELSE 'staging_stale_or_unknown'
           END AS likely_reason
FROM user_order o
LEFT JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
LEFT JOIN user_order_installment i ON i.user_order_id = o.id
LEFT JOIN application_sync_staging a ON a.sn = o.order_no
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND l.application_no IS NULL
GROUP BY o.id, o.order_no, o.app_code, o.disburse_time, o.risk_order_status, a.sn
ORDER BY o.disburse_time DESC
LIMIT 50;

-- ========== 3. 按 app 汇总：已放款缺 loan 宽表 ==========
SELECT o.app_code,
       COUNT(*) AS missing_loan_cnt
FROM user_order o
LEFT JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND l.application_no IS NULL
GROUP BY o.app_code
ORDER BY missing_loan_cnt DESC;

-- ========== 4. 宽表是否过期：已放款时间晚于宽表 rebuild 后仍不在 staging ==========
-- 若 application_sync_staging 有数据，取 MAX 对应订单的 disburse_time 作参考
SELECT MAX(o.disburse_time) AS max_disburse_in_staging
FROM application_sync_staging a
JOIN user_order o ON o.order_no = a.sn;

SELECT MIN(o.disburse_time) AS min_disburse_missing_app_staging
FROM user_order o
LEFT JOIN application_sync_staging a ON a.sn = o.order_no
WHERE o.disburse_time IS NOT NULL
  AND a.sn IS NULL;
