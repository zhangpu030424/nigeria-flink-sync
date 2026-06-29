-- 源库：6 个 app 已放款订单，按「为何不在 loan 宽表」分类
-- 与目标库 missing_loan_cnt 对照；若源库 missing 远小于目标 → 同步/格式问题
-- 用法: mysql -h $SOURCE_MYSQL_HOST ... nigeria_backend < sql/verify/source_disbursed_missing_loan_for_6apps.sql

-- ========== 0. 概览（6 app，已放款）==========
SELECT 'disbursed_6apps' AS metric, COUNT(*) AS cnt
FROM user_order o
WHERE o.disburse_time IS NOT NULL
  AND o.app_code IN (567, 568, 569, 571, 572, 573)
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
UNION ALL
SELECT 'in_loan_staging_6apps', COUNT(*)
FROM user_order o
INNER JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
  AND o.app_code IN (567, 568, 569, 571, 572, 573)
UNION ALL
SELECT 'disbursed_missing_loan_staging_6apps', COUNT(*)
FROM user_order o
LEFT JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
  AND o.app_code IN (567, 568, 569, 571, 572, 573)
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND l.application_no IS NULL;

-- ========== 1. 缺 loan 宽表的原因分类 ==========
SELECT CASE
           WHEN o.risk_order_status IS NULL THEN 'risk_status_null'
           WHEN o.risk_order_status IN (0, 2, 4, 6, 8) THEN 'risk_status_filtered'
           WHEN NOT EXISTS (SELECT 1 FROM user_order_installment i WHERE i.user_order_id = o.id)
               THEN 'no_installment'
           WHEN NOT EXISTS (SELECT 1 FROM application_sync_staging a WHERE a.sn = o.order_no)
               THEN 'not_in_application_staging'
           ELSE 'staging_stale_or_sync_gap'
           END AS likely_reason,
       COUNT(*) AS cnt
FROM user_order o
LEFT JOIN loan_sync_staging l
    ON l.application_no = CONCAT('ng0', TRIM(CAST(o.app_code AS CHAR)), '-', o.order_no)
WHERE o.disburse_time IS NOT NULL
  AND o.app_code IN (567, 568, 569, 571, 572, 573)
  AND o.order_no IS NOT NULL AND TRIM(o.order_no) <> ''
  AND l.application_no IS NULL
GROUP BY likely_reason
ORDER BY cnt DESC;

-- ========== 2. 源库「应有 loan」= 在 loan_sync_staging 的已放款 6 app 数（应对标目标 loan 数）==========
SELECT COUNT(DISTINCT l.application_no) AS source_loan_staging_app_cnt
FROM loan_sync_staging l
WHERE l.application_no REGEXP '^ng0(567|568|569|571|572|573)-';
