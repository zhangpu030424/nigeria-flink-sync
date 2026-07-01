-- 贷超宽表异常金额（负值 / UNSIGNED 绕回超大数）
SELECT 'negative_total' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
WHERE total_amount < 0;

SELECT 'overflow_total' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
WHERE total_amount > 9223372036854775807;

SELECT 'negative_principal' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
WHERE principal < 0;

SELECT 'negative_paid' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
WHERE paid_amount < 0;

SELECT application_no, total_amount, principal, paid_amount, status
FROM loan_dk_ld_sync_staging
WHERE total_amount < 0
   OR total_amount > 9223372036854775807
   OR principal < 0
   OR paid_amount < 0
LIMIT 20;
