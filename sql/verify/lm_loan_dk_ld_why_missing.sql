-- 单条 application 为何不在 loan_dk_ld_sync_staging
-- 用法: mysql ... ng_loan_market -e "SET @aid=4424433; source sql/verify/lm_loan_dk_ld_why_missing.sql"
-- 或: WHERE id=4424433 改成你的 id

SET @aid := 4424433;

SELECT
    a.id,
    a.productId,
    a.appId,
    a.applicationNo,
    a.disburseTime,
    FROM_UNIXTIME(a.disburseTime) AS disburse_dt,
    a.status,
    a.repayment,
    CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo) AS staging_application_no,
    CONCAT('ng-', a.applicationNo, '-01000') AS staging_loan_no,
    CASE
        WHEN a.disburseTime IS NULL OR a.disburseTime = 0 THEN 'NO: disburseTime=0'
        WHEN a.applicationNo IS NULL OR TRIM(a.applicationNo) = '' THEN 'NO: applicationNo 空'
        ELSE 'YES: 满足宽表 WHERE，应进 staging'
    END AS in_staging_rule
FROM application a
WHERE a.id = @aid;

SELECT 'in_staging_table' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging s
WHERE s.application_no = (
    SELECT CONCAT('ng', LPAD(appId, 4, '0'), '-', applicationNo) FROM application WHERE id = @aid
);
