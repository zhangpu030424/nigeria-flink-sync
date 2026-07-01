-- 按 lm_loan_backfill_sn 精确补宽表 + 诊断
-- 前置: bash scripts/run-lm-loan-backfill-by-sn.sh <sn列表文件>
-- 或: INSERT INTO lm_loan_backfill_sn (sn) VALUES ('168707651012017151'), ...;

-- ========== 1. 诊断 ==========
SELECT 'sn_in_list' AS metric, COUNT(*) AS cnt FROM lm_loan_backfill_sn
UNION ALL
SELECT 'sn_not_in_application', COUNT(*)
FROM lm_loan_backfill_sn b
LEFT JOIN application a ON a.applicationNo = b.sn
WHERE a.applicationNo IS NULL
UNION ALL
SELECT 'sn_disburseTime_zero', COUNT(*)
FROM lm_loan_backfill_sn b
INNER JOIN application a ON a.applicationNo = b.sn
WHERE a.disburseTime IS NULL OR a.disburseTime = 0
UNION ALL
SELECT 'sn_ready_for_staging', COUNT(*)
FROM lm_loan_backfill_sn b
INNER JOIN application a ON a.applicationNo = b.sn
WHERE a.disburseTime <> 0
  AND a.applicationNo <> ''
UNION ALL
SELECT 'sn_already_in_staging', COUNT(*)
FROM lm_loan_backfill_sn b
INNER JOIN application a ON a.applicationNo = b.sn
INNER JOIN loan_dk_ld_sync_staging s
    ON s.application_no = CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo);

-- 列表里有、application 里没有
SELECT b.sn AS missing_in_application
FROM lm_loan_backfill_sn b
LEFT JOIN application a ON a.applicationNo = b.sn
WHERE a.applicationNo IS NULL
LIMIT 50;

-- 有 application 但未放款
SELECT b.sn, a.disburseTime, a.appId, a.status
FROM lm_loan_backfill_sn b
INNER JOIN application a ON a.applicationNo = b.sn
WHERE a.disburseTime IS NULL OR a.disburseTime = 0
LIMIT 50;

-- ========== 2. 补宽表 ==========
INSERT INTO loan_dk_ld_sync_staging (
    loan_no, application_no, period, roll_sequence,
    start_date, due_date, due_date_final,
    principal, interest, admin_fee, roll_fee,
    penalty_amount, reduction_amount, total_amount,
    paid_amount, roll_paid_amount, paid_time, paid_off_date,
    created_time, status
)
SELECT CONCAT('ng-', a.applicationNo, '-01000'),
       CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo),
       1,
       0,
       DATE(FROM_UNIXTIME(a.disburseTime)),
       DATE(FROM_UNIXTIME(a.dueDate)),
       DATE(FROM_UNIXTIME(a.dueDate)),
       CAST(GREATEST(COALESCE(a.disburseAmount, 0), 0) AS UNSIGNED),
       0,
       CAST(GREATEST(ROUND(COALESCE(a.amount, 0) * 0.35), 0) AS UNSIGNED),
       0,
       0,
       0,
       CAST(GREATEST(COALESCE(a.repayment, 0), 0) AS UNSIGNED),
       CAST(CASE
                WHEN a.status IN (17, 18, 19) THEN GREATEST(COALESCE(a.paidAmount, 0), 0)
                ELSE 0
           END AS UNSIGNED),
       0,
       CASE WHEN a.paidTime > 0 THEN CAST(a.paidTime * 1000 AS UNSIGNED) END,
       CASE WHEN a.paidTime > 0 THEN DATE(FROM_UNIXTIME(a.paidTime)) END,
       1785340800000,
       CAST(CASE a.status
                WHEN 8 THEN 9
                WHEN 11 THEN 20
                WHEN 13 THEN 20
                WHEN 14 THEN 20
                WHEN 16 THEN 20
                WHEN 15 THEN 23
                WHEN 17 THEN 27
                WHEN 18 THEN 27
                WHEN 19 THEN 27
                ELSE 20
           END AS UNSIGNED)
FROM lm_loan_backfill_sn b
INNER JOIN application a ON a.applicationNo = b.sn
WHERE a.disburseTime <> 0
  AND a.applicationNo <> ''
  AND NOT EXISTS (
      SELECT 1 FROM loan_dk_ld_sync_staging s
      WHERE s.application_no = CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo)
  );

SELECT ROW_COUNT() AS inserted_into_staging;
