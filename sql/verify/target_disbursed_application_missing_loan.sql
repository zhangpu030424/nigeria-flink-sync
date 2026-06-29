-- 目标库：已放款 application 但无 loan（与源库宽表诊断配套）
-- 用法: mysql -h $TARGET_MYSQL_HOST ... ng < sql/verify/target_disbursed_application_missing_loan.sql

-- ========== 0. 复现你的查询 ==========
SELECT COUNT(*) AS missing_loan_cnt
FROM application a
LEFT JOIN loan l ON l.application_no = a.application_no
WHERE a.app_id IN (567, 568, 569, 571, 572, 573)
  AND a.disbursed_time > 0
  AND l.application_no IS NULL;

-- ========== 1. 按 app 分布 + 放款时间范围 ==========
SELECT a.app_id,
       COUNT(*) AS missing_cnt,
       FROM_UNIXTIME(MIN(a.disbursed_time) / 1000) AS min_disbursed,
       FROM_UNIXTIME(MAX(a.disbursed_time) / 1000) AS max_disbursed,
       FROM_UNIXTIME(MIN(a.created_time) / 1000) AS min_created,
       FROM_UNIXTIME(MAX(a.created_time) / 1000) AS max_created
FROM application a
LEFT JOIN loan l ON l.application_no = a.application_no
WHERE a.app_id IN (567, 568, 569, 571, 572, 573)
  AND a.disbursed_time > 0
  AND l.application_no IS NULL
GROUP BY a.app_id
ORDER BY missing_cnt DESC;

-- ========== 2. loan 是否用旧 key 存在（application_no 对不上但 loan_no 含同一 sn）==========
-- 若 cnt > 0：多半是历史 application_no 格式问题，loan 已有只是 JOIN key 不一致
SELECT COUNT(*) AS loan_exists_by_sn_but_not_application_no
FROM application a
LEFT JOIN loan l ON l.application_no = a.application_no
WHERE a.app_id IN (567, 568, 569, 571, 572, 573)
  AND a.disbursed_time > 0
  AND l.application_no IS NULL
  AND EXISTS (
    SELECT 1
    FROM loan l2
    WHERE l2.loan_no LIKE CONCAT('ng-', SUBSTRING_INDEX(a.application_no, '-', -1), '-%')
);

-- ========== 3. 抽样明细（前 20 条）==========
SELECT a.application_no,
       a.app_id,
       a.status AS app_status,
       FROM_UNIXTIME(a.disbursed_time / 1000) AS disbursed_at,
       FROM_UNIXTIME(a.created_time / 1000) AS created_at
FROM application a
LEFT JOIN loan l ON l.application_no = a.application_no
WHERE a.app_id IN (567, 568, 569, 571, 572, 573)
  AND a.disbursed_time > 0
  AND l.application_no IS NULL
ORDER BY a.disbursed_time DESC
LIMIT 20;

-- ========== 4. 对照：6 app 已有 loan 的数量 ==========
SELECT COUNT(DISTINCT l.application_no) AS loan_app_cnt
FROM loan l
WHERE l.application_no REGEXP '^ng0(567|568|569|571|572|573)-';

SELECT COUNT(*) AS disbursed_app_cnt
FROM application a
WHERE a.app_id IN (567, 568, 569, 571, 572, 573)
  AND a.disbursed_time > 0;
