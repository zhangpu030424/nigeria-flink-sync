-- 复现飞书报告 ng01-validation-2026-08-10.md 中的数据质量规则
-- 文档: https://ek8l1y505u.feishu.cn/file/UECMbTqr5orkSbxCFp7cZNgjnTb
-- 在目标库 ng 执行（bid=ng01；165 贷超同步 / 或 101 新系统均可）
--
-- 说明：
-- 1) 报告样本来自 SeaweedFS 归档 payload，这里用目标库 application/loan 近似复现
-- 2) 时间字段在库中多为 NULL；规则里的 "== 0" 统一按 COALESCE(x,0)=0 处理
-- 3) status 口径（本仓 Flink 映射）：20 在贷 / 23 逾期 / 24 部分还 / 25 核销 / 27 结清
--    过审后状态集合（A5/C3）：按报告 DISBURSED ∪ POST_REVIEW_NO_DISB 近似为 status >= 20
-- 4) F17 的 B7（paid_time ≤ DecisionPayload.time）无 payload 时间，用 NOW() 毫秒近似

SET time_zone = 'Africa/Lagos';

-- ========== 汇总（对标报告表）==========
SELECT 'C7' AS rule, 'ERROR' AS lvl,
       COUNT(*) AS hits
FROM application
WHERE bid = 'ng01'
  AND COALESCE(disbursed_time, 0) = 0
  AND COALESCE(disbursed_amount, 0) > 0

UNION ALL
SELECT 'A5', 'ERROR', COUNT(*)
FROM application
WHERE bid = 'ng01'
  AND status >= 20
  AND COALESCE(reviewed_time, 0) = 0

UNION ALL
SELECT 'F17', 'ERROR', COUNT(*)
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE l.paid_time IS NOT NULL
  AND (
        l.paid_time < 1000000000000                              -- B1: 应 ≥ 1e12（毫秒）
     OR CHAR_LENGTH(CAST(l.paid_time AS CHAR)) <> 13             -- B2: 应为 13 位
     OR l.paid_time > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)  -- B7 近似：不应晚于当前
  )

UNION ALL
SELECT 'F11', 'ERROR', COUNT(*)
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE COALESCE(l.paid_time, 0) > 0
  AND COALESCE(a.disbursed_time, 0) > 0
  AND l.paid_time < a.disbursed_time

UNION ALL
SELECT 'C3', 'ERROR', COUNT(*)
FROM application
WHERE bid = 'ng01'
  AND status >= 20
  AND (COALESCE(principal, 0) = 0 OR COALESCE(total_amount, 0) = 0)

UNION ALL
SELECT 'A12', 'ERROR', COUNT(*)
FROM application
WHERE bid = 'ng01'
  AND COALESCE(reviewed_time, 0) = 0
  AND COALESCE(disbursed_time, 0) > 0

UNION ALL
SELECT 'F4', 'WARN', COUNT(*)
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE l.total_amount <> (
        COALESCE(l.principal, 0)
      + COALESCE(l.interest, 0)
      + COALESCE(l.admin_fee, 0)
      + COALESCE(l.roll_fee, 0)
      + COALESCE(l.penalty_amount, 0)
      - COALESCE(l.reduction_amount, 0)
    )

UNION ALL
SELECT 'C4', 'ERROR', COUNT(*)
FROM application
WHERE bid = 'ng01'
  AND COALESCE(total_amount, 0) < COALESCE(principal, 0);

-- ========== C7 抽样 ==========
SELECT application_no, sn, status, disbursed_amount, disbursed_time, loan_amount, principal
FROM application
WHERE bid = 'ng01'
  AND COALESCE(disbursed_time, 0) = 0
  AND COALESCE(disbursed_amount, 0) > 0
ORDER BY application_no
LIMIT 20;

-- ========== A5 抽样 ==========
SELECT application_no, sn, status, reviewed_time, disbursed_time, principal, total_amount
FROM application
WHERE bid = 'ng01'
  AND status >= 20
  AND COALESCE(reviewed_time, 0) = 0
ORDER BY status, application_no
LIMIT 20;

-- ========== F17 抽样：看是秒/毫秒混用还是未来时间 ==========
SELECT l.loan_no, l.application_no, l.period, l.paid_time,
       CHAR_LENGTH(CAST(l.paid_time AS CHAR)) AS paid_digits,
       FROM_UNIXTIME(CASE WHEN l.paid_time > 10000000000 THEN l.paid_time/1000 ELSE l.paid_time END) AS paid_as_dt,
       CASE
         WHEN l.paid_time < 1000000000000 THEN 'B1_lt_1e12'
         WHEN CHAR_LENGTH(CAST(l.paid_time AS CHAR)) <> 13 THEN 'B2_not_13digits'
         WHEN l.paid_time > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED) THEN 'B7_future'
         ELSE 'ok'
       END AS fail_mode
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE l.paid_time IS NOT NULL
  AND (
        l.paid_time < 1000000000000
     OR CHAR_LENGTH(CAST(l.paid_time AS CHAR)) <> 13
     OR l.paid_time > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
  )
ORDER BY l.paid_time DESC
LIMIT 20;

-- ========== F11 抽样 ==========
SELECT l.loan_no, l.application_no, l.paid_time, a.disbursed_time,
       (a.disbursed_time - l.paid_time) AS paid_earlier_by_ms
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE COALESCE(l.paid_time, 0) > 0
  AND COALESCE(a.disbursed_time, 0) > 0
  AND l.paid_time < a.disbursed_time
ORDER BY (a.disbursed_time - l.paid_time) DESC
LIMIT 20;

-- ========== C3 抽样 ==========
SELECT application_no, sn, status, principal, total_amount, disbursed_amount, reviewed_time
FROM application
WHERE bid = 'ng01'
  AND status >= 20
  AND (COALESCE(principal, 0) = 0 OR COALESCE(total_amount, 0) = 0)
ORDER BY application_no
LIMIT 20;

-- ========== A12 抽样 ==========
SELECT application_no, sn, status, reviewed_time, disbursed_time, disbursed_amount
FROM application
WHERE bid = 'ng01'
  AND COALESCE(reviewed_time, 0) = 0
  AND COALESCE(disbursed_time, 0) > 0
ORDER BY application_no
LIMIT 20;

-- ========== F4 抽样 ==========
SELECT l.loan_no, l.application_no, l.period,
       l.principal, l.interest, l.admin_fee, l.roll_fee, l.penalty_amount, l.reduction_amount,
       l.total_amount,
       (COALESCE(l.principal,0)+COALESCE(l.interest,0)+COALESCE(l.admin_fee,0)
        +COALESCE(l.roll_fee,0)+COALESCE(l.penalty_amount,0)-COALESCE(l.reduction_amount,0)) AS expected_total
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE l.total_amount <> (
        COALESCE(l.principal, 0)
      + COALESCE(l.interest, 0)
      + COALESCE(l.admin_fee, 0)
      + COALESCE(l.roll_fee, 0)
      + COALESCE(l.penalty_amount, 0)
      - COALESCE(l.reduction_amount, 0)
    )
LIMIT 20;

-- ========== C4 抽样 ==========
SELECT application_no, sn, status, principal, total_amount, loan_amount, disbursed_amount
FROM application
WHERE bid = 'ng01'
  AND COALESCE(total_amount, 0) < COALESCE(principal, 0)
LIMIT 20;

-- ========== 报告建议的交叉：A5 ∩ C3 ∩ A12 是否同批 ==========
SELECT
  SUM(CASE WHEN status >= 20 AND COALESCE(reviewed_time, 0) = 0 THEN 1 ELSE 0 END) AS a5,
  SUM(CASE WHEN status >= 20 AND (COALESCE(principal,0)=0 OR COALESCE(total_amount,0)=0) THEN 1 ELSE 0 END) AS c3,
  SUM(CASE WHEN COALESCE(reviewed_time,0)=0 AND COALESCE(disbursed_time,0)>0 THEN 1 ELSE 0 END) AS a12,
  SUM(CASE
        WHEN status >= 20
         AND COALESCE(reviewed_time, 0) = 0
         AND (COALESCE(principal,0)=0 OR COALESCE(total_amount,0)=0)
        THEN 1 ELSE 0 END) AS a5_and_c3,
  SUM(CASE
        WHEN status >= 20
         AND COALESCE(reviewed_time, 0) = 0
         AND COALESCE(disbursed_time, 0) > 0
        THEN 1 ELSE 0 END) AS a5_and_a12
FROM application
WHERE bid = 'ng01';

-- ========== F17 ∩ F11 同批 loan ==========
SELECT COUNT(*) AS f17_and_f11
FROM loan l
INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
WHERE COALESCE(l.paid_time, 0) > 0
  AND COALESCE(a.disbursed_time, 0) > 0
  AND l.paid_time < a.disbursed_time
  AND (
        l.paid_time < 1000000000000
     OR CHAR_LENGTH(CAST(l.paid_time AS CHAR)) <> 13
     OR l.paid_time > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)
  );
