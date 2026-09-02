-- ng01 数据质量校验 SQL（对标 validator 报告规则）
-- 在目标库 ng 执行，bid = 'ng01'
--
-- 约定：
--   1) 时间字段 "== 0" 统一写 COALESCE(x, 0) = 0
--   2) status 映射（Flink 写入）：20 在贷 / 23 逾期 / 24 部分还 / 25 核销 / 27 结清
--      13 拒绝 / 15 放款中 / 3,5 审核前态
--   3) 「过审后」≈ status IN (13, 15, 20, 23, 24, 25, 27)
--   4) 「已放款态」≈ status IN (20, 23, 24, 25, 27)
--   5) loan 默认取 period=1, roll_sequence=0 与 application 对齐
--   6) MERGE_DIFF 在 payload 侧指 core↔ng 合并差异；目标库用 app↔loan 主字段不一致近似

SET time_zone = 'Africa/Lagos';

-- =============================================================================
-- 一、汇总计数（对标报告表）
-- =============================================================================
SELECT rule, lvl, hits FROM (
    -- C7  未放款但 disbursed_amount > 0
    SELECT 'C7' AS rule, 'ERROR' AS lvl, COUNT(*) AS hits
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(disbursed_time, 0) = 0
      AND COALESCE(disbursed_amount, 0) > 0

    UNION ALL
    -- A13 submited_time=0 但 reviewed_time>0（ng01 时间链断裂）
    SELECT 'A13', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(submited_time, 0) = 0
      AND COALESCE(reviewed_time, 0) > 0

    UNION ALL
    -- C4  应还 < 本金
    SELECT 'C4', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(total_amount, 0) < COALESCE(principal, 0)

    UNION ALL
    -- C3  过审态 principal/total <= 0
    SELECT 'C3', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status IN (13, 15, 20, 23, 24, 25, 27)
      AND (COALESCE(principal, 0) <= 0 OR COALESCE(total_amount, 0) <= 0)

    UNION ALL
    -- MERGE_DIFF  application ↔ loan 主字段不一致（近似 payload core↔ng）
    SELECT 'MERGE_DIFF', 'WARN', COUNT(*)
    FROM application a
    INNER JOIN loan l
        ON l.application_no = a.application_no
       AND l.period = 1
       AND l.roll_sequence = 0
    WHERE a.bid = 'ng01'
      AND (
            a.principal <> l.principal
         OR a.total_amount <> l.total_amount
         OR COALESCE(a.last_paid_time, 0) <> COALESCE(l.paid_time, 0)
         OR (
                a.status <> l.status
            AND NOT (a.status IN (20, 24) AND l.status IN (20, 24))
            )
      )

    UNION ALL
    -- F4  loan 金额勾稽
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
    -- A5  过审/放款态缺 reviewed_time
    SELECT 'A5', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status IN (13, 15, 20, 23, 24, 25, 27)
      AND COALESCE(reviewed_time, 0) = 0

    UNION ALL
    -- A4  已放款态缺 disbursed_time
    SELECT 'A4', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status IN (20, 23, 24, 25, 27)
      AND COALESCE(disbursed_time, 0) = 0

    UNION ALL
    -- A11 放款中(15)缺 disbursed_time
    SELECT 'A11', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status = 15
      AND COALESCE(disbursed_time, 0) = 0

    UNION ALL
    -- C5  已放款态 disbursed_amount = 0
    SELECT 'C5', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status IN (20, 23, 24, 25, 27)
      AND COALESCE(disbursed_amount, 0) = 0

    UNION ALL
    -- A12 reviewed_time=0 但已放款
    SELECT 'A12', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(reviewed_time, 0) = 0
      AND COALESCE(disbursed_time, 0) > 0

    UNION ALL
    -- A6  未过审态却有 reviewed_time
    SELECT 'A6', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND status NOT IN (13, 15, 20, 23, 24, 25, 27)
      AND COALESCE(reviewed_time, 0) > 0

    UNION ALL
    -- B4  reviewed_time < submited_time
    SELECT 'B4', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(reviewed_time, 0) > 0
      AND COALESCE(submited_time, 0) > 0
      AND reviewed_time < submited_time

    UNION ALL
    -- B3  submited_time < created_time
    SELECT 'B3', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(submited_time, 0) > 0
      AND submited_time < created_time

    UNION ALL
    -- B5  disbursed_time < reviewed_time（放款早于审核）
    SELECT 'B5', 'ERROR', COUNT(*)
    FROM application
    WHERE bid = 'ng01'
      AND COALESCE(disbursed_time, 0) > 0
      AND COALESCE(reviewed_time, 0) > 0
      AND disbursed_time < reviewed_time

    UNION ALL
    -- D.1 application.status 与 loan.status 映射不符
    SELECT 'D.1', 'ERROR', COUNT(*)
    FROM application a
    INNER JOIN loan l
        ON l.application_no = a.application_no
       AND l.period = 1
       AND l.roll_sequence = 0
    WHERE a.bid = 'ng01'
      AND NOT (
            a.status = l.status
         OR (a.status IN (20, 24) AND l.status IN (20, 24))
         OR (a.status = 27 AND l.status = 27)
         OR (a.status = 23 AND l.status = 23)
         OR (a.status = 25 AND l.status = 25)
         OR (a.status = 15 AND l.status IN (1, 20))
         OR (a.status = 13 AND l.status IN (1, 20))
      )

    UNION ALL
    -- F11 loan.paid_time < application.disbursed_time
    SELECT 'F11', 'ERROR', COUNT(*)
    FROM loan l
    INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
    WHERE COALESCE(l.paid_time, 0) > 0
      AND COALESCE(a.disbursed_time, 0) > 0
      AND l.paid_time < a.disbursed_time
) t
ORDER BY FIELD(lvl, 'ERROR', 'WARN'), hits DESC;


-- =============================================================================
-- 二、逐规则明细 + 抽样（按需单独执行某段）
-- =============================================================================

-- ---------- C7 ----------
-- SELECT application_no, status, disbursed_amount, disbursed_time, loan_amount, principal
-- FROM application
-- WHERE bid = 'ng01'
--   AND COALESCE(disbursed_time, 0) = 0
--   AND COALESCE(disbursed_amount, 0) > 0
-- ORDER BY application_no LIMIT 20;

-- ---------- A13 ----------
-- SELECT application_no, status, created_time, submited_time, reviewed_time, disbursed_time
-- FROM application
-- WHERE bid = 'ng01'
--   AND COALESCE(submited_time, 0) = 0
--   AND COALESCE(reviewed_time, 0) > 0
-- ORDER BY application_no LIMIT 20;

-- ---------- C4 ----------
-- SELECT application_no, status, principal, total_amount, loan_amount, disbursed_amount
-- FROM application
-- WHERE bid = 'ng01'
--   AND COALESCE(total_amount, 0) < COALESCE(principal, 0)
-- ORDER BY (principal - total_amount) DESC LIMIT 20;

-- ---------- C3 ----------
-- SELECT application_no, status, principal, total_amount, disbursed_amount, reviewed_time
-- FROM application
-- WHERE bid = 'ng01'
--   AND status IN (13, 15, 20, 23, 24, 25, 27)
--   AND (COALESCE(principal, 0) <= 0 OR COALESCE(total_amount, 0) <= 0)
-- ORDER BY application_no LIMIT 20;

-- ---------- MERGE_DIFF ----------
-- SELECT a.application_no,
--        a.status AS app_status, l.status AS loan_status,
--        a.principal AS app_principal, l.principal AS loan_principal,
--        a.total_amount AS app_total, l.total_amount AS loan_total,
--        a.last_paid_time AS app_last_paid, l.paid_time AS loan_paid
-- FROM application a
-- INNER JOIN loan l ON l.application_no = a.application_no AND l.period = 1 AND l.roll_sequence = 0
-- WHERE a.bid = 'ng01'
--   AND (
--         a.principal <> l.principal
--      OR a.total_amount <> l.total_amount
--      OR COALESCE(a.last_paid_time, 0) <> COALESCE(l.paid_time, 0)
--      OR (a.status <> l.status AND NOT (a.status IN (20, 24) AND l.status IN (20, 24)))
--   )
-- ORDER BY a.application_no LIMIT 20;

-- ---------- F4 ----------
-- SELECT l.loan_no, l.application_no, l.period,
--        l.principal, l.interest, l.admin_fee, l.roll_fee, l.penalty_amount, l.reduction_amount,
--        l.total_amount,
--        (COALESCE(l.principal,0)+COALESCE(l.interest,0)+COALESCE(l.admin_fee,0)
--         +COALESCE(l.roll_fee,0)+COALESCE(l.penalty_amount,0)-COALESCE(l.reduction_amount,0)) AS expected_total
-- FROM loan l
-- INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
-- WHERE l.total_amount <> (
--         COALESCE(l.principal, 0)+COALESCE(l.interest, 0)+COALESCE(l.admin_fee, 0)
--       + COALESCE(l.roll_fee, 0)+COALESCE(l.penalty_amount, 0)-COALESCE(l.reduction_amount, 0)
--     )
-- LIMIT 20;

-- ---------- A5 / A4 / A11 / C5 / A12 / A6 / B3 / B4 / B5 ----------
-- （条件见汇总段，改 SELECT 列即可抽样）

-- ---------- D.1 ----------
-- SELECT a.application_no, a.status AS app_status, l.status AS loan_status
-- FROM application a
-- INNER JOIN loan l ON l.application_no = a.application_no AND l.period = 1 AND l.roll_sequence = 0
-- WHERE a.bid = 'ng01'
--   AND NOT (
--         a.status = l.status
--      OR (a.status IN (20, 24) AND l.status IN (20, 24))
--      OR (a.status = 27 AND l.status = 27)
--      OR (a.status = 23 AND l.status = 23)
--      OR (a.status = 25 AND l.status = 25)
--      OR (a.status = 15 AND l.status IN (1, 20))
--      OR (a.status = 13 AND l.status IN (1, 20))
--   )
-- ORDER BY a.application_no LIMIT 20;

-- ---------- F11 ----------
-- SELECT l.loan_no, l.application_no, l.paid_time, a.disbursed_time,
--        (a.disbursed_time - l.paid_time) AS paid_before_disburse_ms
-- FROM loan l
-- INNER JOIN application a ON a.application_no = l.application_no AND a.bid = 'ng01'
-- WHERE COALESCE(l.paid_time, 0) > 0
--   AND COALESCE(a.disbursed_time, 0) > 0
--   AND l.paid_time < a.disbursed_time
-- ORDER BY (a.disbursed_time - l.paid_time) DESC LIMIT 20;


-- =============================================================================
-- 三、交叉分析（排查是否同一批脏数据）
-- =============================================================================
SELECT
    SUM(CASE WHEN COALESCE(submited_time,0)=0 AND COALESCE(reviewed_time,0)>0 THEN 1 ELSE 0 END) AS a13,
    SUM(CASE WHEN status IN (13,15,20,23,24,25,27) AND COALESCE(reviewed_time,0)=0 THEN 1 ELSE 0 END) AS a5,
    SUM(CASE WHEN status IN (13,15,20,23,24,25,27)
              AND (COALESCE(principal,0)<=0 OR COALESCE(total_amount,0)<=0) THEN 1 ELSE 0 END) AS c3,
    SUM(CASE WHEN COALESCE(total_amount,0) < COALESCE(principal,0) THEN 1 ELSE 0 END) AS c4,
    SUM(CASE WHEN COALESCE(disbursed_time,0)=0 AND COALESCE(disbursed_amount,0)>0 THEN 1 ELSE 0 END) AS c7,
    SUM(CASE WHEN COALESCE(reviewed_time,0)=0 AND COALESCE(disbursed_time,0)>0 THEN 1 ELSE 0 END) AS a12,
    SUM(CASE
          WHEN COALESCE(submited_time,0)=0 AND COALESCE(reviewed_time,0)>0
           AND status IN (13,15,20,23,24,25,27)
           AND COALESCE(reviewed_time,0)=0
          THEN 1 ELSE 0 END) AS a13_and_a5_impossible
FROM application
WHERE bid = 'ng01';

-- C4 与 A13 同单（报告怀疑 ng01 ETL 金额处理问题）
-- SELECT application_no, status, principal, total_amount, submited_time, reviewed_time
-- FROM application
-- WHERE bid = 'ng01'
--   AND COALESCE(total_amount, 0) < COALESCE(principal, 0)
--   AND COALESCE(submited_time, 0) = 0
--   AND COALESCE(reviewed_time, 0) > 0
-- LIMIT 20;
