-- 源库（贷超 application）：已放款 → loan 宽表
-- 宽表列与目标库 ng.loan 一致
--
-- 源字段：applicationNo（非 sn）；application_no = ng + LPAD(appId,4,'0') + '-' + applicationNo
-- 筛选：disburseTime <> 0 且 applicationNo 非空（不按 productId 过滤）

-- 映射：
--   paid_amount: status IN (17,18,19) → paidAmount，否则 0
--   paid_time: paidTime(秒)*1000 → 目标毫秒
--   status: 8→9 | 11,13,14,16→20 | 15→23 | 17,18,19→27 | 其他→20
--   roll_fee=0（原 service_fee）；interest/penalty_amount/reduction_amount/roll_paid_amount=0

-- ========== 0. 预览 10 条（列名与目标 loan 一致）==========
SELECT CONCAT('ng-', a.applicationNo, '-01000')                                   AS loan_no,
       CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo)                  AS application_no,
       CAST(1 AS UNSIGNED)                                                        AS period,
       CAST(0 AS UNSIGNED)                                                        AS roll_sequence,
       DATE(FROM_UNIXTIME(a.disburseTime))                                        AS start_date,
       DATE(FROM_UNIXTIME(a.dueDate))                                             AS due_date,
       DATE(FROM_UNIXTIME(a.dueDate))                                             AS due_date_final,
       CAST(GREATEST(COALESCE(a.disburseAmount, 0), 0) AS UNSIGNED)                            AS principal,
       CAST(0 AS UNSIGNED)                                                        AS interest,
       CAST(GREATEST(ROUND(COALESCE(a.amount, 0) * 0.35), 0) AS UNSIGNED)                    AS admin_fee,
       CAST(0 AS SIGNED)                                                          AS roll_fee,
       CAST(0 AS UNSIGNED)                                                        AS penalty_amount,
       CAST(0 AS UNSIGNED)                                                        AS reduction_amount,
       CAST(GREATEST(COALESCE(a.repayment, 0), 0) AS UNSIGNED)                                 AS total_amount,
       CAST(CASE
                WHEN a.status IN (17, 18, 19) THEN GREATEST(COALESCE(a.paidAmount, 0), 0)
                ELSE 0
           END AS UNSIGNED)                                                        AS paid_amount,
       CAST(0 AS SIGNED)                                                          AS roll_paid_amount,
       CASE WHEN a.paidTime > 0 THEN a.paidTime * 1000 END                        AS paid_time,
       CASE WHEN a.paidTime > 0 THEN DATE(FROM_UNIXTIME(a.paidTime)) END          AS paid_off_date,
       CAST(1785340800000 AS UNSIGNED)                                            AS created_time,
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
           END AS UNSIGNED)                                                        AS status
FROM application a
WHERE a.disburseTime <> 0
  AND a.applicationNo <> ''
ORDER BY a.id DESC
LIMIT 10;

-- ========== 1. 建宽表（结构与目标 loan 一致）==========
SET SESSION wait_timeout = 28800;
SET SESSION net_read_timeout = 7200;
SET SESSION net_write_timeout = 7200;
SET SESSION unique_checks = 0;

DROP TABLE IF EXISTS loan_dk_ld_sync_staging;

CREATE TABLE loan_dk_ld_sync_staging (
    loan_no          VARCHAR(36)  NOT NULL COMMENT '还款计划编号',
    application_no   VARCHAR(36)  NOT NULL COMMENT '申请单号',
    period           TINYINT UNSIGNED NOT NULL DEFAULT 1 COMMENT '期序',
    roll_sequence    TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '展期序号',
    start_date       DATE         NOT NULL COMMENT '计划开始日期',
    due_date         DATE         NOT NULL COMMENT '到期日期',
    due_date_final   DATE         NOT NULL COMMENT '最终到期日期',
    principal        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '应还本金',
    interest         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '应还利息',
    admin_fee        BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '应还管理费',
    roll_fee         BIGINT       NOT NULL DEFAULT 0 COMMENT '应还展期服务费',
    penalty_amount   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '应还罚息',
    reduction_amount BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已减免金额',
    total_amount     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '应还总额',
    paid_amount      BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '结清已还金额',
    roll_paid_amount BIGINT       NOT NULL DEFAULT 0 COMMENT '展期已还金额',
    paid_time        BIGINT UNSIGNED NULL COMMENT '最后还款时间毫秒',
    paid_off_date    DATE         NULL COMMENT '结清日期',
    created_time     BIGINT UNSIGNED NOT NULL COMMENT '创建时间毫秒',
    status           TINYINT      NOT NULL DEFAULT 0 COMMENT '状态',
    PRIMARY KEY (application_no, period, roll_sequence),
    KEY idx_loan_no (loan_no),
    KEY idx_status (status)
) COMMENT '贷超已放款 loan 宽表（列同目标 ng.loan）';

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
FROM application a
WHERE a.disburseTime <> 0
  AND a.applicationNo <> '';

SET SESSION unique_checks = 1;

-- ---------- 校验 ----------
SELECT COUNT(*) AS total_cnt FROM loan_dk_ld_sync_staging;

SELECT status, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
GROUP BY status
ORDER BY status;

SELECT 'repayment_zero' AS check_item, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging WHERE total_amount = 0;

-- 写入目标库（在目标库执行）:
-- INSERT INTO loan SELECT * FROM loan_dk_ld_sync_staging;
-- 或跨库: INSERT INTO ng.loan (...) SELECT ... FROM source_db.loan_dk_ld_sync_staging;

-- ========== 2. 分批灌入（INSERT 超时用）==========
-- AND a.id >= ? AND a.id < ?
