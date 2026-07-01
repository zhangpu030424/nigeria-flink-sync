-- 按 applicationNo 列表精确补数（先灌 lm_loan_backfill_sn，再跑 migrate）
-- 源库 ng_loan_market

CREATE TABLE IF NOT EXISTS lm_loan_backfill_sn (
    sn VARCHAR(64) NOT NULL PRIMARY KEY COMMENT 'application.applicationNo'
) COMMENT '待补 loan 的订单号列表';
