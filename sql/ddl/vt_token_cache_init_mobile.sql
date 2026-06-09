-- 一步完成：建 vt_token_cache + 从 user 灌入去重规范化 mobile（status=0 待 VT）
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_token_cache_init_mobile.sql
--
-- 说明:
--   - INSERT IGNORE：重复执行不会重复插入同一 (vt_type, raw_value)
--   - 不会覆盖已有 status=1 的 token（唯一键冲突则跳过）
--   - 规范化规则与 MobileNormalizer / Flink Job 一致

CREATE TABLE IF NOT EXISTS vt_token_cache (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    vt_type      ENUM('mobile','gaid_idfa','bank_account','id_number','id2') NOT NULL,
    raw_value    VARCHAR(128) NOT NULL COMMENT '规范化后的明文',
    token        VARCHAR(128) NULL,
    masking      VARCHAR(128) NULL,
    status       TINYINT      NOT NULL DEFAULT 0 COMMENT '0待VT 1成功 2失败',
    retry_count  INT          NOT NULL DEFAULT 0,
    last_error   VARCHAR(512) NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_type_raw (vt_type, raw_value),
    KEY idx_status (status, vt_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='VT 去重字典';

INSERT IGNORE INTO vt_token_cache (vt_type, raw_value, status)
SELECT 'mobile',
       norm.mobile_norm,
       0
FROM (
    SELECT DISTINCT
        CASE
            WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
            WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
            WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
            WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
            ELSE CONCAT('+234', TRIM(u.mobile))
        END AS mobile_norm
    FROM `user` u
) norm
WHERE norm.mobile_norm IS NOT NULL
  AND norm.mobile_norm <> '';

SELECT 'vt_token_cache 初始化完成' AS msg;
SELECT status, COUNT(*) AS cnt
FROM vt_token_cache
WHERE vt_type = 'mobile'
GROUP BY status
ORDER BY status;
