-- VT 字典表：明文(规范化后) → token，全表/多 Job 共用
-- 在源库 nigeria_backend 执行:
--   mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_token_cache.sql

CREATE TABLE IF NOT EXISTS vt_token_cache (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    vt_type      ENUM('mobile','gaid_idfa','bank_account','id_number','id2') NOT NULL,
    raw_value    VARCHAR(128) NOT NULL COMMENT '规范化后的明文，与 Flink/SQL 规范化规则一致',
    token        VARCHAR(128) NULL COMMENT '/v2t 返回 token',
    masking      VARCHAR(128) NULL COMMENT '/v2t 返回 masking（可选）',
    status       TINYINT      NOT NULL DEFAULT 0 COMMENT '0待VT 1成功 2失败',
    retry_count  INT          NOT NULL DEFAULT 0,
    last_error   VARCHAR(512) NULL,
    created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_type_raw (vt_type, raw_value),
    KEY idx_status (status, vt_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='VT 去重字典，脚本批量 /v2t 后供宽表 JOIN';
