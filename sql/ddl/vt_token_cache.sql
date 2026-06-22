-- VT 字典表：明文(规范化后) → token，全表/多 Job 共用
-- vt_type 用 TINYINT，避免 ENUM ALTER 锁表；编码见 sql/ddl/vt_type_codes.sql
-- 在源库 nigeria_backend 执行:
--   mysql -h <host> -u ... -p nigeria_backend < sql/ddl/vt_token_cache.sql
-- 已有 ENUM 表无法 ALTER 时:
--   mysql ... < sql/ddl/vt_token_cache_rebuild.sql

CREATE TABLE IF NOT EXISTS vt_token_cache (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    vt_type      TINYINT      NOT NULL COMMENT '1mobile 2gaid 3bank 4id_number 5emergency 6id2',
    raw_value    VARCHAR(128) NOT NULL COLLATE utf8mb4_bin COMMENT '规范化明文，bin 比较避免与源表排序规则冲突',
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
