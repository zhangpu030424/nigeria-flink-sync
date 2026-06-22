-- 大表无法 DROP 时：RENAME 旧表 + 建新表（秒级完成，推荐）
--
-- 前置（避免锁等待）:
--   1. Cancel 所有 Flink Job（尤其 CDC vt_token_cache / user_info）
--   2. 停止 vt-preload
--   3. root/DBA 执行本文件
--
-- 之后:
--   ./scripts/rebuild-all-staging.sh
--   后台清旧表: ./scripts/vt-token-cache-purge.sh --table vt_token_cache_legacy
--   或: DROP TABLE vt_token_cache_legacy;  （purge 完再 DROP）
--
-- mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_token_cache_rebuild_swap.sql

RENAME TABLE vt_token_cache TO vt_token_cache_legacy;

CREATE TABLE vt_token_cache (
    id           BIGINT       NOT NULL AUTO_INCREMENT,
    vt_type      TINYINT      NOT NULL COMMENT '1mobile 2gaid 3bank 4id_number 5emergency 6id2',
    raw_value    VARCHAR(128) NOT NULL COLLATE utf8mb4_bin,
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

-- 触发器见 sql/ddl/vt_token_cache_vt_triggers.sql（换表后须单独执行，或 full-rerun --rebuild-vt-swap）

SELECT 'vt_token_cache 已换表 (TINYINT)；旧数据在 vt_token_cache_legacy；请执行 vt_token_cache_vt_triggers.sql' AS msg;
