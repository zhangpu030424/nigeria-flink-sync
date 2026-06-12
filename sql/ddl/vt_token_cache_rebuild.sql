-- 删表重建 vt_token_cache（TINYINT vt_type）
-- ⚠️ 清空全部 VT token 缓存；须 root/DBA 执行，之后必须:
--   ./scripts/rebuild-all-staging.sh   或  vt_seed_all + vt-preload + source_all_sync_staging
--   再跑全量 Flink Job
--
-- mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_token_cache_rebuild.sql

DROP TABLE IF EXISTS vt_token_cache;

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

SELECT 'vt_token_cache 已重建 (TINYINT vt_type)，请执行 vt_seed_all + vt-preload' AS msg;
