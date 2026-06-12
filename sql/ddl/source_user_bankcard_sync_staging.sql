-- 宽表：user_bank_info + vt_token_cache.bank_account token（Flink 不调 /v2t）
-- 前置: vt_token_cache 已灌满 bank_account（vt-preload --vt-type bank_account）
--
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/source_user_bankcard_sync_staging.sql

DROP TABLE IF EXISTS user_bankcard_sync_staging;

CREATE TABLE user_bankcard_sync_staging AS
SELECT b.id,
       b.user_id,
       b.bank_code,
       TRIM(b.bank_account) AS bank_account_raw,
       vt.token AS bank_account_token,
       b.is_default
FROM user_bank_info b
         LEFT JOIN vt_token_cache vt
                   ON vt.vt_type = 3
                       AND vt.status = 1
                       AND vt.raw_value COLLATE utf8mb4_bin = TRIM(b.bank_account) COLLATE utf8mb4_bin
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL
  AND TRIM(b.bank_account) <> '';

ALTER TABLE user_bankcard_sync_staging
    ADD PRIMARY KEY (id);

SELECT COUNT(*) AS missing_token_cnt
FROM user_bankcard_sync_staging
WHERE bank_account_raw IS NOT NULL
  AND (bank_account_token IS NULL OR bank_account_token = '');
