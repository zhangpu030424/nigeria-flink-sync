-- 宽表：user + adjust，并 JOIN vt_token_cache 得到 mobile_token（Flink 不再调 /v2t）
-- 前置:
--   sql/ddl/source_views_adjust.sql
--   sql/ddl/source_materialize_user_adjust.sql
--   sql/ddl/vt_token_cache.sql
--   sql/ddl/vt_seed_mobile.sql + scripts/vt-preload.sh（status=1 覆盖率足够）
--
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/source_user_sync_staging_vt.sql

DROP TABLE IF EXISTS user_sync_staging;

CREATE TABLE user_sync_staging AS
SELECT u.id,
       u.app_code,
       u.mobile,
       u.device_id,
       u.adid,
       u.create_time,
       a.network_name,
       a.tracker_name,
       a.campaign_tracker,
       a.campaign_name,
       a.creative_name,
       a.adgroup_tracker,
       a.creative_tracker,
       a.adgroup_name,
       CASE
           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
           ELSE CONCAT('+234', TRIM(u.mobile))
       END AS mobile_norm,
       vt.token AS mobile_token
FROM `user` u
         LEFT JOIN adjust_latest_by_adid a
                   ON u.adid IS NOT NULL AND u.adid <> '' AND a.adid = u.adid
         LEFT JOIN vt_token_cache vt
                   ON vt.vt_type = 'mobile'
                       AND vt.status = 1
                       AND vt.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin;

ALTER TABLE user_sync_staging
    ADD PRIMARY KEY (id);

-- 检查未命中 token 的行（应为 0 或极少）
SELECT COUNT(*) AS missing_token_cnt
FROM user_sync_staging
WHERE mobile_norm IS NOT NULL
  AND (mobile_token IS NULL OR mobile_token = '');
