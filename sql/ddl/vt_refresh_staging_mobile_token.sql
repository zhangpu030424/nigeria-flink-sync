-- 增量：为新 user 补 seed + 刷新 user_sync_staging 中缺失的 mobile_token
-- 建议 cron 每 1~5 分钟:
--   mysql ... < sql/ddl/vt_seed_mobile.sql
--   ./scripts/vt-preload.sh --batch-size 1000
--   mysql ... < sql/ddl/vt_refresh_staging_mobile_token.sql

UPDATE user_sync_staging s
    INNER JOIN vt_token_cache vt
    ON vt.vt_type = 'mobile'
        AND vt.status = 1
        AND vt.raw_value = s.mobile_norm
SET s.mobile_token = vt.token
WHERE (s.mobile_token IS NULL OR s.mobile_token = '')
  AND s.mobile_norm IS NOT NULL;
