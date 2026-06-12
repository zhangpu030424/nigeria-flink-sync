-- 增量：为新银行卡补 user_bankcard_sync_staging 中缺失的 bank_account_token
-- 建议 cron: vt_seed（bank_account 在 init_all）→ vt-preload --vt-type bank_account → 本脚本

UPDATE user_bankcard_sync_staging s
    INNER JOIN vt_token_cache vt
    ON vt.vt_type = 3
        AND vt.status = 1
        AND vt.raw_value = s.bank_account_raw
SET s.bank_account_token = vt.token
WHERE (s.bank_account_token IS NULL OR s.bank_account_token = '')
  AND s.bank_account_raw IS NOT NULL;
