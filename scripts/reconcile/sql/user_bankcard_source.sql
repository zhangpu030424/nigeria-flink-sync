SELECT b.id,
       b.user_id,
       b.bank_code,
       TRIM(b.bank_account) AS bank_account_raw,
       vt_b.token AS bank_account_token,
       b.is_default
FROM user_bank_info b
         LEFT JOIN vt_token_cache vt_b
                   ON vt_b.vt_type = 3 AND vt_b.status = 1
                       AND vt_b.raw_value COLLATE utf8mb4_bin = TRIM(b.bank_account) COLLATE utf8mb4_bin
WHERE b.deleted = 0
  AND b.bank_account IS NOT NULL AND TRIM(b.bank_account) <> ''
