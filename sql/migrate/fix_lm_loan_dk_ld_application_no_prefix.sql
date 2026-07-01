-- 按 application 重算 application_no：ng + LPAD(appId,4,'0') + '-' + applicationNo
-- 例: appId=7 → ng0007-... | appId=5011 → ng5011-...
-- 源库宽表（LM_MYSQL / ng_loan_market）:
UPDATE loan_dk_ld_sync_staging s
INNER JOIN application a ON a.applicationNo = SUBSTRING_INDEX(s.application_no, '-', -1)
SET s.application_no = CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo);

-- 目标库 loan（已同步错误 key 时，需能连老库 application 或手工按 sn 修正）:
-- UPDATE loan l
-- INNER JOIN application a ON a.applicationNo = SUBSTRING_INDEX(l.application_no, '-', -1)
-- SET l.application_no = CONCAT('ng', LPAD(a.appId, 4, '0'), '-', a.applicationNo);

SELECT application_no, COUNT(*) AS cnt
FROM loan_dk_ld_sync_staging
WHERE application_no LIKE 'ng%'
GROUP BY application_no
ORDER BY cnt DESC
LIMIT 10;
