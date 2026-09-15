-- 贷超 application 与目标 ng.application 按 app 分桶计数（先跑这个，秒级）
-- LM 库 ng_loan_market:
SELECT appId AS app_id, COUNT(1) AS cnt
FROM application
WHERE appId NOT IN (567, 568, 569, 571, 572, 573)
GROUP BY appId
ORDER BY appId;

-- 目标库 ng:
SELECT app_id, COUNT(1) AS cnt
FROM application
WHERE app_id NOT IN (567, 568, 569, 571, 572, 573)
GROUP BY app_id
ORDER BY app_id;

-- 目标：application_no 是否重复（重复会导致 COUNT 比 LM 少）
SELECT COUNT(1) AS rows_total,
       COUNT(DISTINCT application_no) AS distinct_no,
       COUNT(1) - COUNT(DISTINCT application_no) AS dup_rows
FROM application
WHERE app_id NOT IN (567, 568, 569, 571, 572, 573);
