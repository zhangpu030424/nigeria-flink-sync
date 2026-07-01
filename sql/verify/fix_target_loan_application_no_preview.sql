-- 目标库 loan application_no 修正 — 只读预览（规则：前缀>6 则去掉 ng 后多余 0）

SELECT 'loan_ng0_total' AS metric, COUNT(*) AS cnt
FROM loan WHERE application_no LIKE 'ng0%'
UNION ALL
SELECT 'will_fix_prefix_gt6', COUNT(*)
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6
UNION ALL
SELECT 'will_delete_dup_before_update', COUNT(*)
FROM loan l_bad
INNER JOIN loan l_ok
    ON l_ok.application_no = CONCAT(
           'ng',
           SUBSTRING(SUBSTRING_INDEX(l_bad.application_no, '-', 1), 4),
           '-',
           SUBSTRING_INDEX(l_bad.application_no, '-', -1)
       )
   AND l_ok.period = l_bad.period
   AND l_ok.roll_sequence = l_bad.roll_sequence
WHERE l_bad.application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(l_bad.application_no, '-', 1)) > 6
  AND l_bad.application_no <> l_ok.application_no
UNION ALL
SELECT 'will_update_after_dedup', COUNT(*)
FROM loan l_bad
WHERE l_bad.application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(l_bad.application_no, '-', 1)) > 6
  AND NOT EXISTS (
      SELECT 1 FROM loan l_ok
      WHERE l_ok.application_no = CONCAT(
                'ng',
                SUBSTRING(SUBSTRING_INDEX(l_bad.application_no, '-', 1), 4),
                '-',
                SUBSTRING_INDEX(l_bad.application_no, '-', -1)
            )
        AND l_ok.period = l_bad.period
        AND l_ok.roll_sequence = l_bad.roll_sequence
        AND l_ok.application_no <> l_bad.application_no
  )
UNION ALL
SELECT 'ng0_prefix_le6_unchanged', COUNT(*)
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) <= 6;

-- 按错误前缀分布
SELECT SUBSTRING_INDEX(application_no, '-', 1) AS wrong_prefix,
       COUNT(*) AS cnt
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6
GROUP BY wrong_prefix
ORDER BY cnt DESC
LIMIT 20;

-- 抽样：旧 → 新
SELECT application_no AS old_application_no,
       CONCAT(
           'ng',
           SUBSTRING(SUBSTRING_INDEX(application_no, '-', 1), 4),
           '-',
           SUBSTRING_INDEX(application_no, '-', -1)
       ) AS new_application_no,
       loan_no
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6
LIMIT 10;
