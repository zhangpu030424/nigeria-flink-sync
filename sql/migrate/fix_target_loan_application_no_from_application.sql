-- 目标库 ng.loan：修正错误 application_no 前缀（无需 JOIN application）
-- 规则: ng + appId 段应为 6 位（ng + 4 位 appId）；超过 6 位说明多写了 ng 后的 0
-- 例: ng05011-xxx (7位) → ng5011-xxx | ng0567-xxx (6位) 不动
--
-- 用法:
--   bash scripts/run-fix-target-loan-app-no.sh

-- ---------- 修正前 ----------
SELECT 'before_prefix_gt6' AS metric, COUNT(*) AS cnt
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6;

-- 正确 key 已存在时删错误行（否则 UPDATE 会 1062 Duplicate entry）
DELETE l_bad
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
  AND l_bad.application_no <> l_ok.application_no;

UPDATE loan
SET application_no = CONCAT(
        'ng',
        SUBSTRING(SUBSTRING_INDEX(application_no, '-', 1), 4),
        '-',
        SUBSTRING_INDEX(application_no, '-', -1)
    )
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6;

-- ---------- 修正后 ----------
SELECT 'after_prefix_gt6' AS metric, COUNT(*) AS cnt
FROM loan
WHERE application_no LIKE 'ng0%'
  AND LENGTH(SUBSTRING_INDEX(application_no, '-', 1)) > 6;
