-- user_product 宽表 vs 源库 user_order 差集诊断（在源库 nigeria_backend 执行）
-- 宽表逻辑：每个 (user_id, product_id) 取 user_order.order_time 最新一条的 amount_max
-- 目标主键：group_user_id = user_id + 100000000, product_id

-- ========== 1) 基础计数 ==========
SELECT 'user_order_rows' AS metric, COUNT(*) AS cnt FROM user_order
UNION ALL
SELECT 'user_order_distinct_pk', COUNT(*)
FROM (SELECT DISTINCT user_id, product_id FROM user_order) d
UNION ALL
SELECT 'user_order_expected_latest', COUNT(*)
FROM (
    SELECT o.user_id, o.product_id
    FROM (
        SELECT user_id, product_id,
               ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
        FROM user_order
    ) o
    WHERE o.rn = 1
) e
UNION ALL
SELECT 'staging_rows', COUNT(*) FROM user_product_sync_staging
UNION ALL
SELECT 'staging_null_or_empty_product_id', COUNT(*)
FROM user_product_sync_staging
WHERE product_id IS NULL OR TRIM(product_id) = ''
UNION ALL
SELECT 'order_null_or_empty_product_id', COUNT(*)
FROM user_order
WHERE product_id IS NULL OR TRIM(product_id) = '';

-- ========== 2) 源库应有、宽表没有（缺行） ==========
SELECT 'missing_in_staging' AS label, COUNT(*) AS cnt
FROM (
    SELECT o.user_id, o.product_id
    FROM (
        SELECT user_id, product_id,
               ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
        FROM user_order
    ) o
    WHERE o.rn = 1
) e
LEFT JOIN user_product_sync_staging s
    ON s.user_id = e.user_id AND s.product_id = e.product_id
WHERE s.user_id IS NULL;

-- 缺行样例（前 50）
SELECT e.user_id, e.product_id, uo.id AS latest_order_id, uo.order_time, uo.amount_max
FROM (
    SELECT user_id, product_id,
           ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
    FROM user_order
) e
INNER JOIN user_order uo
    ON uo.user_id = e.user_id AND uo.product_id = e.product_id
LEFT JOIN user_product_sync_staging s
    ON s.user_id = e.user_id AND s.product_id = e.product_id
WHERE e.rn = 1
  AND s.user_id IS NULL
  AND uo.order_time = (
      SELECT MAX(o2.order_time)
      FROM user_order o2
      WHERE o2.user_id = e.user_id AND o2.product_id = e.product_id
  )
ORDER BY e.user_id, e.product_id
LIMIT 50;

-- ========== 3) 宽表有、源库 user_order 没有（多行 / 宽表过期） ==========
SELECT 'extra_in_staging' AS label, COUNT(*) AS cnt
FROM user_product_sync_staging s
LEFT JOIN (
    SELECT DISTINCT user_id, product_id FROM user_order
) o ON o.user_id = s.user_id AND o.product_id = s.product_id
WHERE o.user_id IS NULL;

SELECT s.user_id, s.product_id, s.credit_amount_minor, s.unpaid_amount_minor
FROM user_product_sync_staging s
LEFT JOIN (SELECT DISTINCT user_id, product_id FROM user_order) o
    ON o.user_id = s.user_id AND o.product_id = s.product_id
WHERE o.user_id IS NULL
ORDER BY s.user_id, s.product_id
LIMIT 50;

-- ========== 4) 键存在但金额与源库最新单不一致 ==========
SELECT 'amount_mismatch' AS label, COUNT(*) AS cnt
FROM user_product_sync_staging s
INNER JOIN (
    SELECT user_id, product_id, amount_max
    FROM (
        SELECT user_id, product_id, amount_max,
               ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
        FROM user_order
    ) t
    WHERE rn = 1
) e ON e.user_id = s.user_id AND e.product_id = s.product_id
WHERE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(e.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED)
   <> s.credit_amount_minor
   OR CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(e.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED)
   <> s.unpaid_amount_minor;

SELECT s.user_id, s.product_id,
       s.credit_amount_minor AS staging_credit,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(e.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS order_amount
FROM user_product_sync_staging s
INNER JOIN (
    SELECT user_id, product_id, amount_max
    FROM (
        SELECT user_id, product_id, amount_max,
               ROW_NUMBER() OVER (PARTITION BY user_id, product_id ORDER BY order_time DESC) AS rn
        FROM user_order
    ) t
    WHERE rn = 1
) e ON e.user_id = s.user_id AND e.product_id = s.product_id
WHERE CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(e.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) <> s.credit_amount_minor
ORDER BY s.user_id, s.product_id
LIMIT 30;

-- ========== 5) 同 PK 在 user_order 多行（宽表只保留最新一单） ==========
SELECT 'duplicate_order_pk_groups' AS metric, COUNT(*) AS cnt
FROM (
    SELECT user_id, product_id
    FROM user_order
    GROUP BY user_id, product_id
    HAVING COUNT(*) > 1
) x;

-- ========== 6) 目标库对比（同实例时取消注释，改 platform_db） ==========
/*
SELECT 'target_user_product' AS metric, COUNT(*) AS cnt FROM platform_db.user_product;

SELECT 'staging_missing_in_target' AS label, COUNT(*) AS cnt
FROM user_product_sync_staging s
LEFT JOIN platform_db.user_product t
    ON t.group_user_id = s.user_id + 100000000 AND t.product_id = s.product_id
WHERE t.group_user_id IS NULL;

SELECT s.user_id, s.user_id + 100000000 AS expect_group_user_id, s.product_id
FROM user_product_sync_staging s
LEFT JOIN platform_db.user_product t
    ON t.group_user_id = s.user_id + 100000000 AND t.product_id = s.product_id
WHERE t.group_user_id IS NULL
ORDER BY s.user_id, s.product_id
LIMIT 50;
*/
