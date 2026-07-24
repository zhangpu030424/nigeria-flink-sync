SELECT t.user_id,
       COALESCE(m.dst, t.product_id) AS product_id,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS credit_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS unpaid_amount_minor
FROM (
         SELECT o.user_id,
                TRIM(o.product_id) AS product_id,
                o.amount_max,
                ROW_NUMBER() OVER (PARTITION BY o.user_id, TRIM(o.product_id) ORDER BY o.order_time DESC) AS rn
         FROM user_order o
     ) t
         LEFT JOIN product_id_map m ON m.src = t.product_id
WHERE t.rn = 1
