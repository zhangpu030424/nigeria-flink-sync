SELECT t.user_id,
       COALESCE(m.dst, t.product_id) AS product_id,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS credit_amount_minor,
       CAST(COALESCE(ROUND(CAST(NULLIF(TRIM(t.amount_max), '') AS DECIMAL(20, 2)), 0), 0) AS SIGNED) AS unpaid_amount_minor
FROM (
         SELECT up.user_id,
                TRIM(up.product_id) AS product_id,
                up.amount_max,
                ROW_NUMBER() OVER (
                    PARTITION BY up.user_id, TRIM(up.product_id)
                    ORDER BY up.product_add_time DESC, up.id DESC
                ) AS rn
         FROM user_product up
     ) t
         LEFT JOIN product_id_map m ON m.src = t.product_id
WHERE t.rn = 1
