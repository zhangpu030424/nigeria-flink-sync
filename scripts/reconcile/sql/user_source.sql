SELECT u.id,
       u.app_code,
       u.mobile,
       u.device_id,
       u.adid,
       u.create_time,
       u.update_time,
       UNIX_TIMESTAMP(u.create_time) * 1000 AS reg_time,
       a.network_name,
       a.tracker_name,
       a.campaign_tracker,
       a.campaign_name,
       a.creative_name,
       a.adgroup_tracker,
       a.creative_tracker,
       a.adgroup_name,
       CASE
           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
           ELSE CONCAT('+234', TRIM(u.mobile))
       END AS mobile_norm,
       vt_m.token AS mobile_token
FROM `user` u
         LEFT JOIN adjust_latest_by_adid a
                   ON u.adid IS NOT NULL AND u.adid <> '' AND a.adid = u.adid
         LEFT JOIN vt_token_cache vt_m
                   ON vt_m.vt_type = 1 AND vt_m.status = 1
                       AND vt_m.raw_value COLLATE utf8mb4_bin = (CASE
                           WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
                           WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
                           WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
                           WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
                           ELSE CONCAT('+234', TRIM(u.mobile))
                       END) COLLATE utf8mb4_bin
