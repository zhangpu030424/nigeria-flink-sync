SELECT u.id AS user_id,
       TRIM(p.bvn) AS bvn_raw,
       vt_id.token AS id_number_token,
       TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))) AS full_name,
       JSON_OBJECT(
               'birthday', DATE_FORMAT(p.date_of_birth, '%Y-%m-%d'),
               'job_type', wr.work_type,
               'education', p.education_level,
               'gender', p.gender,
               'registration_ip', reg_ip.ip,
               'salary', CASE
                             WHEN wr.monthly_income IS NULL OR TRIM(wr.monthly_income) = '' THEN NULL
                             WHEN LENGTH(REPLACE(TRIM(wr.monthly_income), ',', '')) BETWEEN 1 AND 19
                                 AND REPLACE(TRIM(wr.monthly_income), ',', '') REGEXP '^[0-9]+$'
                                 THEN CAST(CAST(REPLACE(TRIM(wr.monthly_income), ',', '') AS UNSIGNED) AS JSON)
                             ELSE NULL
                   END,
               'loan_purpose', CAST(NULL AS JSON),
               'face_similarity', CAST(NULL AS JSON),
               'pay_cycle', CAST(NULL AS JSON),
               'salary_yearly', CAST(NULL AS JSON),
               'credit_limit', CASE
                                   WHEN cc.credit_limit IS NULL THEN NULL
                                   WHEN CAST(cc.credit_limit AS CHAR) REGEXP '^[0-9]{1,19}$'
                                       THEN CAST(cc.credit_limit AS UNSIGNED)
                                   ELSE NULL
                   END,
               'company', NULLIF(TRIM(wr.company_name), ''),
               'install_source', CASE
                                     WHEN adj.tracker_name IS NULL OR TRIM(adj.tracker_name) = '' THEN CAST(NULL AS JSON)
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%unattributed%' THEN CAST(NULL AS JSON)
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%organic%' THEN 'ORGANIC'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%google%' THEN 'GG'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%apple%' THEN 'ASA'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%tiktok%' THEN 'TT'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%facebook%'
                                         OR LOWER(TRIM(adj.tracker_name)) LIKE '%instagram%'
                                         OR LOWER(TRIM(adj.tracker_name)) LIKE '%messenger%' THEN 'FB'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%sms%' THEN 'SMS'
                                     WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%kuai%' THEN 'KW'
                                     ELSE TRIM(adj.tracker_name)
                   END,
               'registration_time', UNIX_TIMESTAMP(u.create_time),
               'email', CAST(NULL AS JSON),
               'ocr', CAST(NULL AS JSON),
               'profession', wr.occupation,
               'app', JSON_OBJECT(
                       'name', ac.app_name,
                       'version', ac.version,
                       'app_id', u.app_code
                      ),
               'emergency_contacts', COALESCE(ec.emergency_contacts, CAST('[]' AS JSON)),
               'salary_day', CAST(NULL AS JSON),
               'address', JSON_OBJECT(
                       'province', p.living_address_state,
                       'city', p.living_address_city,
                       'district', CAST(NULL AS JSON),
                       'detail', NULLIF(TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ',
                                                    COALESCE(p.living_address_second_line, ''))), ''),
                       'village', CAST(NULL AS JSON)
                      ),
               'salary_fortnightly', CAST(NULL AS JSON),
               'salary_daily', CAST(NULL AS JSON),
               'salary_monthly', 1,
               'children_num', p.number_of_children,
               'religion', CAST(NULL AS JSON),
               'marital', p.marriage,
               'full_name', NULLIF(TRIM(CONCAT(COALESCE(p.first_name, ''), ' ', COALESCE(p.sur_name, ''))), ''),
               'salary_weekly', CAST(NULL AS JSON),
               'survey', CAST(NULL AS JSON),
               'salary_type', CAST(NULL AS JSON)
       ) AS info_json
FROM `user` u
         LEFT JOIN (
    SELECT user_id,
           bvn,
           first_name,
           sur_name,
           date_of_birth,
           education_level,
           gender,
           living_address_state,
           living_address_city,
           living_address_first_line,
           living_address_second_line,
           number_of_children,
           marriage
    FROM (
             SELECT user_id,
                    bvn,
                    first_name,
                    sur_name,
                    date_of_birth,
                    education_level,
                    gender,
                    living_address_state,
                    living_address_city,
                    living_address_first_line,
                    living_address_second_line,
                    number_of_children,
                    marriage,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
             FROM user_personal_info
         ) t
    WHERE rn = 1
) p ON p.user_id = u.id
         LEFT JOIN user_work_related wr ON wr.user_id = u.id
         LEFT JOIN app_config ac ON ac.app_code = u.app_code
         LEFT JOIN vt_token_cache vt_id
                   ON vt_id.vt_type = 4 AND vt_id.status = 1
                       AND vt_id.raw_value COLLATE utf8mb4_bin = TRIM(p.bvn) COLLATE utf8mb4_bin
         LEFT JOIN (
    SELECT user_id, credit_limit
    FROM (
             SELECT user_id, credit_limit,
                    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
             FROM risk_user_credit_callback
         ) t
    WHERE rn = 1
) cc ON cc.user_id = u.id
         LEFT JOIN adjust_latest_by_adid adj
                   ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid
         LEFT JOIN (
    SELECT user_id, ip
    FROM (
             SELECT u2.id AS user_id,
                    dn.ip,
                    ROW_NUMBER() OVER (PARTITION BY u2.id ORDER BY dn.create_time DESC) AS rn
             FROM `user` u2
                      LEFT JOIN (
                 SELECT device_uuid, session_uuid
                 FROM (
                          SELECT device_uuid, session_uuid,
                                 ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS di_rn
                          FROM device_ids
                          WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
                      ) di0
                 WHERE di_rn = 1
             ) di ON di.device_uuid = u2.device_id
                      INNER JOIN device_network dn
                                 ON dn.ip IS NOT NULL AND TRIM(dn.ip) <> ''
                                     AND (
                                        (u2.device_id IS NOT NULL AND TRIM(u2.device_id) <> '' AND dn.device_uuid = u2.device_id)
                                            OR (di.session_uuid IS NOT NULL AND TRIM(di.session_uuid) <> ''
                                            AND dn.session_uuid = di.session_uuid)
                                        )
         ) rip
    WHERE rn = 1
) reg_ip ON reg_ip.user_id = u.id
         LEFT JOIN (
    SELECT ec.user_id,
           JSON_ARRAYAGG(
                   JSON_OBJECT(
                           'name', NULLIF(TRIM(ec.contact_name), ''),
                           'mobile', CASE
                                         WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = ''
                                             THEN CAST(NULL AS JSON)
                                         WHEN vt.token IS NOT NULL AND TRIM(vt.token) <> ''
                                             THEN vt.token
                                         ELSE (
                                             CASE
                                                 WHEN TRIM(ec.contact_number) LIKE '+%'
                                                     THEN TRIM(ec.contact_number)
                                                 WHEN TRIM(ec.contact_number) LIKE '234%'
                                                     THEN CONCAT('+', TRIM(ec.contact_number))
                                                 WHEN TRIM(ec.contact_number) LIKE '0%'
                                                     THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                                                 ELSE CONCAT('+234', TRIM(ec.contact_number))
                                             END
                                         )
                                   END,
                           'relation', ec.contact_relationship
                   )
           ) AS emergency_contacts
    FROM user_emergency_contact ec
             LEFT JOIN vt_token_cache vt
                       ON vt.vt_type = 5
                           AND vt.status = 1
                           AND vt.token IS NOT NULL
                           AND TRIM(vt.token) <> ''
                           AND vt.raw_value COLLATE utf8mb4_bin = (
                               CASE
                                   WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = '' THEN NULL
                                   WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                                   WHEN TRIM(ec.contact_number) LIKE '234%'
                                       THEN CONCAT('+', TRIM(ec.contact_number))
                                   WHEN TRIM(ec.contact_number) LIKE '0%'
                                       THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                                   ELSE CONCAT('+234', TRIM(ec.contact_number))
                                   END
                               ) COLLATE utf8mb4_bin
    GROUP BY ec.user_id
) ec ON ec.user_id = u.id
