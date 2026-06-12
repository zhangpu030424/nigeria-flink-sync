-- user_info 增量所需 Lookup 视图（DMS 可跑，无 GRANT）
-- 推荐: ./scripts/deploy-source-ddl.sh（含本文件全部视图 + adjust 视图）
-- 单独部署: mysql ... < sql/ddl/user_info_incr_views.sql

USE nigeria_backend;

CREATE OR REPLACE VIEW user_info_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

CREATE OR REPLACE VIEW user_personal_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(bvn AS CHAR) AS bvn,
       CAST(first_name AS CHAR) AS first_name,
       CAST(sur_name AS CHAR) AS sur_name,
       CAST(date_of_birth AS DATE) AS date_of_birth,
       CAST(education_level AS SIGNED) AS education_level,
       CAST(gender AS SIGNED) AS gender,
       CAST(living_address_state AS CHAR) AS living_address_state,
       CAST(living_address_city AS CHAR) AS living_address_city,
       CAST(living_address_first_line AS CHAR) AS living_address_first_line,
       CAST(living_address_second_line AS CHAR) AS living_address_second_line,
       CAST(number_of_children AS SIGNED) AS number_of_children,
       CAST(marriage AS SIGNED) AS marriage
FROM (
         SELECT user_id, bvn, first_name, sur_name, date_of_birth, education_level, gender,
                living_address_state, living_address_city, living_address_first_line,
                living_address_second_line, number_of_children, marriage,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_personal_info
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_id_by_bvn_lookup AS
SELECT CAST(TRIM(bvn) AS CHAR) AS bvn,
       CAST(user_id AS SIGNED) AS user_id
FROM (
         SELECT user_id, bvn,
                ROW_NUMBER() OVER (PARTITION BY TRIM(bvn) ORDER BY id DESC) AS rn
         FROM user_personal_info
         WHERE bvn IS NOT NULL AND TRIM(bvn) <> ''
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW device_uuid_user_lookup AS
SELECT CAST(TRIM(device_id) AS CHAR) AS device_uuid,
       CAST(MAX(id) AS SIGNED) AS user_id
FROM user
WHERE device_id IS NOT NULL AND TRIM(device_id) <> ''
GROUP BY TRIM(device_id);

CREATE OR REPLACE VIEW session_uuid_user_lookup AS
SELECT CAST(di.session_uuid AS CHAR) AS session_uuid,
       CAST(MAX(u.id) AS SIGNED) AS user_id
FROM user u
         INNER JOIN (
    SELECT device_uuid, session_uuid,
           ROW_NUMBER() OVER (PARTITION BY device_uuid ORDER BY id DESC) AS rn
    FROM device_ids
    WHERE device_uuid IS NOT NULL AND TRIM(device_uuid) <> ''
      AND session_uuid IS NOT NULL AND TRIM(session_uuid) <> ''
) di ON di.device_uuid = u.device_id AND di.rn = 1
WHERE u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
GROUP BY di.session_uuid;

CREATE OR REPLACE VIEW app_config_lookup AS
SELECT CAST(app_code AS SIGNED) AS app_code,
       CAST(app_name AS CHAR) AS app_name,
       CAST(version AS CHAR) AS version
FROM app_config;

CREATE OR REPLACE VIEW vt_token_cache_lookup AS
SELECT CAST(CASE vt_type
                WHEN 1 THEN 'mobile'
                WHEN 2 THEN 'gaid_idfa'
                WHEN 3 THEN 'bank_account'
                WHEN 4 THEN 'id_number'
                WHEN 5 THEN 'emergency_contact'
                WHEN 6 THEN 'id2'
                ELSE CAST(vt_type AS CHAR)
            END AS CHAR) AS vt_type,
       CAST(raw_value AS CHAR) AS raw_value,
       CAST(token AS CHAR) AS token,
       CAST(status AS SIGNED) AS status
FROM vt_token_cache;

CREATE OR REPLACE VIEW user_work_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(work_type AS CHAR) AS work_type,
       CAST(occupation AS CHAR) AS occupation,
       CAST(company_name AS CHAR) AS company_name,
       CAST(monthly_income AS CHAR) AS monthly_income
FROM (
         SELECT user_id,
                work_type,
                occupation,
                company_name,
                monthly_income,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY id DESC) AS rn
         FROM user_work_related
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_credit_latest_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(credit_limit AS CHAR) AS credit_limit
FROM (
         SELECT user_id,
                credit_limit,
                ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY create_time DESC) AS rn
         FROM risk_user_credit_callback
     ) t
WHERE rn = 1;

CREATE OR REPLACE VIEW user_reg_ip_lookup AS
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(ip AS CHAR) AS ip
FROM (
         SELECT u2.id AS user_id,
                dn.ip,
                ROW_NUMBER() OVER (PARTITION BY u2.id ORDER BY dn.create_time DESC) AS rn
         FROM user u2
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
WHERE rn = 1;

CREATE OR REPLACE VIEW user_emergency_contacts_lookup AS
SELECT CAST(ec.user_id AS SIGNED) AS user_id,
       CAST(
               COALESCE(
                       JSON_ARRAYAGG(
                               JSON_OBJECT(
                                       'name', NULLIF(TRIM(ec.contact_name), ''),
                                       'mobile', CASE
                                                     WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = ''
                                                         THEN CAST(NULL AS JSON)
                                                     WHEN vt.token IS NOT NULL AND TRIM(vt.token) <> ''
                                                         THEN vt.token
                                                     ELSE CAST(NULL AS JSON)
                                           END,
                                       'relation', ec.contact_relationship
                               )
                       ),
                       JSON_ARRAY()
               ) AS CHAR
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
GROUP BY ec.user_id;

CREATE OR REPLACE VIEW user_info_install_source_lookup AS
SELECT CAST(u.id AS SIGNED) AS user_id,
       CAST(
               CASE
                   WHEN adj.tracker_name IS NULL OR TRIM(adj.tracker_name) = '' THEN NULL
                   WHEN LOWER(TRIM(adj.tracker_name)) LIKE '%unattributed%' THEN NULL
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
               END AS CHAR
       ) AS install_source
FROM user u
         LEFT JOIN v_adjust_latest_by_adid adj
                   ON u.adid IS NOT NULL AND u.adid <> '' AND adj.adid = u.adid;
