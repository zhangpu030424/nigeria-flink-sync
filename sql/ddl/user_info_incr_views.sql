-- user_info 增量所需 Lookup 视图（DMS 可跑，无 GRANT）
-- 推荐: ./scripts/deploy-source-ddl.sh（含本文件全部视图 + adjust 视图）
-- 单独部署: mysql ... < sql/ddl/user_info_incr_views.sql

USE nigeria_backend;

CREATE OR REPLACE VIEW user_info_user_lookup AS
SELECT CAST(id AS SIGNED) AS id,
       CAST(app_code AS SIGNED) AS app_code,
       CAST(create_time AS DATETIME(3)) AS create_time
FROM user;

CREATE OR REPLACE VIEW app_config_lookup AS
SELECT CAST(app_code AS SIGNED) AS app_code,
       CAST(app_name AS CHAR) AS app_name,
       CAST(version AS CHAR) AS version
FROM app_config;

CREATE OR REPLACE VIEW vt_token_cache_lookup AS
SELECT CAST(vt_type AS CHAR) AS vt_type,
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
SELECT CAST(user_id AS SIGNED) AS user_id,
       CAST(
               COALESCE(
                       JSON_ARRAYAGG(
                               JSON_OBJECT(
                                       'name', NULLIF(TRIM(contact_name), ''),
                                       'mobile', CASE
                                                     WHEN contact_number IS NULL OR TRIM(contact_number) = '' THEN NULL
                                                     WHEN TRIM(contact_number) LIKE '+234%' THEN SUBSTRING(TRIM(contact_number), 5)
                                                     WHEN TRIM(contact_number) LIKE '234%' AND CHAR_LENGTH(TRIM(contact_number)) > 3
                                                         THEN SUBSTRING(TRIM(contact_number), 4)
                                                     WHEN TRIM(contact_number) LIKE '0%' AND CHAR_LENGTH(TRIM(contact_number)) = 11
                                                         THEN SUBSTRING(TRIM(contact_number), 2)
                                                     ELSE TRIM(contact_number)
                                           END,
                                       'relation', contact_relationship
                               )
                       ),
                       JSON_ARRAY()
               ) AS CHAR
       ) AS emergency_contacts
FROM user_emergency_contact
GROUP BY user_id;

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
