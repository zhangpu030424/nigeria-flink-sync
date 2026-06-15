-- =============================================================================
-- id_mapping_sync_staging（从 source_all_sync_staging 第 7 段拆出）
-- 前置: application/user/user_info/user_bankcard 宽表已建好，vt_token_cache status=1
--
-- mysql -h <host> -u ... -p nigeria_backend < sql/ddl/id_mapping_sync_staging.sql
-- =============================================================================

SET SESSION wait_timeout = 28800;
SET SESSION net_read_timeout = 7200;
SET SESSION net_write_timeout = 7200;
-- 勿设 max_execution_time / sql_log_bin（RDS 无 SUPER 会 1227）

-- 敏感 ID 关系双写（不去重，每条源事件保留一行有向边）；type = id 的类型；mobile/gaid/bank/id_number 为 VT token
-- device_uuid 为原始 UUID（flink.md 未要求 VT）；id2 源字段待确认，暂不产出
DROP TABLE IF EXISTS id_mapping_sync_staging;

CREATE TABLE id_mapping_sync_staging (
    row_id     BIGINT       NOT NULL AUTO_INCREMENT,
    id         VARCHAR(36)  NOT NULL,
    app_id     INT UNSIGNED NOT NULL,
    mapping_id VARCHAR(36)  NOT NULL,
    type       VARCHAR(32)  NOT NULL,
    event_time BIGINT       NOT NULL,
    PRIMARY KEY (row_id)
);

INSERT INTO id_mapping_sync_staging (id, app_id, mapping_id, type, event_time)
SELECT id,
       app_id,
       mapping_id,
       type,
       event_time
FROM (
         -- application：mobile ↔ gaid_idfa
         SELECT a.mobile_token AS id,
                CAST(a.app_code AS UNSIGNED) AS app_id,
                a.gaid_idfa_token AS mapping_id,
                'mobile' AS type,
                UNIX_TIMESTAMP(a.order_time) * 1000 AS event_time
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.gaid_idfa_token IS NOT NULL AND TRIM(a.gaid_idfa_token) <> ''
           AND a.mobile_token <> a.gaid_idfa_token
         UNION ALL
         SELECT a.gaid_idfa_token,
                CAST(a.app_code AS UNSIGNED),
                a.mobile_token,
                'gaid_idfa',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.gaid_idfa_token IS NOT NULL AND TRIM(a.gaid_idfa_token) <> ''
           AND a.mobile_token <> a.gaid_idfa_token

         UNION ALL
         -- application：mobile ↔ bank_account
         SELECT a.mobile_token,
                CAST(a.app_code AS UNSIGNED),
                a.bank_account_token,
                'mobile',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.bank_account_token IS NOT NULL AND TRIM(a.bank_account_token) <> ''
           AND a.mobile_token <> a.bank_account_token
         UNION ALL
         SELECT a.bank_account_token,
                CAST(a.app_code AS UNSIGNED),
                a.mobile_token,
                'bank_account',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.bank_account_token IS NOT NULL AND TRIM(a.bank_account_token) <> ''
           AND a.mobile_token <> a.bank_account_token

         UNION ALL
         -- application：mobile ↔ id_number
         SELECT a.mobile_token,
                CAST(a.app_code AS UNSIGNED),
                a.id_number_token,
                'mobile',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.id_number_token IS NOT NULL AND TRIM(a.id_number_token) <> ''
           AND a.mobile_token <> a.id_number_token
         UNION ALL
         SELECT a.id_number_token,
                CAST(a.app_code AS UNSIGNED),
                a.mobile_token,
                'id_number',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.id_number_token IS NOT NULL AND TRIM(a.id_number_token) <> ''
           AND a.mobile_token <> a.id_number_token

         UNION ALL
         -- application：mobile ↔ device_uuid
         SELECT a.mobile_token,
                CAST(a.app_code AS UNSIGNED),
                TRIM(a.device_uuid),
                'mobile',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.device_uuid IS NOT NULL AND TRIM(a.device_uuid) <> ''
           AND a.mobile_token <> TRIM(a.device_uuid)
         UNION ALL
         SELECT TRIM(a.device_uuid),
                CAST(a.app_code AS UNSIGNED),
                a.mobile_token,
                'device_uuid',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.mobile_token IS NOT NULL AND TRIM(a.mobile_token) <> ''
           AND a.device_uuid IS NOT NULL AND TRIM(a.device_uuid) <> ''
           AND a.mobile_token <> TRIM(a.device_uuid)

         UNION ALL
         -- application：device_uuid ↔ gaid_idfa
         SELECT TRIM(a.device_uuid),
                CAST(a.app_code AS UNSIGNED),
                a.gaid_idfa_token,
                'device_uuid',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.device_uuid IS NOT NULL AND TRIM(a.device_uuid) <> ''
           AND a.gaid_idfa_token IS NOT NULL AND TRIM(a.gaid_idfa_token) <> ''
           AND TRIM(a.device_uuid) <> a.gaid_idfa_token
         UNION ALL
         SELECT a.gaid_idfa_token,
                CAST(a.app_code AS UNSIGNED),
                TRIM(a.device_uuid),
                'gaid_idfa',
                UNIX_TIMESTAMP(a.order_time) * 1000
         FROM application_sync_staging a
         WHERE a.device_uuid IS NOT NULL AND TRIM(a.device_uuid) <> ''
           AND a.gaid_idfa_token IS NOT NULL AND TRIM(a.gaid_idfa_token) <> ''
           AND TRIM(a.device_uuid) <> a.gaid_idfa_token

         UNION ALL
         -- user 注册：mobile ↔ device_uuid
         SELECT u.mobile_token,
                CAST(u.app_code AS UNSIGNED),
                TRIM(u.device_id),
                'mobile',
                u.reg_time
         FROM user_sync_staging u
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
           AND u.mobile_token <> TRIM(u.device_id)
         UNION ALL
         SELECT TRIM(u.device_id),
                CAST(u.app_code AS UNSIGNED),
                u.mobile_token,
                'device_uuid',
                u.reg_time
         FROM user_sync_staging u
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND u.device_id IS NOT NULL AND TRIM(u.device_id) <> ''
           AND u.mobile_token <> TRIM(u.device_id)

         UNION ALL
         -- user + user_info：mobile ↔ id_number（无申请单用户）
         SELECT u.mobile_token,
                CAST(u.app_code AS UNSIGNED),
                ui.id_number_token,
                'mobile',
                u.reg_time
         FROM user_sync_staging u
                  INNER JOIN user_info_sync_staging ui ON ui.user_id = u.id
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND ui.id_number_token IS NOT NULL AND TRIM(ui.id_number_token) <> ''
           AND u.mobile_token <> ui.id_number_token

         UNION ALL
         SELECT ui.id_number_token,
                CAST(u.app_code AS UNSIGNED),
                u.mobile_token,
                'id_number',
                u.reg_time
         FROM user_sync_staging u
                  INNER JOIN user_info_sync_staging ui ON ui.user_id = u.id
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND ui.id_number_token IS NOT NULL AND TRIM(ui.id_number_token) <> ''
           AND u.mobile_token <> ui.id_number_token

         UNION ALL
         -- user + bankcard：mobile ↔ bank_account
         SELECT u.mobile_token,
                CAST(u.app_code AS UNSIGNED),
                b.bank_account_token,
                'mobile',
                UNIX_TIMESTAMP(u.update_time) * 1000
         FROM user_sync_staging u
                  INNER JOIN user_bankcard_sync_staging b ON b.user_id = u.id
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND b.bank_account_token IS NOT NULL AND TRIM(b.bank_account_token) <> ''
           AND u.mobile_token <> b.bank_account_token
         UNION ALL
         SELECT b.bank_account_token,
                CAST(u.app_code AS UNSIGNED),
                u.mobile_token,
                'bank_account',
                UNIX_TIMESTAMP(u.update_time) * 1000
         FROM user_sync_staging u
                  INNER JOIN user_bankcard_sync_staging b ON b.user_id = u.id
         WHERE u.mobile_token IS NOT NULL AND TRIM(u.mobile_token) <> ''
           AND b.bank_account_token IS NOT NULL AND TRIM(b.bank_account_token) <> ''
           AND u.mobile_token <> b.bank_account_token

         UNION ALL
         -- device_ids + user：device_uuid ↔ gaid_idfa
         SELECT TRIM(d.device_uuid),
                CAST(u.app_code AS UNSIGNED),
                vt_g.token,
                'device_uuid',
                UNIX_TIMESTAMP(COALESCE(d.update_time, d.create_time)) * 1000
         FROM device_ids d
                  INNER JOIN `user` u ON u.device_id = d.device_uuid
                  INNER JOIN vt_token_cache vt_g
                             ON vt_g.vt_type = 2 AND vt_g.status = 1
                                 AND vt_g.raw_value COLLATE utf8mb4_bin = TRIM(COALESCE(
                                     NULLIF(TRIM(d.aaid), ''),
                                     NULLIF(TRIM(d.idfa), '')
                                 )) COLLATE utf8mb4_bin
         WHERE d.device_uuid IS NOT NULL AND TRIM(d.device_uuid) <> ''
           AND COALESCE(NULLIF(TRIM(d.aaid), ''), NULLIF(TRIM(d.idfa), '')) IS NOT NULL
           AND vt_g.token IS NOT NULL AND TRIM(vt_g.token) <> ''
           AND TRIM(d.device_uuid) <> vt_g.token
         UNION ALL
         SELECT vt_g.token,
                CAST(u.app_code AS UNSIGNED),
                TRIM(d.device_uuid),
                'gaid_idfa',
                UNIX_TIMESTAMP(COALESCE(d.update_time, d.create_time)) * 1000
         FROM device_ids d
                  INNER JOIN `user` u ON u.device_id = d.device_uuid
                  INNER JOIN vt_token_cache vt_g
                             ON vt_g.vt_type = 2 AND vt_g.status = 1
                                 AND vt_g.raw_value COLLATE utf8mb4_bin = TRIM(COALESCE(
                                     NULLIF(TRIM(d.aaid), ''),
                                     NULLIF(TRIM(d.idfa), '')
                                 )) COLLATE utf8mb4_bin
         WHERE d.device_uuid IS NOT NULL AND TRIM(d.device_uuid) <> ''
           AND COALESCE(NULLIF(TRIM(d.aaid), ''), NULLIF(TRIM(d.idfa), '')) IS NOT NULL
           AND vt_g.token IS NOT NULL AND TRIM(vt_g.token) <> ''
           AND TRIM(d.device_uuid) <> vt_g.token
     ) raw
WHERE id IS NOT NULL AND TRIM(id) <> ''
  AND mapping_id IS NOT NULL AND TRIM(mapping_id) <> ''
  AND id <> mapping_id;

SELECT 'id_mapping_row_count' AS label,
       COUNT(*) AS id_mapping_cnt
FROM id_mapping_sync_staging;
