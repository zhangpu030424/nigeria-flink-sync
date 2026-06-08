-- 在源库 nigeria_backend 执行一次，供 Flink JDBC Lookup 按设备 ID 取最新 adjust 回调
-- 与 AdjustCallbackQueryServiceImpl#getUtmByDeviceIds 规则一致：ORDER BY create_time DESC LIMIT 1

CREATE OR REPLACE VIEW v_adjust_latest_by_gps_adid AS
SELECT r.gps_adid,
       r.network_name,
       r.tracker_name,
       r.campaign_tracker,
       r.campaign_name,
       r.creative_name,
       r.adgroup_tracker,
       r.creative_tracker,
       r.adgroup_name,
       r.create_time
FROM adjust_callback_record r
         INNER JOIN (SELECT gps_adid, MAX(id) AS max_id
                     FROM adjust_callback_record
                     WHERE gps_adid IS NOT NULL
                       AND gps_adid <> ''
                     GROUP BY gps_adid) t ON r.gps_adid = t.gps_adid AND r.id = t.max_id;

CREATE OR REPLACE VIEW v_adjust_latest_by_idfa AS
SELECT r.idfa,
       r.network_name,
       r.tracker_name,
       r.campaign_tracker,
       r.campaign_name,
       r.creative_name,
       r.adgroup_tracker,
       r.creative_tracker,
       r.adgroup_name,
       r.create_time
FROM adjust_callback_record r
         INNER JOIN (SELECT idfa, MAX(id) AS max_id
                     FROM adjust_callback_record
                     WHERE idfa IS NOT NULL
                       AND idfa <> ''
                     GROUP BY idfa) t ON r.idfa = t.idfa AND r.id = t.max_id;

CREATE OR REPLACE VIEW v_adjust_latest_by_idfv AS
SELECT r.idfv,
       r.network_name,
       r.tracker_name,
       r.campaign_tracker,
       r.campaign_name,
       r.creative_name,
       r.adgroup_tracker,
       r.creative_tracker,
       r.adgroup_name,
       r.create_time
FROM adjust_callback_record r
         INNER JOIN (SELECT idfv, MAX(id) AS max_id
                     FROM adjust_callback_record
                     WHERE idfv IS NOT NULL
                       AND idfv <> ''
                     GROUP BY idfv) t ON r.idfv = t.idfv AND r.id = t.max_id;

-- 按 user.id 预关联最新 adjust（Flink 只需 1 次 LookupJoin，避免 3 链式 Join 卡 INITIALIZING）
-- 规则同 getUtmByDeviceIds：gps_adid / idfa / idfv OR 匹配，create_time 最新
CREATE OR REPLACE VIEW v_user_adjust_latest AS
SELECT u.id AS user_id,
       acr.network_name,
       acr.tracker_name,
       acr.campaign_tracker,
       acr.campaign_name,
       acr.creative_name,
       acr.adgroup_tracker,
       acr.creative_tracker,
       acr.adgroup_name
FROM `user` u
         LEFT JOIN adjust_callback_record acr ON acr.id = (
    SELECT r.id
    FROM adjust_callback_record r
    WHERE (u.gps_adid IS NOT NULL AND u.gps_adid <> '' AND r.gps_adid = u.gps_adid)
       OR (u.idfa IS NOT NULL AND u.idfa <> '' AND r.idfa = u.idfa)
       OR (u.idfv IS NOT NULL AND u.idfv <> '' AND r.idfv = u.idfv)
    ORDER BY r.create_time DESC, r.id DESC
    LIMIT 1
);
