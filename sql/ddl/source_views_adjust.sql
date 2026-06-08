-- 在源库 nigeria_backend 执行一次
-- UTM 关联规则：adjust_callback_record.adid = user.adid，取该 adid 最新一条回调

CREATE OR REPLACE VIEW v_adjust_latest_by_adid AS
SELECT r.adid,
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
         INNER JOIN (SELECT adid, MAX(id) AS max_id
                     FROM adjust_callback_record
                     WHERE adid IS NOT NULL
                       AND adid <> ''
                     GROUP BY adid) t ON r.adid = t.adid AND r.id = t.max_id;

-- 与 SELECT * FROM adjust_callback_record INNER JOIN user ON adjust_callback_record.adid = user.adid 等价（每 adid 最新一条）
CREATE OR REPLACE VIEW v_user_adjust_latest AS
SELECT u.id AS user_id,
       u.adid,
       acr.network_name,
       acr.tracker_name,
       acr.campaign_tracker,
       acr.campaign_name,
       acr.creative_name,
       acr.adgroup_tracker,
       acr.creative_tracker,
       acr.adgroup_name
FROM `user` u
         INNER JOIN v_adjust_latest_by_adid acr ON acr.adid = u.adid
WHERE u.adid IS NOT NULL
  AND u.adid <> '';
