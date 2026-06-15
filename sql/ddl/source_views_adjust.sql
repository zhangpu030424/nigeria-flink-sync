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
