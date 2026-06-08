-- 全量冲刺：在源库预 JOIN user + adjust（adid），Flink 不再做 LookupJoin
-- 依赖：先执行 source_views_adjust.sql + source_materialize_user_adjust.sql
--
-- mysql -h <源库> -u ... -p nigeria_backend < sql/ddl/source_user_sync_staging.sql

DROP TABLE IF EXISTS user_sync_staging;

CREATE TABLE user_sync_staging AS
SELECT u.id,
       u.app_code,
       u.mobile,
       u.device_id,
       u.adid,
       u.create_time,
       a.network_name,
       a.tracker_name,
       a.campaign_tracker,
       a.campaign_name,
       a.creative_name,
       a.adgroup_tracker,
       a.creative_tracker,
       a.adgroup_name
FROM `user` u
         LEFT JOIN adjust_latest_by_adid a
                   ON u.adid IS NOT NULL AND u.adid <> '' AND a.adid = u.adid;

ALTER TABLE user_sync_staging
    ADD PRIMARY KEY (id);

-- 全量前重建：DROP + 本脚本重跑
-- 增量阶段请改回 CDC user 表 + Lookup，或定时刷新本表
