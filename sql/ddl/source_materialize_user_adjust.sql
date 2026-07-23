-- 全量同步前在源库执行：物化 v_adjust_latest_by_adid，Flink Lookup 按 adid 点查
-- mysql -h <源库> -u ... -p nigeria_backend < sql/ddl/source_materialize_user_adjust.sql
--
-- 注意：勿用 CREATE TABLE AS SELECT 推断列宽——Facebook 等 tracker_name 很长，
-- CTAS 常得到过短 VARCHAR，刷新 INSERT 会报 Data truncated for column 'tracker_name'。

DROP TABLE IF EXISTS adjust_latest_by_adid;

CREATE TABLE adjust_latest_by_adid (
    adid              VARCHAR(128)  NOT NULL,
    network_name      VARCHAR(512)  NULL,
    tracker_name      VARCHAR(1024) NULL,
    campaign_tracker  VARCHAR(512)  NULL,
    campaign_name     VARCHAR(1024) NULL,
    creative_name     VARCHAR(1024) NULL,
    adgroup_tracker   VARCHAR(512)  NULL,
    creative_tracker  VARCHAR(512)  NULL,
    adgroup_name      VARCHAR(1024) NULL,
    create_time       DATETIME(3)   NULL,
    PRIMARY KEY (adid)
) COMMENT 'adjust 按 adid 最新一条（全量/Lookup 用）';

INSERT INTO adjust_latest_by_adid
SELECT adid,
       network_name,
       tracker_name,
       campaign_tracker,
       campaign_name,
       creative_name,
       adgroup_tracker,
       creative_tracker,
       adgroup_name,
       create_time
FROM v_adjust_latest_by_adid;

-- 刷新（表已存在且列宽够时）:
-- TRUNCATE adjust_latest_by_adid;
-- INSERT INTO adjust_latest_by_adid SELECT ... FROM v_adjust_latest_by_adid;
