-- 全量同步前在源库执行：物化 v_adjust_latest_by_adid，Flink Lookup 按 adid 点查
-- mysql -h <源库> -u ... -p nigeria_backend < sql/ddl/source_materialize_user_adjust.sql

DROP TABLE IF EXISTS adjust_latest_by_adid;

CREATE TABLE adjust_latest_by_adid AS
SELECT * FROM v_adjust_latest_by_adid;

ALTER TABLE adjust_latest_by_adid
    ADD PRIMARY KEY (adid);

-- 刷新：TRUNCATE adjust_latest_by_adid; INSERT INTO adjust_latest_by_adid SELECT * FROM v_adjust_latest_by_adid;
