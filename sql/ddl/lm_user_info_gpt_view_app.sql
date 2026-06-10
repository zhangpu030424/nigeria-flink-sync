-- v_flink_mkt_app（需 app 表）
CREATE OR REPLACE VIEW v_flink_mkt_app AS
SELECT
    CAST(id AS CHAR)     AS id,
    CAST(`name` AS CHAR) AS `name`
FROM `app`;
