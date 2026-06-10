-- v_flink_uri_latest（需 user_registration_ip 表）
CREATE OR REPLACE VIEW v_flink_uri_latest AS
SELECT
    CAST(r.`userId` AS DECIMAL(20, 0)) AS user_id_part,
    CAST(r.`userId` AS CHAR)           AS `userId`,
    CAST(r.`ip` AS CHAR)               AS ip
FROM user_registration_ip r
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM user_registration_ip
    GROUP BY `userId`
) x ON x.max_id = r.id;
