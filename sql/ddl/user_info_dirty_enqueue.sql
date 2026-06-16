-- 脏队列入队：同 user_id 在 debounce 秒内只 bump 一次 updated_at（减少 binlog 洪水）
-- 分片：user_id % 4 → user_info_dirty_0..3（须与 USER_INFO_DIRTY_SHARDS / Flink 多路 CDC 一致）
-- 由 user_info_dirty.sql 的 TRIGGER 调用；单独执行无效

DELIMITER ;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_upsert_one;;

CREATE PROCEDURE sp_user_info_dirty_upsert_one(IN p_user_id BIGINT, IN p_debounce_sec INT)
upsert_one: BEGIN
    DECLARE v_shard INT;
    DECLARE v_sql TEXT;

    IF p_user_id IS NULL THEN
        LEAVE upsert_one;
    END IF;

    SET v_shard = p_user_id % 4;
    SET v_sql = CONCAT(
        'INSERT INTO user_info_dirty_', v_shard,
        ' (user_id, updated_at) VALUES (', p_user_id, ', CURRENT_TIMESTAMP(3)) ',
        'ON DUPLICATE KEY UPDATE updated_at = IF(',
        'updated_at <= DATE_SUB(NOW(3), INTERVAL ', p_debounce_sec, ' SECOND), ',
        'CURRENT_TIMESTAMP(3), updated_at)'
    );
    SET @sql_stmt = v_sql;
    PREPARE stmt FROM @sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue;;

CREATE PROCEDURE sp_user_info_dirty_enqueue(IN p_user_id BIGINT, IN p_debounce_sec INT)
BEGIN
    CALL sp_user_info_dirty_upsert_one(p_user_id, p_debounce_sec);
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_bvn;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_bvn(IN p_bvn VARCHAR(128), IN p_debounce_sec INT)
enqueue_bvn: BEGIN
    IF p_bvn IS NULL OR TRIM(p_bvn) = '' THEN
        LEAVE enqueue_bvn;
    END IF;

    INSERT INTO user_info_dirty_0 (user_id, updated_at)
    SELECT p.user_id, CURRENT_TIMESTAMP(3)
    FROM user_personal_info p
    WHERE p.user_id IS NOT NULL
      AND MOD(p.user_id, 4) = 0
      AND p.bvn IS NOT NULL
      AND CONVERT(TRIM(p.bvn) USING utf8mb4) COLLATE utf8mb4_bin
          = CONVERT(TRIM(p_bvn) USING utf8mb4) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_0.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_0.updated_at
        );

    INSERT INTO user_info_dirty_1 (user_id, updated_at)
    SELECT p.user_id, CURRENT_TIMESTAMP(3)
    FROM user_personal_info p
    WHERE p.user_id IS NOT NULL
      AND MOD(p.user_id, 4) = 1
      AND p.bvn IS NOT NULL
      AND CONVERT(TRIM(p.bvn) USING utf8mb4) COLLATE utf8mb4_bin
          = CONVERT(TRIM(p_bvn) USING utf8mb4) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_1.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_1.updated_at
        );

    INSERT INTO user_info_dirty_2 (user_id, updated_at)
    SELECT p.user_id, CURRENT_TIMESTAMP(3)
    FROM user_personal_info p
    WHERE p.user_id IS NOT NULL
      AND MOD(p.user_id, 4) = 2
      AND p.bvn IS NOT NULL
      AND CONVERT(TRIM(p.bvn) USING utf8mb4) COLLATE utf8mb4_bin
          = CONVERT(TRIM(p_bvn) USING utf8mb4) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_2.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_2.updated_at
        );

    INSERT INTO user_info_dirty_3 (user_id, updated_at)
    SELECT p.user_id, CURRENT_TIMESTAMP(3)
    FROM user_personal_info p
    WHERE p.user_id IS NOT NULL
      AND MOD(p.user_id, 4) = 3
      AND p.bvn IS NOT NULL
      AND CONVERT(TRIM(p.bvn) USING utf8mb4) COLLATE utf8mb4_bin
          = CONVERT(TRIM(p_bvn) USING utf8mb4) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_3.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_3.updated_at
        );
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_adid;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_adid(IN p_adid VARCHAR(128), IN p_debounce_sec INT)
enqueue_adid: BEGIN
    IF p_adid IS NULL OR TRIM(p_adid) = '' THEN
        LEAVE enqueue_adid;
    END IF;

    INSERT INTO user_info_dirty_0 (user_id, updated_at)
    SELECT u.id, CURRENT_TIMESTAMP(3)
    FROM `user` u
    WHERE u.adid IS NOT NULL
      AND TRIM(u.adid) = TRIM(p_adid)
      AND MOD(u.id, 4) = 0
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_0.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_0.updated_at
        );

    INSERT INTO user_info_dirty_1 (user_id, updated_at)
    SELECT u.id, CURRENT_TIMESTAMP(3)
    FROM `user` u
    WHERE u.adid IS NOT NULL
      AND TRIM(u.adid) = TRIM(p_adid)
      AND MOD(u.id, 4) = 1
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_1.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_1.updated_at
        );

    INSERT INTO user_info_dirty_2 (user_id, updated_at)
    SELECT u.id, CURRENT_TIMESTAMP(3)
    FROM `user` u
    WHERE u.adid IS NOT NULL
      AND TRIM(u.adid) = TRIM(p_adid)
      AND MOD(u.id, 4) = 2
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_2.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_2.updated_at
        );

    INSERT INTO user_info_dirty_3 (user_id, updated_at)
    SELECT u.id, CURRENT_TIMESTAMP(3)
    FROM `user` u
    WHERE u.adid IS NOT NULL
      AND TRIM(u.adid) = TRIM(p_adid)
      AND MOD(u.id, 4) = 3
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_3.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_3.updated_at
        );
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_emergency_mobile;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_emergency_mobile(IN p_raw VARCHAR(128), IN p_debounce_sec INT)
enqueue_emergency: BEGIN
    IF p_raw IS NULL OR TRIM(p_raw) = '' THEN
        LEAVE enqueue_emergency;
    END IF;

    INSERT INTO user_info_dirty_0 (user_id, updated_at)
    SELECT ec.user_id, CURRENT_TIMESTAMP(3)
    FROM user_emergency_contact ec
    WHERE ec.user_id IS NOT NULL
      AND MOD(ec.user_id, 4) = 0
      AND ec.contact_number IS NOT NULL
      AND TRIM(ec.contact_number) <> ''
      AND (
              CASE
                  WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                  WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
                  WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                  ELSE CONCAT('+234', TRIM(ec.contact_number))
              END
              ) COLLATE utf8mb4_bin = TRIM(p_raw) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_0.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_0.updated_at
        );

    INSERT INTO user_info_dirty_1 (user_id, updated_at)
    SELECT ec.user_id, CURRENT_TIMESTAMP(3)
    FROM user_emergency_contact ec
    WHERE ec.user_id IS NOT NULL
      AND MOD(ec.user_id, 4) = 1
      AND ec.contact_number IS NOT NULL
      AND TRIM(ec.contact_number) <> ''
      AND (
              CASE
                  WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                  WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
                  WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                  ELSE CONCAT('+234', TRIM(ec.contact_number))
              END
              ) COLLATE utf8mb4_bin = TRIM(p_raw) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_1.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_1.updated_at
        );

    INSERT INTO user_info_dirty_2 (user_id, updated_at)
    SELECT ec.user_id, CURRENT_TIMESTAMP(3)
    FROM user_emergency_contact ec
    WHERE ec.user_id IS NOT NULL
      AND MOD(ec.user_id, 4) = 2
      AND ec.contact_number IS NOT NULL
      AND TRIM(ec.contact_number) <> ''
      AND (
              CASE
                  WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                  WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
                  WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                  ELSE CONCAT('+234', TRIM(ec.contact_number))
              END
              ) COLLATE utf8mb4_bin = TRIM(p_raw) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_2.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_2.updated_at
        );

    INSERT INTO user_info_dirty_3 (user_id, updated_at)
    SELECT ec.user_id, CURRENT_TIMESTAMP(3)
    FROM user_emergency_contact ec
    WHERE ec.user_id IS NOT NULL
      AND MOD(ec.user_id, 4) = 3
      AND ec.contact_number IS NOT NULL
      AND TRIM(ec.contact_number) <> ''
      AND (
              CASE
                  WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
                  WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
                  WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
                  ELSE CONCAT('+234', TRIM(ec.contact_number))
              END
              ) COLLATE utf8mb4_bin = TRIM(p_raw) COLLATE utf8mb4_bin
    ON DUPLICATE KEY UPDATE updated_at = IF(
            user_info_dirty_3.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
            CURRENT_TIMESTAMP(3),
            user_info_dirty_3.updated_at
        );
END;;

DELIMITER ;
