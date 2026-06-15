-- 脏队列入队：同 user_id 在 debounce 秒内只 bump 一次 updated_at（减少 binlog 洪水）
-- 由 user_info_dirty.sql 的 TRIGGER 调用；单独执行无效

DELIMITER ;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue;;

CREATE PROCEDURE sp_user_info_dirty_enqueue(IN p_user_id BIGINT, IN p_debounce_sec INT)
BEGIN
    IF p_user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id, updated_at)
        VALUES (p_user_id, CURRENT_TIMESTAMP(3))
        ON DUPLICATE KEY UPDATE updated_at = IF(
                updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
                CURRENT_TIMESTAMP(3),
                updated_at
            );
    END IF;
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_bvn;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_bvn(IN p_bvn VARCHAR(128), IN p_debounce_sec INT)
BEGIN
    IF p_bvn IS NOT NULL AND TRIM(p_bvn) <> '' THEN
        INSERT INTO user_info_dirty (user_id, updated_at)
        SELECT p.user_id, CURRENT_TIMESTAMP(3)
        FROM user_personal_info p
        WHERE p.user_id IS NOT NULL
          AND p.bvn IS NOT NULL
          AND CONVERT(TRIM(p.bvn) USING utf8mb4) COLLATE utf8mb4_bin
              = CONVERT(TRIM(p_bvn) USING utf8mb4) COLLATE utf8mb4_bin
        ON DUPLICATE KEY UPDATE updated_at = IF(
                user_info_dirty.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
                CURRENT_TIMESTAMP(3),
                user_info_dirty.updated_at
            );
    END IF;
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_adid;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_adid(IN p_adid VARCHAR(128), IN p_debounce_sec INT)
BEGIN
    IF p_adid IS NOT NULL AND TRIM(p_adid) <> '' THEN
        INSERT INTO user_info_dirty (user_id, updated_at)
        SELECT u.id, CURRENT_TIMESTAMP(3)
        FROM `user` u
        WHERE u.adid IS NOT NULL
          AND TRIM(u.adid) = TRIM(p_adid)
        ON DUPLICATE KEY UPDATE updated_at = IF(
                user_info_dirty.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
                CURRENT_TIMESTAMP(3),
                user_info_dirty.updated_at
            );
    END IF;
END;;

DROP PROCEDURE IF EXISTS sp_user_info_dirty_enqueue_emergency_mobile;;

CREATE PROCEDURE sp_user_info_dirty_enqueue_emergency_mobile(IN p_raw VARCHAR(128), IN p_debounce_sec INT)
BEGIN
    IF p_raw IS NOT NULL AND TRIM(p_raw) <> '' THEN
        INSERT INTO user_info_dirty (user_id, updated_at)
        SELECT ec.user_id, CURRENT_TIMESTAMP(3)
        FROM user_emergency_contact ec
        WHERE ec.user_id IS NOT NULL
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
                user_info_dirty.updated_at <= DATE_SUB(NOW(3), INTERVAL p_debounce_sec SECOND),
                CURRENT_TIMESTAMP(3),
                user_info_dirty.updated_at
            );
    END IF;
END;;

DELIMITER ;
