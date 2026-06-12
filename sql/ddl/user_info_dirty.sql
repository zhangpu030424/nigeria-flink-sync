-- user_info 增量脏队列：源表 TRIGGER 写入 user_id，Flink 单路 CDC 此表即可
-- 部署: ./scripts/deploy-source-ddl.sh（须源库 TRIGGER 权限；无权限时请 DBA 用 root 执行本文件）
-- 回滚: 先 DROP TRIGGER，再 DROP TABLE user_info_dirty

CREATE TABLE IF NOT EXISTS user_info_dirty (
    user_id     BIGINT        NOT NULL COMMENT '源库 user.id',
    updated_at  TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'user_info 增量触发队列（Flink CDC 单表）';

DROP TRIGGER IF EXISTS trg_user_info_dirty_user_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_user_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_personal_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_personal_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_work_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_work_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_emergency_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_emergency_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_credit_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_credit_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_au;
DROP TRIGGER IF EXISTS trg_user_info_dirty_adjust_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_adjust_au;

DELIMITER ;;

CREATE TRIGGER trg_user_info_dirty_user_ai
    AFTER INSERT ON `user`
    FOR EACH ROW
BEGIN
    IF NEW.id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_user_au
    AFTER UPDATE ON `user`
    FOR EACH ROW
BEGIN
    IF NEW.id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_personal_ai
    AFTER INSERT ON user_personal_info
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_personal_au
    AFTER UPDATE ON user_personal_info
    FOR EACH ROW
BEGIN
    IF OLD.user_id IS NOT NULL AND (NEW.user_id IS NULL OR OLD.user_id <> NEW.user_id) THEN
        INSERT INTO user_info_dirty (user_id) VALUES (OLD.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_work_ai
    AFTER INSERT ON user_work_related
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_work_au
    AFTER UPDATE ON user_work_related
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_emergency_ai
    AFTER INSERT ON user_emergency_contact
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_emergency_au
    AFTER UPDATE ON user_emergency_contact
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_credit_ai
    AFTER INSERT ON risk_user_credit_callback
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_credit_au
    AFTER UPDATE ON risk_user_credit_callback
    FOR EACH ROW
BEGIN
    IF NEW.user_id IS NOT NULL THEN
        INSERT INTO user_info_dirty (user_id) VALUES (NEW.user_id)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_vt_ai
    AFTER INSERT ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 'id_number'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        INSERT INTO user_info_dirty (user_id)
        SELECT p.user_id
        FROM user_personal_info p
        WHERE p.user_id IS NOT NULL
          AND p.bvn IS NOT NULL
          AND TRIM(p.bvn) COLLATE utf8mb4_bin = TRIM(NEW.raw_value) COLLATE utf8mb4_bin
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_vt_au
    AFTER UPDATE ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 'id_number'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        INSERT INTO user_info_dirty (user_id)
        SELECT p.user_id
        FROM user_personal_info p
        WHERE p.user_id IS NOT NULL
          AND p.bvn IS NOT NULL
          AND TRIM(p.bvn) COLLATE utf8mb4_bin = TRIM(NEW.raw_value) COLLATE utf8mb4_bin
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_adjust_ai
    AFTER INSERT ON adjust_callback_record
    FOR EACH ROW
BEGIN
    IF NEW.adid IS NOT NULL AND TRIM(NEW.adid) <> '' THEN
        INSERT INTO user_info_dirty (user_id)
        SELECT u.id
        FROM `user` u
        WHERE u.adid IS NOT NULL
          AND TRIM(u.adid) = TRIM(NEW.adid)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_adjust_au
    AFTER UPDATE ON adjust_callback_record
    FOR EACH ROW
BEGIN
    IF NEW.adid IS NOT NULL AND TRIM(NEW.adid) <> '' THEN
        INSERT INTO user_info_dirty (user_id)
        SELECT u.id
        FROM `user` u
        WHERE u.adid IS NOT NULL
          AND TRIM(u.adid) = TRIM(NEW.adid)
        ON DUPLICATE KEY UPDATE updated_at = CURRENT_TIMESTAMP(3);
    END IF;
END;;

DELIMITER ;
