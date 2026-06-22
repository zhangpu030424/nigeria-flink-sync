-- vt_token_cache 上的 user_info 脏队列入队 TRIGGER（重建/换表后须重新执行）
-- mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_token_cache_vt_triggers.sql

DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_ai;
DROP TRIGGER IF EXISTS trg_user_info_dirty_vt_au;

DELIMITER ;;

CREATE TRIGGER trg_user_info_dirty_vt_ai
    AFTER INSERT ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 4
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_bvn(NEW.raw_value, 60);
    ELSEIF NEW.vt_type = 5
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_emergency_mobile(NEW.raw_value, 60);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_vt_au
    AFTER UPDATE ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 4
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_bvn(NEW.raw_value, 60);
    ELSEIF NEW.vt_type = 5
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_emergency_mobile(NEW.raw_value, 60);
    END IF;
END;;

DELIMITER ;

SELECT 'vt_token_cache VT triggers 已就绪' AS msg;
