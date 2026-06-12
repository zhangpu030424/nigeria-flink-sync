-- user_info 增量脏队列：源表 TRIGGER 写入 user_id，Flink 单路 CDC 此表即可
-- 部署: ./scripts/deploy-source-ddl.sh（须源库 TRIGGER + PROCEDURE 权限；无权限时请 DBA 用 root 执行）
-- debounce: 同 user 在 N 秒内多次变更只产生一条 binlog（Lookup 仍取最新宽表）
-- 回滚: 先 DROP TRIGGER，再 DROP PROCEDURE，再 DROP TABLE user_info_dirty

CREATE TABLE IF NOT EXISTS user_info_dirty (
    user_id     BIGINT        NOT NULL COMMENT '源库 user.id',
    updated_at  TIMESTAMP(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4
  COMMENT = 'user_info 增量触发队列（Flink CDC 单表）';

-- 存储过程见 sql/ddl/user_info_dirty_enqueue.sql（deploy-source-ddl.sh 会先执行）

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
    CALL sp_user_info_dirty_enqueue(NEW.id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_user_au
    AFTER UPDATE ON `user`
    FOR EACH ROW
BEGIN
    IF NEW.id IS NOT NULL AND (
        NOT (OLD.app_code <=> NEW.app_code)
            OR NOT (OLD.device_id <=> NEW.device_id)
            OR NOT (OLD.adid <=> NEW.adid)
            OR NOT (OLD.create_time <=> NEW.create_time)
        ) THEN
        CALL sp_user_info_dirty_enqueue(NEW.id, 10);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_personal_ai
    AFTER INSERT ON user_personal_info
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_personal_au
    AFTER UPDATE ON user_personal_info
    FOR EACH ROW
BEGIN
    IF OLD.user_id IS NOT NULL AND (NEW.user_id IS NULL OR OLD.user_id <> NEW.user_id) THEN
        CALL sp_user_info_dirty_enqueue(OLD.user_id, 10);
    END IF;
    IF NEW.user_id IS NOT NULL THEN
        CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_work_ai
    AFTER INSERT ON user_work_related
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_work_au
    AFTER UPDATE ON user_work_related
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_emergency_ai
    AFTER INSERT ON user_emergency_contact
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_emergency_au
    AFTER UPDATE ON user_emergency_contact
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 10);
END;;

CREATE TRIGGER trg_user_info_dirty_credit_ai
    AFTER INSERT ON risk_user_credit_callback
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 30);
END;;

CREATE TRIGGER trg_user_info_dirty_credit_au
    AFTER UPDATE ON risk_user_credit_callback
    FOR EACH ROW
BEGIN
    CALL sp_user_info_dirty_enqueue(NEW.user_id, 30);
END;;

CREATE TRIGGER trg_user_info_dirty_vt_ai
    AFTER INSERT ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 'id_number'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_bvn(NEW.raw_value, 60);
    ELSEIF NEW.vt_type = 'emergency_contact'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_emergency_mobile(NEW.raw_value, 60);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_vt_au
    AFTER UPDATE ON vt_token_cache
    FOR EACH ROW
BEGIN
    IF NEW.vt_type = 'id_number'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_bvn(NEW.raw_value, 60);
    ELSEIF NEW.vt_type = 'emergency_contact'
        AND NEW.raw_value IS NOT NULL
        AND TRIM(NEW.raw_value) <> '' THEN
        CALL sp_user_info_dirty_enqueue_emergency_mobile(NEW.raw_value, 60);
    END IF;
END;;

-- adjust 回调量极大：仅 install_source 相关字段变化时入队，且 60s debounce
CREATE TRIGGER trg_user_info_dirty_adjust_ai
    AFTER INSERT ON adjust_callback_record
    FOR EACH ROW
BEGIN
    IF NEW.adid IS NOT NULL AND TRIM(NEW.adid) <> '' THEN
        CALL sp_user_info_dirty_enqueue_adid(NEW.adid, 60);
    END IF;
END;;

CREATE TRIGGER trg_user_info_dirty_adjust_au
    AFTER UPDATE ON adjust_callback_record
    FOR EACH ROW
BEGIN
    IF NEW.adid IS NOT NULL AND TRIM(NEW.adid) <> ''
        AND NOT (OLD.tracker_name <=> NEW.tracker_name) THEN
        CALL sp_user_info_dirty_enqueue_adid(NEW.adid, 60);
    END IF;
END;;

DELIMITER ;
