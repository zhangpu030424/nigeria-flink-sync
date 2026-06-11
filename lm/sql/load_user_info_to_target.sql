-- 从老库 flink_stg_user_info_ready 生成目标库 INSERT（由 run-ng-user-info-mysql-only.sh 执行）
-- 注意: 本文件在「目标库」执行；staging 数据须先在老库 refresh 完成

INSERT INTO user_info (
    user_id, id_number, full_name, password, live_image, id_card, info, created_at, updated_at
)
SELECT
    s.user_id_part,
    LEFT(s.id_number, 28),
    LEFT(s.full_name, 255),
    LEFT(COALESCE(s.password, ''), 191),
    LEFT(COALESCE(s.live_image, ''), 191),
    LEFT(COALESCE(s.id_card, ''), 191),
    s.info,
    NOW(),
    NOW()
FROM __STAGING_DB__.flink_stg_user_info_ready s
ON DUPLICATE KEY UPDATE
    id_number   = VALUES(id_number),
    full_name   = VALUES(full_name),
    password    = VALUES(password),
    live_image  = VALUES(live_image),
    id_card     = VALUES(id_card),
    info        = VALUES(info),
    updated_at  = NOW();
