-- vt-reconcile 对账查询加速索引
-- 查询形态: WHERE vt_type=? AND status=1 AND id>? ORDER BY id LIMIT N
-- 在源库执行（ONLINE DDL，大表请低峰）:
--   mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_token_cache_reconcile_index.sql
-- 脚本侧:
--   export VT_RECONCILE_INDEX=idx_reconcile
--   pip install pymysql   # 可选
--   ./vt-reconcile.sh mobile --skip-count --db-shards 4
-- 验证索引是否生效:
--   SHOW INDEX FROM vt_token_cache WHERE Key_name='idx_reconcile';
--   EXPLAIN SELECT id, token FROM vt_token_cache FORCE INDEX (idx_reconcile)
--     WHERE vt_type='mobile' AND status=1 AND id>0 ORDER BY id LIMIT 10000;

ALTER TABLE vt_token_cache
    ADD INDEX idx_reconcile (vt_type, status, id),
    ALGORITHM=INPLACE, LOCK=NONE;

SELECT 'idx_reconcile 已创建；对账脚本设 VT_RECONCILE_INDEX=idx_reconcile' AS msg;
