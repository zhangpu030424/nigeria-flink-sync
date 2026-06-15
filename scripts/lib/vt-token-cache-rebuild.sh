#!/usr/bin/env bash
# vt_token_cache 重建：drop | swap | purge-drop
# 依赖: mysql_source_file, mysql_source_query（scripts/lib/mysql-source.sh）

vt_token_cache_rebuild() {
  local mode="${1:-swap}"
  case "$mode" in
    drop)
      echo ">> vt_token_cache: DROP + CREATE（小表适用）"
      mysql_source_file sql/ddl/vt_token_cache_rebuild.sql
      ;;
    swap)
      echo ">> vt_token_cache: RENAME 换表（大表推荐，秒级）"
      mysql_source_file sql/ddl/vt_token_cache_rebuild_swap.sql
      mysql_source_file sql/ddl/vt_token_cache_vt_triggers.sql
      if table_exists vt_token_cache_legacy; then
        echo ">> 旧表 vt_token_cache_legacy 可后台清理:"
        echo "   ./scripts/vt-token-cache-purge.sh --table vt_token_cache_legacy --drop-after"
      fi
      ;;
    purge-drop)
      echo ">> vt_token_cache: 分批 DELETE 后 DROP"
      if table_exists vt_token_cache; then
        bash scripts/vt-token-cache-purge.sh --table vt_token_cache --drop-after
      fi
      mysql_source_file sql/ddl/vt_token_cache.sql
      mysql_source_file sql/ddl/vt_token_cache_vt_triggers.sql
      ;;
    *)
      echo "ERR: 未知 vt rebuild 模式: $mode（drop|swap|purge-drop）" >&2
      return 1
      ;;
  esac
}
