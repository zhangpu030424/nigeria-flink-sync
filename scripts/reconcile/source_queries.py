#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从源库业务表拉期望行（对齐宽表 SELECT 逻辑，但不读 *_sync_staging）。"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import env_util

SQL_DIR = Path(__file__).resolve().parent / "sql"

_TABLE_META = {
    "user": {
        "file": "user_source.sql",
        "id_col": "id",
        "app_col": "app_code",
        "since_col": "reg_time",
        "since_kind": "ms",
        "needs_user_join": False,
    },
    "user_bankcard": {
        "file": "user_bankcard_source.sql",
        "id_col": "id",
        "app_col": None,
        "since_col": None,
        "since_kind": None,
        "needs_user_join": True,
        "user_id_col": "user_id",
    },
    "user_info": {
        "file": "user_info_source.sql",
        "id_col": "user_id",
        "app_col": None,
        "since_col": None,
        "since_kind": None,
        "needs_user_join": True,
        "user_id_col": "user_id",
    },
    "user_product": {
        "file": "user_product_source.sql",
        "id_col": "user_id",
        "product_col": "product_id",
        "app_col": None,
        "since_col": None,
        "since_kind": None,
        "needs_user_join": True,
        "user_id_col": "user_id",
        "composite_page": True,
    },
    "application": {
        "file": "application_source.sql",
        "id_col": "id",
        "app_col": "app_code",
        "since_col": "order_time",
        "since_kind": "datetime",
        "needs_user_join": False,
    },
    "loan": {
        "file": "loan_source.sql",
        "id_col": "id",
        "app_col": None,
        "since_col": "created_time_ms",
        "since_kind": "ms",
        "needs_user_join": False,
        "filter_app_no_prefix": True,
    },
}


def _load_base_sql(table: str) -> str:
    meta = _TABLE_META[table]
    path = SQL_DIR / meta["file"]
    if not path.is_file():
        raise FileNotFoundError("missing source sql: {0}".format(path))
    return path.read_text(encoding="utf-8").strip().rstrip(";")


def _app_placeholders(n: int) -> str:
    return ",".join(["%s"] * n)


def _app_no_prefix_clause(alias: str, include_apps: Sequence[int]) -> Tuple[str, List[Any]]:
    if not include_apps:
        return "1=1", []
    parts = []
    params: List[Any] = []
    for aid in include_apps:
        parts.append("`{0}`.`application_no` LIKE %s".format(alias))
        params.append("ng{0:04d}-%".format(int(aid)))
    return "(" + " OR ".join(parts) + ")", params


def iter_source_rows(
    cfg: Dict[str, Any],
    table: str,
    include_apps: Tuple[int, ...],
    since_ms: Optional[int],
    batch_size: int,
) -> Iterable[dict]:
    """按源表 SELECT（与 staging 同源 JOIN/映射，直接查业务表）分页产出行。"""
    if table not in _TABLE_META:
        raise ValueError("unsupported table: {0}".format(table))
    meta = _TABLE_META[table]
    base = _load_base_sql(table)
    id_col = meta["id_col"]
    batch_size = max(1, int(batch_size))
    alias = "s"

    where: List[str] = []
    params: List[Any] = []

    from_sql = "({0}) `{1}`".format(base, alias)
    if meta.get("needs_user_join"):
        uid_col = meta["user_id_col"]
        from_sql = (
            "({0}) `{1}` "
            "INNER JOIN `user` `_u` ON `_u`.`id` = `{1}`.`{2}`"
        ).format(base, alias, uid_col)
        if include_apps:
            where.append("`_u`.`app_code` IN ({0})".format(_app_placeholders(len(include_apps))))
            params.extend(include_apps)
    elif meta.get("app_col") and include_apps:
        where.append("`{0}`.`{1}` IN ({2})".format(
            alias, meta["app_col"], _app_placeholders(len(include_apps)),
        ))
        params.extend(include_apps)
    elif meta.get("filter_app_no_prefix") and include_apps:
        clause, p = _app_no_prefix_clause(alias, include_apps)
        where.append(clause)
        params.extend(p)

    since_col = meta.get("since_col")
    since_kind = meta.get("since_kind")
    if since_col and since_ms is not None:
        if since_kind == "ms":
            where.append("`{0}`.`{1}` >= %s".format(alias, since_col))
            params.append(int(since_ms))
        elif since_kind == "datetime":
            where.append("`{0}`.`{1}` >= FROM_UNIXTIME(%s)".format(alias, since_col))
            params.append(int(since_ms) // 1000)

    base_where = (" WHERE " + " AND ".join(where)) if where else ""
    composite = bool(meta.get("composite_page"))
    last_id = 0
    last_product = ""

    conn = env_util.connect_source(cfg)
    try:
        with conn.cursor() as cur:
            while True:
                page_params = list(params)
                if composite:
                    prod_col = meta["product_col"]
                    id_pred = (
                        "((`{a}`.`{idc}` > %s) OR "
                        "(`{a}`.`{idc}` = %s AND `{a}`.`{pc}` > %s))"
                    ).format(a=alias, idc=id_col, pc=prod_col)
                    page_params.extend([last_id, last_id, last_product])
                    order = "`{a}`.`{idc}` ASC, `{a}`.`{pc}` ASC".format(
                        a=alias, idc=id_col, pc=prod_col,
                    )
                else:
                    id_pred = "`{0}`.`{1}` > %s".format(alias, id_col)
                    page_params.append(last_id)
                    order = "`{0}`.`{1}` ASC".format(alias, id_col)

                if base_where:
                    page_where = base_where + " AND " + id_pred
                else:
                    page_where = " WHERE " + id_pred

                sql = (
                    "SELECT `{a}`.* FROM {frm}{wh} ORDER BY {ord} LIMIT %s"
                ).format(a=alias, frm=from_sql, wh=page_where, ord=order)
                cur.execute(sql, tuple(page_params) + (batch_size,))
                batch = cur.fetchall()
                if not batch:
                    break
                for r in batch:
                    row = dict(r)
                    last_id = int(row[id_col])
                    if composite:
                        last_product = str(row.get(meta["product_col"]) or "")
                    yield row
                if len(batch) < batch_size:
                    break
    finally:
        env_util.close_conn(conn)
