#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LM 贷超 application 与目标 application 按 application_no 找差集（流式 merge，不全表 JOIN）。

差约几百条时：先 --by-app 定位 app_id，再 --diff 或 --diff --app-id N。

Usage（101 内网，.env 需 LM_MYSQL_* + TARGET_MYSQL_*）:
  python3 scripts/diff_lm_application_keys.py --env ./.env --by-app
  python3 scripts/diff_lm_application_keys.py --env ./.env --diff --output /tmp/lm_app_diff.txt
  python3 scripts/diff_lm_application_keys.py --env ./.env --diff --app-id 5011
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor, SSCursor

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

EXCLUDE_APP_IDS = (567, 568, 569, 571, 572, 573)


def load_env(path: Path) -> dict:
    cfg: dict = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if " #" in line:
            line = line.split(" #", 1)[0].rstrip()
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect(cfg: dict, prefix: str, default_db: str):
    def pick(*keys: str, default: str = "") -> str:
        for key in keys:
            val = cfg.get(key)
            if val not in (None, ""):
                return val
        return default

    if prefix == "LM":
        host = pick("LM_MYSQL_HOST")
        port = pick("LM_MYSQL_PORT", default="3306")
        user = pick("LM_MYSQL_USER", "SOURCE_MYSQL_USER")
        password = pick("LM_MYSQL_PASSWORD", "SOURCE_MYSQL_PASSWORD")
        database = pick("LM_MYSQL_DATABASE", default=default_db)
    else:
        host = pick("TARGET_MYSQL_HOST", "TARGET_HOST")
        port = pick("TARGET_MYSQL_PORT", "TARGET_PORT", default="3306")
        user = pick("TARGET_MYSQL_USER", "TARGET_USER")
        password = pick("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD")
        database = pick("TARGET_MYSQL_DATABASE", "TARGET_DB", default=default_db)

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=120,
        read_timeout=86400,
        write_timeout=86400,
        autocommit=True,
    )
    tune_session(conn)
    return conn


def tune_session(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SET SESSION wait_timeout=28800, "
            "net_read_timeout=86400, net_write_timeout=86400"
        )


def exclude_ph() -> str:
    return ",".join(["%s"] * len(EXCLUDE_APP_IDS))


def stream_count_by_app(
    conn,
    label: str,
    column_sql: str,
    where: str,
    params: Sequence,
    progress_every: int,
) -> Dict[int, int]:
    """全表扫一列 appId，客户端分桶，避免 GROUP BY 大结果集断连。"""
    counts: Dict[int, int] = {}
    sql = "SELECT {0} FROM application WHERE {1}".format(column_sql, where)
    print("# {0}: start scan ...".format(label), flush=True)
    cur = conn.cursor(SSCursor)
    rows_seen = 0
    try:
        cur.execute(sql, params)
        while True:
            batch = cur.fetchmany(20000)
            if not batch:
                break
            for row in batch:
                aid = int(row[0])
                counts[aid] = counts.get(aid, 0) + 1
                rows_seen += 1
                if progress_every > 0 and rows_seen % progress_every == 0:
                    print(
                        "# {0}: scanned {1} rows, distinct_app={2}".format(
                            label, rows_seen, len(counts),
                        ),
                        flush=True,
                    )
    finally:
        cur.close()
    print(
        "# {0}: done rows={1} distinct_app={2}".format(label, rows_seen, len(counts)),
        flush=True,
    )
    return counts


def by_app_counts(cfg: dict, app_id_filter: Optional[int], progress_every: int) -> None:
    lm = connect(cfg, "LM", "ng_loan_market")
    tgt = connect(cfg, "TARGET", "ng")

    lm_where = "appId NOT IN (" + exclude_ph() + ")"
    tgt_where = "app_id NOT IN (" + exclude_ph() + ")"
    params: List = list(EXCLUDE_APP_IDS)
    if app_id_filter is not None:
        lm_where += " AND appId = %s"
        tgt_where += " AND app_id = %s"
        params = params + [app_id_filter]

    lm_map = stream_count_by_app(
        lm, "LM", "appId", lm_where, params, progress_every,
    )
    tgt_map = stream_count_by_app(
        tgt, "TARGET", "app_id", tgt_where, params, progress_every,
    )

    lm.close()
    tgt.close()

    all_apps = sorted(set(lm_map) | set(tgt_map))
    print("app_id\tlm_cnt\ttgt_cnt\tdelta(lm-tgt)")
    total_lm = total_tgt = 0
    mismatch_apps = 0
    for aid in all_apps:
        lc = lm_map.get(aid, 0)
        tc = tgt_map.get(aid, 0)
        d = lc - tc
        total_lm += lc
        total_tgt += tc
        if d != 0:
            mismatch_apps += 1
            print("{0}\t{1}\t{2}\t{3}".format(aid, lc, tc, d))
    print("# apps_with_delta={0} total_lm={1} total_tgt={2} total_delta={3}".format(
        mismatch_apps, total_lm, total_tgt, total_lm - total_tgt,
    ))


def stream_lm_keys(conn, app_id_filter: Optional[int]) -> Iterator[str]:
    where = "appId NOT IN (" + exclude_ph() + ")"
    params: list = list(EXCLUDE_APP_IDS)
    if app_id_filter is not None:
        where += " AND appId = %s"
        params.append(app_id_filter)
    sql = (
        "SELECT CONCAT('ng', LPAD(appId, 4, '0'), '-', applicationNo) AS k "
        "FROM application WHERE {0} AND applicationNo IS NOT NULL AND applicationNo <> '' "
        "ORDER BY k"
    ).format(where)
    cur = conn.cursor(SSCursor)
    try:
        cur.execute(sql, params)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            for row in rows:
                yield str(row[0])
    finally:
        cur.close()


def stream_tgt_keys(conn, app_id_filter: Optional[int]) -> Iterator[str]:
    where = "app_id NOT IN (" + exclude_ph() + ")"
    params: list = list(EXCLUDE_APP_IDS)
    if app_id_filter is not None:
        where += " AND app_id = %s"
        params.append(app_id_filter)
    sql = (
        "SELECT application_no AS k FROM application WHERE {0} "
        "AND application_no IS NOT NULL AND application_no <> '' "
        "ORDER BY k"
    ).format(where)
    cur = conn.cursor(SSCursor)
    try:
        cur.execute(sql, params)
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                break
            for row in rows:
                yield str(row[0])
    finally:
        cur.close()


def merge_diff(
    lm_iter: Iterator[str],
    tgt_iter: Iterator[str],
    out_path: Path,
    max_lines: int,
) -> Tuple[int, int, int]:
    only_lm = only_tgt = matched = 0
    lm_next = next(lm_iter, None)
    tgt_next = next(tgt_iter, None)

    with out_path.open("w", encoding="utf-8") as out:
        while lm_next is not None or tgt_next is not None:
            if lm_next is None:
                only_tgt += 1
                if only_tgt <= max_lines:
                    out.write("ONLY_TARGET\t{0}\n".format(tgt_next))
                tgt_next = next(tgt_iter, None)
                continue
            if tgt_next is None:
                only_lm += 1
                if only_lm <= max_lines:
                    out.write("ONLY_LM\t{0}\n".format(lm_next))
                lm_next = next(lm_iter, None)
                continue
            if lm_next == tgt_next:
                matched += 1
                lm_next = next(lm_iter, None)
                tgt_next = next(tgt_iter, None)
            elif lm_next < tgt_next:
                only_lm += 1
                if only_lm <= max_lines:
                    out.write("ONLY_LM\t{0}\n".format(lm_next))
                lm_next = next(lm_iter, None)
            else:
                only_tgt += 1
                if only_tgt <= max_lines:
                    out.write("ONLY_TARGET\t{0}\n".format(tgt_next))
                tgt_next = next(tgt_iter, None)

    return only_lm, only_tgt, matched


def run_diff(cfg: dict, app_id_filter: Optional[int], output: Path, max_lines: int) -> int:
    lm = connect(cfg, "LM", "ng_loan_market")
    tgt = connect(cfg, "TARGET", "ng")
    print("streaming merge diff (ORDER BY application_no)...", flush=True)
    only_lm, only_tgt, matched = merge_diff(
        stream_lm_keys(lm, app_id_filter),
        stream_tgt_keys(tgt, app_id_filter),
        output,
        max_lines,
    )
    lm.close()
    tgt.close()
    print(
        "matched={0} only_lm={1} only_target={2} (detail capped at {3} per side)".format(
            matched, only_lm, only_tgt, max_lines,
        ),
        flush=True,
    )
    print("wrote {0}".format(output), flush=True)
    return 0 if only_lm == 0 and only_tgt == 0 else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Diff LM vs target application keys")
    p.add_argument("--env", default=str(REPO / ".env"))
    p.add_argument("--by-app", action="store_true", help="按 app_id 对比 COUNT（先跑）")
    p.add_argument("--diff", action="store_true", help="流式 merge 输出 ONLY_LM / ONLY_TARGET")
    p.add_argument("--app-id", type=int, default=None, help="只对比单个 appId")
    p.add_argument("--output", default="/tmp/lm_application_key_diff.txt")
    p.add_argument("--max-lines", type=int, default=5000, help="每侧最多写多少条差集到文件")
    p.add_argument(
        "--progress-every",
        type=int,
        default=500_000,
        help="--by-app 每扫多少行打印进度（0=关闭）",
    )
    args = p.parse_args(argv)

    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: {0}".format(env_path), file=sys.stderr)
        return 2
    cfg = load_env(env_path)

    if not args.by_app and not args.diff:
        args.by_app = True

    if args.by_app:
        by_app_counts(cfg, args.app_id, args.progress_every)

    if args.diff:
        return run_diff(cfg, args.app_id, Path(args.output), args.max_lines)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
