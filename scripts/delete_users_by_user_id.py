#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 user_id 删除 target.user。

strategy=all（默认）：按 (mobile, user_id) 一条 DELETE，走分区键 mobile，一次删掉该用户在该手机号下所有行。
其他 strategy 仍按主键逐行删。

Usage（101）:
  python3 scripts/delete_users_by_user_id.py --env ./.env --ids-file scripts/del_user_ids.txt --apply
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pymysql

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "reconcile"))
import env_util  # noqa: E402

_RETRYABLE = frozenset({1205, 1213, 2006, 2013})


def parse_ids(text: str) -> List[int]:
    out: List[int] = []
    seen: Set[int] = set()
    for part in re.split(r"[\s,;]+", text.strip()):
        if not part:
            continue
        uid = int(part)
        if uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def load_ids(path: Path) -> List[int]:
    raw = path.read_text(encoding="utf-8")
    raw = raw.replace("(", " ").replace(")", " ")
    return parse_ids(raw)


def chunks(items: Sequence[Any], size: int):
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def connect(cfg: dict, read_timeout: int) -> Any:
    return pymysql.connect(
        host=cfg["TARGET_MYSQL_HOST"],
        port=int(cfg.get("TARGET_MYSQL_PORT") or 3306),
        user=cfg["TARGET_MYSQL_USER"],
        password=cfg["TARGET_MYSQL_PASSWORD"],
        database=cfg["TARGET_MYSQL_DATABASE"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=read_timeout,
        write_timeout=read_timeout,
        autocommit=True,
    )


def init_session(conn, read_timeout: int) -> None:
    with conn.cursor() as cur:
        for stmt in (
            "SET SESSION net_read_timeout = {0}".format(read_timeout),
            "SET SESSION net_write_timeout = {0}".format(read_timeout),
            "SET SESSION wait_timeout = {0}".format(max(read_timeout, 28800)),
        ):
            try:
                cur.execute(stmt)
            except pymysql.err.OperationalError:
                pass


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if isinstance(exc, pymysql.err.OperationalError) and exc.args:
        return int(exc.args[0]) in _RETRYABLE
    return isinstance(exc, OSError)


def with_retry(
    cfg: dict,
    label: str,
    read_timeout: int,
    retries: int,
    fn,
) -> Any:
    last: Optional[BaseException] = None
    for attempt in range(retries + 1):
        conn = None
        try:
            conn = connect(cfg, read_timeout)
            init_session(conn, read_timeout)
            return fn(conn)
        except Exception as exc:
            last = exc
            if attempt >= retries or not is_retryable(exc):
                raise
            wait = min(2 ** attempt, 30)
            print("[retry] {0} {1}/{2} wait={3}s err={4}".format(
                label, attempt + 1, retries, wait, exc,
            ), file=sys.stderr)
            time.sleep(wait)
        finally:
            env_util.close_conn(conn)
    if last is not None:
        raise last
    raise RuntimeError("with_retry: no result")


def fetch_pk_rows(conn, user_ids: Sequence[int]) -> List[Dict[str, Any]]:
    if not user_ids:
        return []
    ph = ",".join(["%s"] * len(user_ids))
    sql = (
        "SELECT mobile, app_id, closed_time, user_id, group_user_id "
        "FROM `user` WHERE user_id IN ({0})".format(ph)
    )
    with conn.cursor() as cur:
        cur.execute(sql, list(user_ids))
        return list(cur.fetchall())


def row_sort_key(r: Dict[str, Any]) -> Tuple[int, int, str]:
    return (int(r["closed_time"]), int(r["app_id"]), str(r["mobile"]))


def select_rows_to_delete(
    all_rows: Sequence[Dict[str, Any]],
    strategy: str,
    only_closed_time: Optional[int],
) -> List[Dict[str, Any]]:
    if only_closed_time is not None:
        return [r for r in all_rows if int(r["closed_time"]) == only_closed_time]

    if strategy == "all":
        return list(all_rows)

    if strategy == "closed-only":
        return [r for r in all_rows if int(r["closed_time"]) > 0]

    if strategy == "keep-one":
        by_uid: Dict[int, List[Dict[str, Any]]] = {}
        for r in all_rows:
            by_uid.setdefault(int(r["user_id"]), []).append(r)
        out: List[Dict[str, Any]] = []
        for _uid, rows in by_uid.items():
            if len(rows) <= 1:
                continue
            rows_sorted = sorted(rows, key=row_sort_key)
            out.extend(rows_sorted[1:])
        return out

    raise SystemExit("unknown strategy: {0}".format(strategy))


def delete_one_row(conn, row: Dict[str, Any]) -> int:
    sql = "DELETE FROM `user` WHERE mobile=%s AND app_id=%s AND closed_time=%s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (row["mobile"], row["app_id"], row["closed_time"]))
        return cur.rowcount


def mobile_user_pairs(rows: Sequence[Dict[str, Any]]) -> List[Tuple[str, int]]:
    seen: Set[Tuple[str, int]] = set()
    out: List[Tuple[str, int]] = []
    for r in rows:
        key = (str(r["mobile"]), int(r["user_id"]))
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def delete_by_mobile_user(
    conn,
    mobile: str,
    user_id: int,
    *,
    only_closed_time: Optional[int] = None,
    closed_only: bool = False,
) -> int:
    sql = "DELETE FROM `user` WHERE mobile = %s AND user_id = %s"
    params: List[Any] = [mobile, user_id]
    if only_closed_time is not None:
        sql += " AND closed_time = %s"
        params.append(only_closed_time)
    elif closed_only:
        sql += " AND closed_time > 0"
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def delete_mobile_user_with_retry(
    cfg: dict,
    pairs: Sequence[Tuple[str, int]],
    read_timeout: int,
    retries: int,
    sleep_ms: int,
    start_from: int,
    *,
    only_closed_time: Optional[int] = None,
    closed_only: bool = False,
) -> int:
    """每个 (mobile, user_id) 一条 DELETE；单连接连续执行。"""
    deleted_rows = 0
    idx = max(0, start_from - 1)
    total = len(pairs)

    if idx > 0:
        print("resume from pair {0}/{1}".format(idx + 1, total), file=sys.stderr)

    while idx < total:
        attempt = 0
        while idx < total:
            conn = None
            try:
                conn = connect(cfg, read_timeout)
                init_session(conn, read_timeout)
                while idx < total:
                    mobile, user_id = pairs[idx]
                    n = delete_by_mobile_user(
                        conn, mobile, user_id,
                        only_closed_time=only_closed_time,
                        closed_only=closed_only,
                    )
                    deleted_rows += n
                    idx += 1
                    if idx <= 5 or idx % 50 == 0 or idx == total:
                        print(
                            "  [{0}/{1}] mobile={2} user_id={3} -> {4} row(s)".format(
                                idx, total, mobile, user_id, n,
                            ),
                            file=sys.stderr,
                        )
                    if sleep_ms > 0:
                        time.sleep(sleep_ms / 1000.0)
                return deleted_rows
            except Exception as exc:
                env_util.close_conn(conn)
                conn = None
                if not is_retryable(exc) or attempt >= retries:
                    print(
                        "abort at pair {0}/{1}, use --start-from {0} to resume".format(
                            idx + 1, total,
                        ),
                        file=sys.stderr,
                    )
                    raise
                attempt += 1
                wait = min(2 ** attempt, 30)
                print(
                    "[retry] pair {0}/{1} attempt={2}/{3} wait={4}s err={5}".format(
                        idx + 1, total, attempt, retries, wait, exc,
                    ),
                    file=sys.stderr,
                )
                time.sleep(wait)
            finally:
                env_util.close_conn(conn)

    return deleted_rows


def delete_rows_with_retry(
    cfg: dict,
    rows: Sequence[Dict[str, Any]],
    read_timeout: int,
    retries: int,
    sleep_ms: int,
    start_from: int,
) -> int:
    """单连接连续删；断连后从当前行重连续删（已删行 autocommit 不会重复）。"""
    deleted = 0
    # start_from: 1-based，表示从第几条开始（便于断点续删）
    idx = max(0, start_from - 1)
    total = len(rows)

    if idx > 0:
        print("resume from row {0}/{1}".format(idx + 1, total), file=sys.stderr)

    while idx < total:
        attempt = 0
        while idx < total:
            conn = None
            try:
                conn = connect(cfg, read_timeout)
                init_session(conn, read_timeout)
                while idx < total:
                    row = rows[idx]
                    n = delete_one_row(conn, row)
                    deleted += n
                    idx += 1
                    if idx <= 5 or idx % 50 == 0 or idx == total:
                        print(
                            "  deleted {0}/{1} uid={2} pk=({3},{4},{5})".format(
                                idx, total, row["user_id"],
                                row["mobile"], row["app_id"], row["closed_time"],
                            ),
                            file=sys.stderr,
                        )
                    if sleep_ms > 0:
                        time.sleep(sleep_ms / 1000.0)
                return deleted
            except Exception as exc:
                env_util.close_conn(conn)
                conn = None
                if not is_retryable(exc) or attempt >= retries:
                    print(
                        "abort at row {0}/{1}, use --start-from {0} to resume".format(
                            idx + 1, total,
                        ),
                        file=sys.stderr,
                    )
                    raise
                attempt += 1
                wait = min(2 ** attempt, 30)
                print(
                    "[retry] resume row {0}/{1} attempt={2}/{3} wait={4}s err={5}".format(
                        idx + 1, total, attempt, retries, wait, exc,
                    ),
                    file=sys.stderr,
                )
                time.sleep(wait)
            finally:
                env_util.close_conn(conn)

    return deleted


def main() -> int:
    p = argparse.ArgumentParser(description="Batch delete target.user by user_id via PK")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--ids-file", help="file with user_id list")
    p.add_argument("--user-ids", help="comma-separated user_ids")
    p.add_argument("--apply", action="store_true", help="execute delete (default dry-run)")
    p.add_argument(
        "--strategy",
        choices=("all", "keep-one", "closed-only"),
        default="all",
        help="all=删该 user_id 全部行(默认); keep-one=每 user_id 留 1 行; closed-only=只删 closed_time>0",
    )
    p.add_argument(
        "--only-closed-time",
        type=int,
        default=None,
        help="只删指定 closed_time，如 1（覆盖 strategy）",
    )
    p.add_argument("--lookup-batch", type=int, default=30, help="user_id IN 查询批次")
    p.add_argument("--sleep-ms", type=int, default=10, help="每条 DELETE 间隔毫秒")
    p.add_argument("--retries", type=int, default=8, help="断连重试次数")
    p.add_argument("--query-timeout", type=int, default=600, help="MySQL 读写超时秒")
    p.add_argument(
        "--start-from",
        type=int,
        default=1,
        help="从第几个 (mobile,user_id) 开始删（1-based；已删过的重跑会自动跳过）",
    )
    args = p.parse_args()

    if args.ids_file:
        ids = load_ids(Path(args.ids_file))
    elif args.user_ids:
        ids = parse_ids(args.user_ids)
    else:
        print("need --ids-file or --user-ids", file=sys.stderr)
        return 2

    if not ids:
        print("no user_id parsed", file=sys.stderr)
        return 2

    cfg = env_util.load_env(Path(args.env))

    all_rows: List[Dict[str, Any]] = []
    print("lookup {0} distinct user_id(s)...".format(len(ids)), file=sys.stderr)
    for batch_no, batch in enumerate(chunks(ids, args.lookup_batch), 1):
        def _fetch(conn) -> List[Dict[str, Any]]:
            return fetch_pk_rows(conn, batch)

        found = with_retry(
            cfg, "lookup-{0}".format(batch_no), args.query_timeout, args.retries, _fetch,
        )
        all_rows.extend(found)
        print("  batch {0}: +{1} rows, total={2}".format(
            batch_no, len(found), len(all_rows),
        ), file=sys.stderr)

    if not all_rows:
        print("no matching rows in user table", file=sys.stderr)
        return 0

    by_uid: Dict[int, int] = {}
    for r in all_rows:
        uid = int(r["user_id"])
        by_uid[uid] = by_uid.get(uid, 0) + 1

    print(
        "lookup: {0} row(s), {1} user_id(s) hit, {2} id(s) not in table".format(
            len(all_rows), len(by_uid), len(ids) - len(by_uid),
        ),
        file=sys.stderr,
    )
    multi = [(u, c) for u, c in by_uid.items() if c > 1]
    if multi:
        print("duplicate user_id (U1): {0} ids, e.g. {1}".format(len(multi), multi[:5]), file=sys.stderr)

    to_delete = select_rows_to_delete(all_rows, args.strategy, args.only_closed_time)

    use_mobile = args.strategy == "all" and args.only_closed_time is None
    if use_mobile:
        pairs = mobile_user_pairs(all_rows)
        est_rows = len(all_rows)
        print(
            "strategy=all -> {0} DELETE by (mobile, user_id), ~{1} row(s)".format(
                len(pairs), est_rows,
            ),
            file=sys.stderr,
        )
        print("delete plan sample (first 10):", file=sys.stderr)
        for mobile, uid in pairs[:10]:
            print("  DEL mobile={0} user_id={1}".format(mobile, uid), file=sys.stderr)
        if not args.apply:
            print("re-run with --apply to execute", file=sys.stderr)
            return 0
        deleted = delete_mobile_user_with_retry(
            cfg, pairs, args.query_timeout, args.retries, args.sleep_ms, args.start_from,
        )
        print("done deleted_rows={0}".format(deleted), file=sys.stderr)
        return 0

    print(
        "strategy={0} only_closed_time={1} -> will delete {2} row(s), keep {3}".format(
            args.strategy,
            args.only_closed_time,
            len(to_delete),
            len(all_rows) - len(to_delete),
        ),
        file=sys.stderr,
    )

    if not to_delete:
        print("nothing to delete", file=sys.stderr)
        return 0

    print("delete plan sample (first 10):", file=sys.stderr)
    for r in to_delete[:10]:
        print(
            "  DEL uid={user_id} pk=({mobile},{app_id},{closed_time})".format(**r),
            file=sys.stderr,
        )

    if not args.apply:
        print("re-run with --apply to execute", file=sys.stderr)
        return 0

    if args.strategy == "closed-only" or args.only_closed_time is not None:
        pairs = mobile_user_pairs(to_delete)
        deleted = delete_mobile_user_with_retry(
            cfg,
            pairs,
            args.query_timeout,
            args.retries,
            args.sleep_ms,
            args.start_from,
            only_closed_time=args.only_closed_time,
            closed_only=(args.strategy == "closed-only" and args.only_closed_time is None),
        )
    else:
        deleted = delete_rows_with_retry(
            cfg, to_delete, args.query_timeout, args.retries, args.sleep_ms, args.start_from,
        )
    print("done deleted_rows={0}".format(deleted), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
