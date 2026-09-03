#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 user_id 清理 target.user 重复行（U1）。

典型场景：同一 user_id 占多条 (mobile, app_id, closed_time)，留 1 删其余。
  SELECT user_id, COUNT(*) FROM user GROUP BY user_id HAVING COUNT(*) > 1

默认 --strategy keep-one：每 user_id 保留 closed_time 最小的一行（通常 closed_time=0）。

Usage:
  # 自动从库内查重复 user_id，dry-run
  python3 scripts/delete_users_by_user_id.py --env ./.env --from-duplicates

  # 执行删除（先停 165 Flink user 增量）
  python3 scripts/delete_users_by_user_id.py --env ./.env --from-duplicates --via-mysql --apply

  # 或沿用 ids 文件
  python3 scripts/delete_users_by_user_id.py --env ./.env --ids-file scripts/del_user_ids.txt --strategy keep-one --apply
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pymysql
from pymysql.converters import escape_string

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "reconcile"))
import env_util  # noqa: E402

_MYSQL_RETRY_MARKERS = ("2013", "2006", "1205", "Lost connection", "Lock wait timeout")
_RETRYABLE = frozenset({1205, 1213, 2003, 2006, 2013, 2014})
_MYSQL_INIT = (
    "SET SESSION net_read_timeout=7200, net_write_timeout=7200, "
    "wait_timeout=7200, interactive_timeout=7200, innodb_lock_wait_timeout=120"
)


def sql_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        return str(int(val) if isinstance(val, float) and val.is_integer() else val)
    return "'{0}'".format(escape_string(str(val)))


def delete_sql(row: Dict[str, Any]) -> str:
    return (
        "DELETE FROM `user` WHERE mobile={mobile} AND app_id={app_id} "
        "AND closed_time={closed_time} LIMIT 1;".format(
            mobile=sql_literal(row["mobile"]),
            app_id=int(row["app_id"]),
            closed_time=int(row["closed_time"]),
        )
    )


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
        connect_timeout=60,
        read_timeout=read_timeout,
        write_timeout=read_timeout,
        autocommit=True,
    )


def mysql_cmd_base(cfg: dict) -> List[str]:
    return [
        "mysql",
        "-h", str(cfg["TARGET_MYSQL_HOST"]),
        "-P", str(int(cfg.get("TARGET_MYSQL_PORT") or 3306)),
        "-u", str(cfg["TARGET_MYSQL_USER"]),
        str(cfg["TARGET_MYSQL_DATABASE"]),
        "--connect-timeout=60",
        "--init-command", _MYSQL_INIT,
    ]


def mysql_env(cfg: dict) -> dict:
    env = os.environ.copy()
    env["MYSQL_PWD"] = str(cfg["TARGET_MYSQL_PASSWORD"])
    return env


def mysql_query(cfg: dict, sql: str, *, retries: int = 3) -> str:
    cmd = mysql_cmd_base(cfg) + ["-N", "-B", "-e", sql]
    last_err = ""
    for attempt in range(retries + 1):
        proc = subprocess.run(
            cmd, env=mysql_env(cfg), capture_output=True, text=True,
        )
        if proc.returncode == 0:
            return proc.stdout
        last_err = (proc.stderr or proc.stdout or "").strip()
        if attempt >= retries or not any(m in last_err for m in _MYSQL_RETRY_MARKERS):
            raise RuntimeError("mysql failed: {0}".format(last_err))
        time.sleep(min(2 ** attempt, 30))
    raise RuntimeError("mysql failed: {0}".format(last_err))


def print_2013_hint() -> None:
    print(
        "\n2013 on DELETE (SELECT ok, read_only=0) usually means:\n"
        "  1) Flink user incr JDBC sink holds row locks -> cancel sink_user Job on 165 first\n"
        "  2) vedbm proxy kills connection while waiting for lock\n"
        "Try: bash scripts/cancel-flink-jobs.sh --yes   # or cancel only user incr\n"
        "Then: python3 scripts/delete_users_by_user_id.py ... --diagnose-locks --probe\n",
        file=sys.stderr,
    )


def diagnose_locks(cfg: dict) -> None:
    db = str(cfg["TARGET_MYSQL_DATABASE"])
    print("== SHOW FULL PROCESSLIST ==" , file=sys.stderr)
    print(
        "note: ng-export 无 PROCESS 权限时只能看到自己的连接，看不到 Flink 占锁会话",
        file=sys.stderr,
    )
    try:
        out = mysql_query(cfg, "SHOW FULL PROCESSLIST")
        lines = [ln for ln in out.strip().splitlines() if ln.strip()]
        shown = 0
        for ln in lines:
            if "\tQuery\t" in ln or "\tExecute\t" in ln or "DELETE" in ln or "INSERT" in ln or "UPDATE" in ln:
                print(ln, file=sys.stderr)
                shown += 1
        if shown == 0:
            print("(no Query/Execute sessions visible to current user)", file=sys.stderr)
    except Exception as exc:
        print("processlist: {0}".format(exc), file=sys.stderr)

    print("== triggers on `{0}.user` ==".format(db), file=sys.stderr)
    try:
        out = mysql_query(cfg, "SHOW TRIGGERS FROM `{0}` LIKE 'user'".format(db))
        print(out.strip() or "(none)", file=sys.stderr)
    except Exception as exc:
        print("triggers: {0}".format(exc), file=sys.stderr)

    print("== innodb trx ==" , file=sys.stderr)
    try:
        out = mysql_query(
            cfg,
            "SELECT trx_id, trx_state, trx_started, trx_mysql_thread_id, "
            "trx_rows_locked, trx_rows_modified, LEFT(trx_query, 160) "
            "FROM information_schema.innodb_trx ORDER BY trx_started LIMIT 15",
        )
        print(out.strip() or "(empty)", file=sys.stderr)
    except Exception as exc:
        print("innodb_trx: {0}".format(exc), file=sys.stderr)

    print("== grants ==" , file=sys.stderr)
    try:
        out = mysql_query(cfg, "SHOW GRANTS FOR CURRENT_USER()")
        print(out, file=sys.stderr)
    except Exception as exc:
        print("grants: {0}".format(exc), file=sys.stderr)


def check_target_writable(cfg: dict) -> None:
    out = mysql_query(
        cfg,
        "SELECT @@hostname, @@port, @@read_only, @@innodb_read_only, @@super_read_only",
    )
    print("target: {0}".format(out.replace("\t", " ")), file=sys.stderr)
    parts = out.strip().split("\t")
    if len(parts) >= 3 and any(p == "1" for p in parts[2:]):
        raise SystemExit(
            "target looks read-only (read_only=1); use writer endpoint, not replica",
        )


def probe_first_row(cfg: dict, row: Dict[str, Any], read_timeout: int) -> None:
    print("probe row uid={0} pk=({1},{2},{3})".format(
        row["user_id"], row["mobile"], row["app_id"], row["closed_time"],
    ), file=sys.stderr)
    check_target_writable(cfg)
    diagnose_locks(cfg)

    conn = connect(cfg, read_timeout)
    try:
        init_session(conn, read_timeout)
        with conn.cursor() as cur:
            cur.execute("SELECT @@read_only, @@innodb_read_only")
            ro = cur.fetchone()
            print("pymysql session read_only={0}".format(ro), file=sys.stderr)

            t0 = time.time()
            cur.execute(
                "SELECT user_id FROM `user` WHERE mobile=%s AND app_id=%s AND closed_time=%s",
                (row["mobile"], row["app_id"], row["closed_time"]),
            )
            found = cur.fetchone()
            print("select pk: {0:.3f}s found={1}".format(time.time() - t0, bool(found)), file=sys.stderr)

            cur.execute("START TRANSACTION")
            try:
                t1 = time.time()
                cur.execute(
                    "DELETE FROM `user` WHERE mobile=%s AND app_id=%s AND closed_time=%s LIMIT 1",
                    (row["mobile"], row["app_id"], row["closed_time"]),
                )
                rc = cur.rowcount
                print("delete (will rollback): {0:.3f}s rowcount={1}".format(
                    time.time() - t1, rc,
                ), file=sys.stderr)
            except pymysql.err.OperationalError as exc:
                cur.execute("ROLLBACK")
                if exc.args and int(exc.args[0]) == 1205:
                    print("delete blocked: lock wait timeout (1205) -> stop Flink user incr first", file=sys.stderr)
                raise
            finally:
                try:
                    cur.execute("ROLLBACK")
                except pymysql.err.OperationalError:
                    pass
                print("rolled back", file=sys.stderr)
    finally:
        env_util.close_conn(conn)

    print("probe done (no rows deleted)", file=sys.stderr)


def init_session(conn, read_timeout: int) -> None:
    wt = max(read_timeout, 28800)
    with conn.cursor() as cur:
        for stmt in (
            "SET SESSION net_read_timeout = {0}".format(read_timeout),
            "SET SESSION net_write_timeout = {0}".format(read_timeout),
            "SET SESSION wait_timeout = {0}".format(wt),
            "SET SESSION interactive_timeout = {0}".format(wt),
            "SET SESSION innodb_lock_wait_timeout = 120",
        ):
            try:
                cur.execute(stmt)
            except pymysql.err.OperationalError:
                pass


def ensure_conn(conn, cfg: dict, read_timeout: int):
    try:
        conn.ping(reconnect=True)
        return conn
    except Exception:
        env_util.close_conn(conn)
        conn = connect(cfg, read_timeout)
        init_session(conn, read_timeout)
        return conn


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
        "SELECT mobile, app_id, closed_time, user_id, group_user_id, reg_time "
        "FROM `user` WHERE user_id IN ({0})".format(ph)
    )
    with conn.cursor() as cur:
        cur.execute(sql, list(user_ids))
        return list(cur.fetchall())


def fetch_duplicate_user_ids(conn) -> List[int]:
    sql = (
        "SELECT user_id FROM `user` "
        "GROUP BY user_id HAVING COUNT(*) > 1 "
        "ORDER BY COUNT(*) DESC, user_id"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        return [int(r["user_id"]) for r in cur.fetchall()]


def fetch_duplicate_rows(conn) -> List[Dict[str, Any]]:
    """一次查出所有重复 user_id 的全部行（含 reg_time 供 keep-one 排序）。"""
    sql = (
        "SELECT u.mobile, u.app_id, u.closed_time, u.user_id, u.group_user_id, u.reg_time "
        "FROM `user` u "
        "INNER JOIN ("
        "  SELECT user_id FROM `user` GROUP BY user_id HAVING COUNT(*) > 1"
        ") d ON d.user_id = u.user_id "
        "ORDER BY u.user_id, u.closed_time, u.app_id, u.mobile"
    )
    with conn.cursor() as cur:
        cur.execute(sql)
        return list(cur.fetchall())


def row_sort_key(r: Dict[str, Any]) -> Tuple[int, int, int, str]:
    """keep-one 保留优先级：closed_time=0 > 更早 reg_time > 更小 app_id > mobile。"""
    return (
        int(r["closed_time"]),
        int(r.get("reg_time") or 0),
        int(r["app_id"]),
        str(r["mobile"]),
    )


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


def preview_keep_one(all_rows: Sequence[Dict[str, Any]], limit: int = 10) -> None:
    by_uid: Dict[int, List[Dict[str, Any]]] = {}
    for r in all_rows:
        by_uid.setdefault(int(r["user_id"]), []).append(r)
    print("keep-one plan sample (first {0} duplicate user_id(s)):".format(limit), file=sys.stderr)
    shown = 0
    for uid, rows in sorted(by_uid.items()):
        if len(rows) <= 1:
            continue
        rows_sorted = sorted(rows, key=row_sort_key)
        keep = rows_sorted[0]
        print(
            "  uid={0}: KEEP ({1},{2},{3})  DROP {4} row(s)".format(
                uid,
                keep["mobile"], keep["app_id"], keep["closed_time"],
                len(rows_sorted) - 1,
            ),
            file=sys.stderr,
        )
        for drop in rows_sorted[1:]:
            print(
                "    DEL ({mobile},{app_id},{closed_time})".format(**drop),
                file=sys.stderr,
            )
        shown += 1
        if shown >= limit:
            break


def delete_one_row(conn, row: Dict[str, Any]) -> int:
    sql = "DELETE FROM `user` WHERE mobile=%s AND app_id=%s AND closed_time=%s LIMIT 1"
    with conn.cursor() as cur:
        cur.execute(sql, (row["mobile"], row["app_id"], row["closed_time"]))
        return cur.rowcount


def export_delete_sql(rows: Sequence[Dict[str, Any]], path: Path) -> None:
    lines = [
        "-- generated by delete_users_by_user_id.py",
        "SET SESSION net_read_timeout=7200, net_write_timeout=7200;",
        "SET SESSION innodb_lock_wait_timeout=120;",
    ]
    lines.extend(delete_sql(r) for r in rows)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("wrote {0} DELETE statement(s) to {1}".format(len(rows), path), file=sys.stderr)


def delete_via_mysql_cli(
    cfg: dict,
    rows: Sequence[Dict[str, Any]],
    retries: int,
    sleep_ms: int,
    start_from: int,
) -> int:
    row_list = list(rows)
    total = len(row_list)
    idx = max(0, start_from - 1)
    deleted = 0

    if idx > 0:
        print("resume from row {0}/{1}".format(idx + 1, total), file=sys.stderr)

    while idx < total:
        row = row_list[idx]
        sql = delete_sql(row).rstrip(";")
        attempt = 0
        while True:
            proc = subprocess.run(
                mysql_cmd_base(cfg) + ["-e", sql],
                env=mysql_env(cfg),
                capture_output=True,
                text=True,
            )
            if proc.returncode == 0:
                deleted += 1
                idx += 1
                if idx <= 5 or idx % 50 == 0 or idx == total:
                    print(
                        "  deleted {0}/{1} uid={2} pk=({3},{4},{5})".format(
                            idx, total, row["user_id"],
                            row["mobile"], row["app_id"], row["closed_time"],
                        ),
                        file=sys.stderr,
                    )
                if sleep_ms > 0 and idx < total:
                    time.sleep(sleep_ms / 1000.0)
                break
            err = (proc.stderr or proc.stdout or "").strip()
            if attempt >= retries or not any(m in err for m in _MYSQL_RETRY_MARKERS):
                print(
                    "abort at row {0}/{1}, use --start-from {0} to resume\n{2}".format(
                        idx + 1, total, err,
                    ),
                    file=sys.stderr,
                )
                if "2013" in err or "Lost connection" in err:
                    print_2013_hint()
                raise RuntimeError(err)
            attempt += 1
            wait = min(2 ** attempt, 30)
            print(
                "[retry] row {0}/{1} attempt={2}/{3} wait={4}s err={5}".format(
                    idx + 1, total, attempt, retries, wait, err,
                ),
                file=sys.stderr,
            )
            time.sleep(wait)

    return deleted


def _run_delete_loop(
    cfg: dict,
    total_units: int,
    start_from: int,
    read_timeout: int,
    retries: int,
    sleep_ms: int,
    unit_label: str,
    delete_fn,
) -> int:
    """单连接执行 delete_fn(conn) -> deleted_count；断连后从当前 unit 重试。"""
    deleted_rows = 0
    idx = max(0, start_from - 1)

    if idx > 0:
        print("resume from {0} {1}/{2}".format(unit_label, idx + 1, total_units), file=sys.stderr)

    conn = None
    while idx < total_units:
        attempt = 0
        while idx < total_units:
            try:
                if conn is None:
                    conn = connect(cfg, read_timeout)
                    init_session(conn, read_timeout)
                else:
                    conn = ensure_conn(conn, cfg, read_timeout)
                n, idx = delete_fn(conn, idx)
                deleted_rows += n
                if sleep_ms > 0 and idx < total_units:
                    time.sleep(sleep_ms / 1000.0)
                attempt = 0
            except Exception as exc:
                env_util.close_conn(conn)
                conn = None
                if not is_retryable(exc) or attempt >= retries:
                    print(
                        "abort at {0} {1}/{2}, use --start-from {1} to resume".format(
                            unit_label, idx + 1, total_units,
                        ),
                        file=sys.stderr,
                    )
                    raise
                attempt += 1
                wait = min(2 ** attempt, 30)
                print(
                    "[retry] {0} {1}/{2} attempt={3}/{4} wait={5}s err={6}".format(
                        unit_label, idx + 1, total_units, attempt, retries, wait, exc,
                    ),
                    file=sys.stderr,
                )
                time.sleep(wait)
        break

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
    row_list = list(rows)

    def delete_fn(conn, idx: int) -> Tuple[int, int]:
        row = row_list[idx]
        n = delete_one_row(conn, row)
        next_idx = idx + 1
        total = len(row_list)
        if next_idx <= 5 or next_idx % 50 == 0 or next_idx == total:
            print(
                "  deleted {0}/{1} uid={2} pk=({3},{4},{5})".format(
                    next_idx, total, row["user_id"],
                    row["mobile"], row["app_id"], row["closed_time"],
                ),
                file=sys.stderr,
            )
        return n, next_idx

    return _run_delete_loop(
        cfg, len(row_list), start_from, read_timeout, retries, sleep_ms, "row", delete_fn,
    )


def main() -> int:
    p = argparse.ArgumentParser(description="Batch delete target.user by user_id via PK")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--ids-file", help="file with user_id list")
    p.add_argument("--user-ids", help="comma-separated user_ids")
    p.add_argument(
        "--from-duplicates",
        action="store_true",
        help="从库内查 HAVING COUNT(*)>1 的 user_id（U1 重复），无需 ids 文件",
    )
    p.add_argument("--apply", action="store_true", help="execute delete (default dry-run)")
    p.add_argument(
        "--strategy",
        choices=("all", "keep-one", "closed-only"),
        default=None,
        help="keep-one=每 user_id 留 1 删其余(U1默认); all=删该 user_id 全部行; closed-only=只删 closed_time>0",
    )
    p.add_argument(
        "--only-closed-time",
        type=int,
        default=None,
        help="只删指定 closed_time，如 1（覆盖 strategy）",
    )
    p.add_argument("--lookup-batch", type=int, default=30, help="user_id IN 查询批次")
    p.add_argument("--sleep-ms", type=int, default=0, help="每条 DELETE 间隔毫秒")
    p.add_argument("--retries", type=int, default=8, help="断连重试次数")
    p.add_argument("--query-timeout", type=int, default=7200, help="MySQL 读写超时秒")
    p.add_argument(
        "--start-from",
        type=int,
        default=1,
        help="断点续删：从第几行开始(1-based)",
    )
    p.add_argument(
        "--via-mysql",
        action="store_true",
        help="用 mysql 客户端逐条删（推荐；避开 pymysql 长连接 2013）",
    )
    p.add_argument(
        "--export-sql",
        metavar="FILE",
        help="只导出 DELETE SQL 文件，不连库执行",
    )
    p.add_argument(
        "--probe",
        action="store_true",
        help="诊断首行：检查只读 + 事务内试删并 rollback（不真删）",
    )
    p.add_argument(
        "--diagnose-locks",
        action="store_true",
        help="打印 PROCESSLIST / innodb_trx / GRANTS（apply 前也会自动跑）",
    )
    p.add_argument(
        "--skip-writable-check",
        action="store_true",
        help="跳过 @@read_only 检查",
    )
    args = p.parse_args()

    if args.strategy is None:
        args.strategy = "keep-one"

    if args.from_duplicates:
        if args.ids_file or args.user_ids:
            print("warn: --from-duplicates ignores --ids-file / --user-ids", file=sys.stderr)
    elif args.ids_file:
        ids = load_ids(Path(args.ids_file))
    elif args.user_ids:
        ids = parse_ids(args.user_ids)
    else:
        print("need --from-duplicates or --ids-file or --user-ids", file=sys.stderr)
        return 2

    cfg = env_util.load_env(Path(args.env))

    if args.from_duplicates:
        print("discover duplicate user_id(s) from target...", file=sys.stderr)

        def _fetch_dup(conn) -> List[Dict[str, Any]]:
            return fetch_duplicate_rows(conn)

        all_rows = with_retry(
            cfg, "duplicate-rows", args.query_timeout, args.retries, _fetch_dup,
        )
        dup_ids = sorted({int(r["user_id"]) for r in all_rows})
        print(
            "duplicates: {0} user_id(s), {1} row(s) total".format(
                len(dup_ids), len(all_rows),
            ),
            file=sys.stderr,
        )
    else:
        if not ids:
            print("no user_id parsed", file=sys.stderr)
            return 2

        all_rows = []
        print("lookup {0} distinct user_id(s)...".format(len(ids)), file=sys.stderr)
        for batch_no, batch in enumerate(chunks(ids, args.lookup_batch), 1):
            def _fetch(conn, b=batch) -> List[Dict[str, Any]]:
                return fetch_pk_rows(conn, b)

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

    dup_uid_count = sum(1 for c in by_uid.values() if c > 1)
    print(
        "lookup: {0} row(s), {1} user_id(s), {2} with duplicates".format(
            len(all_rows), len(by_uid), dup_uid_count,
        ),
        file=sys.stderr,
    )

    to_delete = select_rows_to_delete(all_rows, args.strategy, args.only_closed_time)
    keep_count = len(all_rows) - len(to_delete)

    print(
        "strategy={0}: delete {1} row(s), keep {2} row(s)".format(
            args.strategy, len(to_delete), keep_count,
        ),
        file=sys.stderr,
    )
    if args.strategy == "keep-one":
        preview_keep_one(all_rows)
    else:
        print("delete plan sample (first 10):", file=sys.stderr)
        for r in to_delete[:10]:
            print(
                "  DEL uid={user_id} mobile={mobile} app_id={app_id} closed_time={closed_time}".format(**r),
                file=sys.stderr,
            )

    if not to_delete:
        print("nothing to delete", file=sys.stderr)
        return 0

    if args.export_sql:
        export_delete_sql(to_delete, Path(args.export_sql))
        return 0

    if args.probe:
        probe_first_row(cfg, to_delete[0], args.query_timeout)
        return 0

    if args.diagnose_locks:
        if not args.skip_writable_check:
            check_target_writable(cfg)
        diagnose_locks(cfg)
        if not args.apply:
            return 0

    if not args.apply:
        print("re-run with --apply to execute", file=sys.stderr)
        return 0

    if not args.skip_writable_check:
        check_target_writable(cfg)
    diagnose_locks(cfg)

    if args.via_mysql:
        deleted = delete_via_mysql_cli(
            cfg, to_delete, args.retries, args.sleep_ms, args.start_from,
        )
    else:
        deleted = delete_rows_with_retry(
            cfg, to_delete, args.query_timeout, args.retries, args.sleep_ms, args.start_from,
        )
    print("done deleted_rows={0}".format(deleted), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
