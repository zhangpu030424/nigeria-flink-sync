#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 user_id 分批删除 target.user（走主键，避免大 IN 全表扫挂死）。

user 主键 (mobile, app_id, closed_time)，无 user_id 索引；勿直接 DELETE ... WHERE user_id IN (几百个)。

Usage（101）:
  # 从文件读 user_id（一行一个或空格/逗号分隔）
  python3 scripts/delete_users_by_user_id.py --env ./.env --ids-file /tmp/user_ids.txt --dry-run

  # 确认后执行
  python3 scripts/delete_users_by_user_id.py --env ./.env --ids-file /tmp/user_ids.txt --apply

  # 或直接传 ID
  python3 scripts/delete_users_by_user_id.py --env ./.env --user-ids 418612,454725 --apply
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "reconcile"))
import env_util  # noqa: E402


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
    # 支持 SQL IN (...) 粘贴
    raw = raw.replace("(", " ").replace(")", " ")
    return parse_ids(raw)


def chunks(items: Sequence[int], size: int):
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


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


def delete_pk_batch(conn, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = "DELETE FROM `user` WHERE mobile=%s AND app_id=%s AND closed_time=%s"
    with conn.cursor() as cur:
        cur.executemany(
            sql,
            [(r["mobile"], r["app_id"], r["closed_time"]) for r in rows],
        )
        return cur.rowcount


def main() -> int:
    p = argparse.ArgumentParser(description="Batch delete target.user by user_id via PK")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--ids-file", help="file with user_id list")
    p.add_argument("--user-ids", help="comma-separated user_ids")
    p.add_argument("--apply", action="store_true", help="execute delete (default dry-run)")
    p.add_argument("--lookup-batch", type=int, default=50, help="user_id IN batch for SELECT")
    p.add_argument("--delete-batch", type=int, default=100, help="rows per DELETE executemany")
    p.add_argument("--sleep-ms", type=int, default=100, help="pause between delete batches")
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
    conn = env_util.connect_target(cfg)
    try:
        all_rows: List[Dict[str, Any]] = []
        print("lookup {0} distinct user_id(s)...".format(len(ids)), file=sys.stderr)
        for batch in chunks(ids, args.lookup_batch):
            all_rows.extend(fetch_pk_rows(conn, batch))
            print("  found {0} rows so far".format(len(all_rows)), file=sys.stderr)

        if not all_rows:
            print("no matching rows in user table", file=sys.stderr)
            return 0

        by_uid: Dict[int, int] = {}
        for r in all_rows:
            uid = int(r["user_id"])
            by_uid[uid] = by_uid.get(uid, 0) + 1

        print(
            "match: {0} row(s), {1} user_id(s) with rows, {2} user_id(s) not found".format(
                len(all_rows),
                len(by_uid),
                len(ids) - len(by_uid),
            ),
            file=sys.stderr,
        )
        multi = [(u, c) for u, c in by_uid.items() if c > 1]
        if multi:
            print("duplicate user_id rows (U1): {0} ids, e.g. {1}".format(
                len(multi), multi[:5],
            ), file=sys.stderr)

        if not args.apply:
            print("dry-run sample (first 10):", file=sys.stderr)
            for r in all_rows[:10]:
                print(
                    "  uid={user_id} pk=({mobile},{app_id},{closed_time})".format(**r),
                    file=sys.stderr,
                )
            print("re-run with --apply to delete", file=sys.stderr)
            return 0

        deleted = 0
        for i, batch in enumerate(chunks(all_rows, args.delete_batch), 1):
            n = delete_pk_batch(conn, batch)
            deleted += n
            print("delete batch {0}: {1} row(s), total={2}".format(i, n, deleted), file=sys.stderr)
            if args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

        print("done deleted={0}".format(deleted), file=sys.stderr)
    finally:
        env_util.close_conn(conn)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
