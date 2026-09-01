#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 product_id 批量修正 loan.admin_fee。

公式（与手工 SQL 一致）：
  new_admin_fee = ROUND(admin_fee / 0.35 - principal, 0)

只更新 new_admin_fee >= 0 且与当前 admin_fee 不同的行。
按主键 (application_no, period, roll_sequence) 分批 UPDATE，避免大 JOIN 锁超时。

Usage:
  # 先看有多少行、抽样
  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env .env --scan

  # 生成 plan
  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env .env --build-plan \\
    --plan-file /tmp/fix_loan_admin_fee_plan.jsonl

  # 执行（165 上可用 ng_migration.env）
  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env ./ng_migration.env --apply \\
    --plan-file /tmp/fix_loan_admin_fee_plan.jsonl \\
    --batch-size 100
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

PRODUCT_IDS: Tuple[int, ...] = (
    502, 503, 604, 609, 620, 622, 623, 626, 627, 628, 629, 630, 631, 632, 633, 634, 635, 638, 639, 655,
    656, 657, 658, 659, 701, 702, 710, 711, 712, 713, 714, 715, 716, 718, 725, 726, 727, 729, 730, 731,
    732, 733, 734, 735, 736, 738, 739, 740, 741, 742, 743, 744, 745, 746, 747, 748, 749, 750, 752, 753,
    765, 766, 767, 1012, 1013, 1014, 1015, 1016, 1017, 1021, 1022, 1023, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
)

SELECT_SQL = """
SELECT
    l.application_no,
    l.period,
    l.roll_sequence,
    l.loan_no,
    l.admin_fee AS old_admin_fee,
    l.principal,
    CAST(ROUND(l.admin_fee / 0.35 - l.principal, 0) AS SIGNED) AS new_admin_fee,
    a.product_id
FROM loan l
INNER JOIN application a ON l.application_no = a.application_no
WHERE CAST(a.product_id AS UNSIGNED) IN ({product_ph})
  AND CAST(ROUND(l.admin_fee / 0.35 - l.principal, 0) AS SIGNED) >= 0
  AND l.admin_fee <> CAST(ROUND(l.admin_fee / 0.35 - l.principal, 0) AS SIGNED)
"""


def load_env(path: Path) -> Dict[str, str]:
    cfg: Dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if " #" in line:
            line = line.split(" #", 1)[0].rstrip()
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect_target(cfg: Dict[str, str], for_apply: bool = False):
    host = cfg.get("TARGET_MYSQL_HOST") or cfg.get("TARGET_HOST") or "127.0.0.1"
    port = int(cfg.get("TARGET_MYSQL_PORT") or cfg.get("TARGET_PORT") or 3306)
    user = cfg.get("TARGET_MYSQL_USER") or cfg.get("TARGET_USER") or "root"
    password = cfg.get("TARGET_MYSQL_PASSWORD") or cfg.get("TARGET_PASSWORD") or ""
    database = cfg.get("TARGET_MYSQL_DATABASE") or cfg.get("TARGET_DB") or "ng"
    timeout = 120 if for_apply else 3600
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=60,
        read_timeout=timeout,
        write_timeout=timeout,
        autocommit=False,
    )


def compute_new_admin_fee(admin_fee: Any, principal: Any) -> int:
    try:
        af = float(admin_fee or 0)
    except (TypeError, ValueError):
        af = 0.0
    try:
        pr = float(principal or 0)
    except (TypeError, ValueError):
        pr = 0.0
    return int(round(af / 0.35 - pr, 0))


def fetch_rows(conn, product_ids: Sequence[int]) -> List[dict]:
    ph = ",".join(["%s"] * len(product_ids))
    sql = SELECT_SQL.format(product_ph=ph)
    with conn.cursor() as cur:
        cur.execute(sql, list(product_ids))
        return list(cur.fetchall())


def write_jsonl(path: Path, rows: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def read_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def exec_with_retry(conn, fn, label: str, retries: int = 5):
    for attempt in range(retries):
        try:
            return fn()
        except pymysql.Error as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            if attempt >= retries - 1:
                raise
            print("%s retry %s err=%s" % (label, attempt + 1, exc), flush=True)
            try:
                conn.ping(reconnect=True)
            except Exception:
                pass
            time.sleep(2)
    return None


def apply_batch(conn, batch: List[dict]) -> int:
    affected = 0
    with conn.cursor() as cur:
        for row in batch:
            cur.execute(
                """
                UPDATE loan
                SET admin_fee = %s
                WHERE application_no = %s
                  AND period = %s
                  AND roll_sequence = %s
                  AND admin_fee = %s
                """,
                (
                    int(row["new_admin_fee"]),
                    str(row["application_no"]),
                    int(row["period"]),
                    int(row["roll_sequence"]),
                    int(row["old_admin_fee"]),
                ),
            )
            affected += int(cur.rowcount or 0)
    conn.commit()
    return affected


def scan(conn, product_ids: Sequence[int], sample: int = 10) -> Dict[str, Any]:
    rows = fetch_rows(conn, product_ids)
    print("match_rows=%s product_ids=%s" % (len(rows), len(product_ids)), flush=True)
    if rows:
        for row in rows[:sample]:
            print(
                " sample loan_no=%s app=%s p%s r%s product_id=%s "
                "admin_fee %s -> %s (principal=%s)"
                % (
                    row.get("loan_no"),
                    row.get("application_no"),
                    row.get("period"),
                    row.get("roll_sequence"),
                    row.get("product_id"),
                    row.get("old_admin_fee"),
                    row.get("new_admin_fee"),
                    row.get("principal"),
                ),
                flush=True,
            )
    return {"match_rows": len(rows)}


def build_plan(conn, product_ids: Sequence[int]) -> Tuple[List[dict], Dict[str, int]]:
    rows = fetch_rows(conn, product_ids)
    plan: List[dict] = []
    for row in rows:
        plan.append({
            "application_no": row["application_no"],
            "period": int(row["period"]),
            "roll_sequence": int(row["roll_sequence"]),
            "loan_no": row.get("loan_no"),
            "product_id": row.get("product_id"),
            "old_admin_fee": int(row["old_admin_fee"]),
            "new_admin_fee": int(row["new_admin_fee"]),
            "principal": int(row.get("principal") or 0),
        })
    stats = {"plan_rows": len(plan)}
    return plan, stats


def apply_plan(conn, plan: List[dict], batch_size: int, dry_run: bool) -> Dict[str, int]:
    if dry_run:
        print("dry-run rows=%s" % len(plan), flush=True)
        for row in plan[:10]:
            print(
                "  %s admin_fee %s -> %s"
                % (row.get("loan_no"), row.get("old_admin_fee"), row.get("new_admin_fee")),
                flush=True,
            )
        return {"dry_run": len(plan)}

    stats = {"updated": 0, "skipped": 0}
    total_batches = (len(plan) + batch_size - 1) // batch_size
    for bi in range(0, len(plan), batch_size):
        part = plan[bi:bi + batch_size]
        bno = bi // batch_size + 1

        def _run():
            return apply_batch(conn, part)

        n = exec_with_retry(conn, _run, "batch %s/%s" % (bno, total_batches))
        stats["updated"] += int(n or 0)
        stats["skipped"] += len(part) - int(n or 0)
        if bno == 1 or bno % 20 == 0 or bno == total_batches:
            print(
                "batch %s/%s affected=%s total_updated=%s"
                % (bno, total_batches, n, stats["updated"]),
                flush=True,
            )
    return stats


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fix loan.admin_fee by product_id list")
    p.add_argument("--env", default=str(REPO / ".env"))
    p.add_argument("--scan", action="store_true", help="count + sample only")
    p.add_argument("--build-plan", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--plan-file", default="/tmp/fix_loan_admin_fee_plan.jsonl")
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--sample", type=int, default=10)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if not args.scan and not args.build_plan and not args.apply and not args.dry_run:
        print("specify --scan, --build-plan, and/or --apply", file=sys.stderr)
        return 2
    if args.apply and args.dry_run:
        print("use either --apply or --dry-run", file=sys.stderr)
        return 2

    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: %s" % env_path, file=sys.stderr)
        return 2
    cfg = load_env(env_path)
    plan_path = Path(args.plan_file)
    t0 = time.time()

    if args.scan or args.build_plan:
        conn = connect_target(cfg, for_apply=False)
        try:
            if args.scan:
                scan(conn, PRODUCT_IDS, sample=args.sample)
            if args.build_plan:
                plan, stats = build_plan(conn, PRODUCT_IDS)
                write_jsonl(plan_path, plan)
                print(
                    "plan written file=%s rows=%s stats=%s elapsed=%.1fs"
                    % (plan_path, len(plan), stats, time.time() - t0),
                    flush=True,
                )
        finally:
            conn.close()

    if args.apply or args.dry_run:
        if not plan_path.is_file():
            print("missing plan: %s (run --build-plan first)" % plan_path, file=sys.stderr)
            return 2
        plan = read_jsonl(plan_path)
        print("loaded plan rows=%s" % len(plan), flush=True)
        conn = connect_target(cfg, for_apply=True)
        try:
            stats = apply_plan(conn, plan, args.batch_size, dry_run=bool(args.dry_run))
            print("apply stats=%s elapsed=%.1fs" % (stats, time.time() - t0), flush=True)
        finally:
            conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
