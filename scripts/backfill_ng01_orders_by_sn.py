#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 order_no(sn) 列表补目标库缺失的 ng01 application / loan。

映射逻辑复用 scripts/reconcile（与 Flink 02_sync_* 对齐）。

Usage（101 内网）:
  python3 scripts/backfill_ng01_orders_by_sn.py \\
    --env ./.env \\
    --list-file scripts/order_backfill_sn_20260902.txt

  python3 scripts/backfill_ng01_orders_by_sn.py \\
    --env ./.env \\
    --list-file scripts/order_backfill_sn_20260902.txt \\
    --apply
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
RECON = HERE / "reconcile"
sys.path.insert(0, str(RECON))

import env_util  # noqa: E402
import mapping as M  # noqa: E402
import source_queries  # noqa: E402
from reconcile_tables import (  # noqa: E402
    _insert_batch,
    loan_key,
    resolve_columns,
)

LOAN_RISK_BLOCK = frozenset({0, 2, 4, 6, 8})
DEFAULT_APPS = M.DEFAULT_INCLUDE_APP_IDS


def read_sn_list(args: argparse.Namespace) -> List[str]:
    lines: List[str] = []
    if args.list_file:
        lines.extend(Path(args.list_file).read_text(encoding="utf-8").splitlines())
    if args.sn:
        lines.extend(args.sn)
    out: List[str] = []
    seen: Set[str] = set()
    for line in lines:
        s = str(line or "").strip().strip("'\",")
        if not s or s.startswith("#"):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def fetch_order_meta(cfg: dict, sn: str) -> Optional[dict]:
    conn = env_util.connect_source(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.order_no, o.app_code, o.risk_order_status,
                       COUNT(i.id) AS installment_cnt
                FROM user_order o
                LEFT JOIN user_order_installment i ON i.user_order_id = o.id
                WHERE o.order_no = %s
                GROUP BY o.order_no, o.app_code, o.risk_order_status
                """,
                (sn,),
            )
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        env_util.close_conn(conn)


def fetch_by_sn(cfg: dict, table: str, sns: Sequence[str]) -> List[dict]:
    if not sns:
        return []
    base = source_queries._load_base_sql(table)
    alias = "s"
    ph = ",".join(["%s"] * len(sns))
    if table == "application":
        pred = "`{a}`.`sn` IN ({ph})".format(a=alias, ph=ph)
    elif table == "loan":
        # loan 宽表无 sn；用 application_no 精确匹配
        app_nos = []
        for sn in sns:
            app_code = int(str(sn)[:3])
            app_nos.append("ng{0:04d}-{1}".format(app_code, sn))
        ph2 = ",".join(["%s"] * len(app_nos))
        pred = "`{a}`.`application_no` IN ({ph})".format(a=alias, ph=ph2)
        sns = app_nos
    else:
        raise ValueError("unsupported table: {0}".format(table))
    sql = "SELECT `{a}`.* FROM ({base}) `{a}` WHERE {pred}".format(
        a=alias, base=base, pred=pred,
    )
    conn = env_util.connect_source(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(sns))
            return [dict(r) for r in cur.fetchall()]
    finally:
        env_util.close_conn(conn)


def finalize_application(row: dict) -> dict:
    st = int(row.get("status") or 0)
    if st < 20:
        row["disbursed_amount"] = 0
    return row


def loan_source_eligible(_src: dict) -> bool:
    """loan_source.sql 已在 WHERE 中过滤 risk_order_status；能查到的行视为可写。"""
    return True


def diagnose_sn(cfg: dict, sn: str, offset: int) -> dict:
    meta = fetch_order_meta(cfg, sn)
    app_src = fetch_by_sn(cfg, "application", [sn])
    loan_src = fetch_by_sn(cfg, "loan", [sn])
    rec = {
        "sn": sn,
        "source_order": bool(meta),
        "source_application": bool(app_src),
        "source_loan_rows": len(loan_src),
        "installment_cnt": int(meta.get("installment_cnt") or 0) if meta else 0,
        "loan_risk_blocked": False,
        "vt_skip_application": False,
        "target_application": False,
        "target_loan_rows": 0,
        "application_no": None,
        "notes": [],
    }
    if meta:
        rec["source_risk_order_status"] = meta.get("risk_order_status")
        ros = meta.get("risk_order_status")
        try:
            rec["loan_risk_blocked"] = int(ros) in LOAN_RISK_BLOCK
        except (TypeError, ValueError):
            rec["loan_risk_blocked"] = True
    if not meta:
        rec["notes"].append("source_missing_user_order")
        return rec

    if not app_src:
        rec["notes"].append("source_missing_application(vt/join)")
        return rec
    src = app_src[0]
    rec["application_no"] = src.get("application_no")
    app_code = int(src.get("app_code") or 0)
    if app_code not in DEFAULT_APPS:
        rec["notes"].append("app_code_not_in_flink_sync({0})".format(app_code))
    if rec["loan_risk_blocked"]:
        rec["notes"].append("loan_risk_status_filtered")
    if rec["installment_cnt"] == 0:
        rec["notes"].append("no_installment")

    exp_app = M.expected_application(src, offset)
    if exp_app is None:
        rec["vt_skip_application"] = True
        rec["notes"].append("vt_skip(mobile/bank/id)")
    else:
        exp_app = finalize_application(exp_app)
        rec["target_application"] = target_exists_application(cfg, exp_app)

    if not loan_src:
        if not rec["loan_risk_blocked"] and rec["installment_cnt"] > 0:
            rec["notes"].append("source_missing_loan(sql_join)")
        elif not rec["loan_risk_blocked"] and rec["installment_cnt"] == 0:
            pass
        elif rec["loan_risk_blocked"]:
            pass
        else:
            rec["notes"].append("source_missing_loan")

    if loan_src:
        for ls in loan_src:
            exp_loan = M.expected_loan(ls)
            if target_exists_loan(cfg, exp_loan):
                rec["target_loan_rows"] += 1
    return rec


def target_exists_application(cfg: dict, row: dict) -> bool:
    conn = env_util.connect_target(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM application WHERE mobile=%s AND group_user_id=%s AND sn=%s LIMIT 1",
                (row["mobile"], row["group_user_id"], row["sn"]),
            )
            return cur.fetchone() is not None
    finally:
        env_util.close_conn(conn)


def target_exists_loan(cfg: dict, row: dict) -> bool:
    conn = env_util.connect_target(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM loan WHERE application_no=%s AND period=%s AND roll_sequence=%s LIMIT 1",
                (row["application_no"], row["period"], row["roll_sequence"]),
            )
            return cur.fetchone() is not None
    finally:
        env_util.close_conn(conn)


def build_plan(
    cfg: dict,
    sns: Sequence[str],
    offset: int,
    app_columns: Sequence[str],
    loan_columns: Sequence[str],
) -> Tuple[List[dict], List[dict], List[dict], Dict[str, int]]:
    stats = {
        "sns": len(sns),
        "source_app": 0,
        "source_loan": 0,
        "insert_app": 0,
        "insert_loan": 0,
        "skip_app_exists": 0,
        "skip_loan_exists": 0,
        "vt_skip": 0,
        "source_missing": 0,
    }
    app_inserts: List[dict] = []
    loan_inserts: List[dict] = []
    diag: List[dict] = []

    app_src_rows = fetch_by_sn(cfg, "application", sns)
    by_sn = {str(r.get("sn")): r for r in app_src_rows}
    stats["source_app"] = len(by_sn)

    loan_src_rows = fetch_by_sn(cfg, "loan", sns)
    stats["source_loan"] = len(loan_src_rows)

    for sn in sns:
        d = diagnose_sn(cfg, sn, offset)
        diag.append(d)
        src = by_sn.get(sn)
        if not src:
            stats["source_missing"] += 1
            continue

        exp_app = M.expected_application(src, offset)
        if exp_app is None:
            stats["vt_skip"] += 1
            continue
        exp_app = finalize_application(exp_app)
        if "coupon_code" in app_columns:
            exp_app["coupon_code"] = ""
        if target_exists_application(cfg, exp_app):
            stats["skip_app_exists"] += 1
        else:
            stats["insert_app"] += 1
            app_inserts.append({c: exp_app.get(c) for c in app_columns})

    seen_loan: Set[Tuple[str, int, int]] = set()
    for ls in loan_src_rows:
        if not loan_source_eligible(ls):
            continue
        exp_loan = M.expected_loan(ls)
        k = loan_key(exp_loan)
        if k in seen_loan:
            continue
        seen_loan.add(k)
        if target_exists_loan(cfg, exp_loan):
            stats["skip_loan_exists"] += 1
            continue
        stats["insert_loan"] += 1
        loan_inserts.append({c: exp_loan.get(c) for c in loan_columns})

    return app_inserts, loan_inserts, diag, stats


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Backfill missing ng01 application/loan by sn list")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--list-file", default="")
    p.add_argument("--sn", nargs="*", default=[])
    p.add_argument("--apply", action="store_true")
    p.add_argument("--diag-file", default="/tmp/ng01_order_backfill_diag.jsonl")
    p.add_argument("--plan-file", default="/tmp/ng01_order_backfill_plan.json")
    args = p.parse_args(argv)

    sns = read_sn_list(args)
    if not sns:
        print("empty sn list", file=sys.stderr)
        return 2

    env_path = Path(args.env)
    if not env_path.is_file():
        print("env not found: {0}".format(env_path), file=sys.stderr)
        return 1
    cfg = env_util.load_env(env_path)
    offset = int(cfg["user_id_offset"])

    app_columns = resolve_columns(cfg, "application", M.APPLICATION_COLS)
    loan_columns = resolve_columns(cfg, "loan", M.LOAN_COLS)

    t0 = time.time()
    app_inserts, loan_inserts, diag, stats = build_plan(
        cfg, sns, offset, app_columns, loan_columns,
    )

    diag_path = Path(args.diag_file)
    with diag_path.open("w", encoding="utf-8") as fp:
        for row in diag:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    plan = {"application": app_inserts, "loan": loan_inserts, "stats": stats}
    Path(args.plan_file).write_text(
        json.dumps(plan, ensure_ascii=False, default=str, indent=2),
        encoding="utf-8",
    )

    print("sns={0} stats={1}".format(len(sns), stats))
    print("diag -> {0}".format(diag_path))
    print("plan -> {0}".format(args.plan_file))

    missing_app = [d for d in diag if not d.get("target_application")]
    if missing_app:
        print("target missing application ({0}):".format(len(missing_app)))
        for d in missing_app[:10]:
            print("  {0} {1} notes={2}".format(
                d.get("sn"), d.get("application_no"), d.get("notes"),
            ))
        if len(missing_app) > 10:
            print("  ... +{0}".format(len(missing_app) - 10))

    if not args.apply:
        print("dry-run only; add --apply to insert")
        print("elapsed={0:.1f}s".format(time.time() - t0))
        return 0

    if app_inserts:
        n = _insert_batch(cfg, "application", app_columns, app_inserts)
        print("inserted application rows={0}".format(n))
    if loan_inserts:
        n = _insert_batch(cfg, "loan", loan_columns, loan_inserts)
        print("inserted loan rows={0}".format(n))
    print("done elapsed={0:.1f}s".format(time.time() - t0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
