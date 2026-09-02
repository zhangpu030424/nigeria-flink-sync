#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 application_no 列表补 LM 贷超缺失的 target.loan（映射同 loan_dk_ld_sync_staging）。

LM application 不在本仓 Flink 同步范围内；本脚本只补 loan，并诊断 target.application 是否存在。

Usage（101 内网，.env 需 LM_MYSQL_* + TARGET_MYSQL_*）:
  python3 scripts/backfill_lm_orders_by_application_no.py \\
    --env ./.env \\
    --list-file scripts/order_backfill_lm_20260902.txt

  python3 scripts/backfill_lm_orders_by_application_no.py \\
    --env ./.env \\
    --list-file scripts/order_backfill_lm_20260902.txt \\
    --apply
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
RECON = HERE / "reconcile"
sys.path.insert(0, str(RECON))

import env_util  # noqa: E402
import mapping as M  # noqa: E402
from reconcile_tables import _insert_batch, resolve_columns  # noqa: E402

LM_LOAN_CREATED_MS = 1785340800000


def load_compare_module():
    path = HERE / "compare_orders_source_target.py"
    spec = importlib.util.spec_from_file_location("order_compare", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_application_nos(args: argparse.Namespace) -> List[str]:
    lines: List[str] = []
    if args.list_file:
        lines.extend(Path(args.list_file).read_text(encoding="utf-8").splitlines())
    if args.application_no:
        lines.extend(args.application_no)
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


def unix_to_date(ts: int) -> Optional[str]:
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def build_lm_loan_row(cmp_mod, row: dict) -> Optional[dict]:
    if cmp_mod.to_int(row.get("disburseTime")) == 0:
        return None
    app_id = cmp_mod.to_int(row.get("appId"))
    sn = str(row.get("applicationNo"))
    application_no = "ng{0:04d}-{1}".format(app_id, sn)
    src_status = cmp_mod.to_int(row.get("status"))
    amount = cmp_mod.to_int(row.get("amount"))
    disburse = cmp_mod.to_int(row.get("disburseAmount"))
    admin_fee = max(amount - disburse, 0)
    principal = max(disburse, 0)
    total_amount = max(cmp_mod.to_int(row.get("repayment")), 0)
    paid_amount = cmp_mod.to_int(row.get("paidAmount")) if src_status in (17, 18, 19) else 0
    paid_ts = cmp_mod.to_int(row.get("paidTime"))
    paid_time = paid_ts * 1000 if paid_ts > 0 else None
    paid_off_date = unix_to_date(paid_ts) if paid_ts > 0 else None
    disburse_ts = cmp_mod.to_int(row.get("disburseTime"))
    due_ts = cmp_mod.to_int(row.get("dueDate"))
    return {
        "loan_no": "ng-{0}-01000".format(sn),
        "application_no": application_no,
        "period": 1,
        "roll_sequence": 0,
        "start_date": unix_to_date(disburse_ts),
        "due_date": unix_to_date(due_ts),
        "due_date_final": unix_to_date(due_ts),
        "principal": principal,
        "interest": 0,
        "admin_fee": admin_fee,
        "penalty_amount": 0,
        "reduction_amount": 0,
        "total_amount": total_amount,
        "paid_amount": paid_amount,
        "paid_time": paid_time,
        "paid_off_date": paid_off_date,
        "created_time": LM_LOAN_CREATED_MS,
        "status": cmp_mod.map_lm_status(src_status),
    }


def fetch_lm_rows(cmp_mod, cfg: dict, keys: Sequence[Tuple[int, str]]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    by_app: Dict[int, List[str]] = {}
    for app_id, sn in keys:
        by_app.setdefault(int(app_id), []).append(str(sn))
    conn = cmp_mod.connect_lm_source(cfg)
    try:
        with conn.cursor() as cur:
            for app_id, sns in by_app.items():
                for batch in cmp_mod.chunks(sorted(set(sns)), 200):
                    ph = ",".join(["%s"] * len(batch))
                    sql = (
                        "SELECT appId, applicationNo, status, amount, disburseAmount, "
                        "repayment, paidAmount, paidTime, disburseTime, dueDate "
                        "FROM application "
                        "WHERE appId = %s AND applicationNo IN (" + ph + ")"
                    )
                    cur.execute(sql, [app_id] + batch)
                    for row in cur.fetchall():
                        app_no = "ng{0:04d}-{1}".format(
                            cmp_mod.to_int(row.get("appId")),
                            str(row.get("applicationNo")),
                        )
                        out[app_no] = dict(row)
    finally:
        conn.close()
    return out


def target_has_application(cmp_mod, cfg: dict, application_no: str) -> bool:
    conn = cmp_mod.connect_target(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM application WHERE application_no = %s LIMIT 1",
                (application_no,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def target_has_loan(cmp_mod, cfg: dict, application_no: str) -> bool:
    conn = cmp_mod.connect_target(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM loan WHERE application_no = %s AND period = 1 "
                "AND roll_sequence = 0 LIMIT 1",
                (application_no,),
            )
            return cur.fetchone() is not None
    finally:
        conn.close()


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Backfill missing LM loan by application_no list")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--list-file", default="")
    p.add_argument("--application-no", nargs="*", default=[])
    p.add_argument("--apply", action="store_true")
    p.add_argument("--diag-file", default="/tmp/lm_order_backfill_diag.jsonl")
    p.add_argument("--plan-file", default="/tmp/lm_order_backfill_plan.json")
    args = p.parse_args(argv)

    app_nos = read_application_nos(args)
    if not app_nos:
        print("empty application_no list", file=sys.stderr)
        return 2

    env_path = Path(args.env)
    if not env_path.is_file():
        print("env not found: {0}".format(env_path), file=sys.stderr)
        return 1
    cfg = env_util.load_env(env_path)
    cmp_mod = load_compare_module()
    loan_columns = resolve_columns(cfg, "loan", M.LOAN_COLS)

    keys: List[Tuple[int, str]] = []
    bad: List[str] = []
    for app_no in app_nos:
        try:
            keys.append(cmp_mod.parse_application_no(app_no))
        except ValueError:
            bad.append(app_no)
    if bad:
        print("bad application_no:", bad, file=sys.stderr)
        return 2

    t0 = time.time()
    src_by_no = fetch_lm_rows(cmp_mod, cfg, keys)
    diag: List[dict] = []
    loan_inserts: List[dict] = []
    stats = {
        "application_nos": len(app_nos),
        "source_hit": 0,
        "source_not_disbursed": 0,
        "source_missing": 0,
        "target_app_missing": 0,
        "insert_loan": 0,
        "skip_loan_exists": 0,
    }

    for app_no in app_nos:
        rec = {"application_no": app_no, "notes": []}
        src = src_by_no.get(app_no)
        if not src:
            stats["source_missing"] += 1
            rec["notes"].append("source_missing")
            diag.append(rec)
            continue
        stats["source_hit"] += 1
        if cmp_mod.to_int(src.get("disburseTime")) == 0:
            stats["source_not_disbursed"] += 1
            rec["notes"].append("disburseTime_zero")
            diag.append(rec)
            continue

        has_app = target_has_application(cmp_mod, cfg, app_no)
        rec["target_application"] = has_app
        if not has_app:
            stats["target_app_missing"] += 1
            rec["notes"].append("target_application_missing")

        loan_row = build_lm_loan_row(cmp_mod, src)
        if loan_row is None:
            rec["notes"].append("loan_row_build_failed")
            diag.append(rec)
            continue

        rec["target_loan"] = target_has_loan(cmp_mod, cfg, app_no)
        if rec["target_loan"]:
            stats["skip_loan_exists"] += 1
            rec["notes"].append("loan_exists")
        else:
            stats["insert_loan"] += 1
            loan_inserts.append({c: loan_row.get(c) for c in loan_columns})
            rec["notes"].append("will_insert_loan")
        diag.append(rec)

    diag_path = Path(args.diag_file)
    with diag_path.open("w", encoding="utf-8") as fp:
        for row in diag:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    plan = {"loan": loan_inserts, "stats": stats}
    Path(args.plan_file).write_text(
        json.dumps(plan, ensure_ascii=False, default=str, indent=2),
        encoding="utf-8",
    )

    print("stats={0}".format(stats))
    print("diag -> {0}".format(diag_path))
    print("plan -> {0}".format(args.plan_file))
    if stats["target_app_missing"]:
        print("WARN: {0} orders missing target.application (this script only inserts loan)".format(
            stats["target_app_missing"],
        ))

    if not args.apply:
        print("dry-run only; add --apply to insert")
        print("elapsed={0:.1f}s".format(time.time() - t0))
        return 0

    if loan_inserts:
        n = _insert_batch(cfg, "loan", loan_columns, loan_inserts)
        print("inserted loan rows={0}".format(n))
    print("done elapsed={0:.1f}s".format(time.time() - t0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
