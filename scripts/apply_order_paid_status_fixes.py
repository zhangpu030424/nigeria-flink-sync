#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""仅修复 application/loan 的状态与还款时间/金额字段（不动 total_amount/admin_fee 等）。

可修改字段：
  application: last_paid_time, paid_off_time, status
  loan:        paid_amount, paid_time, paid_off_date, status

Usage:
  # 1) 清洗 report，只保留可修字段差异
  python3 scripts/apply_order_paid_status_fixes.py \\
    --report /tmp/order_compare_report.jsonl \\
    --sanitize /tmp/order_compare_report_fixable.jsonl

  # 2) 按 order_list 从源库拉期望并 apply（推荐）
  python3 scripts/apply_order_paid_status_fixes.py \\
    --env ./.env --list-file /tmp/order_list.txt --apply

  # 3) 仅处理 report 里出现过的单号
  python3 scripts/apply_order_paid_status_fixes.py \\
    --env ./.env --report /tmp/order_compare_report.jsonl --apply --dry-run
"""
import argparse
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent


def load_compare_module():
    path = HERE / "compare_orders_source_target.py"
    spec = importlib.util.spec_from_file_location("order_compare", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


APP_FIX_FIELDS = ("last_paid_time", "paid_off_time", "status")
LOAN_FIX_FIELDS = ("paid_amount", "paid_time", "paid_off_date", "status")
ALLOWED = {
    "application": APP_FIX_FIELDS,
    "loan": LOAN_FIX_FIELDS,
}


def sanitize_report(in_path: Path, out_path: Path) -> Dict[str, int]:
    stats = {"in": 0, "out": 0, "skipped_ok": 0}
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with in_path.open(encoding="utf-8") as fin, out_path.open("w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            stats["in"] += 1
            rec = json.loads(line)
            entity = rec.get("entity")
            if entity not in ALLOWED:
                continue
            mismatches = [
                m for m in (rec.get("mismatches") or [])
                if m.get("field") in ALLOWED[entity]
            ]
            if not mismatches:
                stats["skipped_ok"] += 1
                continue
            rec = dict(rec)
            rec["mismatches"] = mismatches
            rec["result"] = "mismatch"
            rec["fixable_only"] = True
            fout.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")
            stats["out"] += 1
    return stats


def application_nos_from_report(report_path: Path) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for line in report_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        app_no = str(rec.get("application_no") or "").strip()
        if app_no and app_no not in seen:
            seen.add(app_no)
            out.append(app_no)
    return out


def norm_val(field: str, val: Any) -> Any:
    if field in ("last_paid_time", "paid_off_time", "paid_time"):
        if val is None or val == "":
            return None
        return int(val)
    if field == "paid_off_date":
        if val is None or val == "":
            return None
        if hasattr(val, "isoformat"):
            return val.isoformat()
        return str(val)[:10]
    if field in ("status", "paid_amount"):
        return int(val or 0)
    return val


def diff_fixable(expected: dict, actual: dict, fields: Sequence[str]) -> Dict[str, Tuple[Any, Any]]:
    changes: Dict[str, Tuple[Any, Any]] = {}
    for field in fields:
        exp = norm_val(field, expected.get(field))
        act = norm_val(field, actual.get(field))
        if exp != act:
            changes[field] = (act, exp)
    return changes


def fetch_target_fixable(cmp, conn, application_nos: Sequence[str]) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    apps: Dict[str, dict] = {}
    loans: Dict[str, dict] = {}
    for batch in cmp.chunks(list(application_nos), 200):
        ph = ",".join(["%s"] * len(batch))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT application_no, status, last_paid_time, paid_off_time "
                "FROM application WHERE application_no IN (" + ph + ")",
                batch,
            )
            for row in cur.fetchall():
                apps[str(row["application_no"])] = row
            cur.execute(
                "SELECT application_no, status, paid_amount, paid_time, paid_off_date "
                "FROM loan WHERE application_no IN (" + ph + ") "
                "AND period = 1 AND roll_sequence = 0",
                batch,
            )
            for row in cur.fetchall():
                loans[str(row["application_no"])] = row
    return apps, loans


def apply_application(conn, application_no: str, new_vals: Dict[str, Any]) -> int:
    if not new_vals:
        return 0
    sets = []
    params: List[Any] = []
    for field in APP_FIX_FIELDS:
        if field not in new_vals:
            continue
        sets.append("`%s` = %%s" % field)
        params.append(new_vals[field])
    if not sets:
        return 0
    sql = "UPDATE application SET " + ", ".join(sets) + " WHERE application_no = %s"
    params.append(application_no)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.rowcount or 0)


def apply_loan(conn, application_no: str, new_vals: Dict[str, Any]) -> int:
    if not new_vals:
        return 0
    sets = []
    params: List[Any] = []
    for field in LOAN_FIX_FIELDS:
        if field not in new_vals:
            continue
        sets.append("`%s` = %%s" % field)
        params.append(new_vals[field])
    if not sets:
        return 0
    sql = (
        "UPDATE loan SET " + ", ".join(sets)
        + " WHERE application_no = %s AND period = 1 AND roll_sequence = 0"
    )
    params.append(application_no)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.rowcount or 0)


def build_plan(
    cmp,
    application_nos: Sequence[str],
    cfg: dict,
) -> Tuple[List[dict], Dict[str, int]]:
    ng01_keys: List[str] = []
    lm_keys: List[Tuple[int, str]] = []
    pipeline_by_no: Dict[str, str] = {}
    for app_no in application_nos:
        app_id, sn = cmp.parse_application_no(app_no)
        pipe = cmp.pipeline_for_app(app_id)
        pipeline_by_no[app_no] = pipe
        if pipe == "ng01":
            ng01_keys.append(sn)
        else:
            lm_keys.append((app_id, sn))

    source_by_no: Dict[str, dict] = {}
    if ng01_keys:
        conn = cmp.connect_ng01_source(cfg)
        try:
            source_by_no.update(cmp.fetch_ng01_source(conn, sorted(set(ng01_keys))))
        finally:
            conn.close()
    if lm_keys:
        conn = cmp.connect_lm_source(cfg)
        try:
            source_by_no.update(cmp.fetch_lm_source(conn, sorted(set(lm_keys))))
        finally:
            conn.close()

    conn = cmp.connect_target(cfg)
    try:
        target_apps, target_loans = fetch_target_fixable(cmp, conn, application_nos)
    finally:
        conn.close()

    plan: List[dict] = []
    stats = {
        "input": len(application_nos),
        "source_missing": 0,
        "app_need_fix": 0,
        "loan_need_fix": 0,
        "app_fields": 0,
        "loan_fields": 0,
    }

    for app_no in application_nos:
        src = source_by_no.get(app_no)
        if not src:
            stats["source_missing"] += 1
            continue
        pipe = pipeline_by_no[app_no]
        exp_app = {k: src["expected_application"].get(k) for k in APP_FIX_FIELDS}
        exp_loan = {k: src["expected_loan"].get(k) for k in LOAN_FIX_FIELDS}
        act_app = target_apps.get(app_no) or {}
        act_loan = target_loans.get(app_no) or {}
        app_changes = diff_fixable(exp_app, act_app, APP_FIX_FIELDS)
        loan_changes = diff_fixable(exp_loan, act_loan, LOAN_FIX_FIELDS)
        if not app_changes and not loan_changes:
            continue
        item = {
            "application_no": app_no,
            "pipeline": pipe,
            "application_changes": {k: {"old": v[0], "new": v[1]} for k, v in app_changes.items()},
            "loan_changes": {k: {"old": v[0], "new": v[1]} for k, v in loan_changes.items()},
            "apply_application": {k: v[1] for k, v in app_changes.items()},
            "apply_loan": {k: v[1] for k, v in loan_changes.items()},
        }
        plan.append(item)
        if app_changes:
            stats["app_need_fix"] += 1
            stats["app_fields"] += len(app_changes)
        if loan_changes:
            stats["loan_need_fix"] += 1
            stats["loan_fields"] += len(loan_changes)
    return plan, stats


def run_apply(cmp_mod, cfg: dict, plan: List[dict], dry_run: bool) -> Dict[str, int]:
    stats = {"app_updated": 0, "loan_updated": 0, "app_rows": 0, "loan_rows": 0}
    if dry_run:
        for item in plan[:20]:
            print(
                "dry-run %(application_no)s app=%(application_changes)s loan=%(loan_changes)s"
                % item,
                flush=True,
            )
        if len(plan) > 20:
            print("dry-run ... and %s more" % (len(plan) - 20), flush=True)
        return stats

    conn = cmp_mod.connect_mysql(
        cfg,
        host_keys=("TARGET_MYSQL_HOST", "TARGET_HOST"),
        port_keys=("TARGET_MYSQL_PORT", "TARGET_PORT"),
        user_keys=("TARGET_MYSQL_USER", "TARGET_USER"),
        password_keys=("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD"),
        db_keys=("TARGET_MYSQL_DATABASE", "TARGET_DB"),
        default_db="ng",
    )
    conn.autocommit(False)
    try:
        for item in plan:
            app_no = item["application_no"]
            if item.get("apply_application"):
                n = apply_application(conn, app_no, item["apply_application"])
                stats["app_rows"] += n
                if n:
                    stats["app_updated"] += 1
            if item.get("apply_loan"):
                n = apply_loan(conn, app_no, item["apply_loan"])
                stats["loan_rows"] += n
                if n:
                    stats["loan_updated"] += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return stats


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fix only status/paid fields on application & loan")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--report", help="input compare report jsonl")
    p.add_argument("--sanitize", help="write fixable-only report jsonl")
    p.add_argument("--list-file", help="application_no list (one per line)")
    p.add_argument("--plan-file", default="/tmp/order_paid_status_fix_plan.jsonl")
    p.add_argument("--apply", action="store_true", help="apply updates to target")
    p.add_argument("--dry-run", action="store_true")
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    cmp = load_compare_module()

    if args.sanitize:
        if not args.report:
            print("--sanitize requires --report", file=sys.stderr)
            return 2
        stats = sanitize_report(Path(args.report), Path(args.sanitize))
        print("sanitize in=%s out=%s skipped_ok=%s -> %s" % (
            stats["in"], stats["out"], stats["skipped_ok"], args.sanitize,
        ), flush=True)
        if not args.apply and not args.dry_run and not args.list_file:
            return 0

    application_nos: List[str] = []
    if args.list_file:
        application_nos.extend(
            cmp.read_application_nos(argparse.Namespace(
                stdin=False, list_file=args.list_file, application_no=[],
            ))
        )
    elif args.report:
        application_nos.extend(application_nos_from_report(Path(args.report)))
    if not application_nos:
        print("need --list-file or --report", file=sys.stderr)
        return 2

    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: %s" % env_path, file=sys.stderr)
        return 2
    cfg = cmp.load_env(env_path)
    cmp.print_connection_plan(cfg)

    t0 = time.time()
    plan, stats = build_plan(cmp, application_nos, cfg)
    plan_path = Path(args.plan_file)
    with plan_path.open("w", encoding="utf-8") as fp:
        for item in plan:
            fp.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
    print("plan rows=%s stats=%s file=%s" % (len(plan), stats, plan_path), flush=True)

    if args.dry_run:
        apply_stats = run_apply(cmp, cfg, plan, dry_run=True)
        print("apply stats=%s elapsed=%.1fs" % (apply_stats, time.time() - t0), flush=True)
    elif args.apply:
        apply_stats = run_apply(cmp, cfg, plan, dry_run=False)
        print("apply stats=%s elapsed=%.1fs" % (apply_stats, time.time() - t0), flush=True)
    else:
        print("plan only; add --apply or --dry-run", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
