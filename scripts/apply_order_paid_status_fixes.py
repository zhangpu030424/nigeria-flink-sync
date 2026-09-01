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

  # 4) 已有 plan，按主键 apply（旧 plan 无 PK 会在 apply 前自动补查）
  python3 scripts/apply_order_paid_status_fixes.py \\
    --env ./.env --apply-from-plan --plan-file /tmp/order_paid_status_fix_plan.jsonl --apply
"""
import argparse
import importlib.util
import json
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import pymysql

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


class Progress:
    """终端单行进度条。"""

    def __init__(self, label: str, total: int, width: int = 36):
        self.label = label
        self.total = max(1, int(total))
        self.done = 0
        self.width = width
        self.t0 = time.time()
        self._lock = threading.Lock()
        self._last_len = 0
        self.extra = ""

    def set_extra(self, text: str) -> None:
        self.extra = text

    def increment(self, n: int = 1) -> None:
        with self._lock:
            self.done = min(self.total, self.done + n)
            self._render()

    def _render(self) -> None:
        pct = self.done / self.total
        filled = int(self.width * pct)
        bar = "#" * filled + "-" * (self.width - filled)
        elapsed = time.time() - self.t0
        rate = self.done / elapsed if elapsed > 0 else 0.0
        eta = int((self.total - self.done) / rate) if rate > 0 else 0
        line = (
            "\r%s [%s] %s/%s %5.1f%% %ss eta=%ss %s"
            % (self.label, bar, self.done, self.total, pct * 100, int(elapsed), eta, self.extra)
        )
        pad = max(0, self._last_len - len(line))
        self._last_len = len(line)
        sys.stdout.write(line + " " * pad)
        sys.stdout.flush()

    def finish(self, msg: str = "") -> None:
        with self._lock:
            self.done = self.total
            self._render()
            sys.stdout.write(" " + msg + "\n")
            sys.stdout.flush()


def connect_target_apply(cmp_mod, cfg: dict):
    conn = cmp_mod.connect_mysql(
        cfg,
        host_keys=("TARGET_MYSQL_HOST", "TARGET_HOST"),
        port_keys=("TARGET_MYSQL_PORT", "TARGET_PORT"),
        user_keys=("TARGET_MYSQL_USER", "TARGET_USER"),
        password_keys=("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD"),
        db_keys=("TARGET_MYSQL_DATABASE", "TARGET_DB"),
        default_db="ng",
    )
    conn.read_timeout = 120
    conn.write_timeout = 120
    return conn


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
                "SELECT application_no, mobile, group_user_id, sn, "
                "status, last_paid_time, paid_off_time "
                "FROM application WHERE application_no IN (" + ph + ")",
                batch,
            )
            for row in cur.fetchall():
                apps[str(row["application_no"])] = row
            cur.execute(
                "SELECT application_no, period, roll_sequence, "
                "status, paid_amount, paid_time, paid_off_date "
                "FROM loan WHERE application_no IN (" + ph + ") "
                "AND period = 1 AND roll_sequence = 0",
                batch,
            )
            for row in cur.fetchall():
                loans[str(row["application_no"])] = row
    return apps, loans


def application_pk_from_row(row: dict) -> Optional[Dict[str, Any]]:
    mobile = row.get("mobile")
    group_user_id = row.get("group_user_id")
    sn = row.get("sn")
    if mobile is None or group_user_id is None or sn is None:
        return None
    return {
        "mobile": str(mobile),
        "group_user_id": int(group_user_id),
        "sn": str(sn),
    }


def loan_pk_from_row(row: dict, application_no: str) -> Dict[str, Any]:
    return {
        "application_no": str(row.get("application_no") or application_no),
        "period": int(row.get("period") or 1),
        "roll_sequence": int(row.get("roll_sequence") or 0),
    }


def enrich_plan_pks(cmp, conn, plan: List[dict]) -> Dict[str, int]:
    """已有 plan 缺主键时，按 application_no 回查目标库补 PK。"""
    need: List[str] = []
    for item in plan:
        if item.get("apply_application") and not item.get("application_pk"):
            need.append(str(item["application_no"]))
        if item.get("apply_loan") and not item.get("loan_pk"):
            need.append(str(item["application_no"]))
    need = sorted(set(need))
    if not need:
        return {"pk_lookup": 0}
    apps, loans = fetch_target_fixable(cmp, conn, need)
    stats = {"pk_lookup": len(need), "app_pk_missing": 0, "loan_pk_missing": 0}
    for item in plan:
        app_no = str(item["application_no"])
        if item.get("apply_application") and not item.get("application_pk"):
            pk = application_pk_from_row(apps.get(app_no) or {})
            if pk:
                item["application_pk"] = pk
            else:
                stats["app_pk_missing"] += 1
        if item.get("apply_loan") and not item.get("loan_pk"):
            row = loans.get(app_no) or {}
            if row:
                item["loan_pk"] = loan_pk_from_row(row, app_no)
            else:
                item["loan_pk"] = {
                    "application_no": app_no,
                    "period": 1,
                    "roll_sequence": 0,
                }
                stats["loan_pk_missing"] += 1
    return stats


def apply_application(conn, pk: Dict[str, Any], new_vals: Dict[str, Any]) -> int:
    if not new_vals or not pk:
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
    sql = (
        "UPDATE application SET " + ", ".join(sets)
        + " WHERE mobile = %s AND group_user_id = %s AND sn = %s"
    )
    params.extend([pk["mobile"], pk["group_user_id"], pk["sn"]])
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return int(cur.rowcount or 0)


def apply_loan(conn, pk: Dict[str, Any], new_vals: Dict[str, Any]) -> int:
    if not new_vals or not pk:
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
        + " WHERE application_no = %s AND period = %s AND roll_sequence = %s"
    )
    params.extend([pk["application_no"], pk["period"], pk["roll_sequence"]])
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
        app_pk = application_pk_from_row(act_app)
        loan_pk = loan_pk_from_row(act_loan, app_no) if act_loan else {
            "application_no": app_no,
            "period": 1,
            "roll_sequence": 0,
        }
        item = {
            "application_no": app_no,
            "pipeline": pipe,
            "application_pk": app_pk,
            "loan_pk": loan_pk,
            "application_changes": {k: {"old": v[0], "new": v[1]} for k, v in app_changes.items()},
            "loan_changes": {k: {"old": v[0], "new": v[1]} for k, v in loan_changes.items()},
            "apply_application": {k: v[1] for k, v in app_changes.items()},
            "apply_loan": {k: v[1] for k, v in loan_changes.items()},
        }
        plan.append(item)
        if app_changes and not app_pk:
            stats.setdefault("app_pk_missing", 0)
            stats["app_pk_missing"] += 1
        if app_changes:
            stats["app_need_fix"] += 1
            stats["app_fields"] += len(app_changes)
        if loan_changes:
            stats["loan_need_fix"] += 1
            stats["loan_fields"] += len(loan_changes)
    return plan, stats


def apply_plan_item(conn, item: dict) -> Dict[str, Any]:
    """单条 plan：在同一连接上 UPDATE，由调用方 commit。"""
    out = {
        "application_no": item.get("application_no"),
        "app_rows": 0,
        "loan_rows": 0,
        "app_pk_skip": 0,
    }
    if item.get("apply_application"):
        pk = item.get("application_pk")
        if not pk:
            out["app_pk_skip"] = 1
        else:
            out["app_rows"] = apply_application(conn, pk, item["apply_application"])
    if item.get("apply_loan"):
        pk = item.get("loan_pk") or {
            "application_no": item["application_no"],
            "period": 1,
            "roll_sequence": 0,
        }
        out["loan_rows"] = apply_loan(conn, pk, item["apply_loan"])
    return out


def apply_one_row(
    cmp_mod,
    cfg: dict,
    item: dict,
    retries: int = 3,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """每条 plan 独立连接，失败重试。"""
    last_err: Optional[str] = None
    for attempt in range(max(1, retries)):
        conn = None
        try:
            conn = connect_target_apply(cmp_mod, cfg)
            conn.autocommit(False)
            res = apply_plan_item(conn, item)
            conn.commit()
            return res, None
        except pymysql.Error as exc:
            last_err = str(exc)
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            if attempt + 1 < retries:
                time.sleep(0.5 * (attempt + 1))
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    return None, last_err


def filter_plan(plan: List[dict], application_nos: Optional[Set[str]]) -> List[dict]:
    if not application_nos:
        return plan
    return [p for p in plan if str(p.get("application_no")) in application_nos]


def load_application_no_set(path: Path) -> Set[str]:
    out: Set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("{"):
            rec = json.loads(line)
            app_no = str(rec.get("application_no") or "").strip()
        else:
            app_no = line.strip(",").split()[0]
        if app_no:
            out.add(app_no)
    return out


def run_apply(
    cmp_mod,
    cfg: dict,
    plan: List[dict],
    dry_run: bool,
    retries: int = 3,
    failed_file: Optional[Path] = None,
) -> Dict[str, int]:
    stats = {
        "app_updated": 0,
        "loan_updated": 0,
        "app_rows": 0,
        "loan_rows": 0,
        "app_pk_skip": 0,
        "errors": 0,
    }
    conn = connect_target_apply(cmp_mod, cfg)
    try:
        pk_stats = enrich_plan_pks(cmp_mod, conn, plan)
        stats.update({k: pk_stats.get(k, 0) for k in ("app_pk_missing", "pk_lookup", "loan_pk_missing")})
    finally:
        conn.close()

    if dry_run:
        for item in plan[:20]:
            print(
                "dry-run %(application_no)s app_pk=%(application_pk)s loan_pk=%(loan_pk)s "
                "app=%(application_changes)s loan=%(loan_changes)s"
                % {
                    "application_no": item.get("application_no"),
                    "application_pk": item.get("application_pk"),
                    "loan_pk": item.get("loan_pk"),
                    "application_changes": item.get("application_changes"),
                    "loan_changes": item.get("loan_changes"),
                },
                flush=True,
            )
        if len(plan) > 20:
            print("dry-run ... and %s more" % (len(plan) - 20), flush=True)
        return stats

    print(
        "apply start rows=%s (sequential, commit per row, retries=%s)"
        % (len(plan), retries),
        flush=True,
    )
    prog = Progress("apply", len(plan))
    errors: List[str] = []
    failed_items: List[dict] = []

    for item in plan:
        app_no = item.get("application_no")
        res, err = apply_one_row(cmp_mod, cfg, item, retries=retries)
        if err:
            stats["errors"] += 1
            failed_items.append(item)
            if len(errors) < 10:
                errors.append("%s: %s" % (app_no, err))
        else:
            stats["app_rows"] += int(res.get("app_rows") or 0)
            stats["loan_rows"] += int(res.get("loan_rows") or 0)
            stats["app_pk_skip"] += int(res.get("app_pk_skip") or 0)
            if int(res.get("app_rows") or 0):
                stats["app_updated"] += 1
            if int(res.get("loan_rows") or 0):
                stats["loan_updated"] += 1

        prog.set_extra("app=%s loan=%s err=%s" % (
            stats["app_updated"], stats["loan_updated"], stats["errors"],
        ))
        prog.increment(1)

    prog.finish("done")

    if failed_file and failed_items:
        with failed_file.open("w", encoding="utf-8") as fp:
            for row in failed_items:
                fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
        print("failed plan rows=%s -> %s" % (len(failed_items), failed_file), flush=True)

    if errors:
        print("apply errors sample:", flush=True)
        for e in errors:
            print("  %s" % e, flush=True)
    return stats


def load_plan(plan_path: Path) -> List[dict]:
    out: List[dict] = []
    for line in plan_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fix only status/paid fields on application & loan")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--report", help="input compare report jsonl")
    p.add_argument("--sanitize", help="write fixable-only report jsonl")
    p.add_argument("--list-file", help="application_no list (one per line)")
    p.add_argument("--plan-file", default="/tmp/order_paid_status_fix_plan.jsonl")
    p.add_argument("--apply-from-plan", action="store_true",
                   help="跳过 build-plan，直接加载 --plan-file 并 apply")
    p.add_argument("--apply", action="store_true", help="apply updates to target")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--retries", type=int, default=3, help="每条失败重试次数")
    p.add_argument("--failed-file", default="/tmp/order_paid_status_fix_failed.jsonl",
                   help="失败 plan 行输出路径")
    p.add_argument("--retry-file", help="仅 apply 此文件中的 application_no（补跑）")
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

    if args.apply_from_plan and not args.apply and not args.dry_run:
        print("use --apply or --dry-run with --apply-from-plan", flush=True)
        return 0

    application_nos: List[str] = []
    if not args.apply_from_plan:
        if args.list_file:
            application_nos.extend(
                cmp.read_application_nos(argparse.Namespace(
                    stdin=False, list_file=args.list_file, application_no=[],
                ))
            )
        elif args.report:
            application_nos.extend(application_nos_from_report(Path(args.report)))
        if not application_nos:
            print("need --list-file or --report (or --apply-from-plan)", file=sys.stderr)
            return 2

    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: %s" % env_path, file=sys.stderr)
        return 2
    cfg = cmp.load_env(env_path)
    cmp.print_connection_plan(cfg)

    t0 = time.time()
    if args.apply_from_plan:
        plan_path = Path(args.plan_file)
        if not plan_path.is_file():
            print("missing plan: %s" % plan_path, file=sys.stderr)
            return 2
        plan = load_plan(plan_path)
        print("loaded plan rows=%s file=%s" % (len(plan), plan_path), flush=True)
        if args.retry_file:
            retry_set = load_application_no_set(Path(args.retry_file))
            plan = filter_plan(plan, retry_set)
            print("retry filter rows=%s from %s" % (len(plan), args.retry_file), flush=True)
    else:
        plan, stats = build_plan(cmp, application_nos, cfg)
        plan_path = Path(args.plan_file)
        with plan_path.open("w", encoding="utf-8") as fp:
            for item in plan:
                fp.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
        print("plan rows=%s stats=%s file=%s" % (len(plan), stats, plan_path), flush=True)

    failed_path = Path(args.failed_file) if args.failed_file else None

    if args.dry_run:
        apply_stats = run_apply(
            cmp, cfg, plan, dry_run=True, retries=args.retries, failed_file=failed_path,
        )
        print("apply stats=%s elapsed=%.1fs" % (apply_stats, time.time() - t0), flush=True)
    elif args.apply:
        apply_stats = run_apply(
            cmp, cfg, plan, dry_run=False, retries=args.retries, failed_file=failed_path,
        )
        print("apply stats=%s elapsed=%.1fs" % (apply_stats, time.time() - t0), flush=True)
        if apply_stats.get("errors"):
            return 1
    else:
        print("plan only; add --apply or --dry-run", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
