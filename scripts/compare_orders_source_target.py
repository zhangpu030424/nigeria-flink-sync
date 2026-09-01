#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 application_no 列表对比源库与目标库 application/loan。

ng01 主链路 app_code：567, 568, 569, 571, 572, 573（查 nigeria_backend.user_order）
其余为贷超 LM（查 ng_loan_market.application）

env 需同时配置：
  ng01 源：SOURCE_MYSQL_*  → nigeria_backend
  LM 源：  LM_MYSQL_*      → ng_loan_market（101 上 .env 通常两套都有）
  目标：   TARGET_MYSQL_*  → ng

Usage（101 内网，推荐）:
  python3 scripts/compare_orders_source_target.py \\
    --env ./.env \\
    --list-file /tmp/order_list.txt \\
    --output /tmp/order_compare_report.jsonl

Usage（165 迁移机，若 env 只有 LM 源则 ng01 查数会失败）:
  python3 scripts/compare_orders_source_target.py \\
    --env ./ng_migration.env \\
    --list-file /tmp/order_list.txt
"""
import argparse
import json
import re
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

NG01_APP_CODES = frozenset({567, 568, 569, 571, 572, 573})
APP_NO_RE = re.compile(r"^ng(\d+)-(.+)$", re.IGNORECASE)

COMPARE_APP_FIELDS = ("status", "principal", "total_amount", "disbursed_amount", "last_paid_time")
COMPARE_LOAN_FIELDS = (
    "status", "principal", "admin_fee", "total_amount",
    "paid_amount", "paid_time",
)


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


def resolve_db_endpoint(
    cfg: Dict[str, str],
    *,
    host_keys: Sequence[str],
    port_keys: Sequence[str],
    user_keys: Sequence[str],
    db_keys: Sequence[str],
    default_db: str,
) -> Dict[str, str]:
    def pick(keys: Sequence[str], default: str = "") -> str:
        for key in keys:
            val = cfg.get(key)
            if val not in (None, ""):
                return val
        return default

    return {
        "host": pick(host_keys, "127.0.0.1"),
        "port": pick(port_keys, "3306"),
        "user": pick(user_keys, "root"),
        "database": pick(db_keys, default_db),
    }


def print_connection_plan(cfg: Dict[str, str]) -> None:
    lm_db = cfg.get("LM_MYSQL_DATABASE") or cfg.get("VT_TOKEN_DB") or "ng_loan_market"
    ng01 = resolve_db_endpoint(
        cfg,
        host_keys=("SOURCE_MYSQL_HOST", "SOURCE_HOST"),
        port_keys=("SOURCE_MYSQL_PORT", "SOURCE_PORT"),
        user_keys=("SOURCE_MYSQL_USER", "SOURCE_USER"),
        db_keys=("SOURCE_MYSQL_DATABASE",),
        default_db="nigeria_backend",
    )
    lm = resolve_db_endpoint(
        cfg,
        host_keys=("LM_MYSQL_HOST",),
        port_keys=("LM_MYSQL_PORT",),
        user_keys=("LM_MYSQL_USER",),
        db_keys=("LM_MYSQL_DATABASE",),
        default_db=lm_db,
    )
    target = resolve_db_endpoint(
        cfg,
        host_keys=("TARGET_MYSQL_HOST", "TARGET_HOST"),
        port_keys=("TARGET_MYSQL_PORT", "TARGET_PORT"),
        user_keys=("TARGET_MYSQL_USER", "TARGET_USER"),
        db_keys=("TARGET_MYSQL_DATABASE", "TARGET_DB"),
        default_db="ng",
    )
    print(
        "connections ng01=%(host)s:%(port)s/%(database)s user=%(user)s"
        % ng01,
        flush=True,
    )
    if lm.get("host"):
        print(
            "connections lm=%(host)s:%(port)s/%(database)s user=%(user)s"
            % lm,
            flush=True,
        )
    else:
        print(
            "connections lm=MISSING LM_MYSQL_HOST (will not query ng_loan_market)",
            flush=True,
        )
    print(
        "connections target=%(host)s:%(port)s/%(database)s user=%(user)s"
        % target,
        flush=True,
    )


def connect_mysql(
    cfg: Dict[str, str],
    *,
    host_keys: Sequence[str],
    port_keys: Sequence[str],
    user_keys: Sequence[str],
    password_keys: Sequence[str],
    db_keys: Sequence[str],
    default_db: str,
):
    def pick(keys: Sequence[str], default: str = "") -> str:
        for key in keys:
            val = cfg.get(key)
            if val not in (None, ""):
                return val
        return default

    return pymysql.connect(
        host=pick(host_keys, "127.0.0.1"),
        port=int(pick(port_keys, "3306")),
        user=pick(user_keys, "root"),
        password=pick(password_keys, ""),
        database=pick(db_keys, default_db),
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=60,
        read_timeout=3600,
        write_timeout=3600,
        autocommit=True,
    )


def connect_ng01_source(cfg: Dict[str, str]):
    return connect_mysql(
        cfg,
        host_keys=("SOURCE_MYSQL_HOST", "SOURCE_HOST"),
        port_keys=("SOURCE_MYSQL_PORT", "SOURCE_PORT"),
        user_keys=("SOURCE_MYSQL_USER", "SOURCE_USER"),
        password_keys=("SOURCE_MYSQL_PASSWORD", "SOURCE_PASSWORD"),
        db_keys=("SOURCE_MYSQL_DATABASE",),
        default_db="nigeria_backend",
    )


def connect_lm_source(cfg: Dict[str, str]):
    db = cfg.get("LM_MYSQL_DATABASE") or cfg.get("VT_TOKEN_DB") or "ng_loan_market"
    if not (cfg.get("LM_MYSQL_HOST") or "").strip():
        raise RuntimeError(
            "LM_MYSQL_HOST not set; 101 上请用含 LM 源库的 .env，勿用只有 SOURCE_* 的 ng_migration.env"
        )
    merged = dict(cfg)
    merged.setdefault("LM_MYSQL_DATABASE", db)
    return connect_mysql(
        merged,
        host_keys=("LM_MYSQL_HOST",),
        port_keys=("LM_MYSQL_PORT",),
        user_keys=("LM_MYSQL_USER",),
        password_keys=("LM_MYSQL_PASSWORD",),
        db_keys=("LM_MYSQL_DATABASE",),
        default_db=db,
    )


def connect_target(cfg: Dict[str, str]):
    return connect_mysql(
        cfg,
        host_keys=("TARGET_MYSQL_HOST", "TARGET_HOST"),
        port_keys=("TARGET_MYSQL_PORT", "TARGET_PORT"),
        user_keys=("TARGET_MYSQL_USER", "TARGET_USER"),
        password_keys=("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD"),
        db_keys=("TARGET_MYSQL_DATABASE", "TARGET_DB"),
        default_db="ng",
    )


def parse_application_no(application_no: str) -> Tuple[int, str]:
    m = APP_NO_RE.match(str(application_no or "").strip())
    if not m:
        raise ValueError("bad application_no: %s" % application_no)
    return int(m.group(1)), m.group(2)


def pipeline_for_app(app_id: int) -> str:
    return "ng01" if app_id in NG01_APP_CODES else "lm"


def to_int(val: Any, default: int = 0) -> int:
    if val is None or val == "":
        return default
    if isinstance(val, Decimal):
        return int(val)
    try:
        return int(val)
    except (TypeError, ValueError):
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return default


def to_opt_int(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    n = to_int(val, 0)
    return n if n > 0 else None


def to_money_minor(val: Any) -> int:
    if val is None or val == "":
        return 0
    if isinstance(val, Decimal):
        return int(val.quantize(Decimal("1")))
    try:
        return int(round(float(val), 0))
    except (TypeError, ValueError):
        return 0


def dt_to_ms(val: Any) -> Optional[int]:
    if val is None:
        return None
    if hasattr(val, "timestamp"):
        return int(val.timestamp() * 1000)
    return None


def map_ng01_app_status(risk_order_status: int, is_overdue_max: int) -> int:
    if risk_order_status == 10:
        return 23 if is_overdue_max else 20
    table = {
        2: 3, 4: 5, 6: 13, 8: 15, 11: 23, 40: 25,
        20: 27, 30: 27, 50: 27,
    }
    return table.get(risk_order_status, 1)


def map_ng01_loan_status(risk_order_status: int, is_overdue: int, repaid_amount: Any) -> int:
    repaid = to_money_minor(repaid_amount)
    if risk_order_status == 10:
        if is_overdue:
            return 23
        if repaid == 0:
            return 20
        return 24
    if risk_order_status == 11:
        return 23
    if risk_order_status == 40:
        return 25
    if risk_order_status in (20, 30, 50):
        return 27
    return 20


def map_lm_status(status: int) -> int:
    table = {
        8: 9,
        11: 20, 13: 20, 14: 20, 16: 20,
        15: 23,
        17: 27, 18: 27, 19: 27,
    }
    return table.get(status, 20)


def chunks(items: Sequence[Any], size: int) -> Iterable[List[Any]]:
    n = max(1, size)
    for i in range(0, len(items), n):
        yield list(items[i:i + n])


def read_application_nos(args: argparse.Namespace) -> List[str]:
    lines: List[str] = []
    if args.stdin:
        lines.extend(sys.stdin.read().splitlines())
    if args.list_file:
        lines.extend(Path(args.list_file).read_text(encoding="utf-8").splitlines())
    if args.application_no:
        lines.extend(args.application_no)
    out: List[str] = []
    seen: Set[str] = set()
    for line in lines:
        s = str(line or "").strip().strip(",")
        if not s or s.startswith("#"):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


NG01_SOURCE_SQL = """
SELECT
    o.order_no,
    o.app_code,
    o.risk_order_status,
    o.received,
    o.repayment,
    o.disburse_time,
    o.settled_time,
    COALESCE(inst.is_overdue_max, 0) AS is_overdue_max,
    ur_lp.callback_time AS last_paid_time,
    i.current_period,
    COALESCE(i.is_overdue, 0) AS is_overdue,
    i.repaid_amount,
    i.received AS inst_received,
    i.interests,
    i.poundage_fees,
    i.penalty_amount,
    i.amt_due,
    i.repayment_time,
    ur_cb.callback_time AS period_paid_time
FROM user_order o
LEFT JOIN (
    SELECT user_order_id, MAX(COALESCE(is_overdue, 0)) AS is_overdue_max
    FROM user_order_installment
    GROUP BY user_order_id
) inst ON inst.user_order_id = o.id
LEFT JOIN user_order_installment i
    ON i.user_order_id = o.id AND i.current_period = 1
LEFT JOIN (
    SELECT order_no, MAX(callback_time) AS callback_time
    FROM user_repay
    WHERE status = 2 AND callback_time IS NOT NULL
      AND order_no IS NOT NULL AND TRIM(order_no) <> ''
    GROUP BY order_no
) ur_lp ON ur_lp.order_no = o.order_no
LEFT JOIN (
    SELECT order_no, current_period, MAX(callback_time) AS callback_time
    FROM user_repay
    WHERE status = 2 AND callback_time IS NOT NULL
      AND order_no IS NOT NULL AND TRIM(order_no) <> ''
    GROUP BY order_no, current_period
) ur_cb ON ur_cb.order_no = o.order_no AND ur_cb.current_period = i.current_period
WHERE o.order_no IN ({placeholders})
"""


def fetch_ng01_source(conn, order_nos: Sequence[str]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for batch in chunks(list(order_nos), 200):
        ph = ",".join(["%s"] * len(batch))
        sql = NG01_SOURCE_SQL.format(placeholders=ph)
        with conn.cursor() as cur:
            cur.execute(sql, batch)
            for row in cur.fetchall():
                app_code = to_int(row.get("app_code"))
                order_no = str(row.get("order_no"))
                application_no = "ng{0:04d}-{1}".format(app_code, order_no)
                risk_status = to_int(row.get("risk_order_status"))
                is_overdue_max = to_int(row.get("is_overdue_max"))
                is_overdue = to_int(row.get("is_overdue"))
                repaid = row.get("repaid_amount")

                admin_fee = to_money_minor(row.get("poundage_fees"))
                penalty = to_money_minor(row.get("penalty_amount"))
                amt_due = to_money_minor(row.get("amt_due"))
                total_amount = amt_due + penalty if (amt_due or penalty) else to_money_minor(row.get("repayment"))
                principal = to_money_minor(row.get("inst_received") or row.get("received"))
                paid_amount = to_money_minor(repaid)

                paid_time = dt_to_ms(row.get("period_paid_time"))
                if paid_time is None and risk_status in (20, 30, 50):
                    paid_time = dt_to_ms(row.get("settled_time"))

                out[application_no] = {
                    "pipeline": "ng01",
                    "application_no": application_no,
                    "order_no": order_no,
                    "app_id": app_code,
                    "source_risk_order_status": risk_status,
                    "source_is_overdue_max": is_overdue_max,
                    "source_is_overdue_p1": is_overdue,
                    "source_repaid_amount": str(repaid) if repaid is not None else "0",
                    "expected_application": {
                        "status": map_ng01_app_status(risk_status, is_overdue_max),
                        "principal": principal,
                        "total_amount": to_money_minor(row.get("repayment")),
                        "disbursed_amount": to_money_minor(row.get("received")),
                        "last_paid_time": dt_to_ms(row.get("last_paid_time")),
                    },
                    "expected_loan": {
                        "status": map_ng01_loan_status(risk_status, is_overdue, repaid),
                        "principal": principal,
                        "admin_fee": admin_fee,
                        "total_amount": total_amount,
                        "paid_amount": paid_amount,
                        "paid_time": paid_time,
                    },
                }
    return out


LM_SOURCE_SQL = """
SELECT
    appId,
    applicationNo,
    status,
    amount,
    disburseAmount,
    repayment,
    paidAmount,
    paidTime,
    disburseTime,
    dueDate
FROM application
WHERE appId = %s
  AND applicationNo IN ({placeholders})
  AND disburseTime <> 0
"""


def _lm_row_to_entry(row: dict) -> Tuple[str, dict]:
    app_id = to_int(row.get("appId"))
    sn = str(row.get("applicationNo"))
    application_no = "ng{0:04d}-{1}".format(app_id, sn)
    src_status = to_int(row.get("status"))
    amount = to_int(row.get("amount"))
    disburse = to_int(row.get("disburseAmount"))
    admin_fee = max(amount - disburse, 0)
    paid_amount = to_int(row.get("paidAmount")) if src_status in (17, 18, 19) else 0
    paid_time = to_int(row.get("paidTime")) * 1000 if to_int(row.get("paidTime")) > 0 else None
    principal = max(disburse, 0)
    total_amount = max(to_int(row.get("repayment")), 0)
    return application_no, {
        "pipeline": "lm",
        "application_no": application_no,
        "app_id": app_id,
        "sn": sn,
        "source_status": src_status,
        "expected_application": {
            "status": map_lm_status(src_status),
            "principal": principal,
            "total_amount": total_amount,
            "disbursed_amount": principal,
            "last_paid_time": paid_time,
        },
        "expected_loan": {
            "status": map_lm_status(src_status),
            "principal": principal,
            "admin_fee": admin_fee,
            "total_amount": total_amount,
            "paid_amount": paid_amount,
            "paid_time": paid_time,
        },
    }


def fetch_lm_source(conn, keys: Sequence[Tuple[int, str]]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    by_app: Dict[int, List[str]] = {}
    for app_id, sn in keys:
        by_app.setdefault(int(app_id), []).append(str(sn))

    with conn.cursor() as cur:
        for app_id, sns in by_app.items():
            uniq_sns = sorted(set(sns))
            for batch in chunks(uniq_sns, 200):
                ph = ",".join(["%s"] * len(batch))
                sql = LM_SOURCE_SQL.format(placeholders=ph)
                cur.execute(sql, [app_id] + batch)
                for row in cur.fetchall():
                    application_no, entry = _lm_row_to_entry(row)
                    out[application_no] = entry
    return out


def fetch_target(conn, application_nos: Sequence[str]) -> Tuple[Dict[str, dict], Dict[str, dict]]:
    apps: Dict[str, dict] = {}
    loans: Dict[str, dict] = {}
    for batch in chunks(list(application_nos), 200):
        ph = ",".join(["%s"] * len(batch))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT application_no, status, principal, total_amount, disbursed_amount, last_paid_time "
                "FROM application WHERE application_no IN (" + ph + ")",
                batch,
            )
            for row in cur.fetchall():
                apps[str(row["application_no"])] = row
            cur.execute(
                "SELECT application_no, loan_no, period, roll_sequence, status, principal, admin_fee, "
                "total_amount, paid_amount, paid_time "
                "FROM loan WHERE application_no IN (" + ph + ") "
                "AND period = 1 AND roll_sequence = 0",
                batch,
            )
            for row in cur.fetchall():
                loans[str(row["application_no"])] = row
    return apps, loans


def norm_target_val(field: str, val: Any) -> Any:
    if field in ("last_paid_time", "paid_time"):
        return to_opt_int(val)
    if field in ("status", "principal", "total_amount", "disbursed_amount", "admin_fee", "paid_amount"):
        return to_int(val, 0)
    return val


def diff_record(
    application_no: str,
    pipeline: str,
    entity: str,
    expected: Optional[dict],
    actual: Optional[dict],
    fields: Sequence[str],
    source_meta: Optional[dict] = None,
) -> dict:
    mismatches: List[dict] = []
    if expected is None and actual is None:
        return {
            "application_no": application_no,
            "pipeline": pipeline,
            "entity": entity,
            "result": "missing_both",
            "mismatches": mismatches,
        }
    if expected is None:
        return {
            "application_no": application_no,
            "pipeline": pipeline,
            "entity": entity,
            "result": "missing_source",
            "target": actual,
            "mismatches": mismatches,
        }
    if actual is None:
        return {
            "application_no": application_no,
            "pipeline": pipeline,
            "entity": entity,
            "result": "missing_target",
            "expected": expected,
            "mismatches": mismatches,
        }
    for field in fields:
        exp = norm_target_val(field, expected.get(field))
        act = norm_target_val(field, actual.get(field))
        if exp != act:
            mismatches.append({
                "field": field,
                "expected": exp,
                "actual": act,
            })
    out = {
        "application_no": application_no,
        "pipeline": pipeline,
        "entity": entity,
        "result": "ok" if not mismatches else "mismatch",
        "mismatches": mismatches,
    }
    if source_meta:
        out["source_meta"] = source_meta
    return out


def summarize(records: List[dict]) -> dict:
    stats = {
        "total": 0,
        "ng01": 0,
        "lm": 0,
        "bad_application_no": 0,
        "app_ok": 0,
        "app_mismatch": 0,
        "app_missing_source": 0,
        "app_missing_target": 0,
        "loan_ok": 0,
        "loan_mismatch": 0,
        "loan_missing_source": 0,
        "loan_missing_target": 0,
        "loan_status_mismatch": 0,
    }
    seen: Set[str] = set()
    for rec in records:
        app_no = rec.get("application_no")
        if app_no not in seen:
            seen.add(app_no)
            stats["total"] += 1
            if rec.get("pipeline") == "ng01":
                stats["ng01"] += 1
            elif rec.get("pipeline") == "lm":
                stats["lm"] += 1
        entity = rec.get("entity")
        result = rec.get("result")
        key = "{0}_{1}".format(entity, result)
        if key in stats:
            stats[key] += 1
        if entity == "loan" and result == "mismatch":
            for m in rec.get("mismatches") or []:
                if m.get("field") == "status":
                    stats["loan_status_mismatch"] += 1
                    break
    return stats


def print_summary(stats: dict, mismatch_only: List[dict]) -> None:
    print("summary=%s" % json.dumps(stats, ensure_ascii=False), flush=True)
    print("--- mismatches (first 50) ---", flush=True)
    n = 0
    for rec in mismatch_only:
        if rec.get("result") not in ("mismatch", "missing_target", "missing_source"):
            continue
        print(
            "%(pipeline)s %(application_no)s %(entity)s %(result)s %(detail)s"
            % {
                "pipeline": rec.get("pipeline"),
                "application_no": rec.get("application_no"),
                "entity": rec.get("entity"),
                "result": rec.get("result"),
                "detail": json.dumps(
                    rec.get("mismatches") or rec.get("expected") or rec.get("target") or rec.get("source_meta"),
                    ensure_ascii=False,
                ),
            },
            flush=True,
        )
        n += 1
        if n >= 50:
            break


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compare source vs target for application_no list")
    p.add_argument("--env", default=str(REPO / ".env"))
    p.add_argument("--list-file", help="one application_no per line")
    p.add_argument("--stdin", action="store_true", help="read application_no list from stdin")
    p.add_argument("--application-no", action="append", default=[], help="repeatable single application_no")
    p.add_argument("--output", default="/tmp/order_compare_report.jsonl")
    p.add_argument("--batch-size", type=int, default=200)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: %s" % env_path, file=sys.stderr)
        return 2

    application_nos = read_application_nos(args)
    if not application_nos:
        print("no application_no input", file=sys.stderr)
        return 2

    cfg = load_env(env_path)
    print_connection_plan(cfg)

    ng01_keys: List[str] = []
    lm_keys: List[Tuple[int, str]] = []
    pipeline_by_no: Dict[str, str] = {}
    bad: List[str] = []

    for app_no in application_nos:
        try:
            app_id, sn = parse_application_no(app_no)
        except ValueError:
            bad.append(app_no)
            continue
        pipe = pipeline_for_app(app_id)
        pipeline_by_no[app_no] = pipe
        if pipe == "ng01":
            ng01_keys.append(sn)
        else:
            lm_keys.append((app_id, sn))

    ng01_order_set = sorted(set(ng01_keys))
    lm_key_set = sorted(set(lm_keys))

    print(
        "input total=%s ng01_orders=%s lm_orders=%s bad=%s"
        % (len(application_nos), len(ng01_order_set), len(lm_key_set), len(bad)),
        flush=True,
    )

    t0 = time.time()
    source_by_no: Dict[str, dict] = {}

    if ng01_order_set:
        conn = connect_ng01_source(cfg)
        try:
            source_by_no.update(fetch_ng01_source(conn, ng01_order_set))
        finally:
            conn.close()
        print("ng01 source hits=%s" % sum(1 for k in source_by_no if pipeline_by_no.get(k) == "ng01"), flush=True)

    if lm_key_set:
        conn = connect_lm_source(cfg)
        try:
            source_by_no.update(fetch_lm_source(conn, lm_key_set))
        finally:
            conn.close()
        print("lm source hits=%s" % sum(1 for k in source_by_no if pipeline_by_no.get(k) == "lm"), flush=True)

    conn = connect_target(cfg)
    try:
        target_apps, target_loans = fetch_target(conn, application_nos)
    finally:
        conn.close()
    print("target application=%s loan=%s" % (len(target_apps), len(target_loans)), flush=True)

    records: List[dict] = []
    for app_no in application_nos:
        if app_no in bad:
            records.append({
                "application_no": app_no,
                "pipeline": "unknown",
                "entity": "meta",
                "result": "bad_application_no",
                "mismatches": [],
            })
            continue
        pipe = pipeline_by_no[app_no]
        src = source_by_no.get(app_no)
        source_meta = None
        if src:
            source_meta = {k: v for k, v in src.items() if k.startswith("source_")}

        exp_app = src.get("expected_application") if src else None
        exp_loan = src.get("expected_loan") if src else None
        act_app = target_apps.get(app_no)
        act_loan = target_loans.get(app_no)

        records.append(diff_record(app_no, pipe, "application", exp_app, act_app, COMPARE_APP_FIELDS, source_meta))
        records.append(diff_record(app_no, pipe, "loan", exp_loan, act_loan, COMPARE_LOAN_FIELDS, source_meta))

    out_path = Path(args.output)
    with out_path.open("w", encoding="utf-8") as fp:
        for rec in records:
            fp.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

    stats = summarize(records)
    stats["bad_application_no"] = len(bad)
    stats["elapsed_sec"] = round(time.time() - t0, 1)
    stats["report"] = str(out_path)

    mismatch_only = [r for r in records if r.get("result") != "ok"]
    print_summary(stats, mismatch_only)
    return 1 if any(r.get("result") not in ("ok",) for r in records) else 0


if __name__ == "__main__":
    sys.exit(main())
