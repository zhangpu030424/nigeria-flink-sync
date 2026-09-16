#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 diff/only_lm.txt 拉 LM 源行，生成目标 ng.application 的 INSERT SQL（或 --apply 直写）。

映射逻辑与老库迁移 application 校验 SQL 一致（coreAppId、group_user_id、status 等）；
application_no = ng + LPAD(appId,4) + '-' + applicationNo。

Usage（101 内网，.env 需 LM_MYSQL_*；可选 LM_CORE_MYSQL_* 补 submited/last_paid）:
  python3 scripts/generate_lm_application_inserts.py --env ./.env \\
    --list-file /tmp/lm_application_diff/diff/only_lm.txt \\
    --output /tmp/lm_application_diff/insert_application.sql

  python3 scripts/generate_lm_application_inserts.py --env ./.env \\
    --list-file /tmp/lm_application_diff/diff/only_lm.txt --apply
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
RECON = HERE / "reconcile"
sys.path.insert(0, str(RECON))

import env_util  # noqa: E402
import mapping as M  # noqa: E402
from reconcile_tables import _insert_batch, quote_cols, resolve_columns  # noqa: E402

PRODUCT_CALC_VER = "1"
REPAY_CALC_VER = "50"
ROLLOVER_CALC_VER = "49"

# 占位：{mkt_db} {core_db} {keys_in_clause}
LM_APPLICATION_SQL = """
SELECT
    CONCAT('ng', LPAD(a.`appId`, 4, '0'), '-', a.`applicationNo`) AS application_no,
    CASE
        WHEN a.`mobile` LIKE '+234%%' THEN a.`mobile`
        WHEN a.`mobile` LIKE '234%%'  THEN CONCAT('+', a.`mobile`)
        WHEN a.`mobile` LIKE '0%%'    THEN CONCAT('+234', SUBSTRING(a.`mobile`, 2))
        ELSE CONCAT('+234', a.`mobile`)
    END AS mobile,
    'ng01' AS bid,
    a.`appId` AS app_id,
    '1.0.0' AS app_version,
    a.`userId` AS user_id,
    COALESCE(
        (
            SELECT u2.`id`
            FROM `{mkt_db}`.`user` u2
            WHERE u2.`mobile` = u.`mobile`
              AND u2.`created` <= u.`created`
              AND u2.`appId` = COALESCE(cam.`main_app_id`, u.`appId`)
            ORDER BY u2.`created` ASC, u2.`id` ASC
            LIMIT 1
        ),
        a.`userId`
    ) AS group_user_id,
    a.`applicationNo` AS sn,
    0 AS is_test,
    CASE WHEN a.`repeatLoan` = 0 THEN 1 ELSE 0 END AS is_first_apply,
    0 AS is_auto_apply,
    IFNULL(ud.`bvn`, '') AS id_number,
    IFNULL(a.`gaid`, '') AS gaid_idfa,
    IFNULL(d.`deviceUUID`, '') AS device_uuid,
    NULL AS session_id,
    IFNULL(a.`bankCode`, '') AS bank_code,
    '' AS bank_account_name,
    IFNULL(a.`bankAccount`, '') AS bank_account_number,
    CAST(a.`productId` AS CHAR) AS product_id,
    'PROD-002-D7' AS product_scheme_id,
    '{product_calc}' AS product_calculator_version,
    '{repay_calc}' AS repay_calculator_version,
    '{rollover_calc}' AS rollover_calculator_version,
    JSON_OBJECT(
        'penalty_rate', 0.05, 'upfront_rate', 0.35,
        'interest_rate', 0, 'post_paid_rate', 0.05
    ) AS product_scheme_param,
    a.`term` AS term,
    1 AS periods,
    1 AS repayment_method,
    JSON_OBJECT(
        'roll_sequence', 0, 'period', 1,
        'principal', a.`shouldLoanAmount`, 'disbursed_amount', a.`disburseAmount`,
        'interest', 0, 'admin_fee', GREATEST(a.`amount` - a.`shouldLoanAmount`, 0),
        'service_fee', 0, 'tax_fee', 0, 'reduction_amount', 0,
        'total_amount', a.`repayment`, 'term', a.`term`,
        'start_date', DATE(FROM_UNIXTIME(a.`applyDate`)),
        'due_date', DATE(FROM_UNIXTIME(a.`dueDate`)),
        'roll_allowed', 0
    ) AS repayment_plan,
    a.`amount` AS credit_limit,
    a.`amount` AS loan_amount,
    a.`shouldLoanAmount` AS principal,
    a.`repayment` AS total_amount,
    a.`disburseAmount` AS disbursed_amount,
    a.`applyDate` * 1000 AS created_time,
    {submited_expr} AS submited_time,
    {reviewed_expr} AS reviewed_time,
    a.`disburseTime` * 1000 AS disbursed_time,
    {last_paid_expr} AS last_paid_time,
    a.`paidTime` * 1000 AS paid_off_time,
    (a.`applyDate` + 7 * 86400) * 1000 AS lock_expire_time,
    DATE(FROM_UNIXTIME(a.`dueDate`)) AS due_date,
    DATE(FROM_UNIXTIME(a.`dueDate`)) AS due_date_final,
    CASE a.`status`
        WHEN 0 THEN 1 WHEN 1 THEN 1 WHEN 2 THEN 1 WHEN 4 THEN 1 WHEN 5 THEN 3
        WHEN 3 THEN 5 WHEN 6 THEN 5 WHEN 8 THEN 7 WHEN 7 THEN 11 WHEN 9 THEN 13
        WHEN 12 THEN 15 WHEN 13 THEN 20 WHEN 14 THEN 20
        WHEN 15 THEN 23 WHEN 17 THEN 27 WHEN 18 THEN 27 WHEN 19 THEN 27
        ELSE a.`status`
    END AS status
FROM `{mkt_db}`.`application` a
INNER JOIN `{mkt_db}`.`user` u ON u.`id` = a.`userId`
LEFT JOIN (
    SELECT CAST(ac.`value` AS UNSIGNED) AS sub_app_id, ac.`appId` AS main_app_id
    FROM `{mkt_db}`.`app_config` ac
    INNER JOIN (
        SELECT CAST(`value` AS UNSIGNED) AS sub_app_id, MAX(`id`) AS max_id
        FROM `{mkt_db}`.`app_config` WHERE `key` = 'coreAppId'
        GROUP BY CAST(`value` AS UNSIGNED)
    ) pick ON pick.`max_id` = ac.`id`
) cam ON cam.`sub_app_id` = u.`appId`
LEFT JOIN `{mkt_db}`.`user_data` ud
    ON ud.`userId` = a.`userId`
   AND ud.`id` = (
       SELECT MAX(ud2.`id`) FROM `{mkt_db}`.`user_data` ud2 WHERE ud2.`userId` = a.`userId`
   )
LEFT JOIN `{mkt_db}`.`device` d ON d.`id` = a.`deviceId`
{core_joins}
WHERE ({keys_in_clause})
"""


def load_compare_module():
    path = HERE / "compare_orders_source_target.py"
    spec = importlib.util.spec_from_file_location("order_compare", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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
        s = str(line or "").strip().strip("'\",")
        if not s or s.startswith("#"):
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def has_lm_core(cfg: dict) -> bool:
    return bool((cfg.get("LM_CORE_MYSQL_HOST") or "").strip())


def build_sql(cfg: dict, keys: Sequence[Tuple[int, str]]) -> Tuple[str, List[Any]]:
    mkt_db = cfg.get("LM_MYSQL_DATABASE") or "ng_loan_market"
    core_db = cfg.get("LM_CORE_MYSQL_DATABASE") or "ng_loan_core"
    parts: List[str] = []
    where_params: List[Any] = []
    for app_id, sn in keys:
        parts.append("(a.`appId` = %s AND a.`applicationNo` = %s)")
        where_params.extend([app_id, sn])
    keys_in = " OR ".join(parts)

    if has_lm_core(cfg):
        sns_ph = ",".join(["%s"] * len(keys))
        core_joins = """
LEFT JOIN `{core_db}`.`application` ca ON ca.`ext_sn` = a.`applicationNo`
LEFT JOIN (
    SELECT ca2.`ext_sn`, MAX(rr.`repay_time`) AS last_paid_time
    FROM `{core_db}`.`application` ca2
    INNER JOIN `{core_db}`.`repay_record` rr ON rr.`sn` = ca2.`sn`
    WHERE ca2.`ext_sn` IN ({sns_ph})
    GROUP BY ca2.`ext_sn`
) lpt ON lpt.`ext_sn` = a.`applicationNo`
""".format(core_db=core_db, sns_ph=sns_ph)
        submited = "IFNULL(ca.`apply_time`, 0) * 1000"
        reviewed = "IFNULL(ca.`audit_time`, 0) * 1000"
        last_paid = "IFNULL(lpt.`last_paid_time`, 0) * 1000"
    else:
        core_joins = ""
        submited = "0"
        reviewed = "0"
        last_paid = "0"

    sql = LM_APPLICATION_SQL.format(
        mkt_db=mkt_db,
        product_calc=PRODUCT_CALC_VER,
        repay_calc=REPAY_CALC_VER,
        rollover_calc=ROLLOVER_CALC_VER,
        submited_expr=submited,
        reviewed_expr=reviewed,
        last_paid_expr=last_paid,
        core_joins=core_joins,
        keys_in_clause=keys_in,
    )
    params: List[Any] = []
    if has_lm_core(cfg):
        params.extend([sn for _, sn in keys])
    params.extend(where_params)
    return sql, params


def fetch_rows(cmp_mod, cfg: dict, keys: Sequence[Tuple[int, str]]) -> Dict[str, dict]:
    if not keys:
        return {}
    sql, params = build_sql(cfg, keys)
    conn = cmp_mod.connect_lm_source(cfg)
    out: Dict[str, dict] = {}
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            for row in cur.fetchall():
                app_no = str(row.get("application_no") or "")
                if app_no:
                    out[app_no] = dict(row)
    finally:
        conn.close()
    return out


def sql_literal(val: Any) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        if isinstance(val, float) and val.is_integer():
            return str(int(val))
        return str(int(val)) if isinstance(val, int) else repr(val)
    if isinstance(val, (datetime, date)):
        return "'{0}'".format(val.strftime("%Y-%m-%d %H:%M:%S") if isinstance(val, datetime) else val.isoformat())
    if isinstance(val, (dict, list)):
        val = json.dumps(val, ensure_ascii=False, separators=(",", ":"))
    s = str(val)
    s = s.replace("\\", "\\\\").replace("'", "''")
    return "'{0}'".format(s)


def row_for_insert(raw: dict, columns: Sequence[str]) -> dict:
    row: dict = {}
    for c in columns:
        v = raw.get(c)
        if c == "coupon_code" and v is None:
            v = ""
        if c in ("session_id", "bank_account_name") and v is None:
            v = ""
        row[c] = v
    return row


def format_insert(table: str, columns: Sequence[str], row: dict, ignore: bool) -> str:
    vals = ", ".join(sql_literal(row.get(c)) for c in columns)
    verb = "INSERT IGNORE" if ignore else "INSERT"
    return "{0} INTO `{1}` ({2}) VALUES ({3});".format(
        verb, table, quote_cols(columns), vals,
    )


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Generate INSERT SQL for missing LM application rows")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--list-file", default="")
    p.add_argument("--application-no", nargs="*", default=[])
    p.add_argument("--stdin", action="store_true")
    p.add_argument("--output", default="/tmp/insert_lm_application.sql")
    p.add_argument("--apply", action="store_true", help="execute INSERT on target (not dry-run)")
    p.add_argument("--insert-ignore", action="store_true", default=True)
    p.add_argument("--no-insert-ignore", action="store_false", dest="insert_ignore")
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--diag-file", default="/tmp/lm_application_insert_diag.jsonl")
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

    base_cols = list(M.APPLICATION_COLS) + ["due_date", "due_date_final"]
    columns = resolve_columns(cfg, "application", base_cols)

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
    by_no: Dict[str, dict] = {}
    batch_size = max(1, args.batch_size)
    for i in range(0, len(keys), batch_size):
        chunk = keys[i:i + batch_size]
        by_no.update(fetch_rows(cmp_mod, cfg, chunk))

    inserts: List[dict] = []
    diag: List[dict] = []
    stats = {
        "requested": len(app_nos),
        "source_hit": 0,
        "source_missing": 0,
        "skipped_empty_bank": 0,
    }

    for app_no in app_nos:
        rec = {"application_no": app_no, "notes": []}
        raw = by_no.get(app_no)
        if not raw:
            stats["source_missing"] += 1
            rec["notes"].append("source_missing")
            diag.append(rec)
            continue
        stats["source_hit"] += 1
        if not str(raw.get("bank_account_number") or "").strip():
            stats["skipped_empty_bank"] += 1
            rec["notes"].append("empty_bank_account")
            diag.append(rec)
            continue
        row = row_for_insert(raw, columns)
        inserts.append(row)
        rec["notes"].append("ok")
        diag.append(rec)

    out_path = Path(args.output)
    lines = [
        "-- LM application backfill: {0} rows (requested {1})".format(len(inserts), len(app_nos)),
        "-- generated by scripts/generate_lm_application_inserts.py",
        "SET NAMES utf8mb4;",
        "",
    ]
    for row in inserts:
        lines.append(format_insert("application", columns, row, args.insert_ignore))
    lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")

    diag_path = Path(args.diag_file)
    with diag_path.open("w", encoding="utf-8") as fp:
        for row in diag:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    print("stats={0}".format(stats))
    print("sql -> {0} ({1} statements)".format(out_path, len(inserts)))
    print("diag -> {0}".format(diag_path))
    if not has_lm_core(cfg):
        print("NOTE: LM_CORE_MYSQL_* unset; submited_time/reviewed_time/last_paid_time use 0")

    if args.apply:
        if inserts:
            n = _insert_batch(cfg, "application", columns, inserts)
            print("inserted application rows={0}".format(n))
    else:
        print("dry-run; add --apply to insert, or run the .sql on target")

    print("elapsed={0:.1f}s".format(time.time() - t0))
    return 0 if inserts else (1 if stats["source_missing"] else 0)


if __name__ == "__main__":
    raise SystemExit(main())
