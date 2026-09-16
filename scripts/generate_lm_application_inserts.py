#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 only_lm 列表拉 LM 源行，VT 后生成 application +（已放款）loan 的 INSERT / --apply。

敏感字段 mobile / BVN / 银行卡 / GAID → VT（vt_token_cache + /v2t，对齐 ng01 Flink）。
loan：disburseTime<>0 时写入（同 backfill_lm_orders_by_application_no.py）。

Usage: LM_MYSQL_* + SOURCE_*（cache 库）+ VT_BASE_URL；可选 LM_CORE_*。
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
LM_LOAN_CREATED_MS = 1785340800000
LM_LOAN_EXTRA_COLS = (
    "disburseTime", "lm_status", "amount", "disburseAmount", "repayment",
    "paidAmount", "paidTime", "dueDate", "appId", "applicationNo",
)
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
    END AS status,
    a.`disburseTime` AS disburseTime,
    a.`status` AS lm_status,
    a.`amount` AS amount,
    a.`disburseAmount` AS disburseAmount,
    a.`repayment` AS repayment,
    a.`paidAmount` AS paidAmount,
    a.`paidTime` AS paidTime,
    a.`dueDate` AS dueDate,
    a.`appId` AS appId,
    a.`applicationNo` AS applicationNo
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


def load_migrate_collection():
    path = HERE / "migrate_collection.py"
    spec = importlib.util.spec_from_file_location("migrate_collection", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


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


def vt_db_from_cfg(cfg: dict, mc_mod) -> Optional[Any]:
    host = (cfg.get("SOURCE_MYSQL_HOST") or cfg.get("SOURCE_HOST") or "").strip()
    if not host:
        return None
    return mc_mod.DB(
        mc_mod.DbConfig(
            host=host,
            port=int(cfg.get("SOURCE_MYSQL_PORT") or cfg.get("SOURCE_PORT") or 3306),
            user=cfg.get("SOURCE_MYSQL_USER") or cfg.get("SOURCE_USER") or "root",
            password=cfg.get("SOURCE_MYSQL_PASSWORD") or cfg.get("SOURCE_PASSWORD") or "",
            database=cfg.get("SOURCE_MYSQL_DATABASE") or "nigeria_backend",
        ),
        readonly=True,
    )


def build_vt_client(cfg: dict, mc_mod, *, use_cache: bool, dry_run: bool):
    vt_url = (
        (cfg.get("VT_BASE_URL") or cfg.get("VT_URL") or "").strip()
        or mc_mod.DEFAULT_VT_URL
    )
    db = vt_db_from_cfg(cfg, mc_mod) if use_cache else None
    return mc_mod.VtClient(vt_url, dry_run=dry_run, db=db)


def looks_like_vt_token(val: str) -> bool:
    s = (val or "").strip()
    return bool(s) and (s.startswith("tk_") or (not s.startswith("+") and len(s) >= 20))


def tokenize_application_fields(
    vt: Any,
    raw: dict,
    *,
    no_vt: bool,
) -> Optional[str]:
    """明文 → VT token；失败返回错误说明，成功写回 raw 的 mobile/id_number/bank/gaid。"""
    mobile_p = str(raw.get("mobile") or "").strip()
    bank_p = str(raw.get("bank_account_number") or "").strip()
    id_p = str(raw.get("id_number") or "").strip()
    gaid_p = str(raw.get("gaid_idfa") or "").strip()

    if no_vt:
        if not mobile_p or not bank_p:
            return "empty_mobile_or_bank"
        return None

    pairs: List[Tuple[int, str]] = []
    if mobile_p and not looks_like_vt_token(mobile_p):
        pairs.append((vt.VT_MOBILE, mobile_p))
    if bank_p and not looks_like_vt_token(bank_p):
        pairs.append((vt.VT_BANK, bank_p))
    if id_p and not looks_like_vt_token(id_p):
        pairs.append((vt.VT_ID_NUMBER, id_p))
    if gaid_p and not looks_like_vt_token(gaid_p):
        pairs.append((vt.VT_GAID, gaid_p))

    resolved: Dict[str, str] = {}
    if pairs:
        resolved = vt.resolve(pairs)

    if mobile_p:
        raw["mobile"] = (resolved.get(mobile_p) if not looks_like_vt_token(mobile_p) else mobile_p)[:28]
    if bank_p:
        raw["bank_account_number"] = resolved.get(bank_p) if not looks_like_vt_token(bank_p) else bank_p
    if id_p:
        raw["id_number"] = resolved.get(id_p) if not looks_like_vt_token(id_p) else id_p
    elif not id_p:
        raw["id_number"] = ""
    if gaid_p:
        tok = resolved.get(gaid_p) if not looks_like_vt_token(gaid_p) else gaid_p
        raw["gaid_idfa"] = tok or None
    else:
        raw["gaid_idfa"] = None

    if not str(raw.get("mobile") or "").strip():
        return "vt_mobile_empty"
    if not str(raw.get("bank_account_number") or "").strip():
        return "vt_bank_empty"
    if id_p and not str(raw.get("id_number") or "").strip():
        return "vt_id_number_empty"
    return None


def strip_lm_loan_fields(raw: dict) -> dict:
    return {k: raw.pop(k, None) for k in LM_LOAN_EXTRA_COLS if k in raw}


def resolve_vt_batch(vt: Any, raws: Sequence[dict]) -> None:
    pairs: List[Tuple[int, str]] = []
    for raw in raws:
        for field, vt_type in (
            ("mobile", vt.VT_MOBILE),
            ("bank_account_number", vt.VT_BANK),
            ("id_number", vt.VT_ID_NUMBER),
            ("gaid_idfa", vt.VT_GAID),
        ):
            plain = str(raw.get(field) or "").strip()
            if plain and not looks_like_vt_token(plain):
                pairs.append((vt_type, plain))
    if pairs:
        vt.resolve(pairs)


def unix_to_date(ts: int) -> Optional[str]:
    if ts <= 0:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def build_lm_loan_row(cmp_mod, lm: dict, application_no: str) -> Optional[dict]:
    if cmp_mod.to_int(lm.get("disburseTime")) == 0:
        return None
    sn = str(lm.get("applicationNo") or "")
    src_status = cmp_mod.to_int(lm.get("lm_status"))
    amount = cmp_mod.to_int(lm.get("amount"))
    disburse = cmp_mod.to_int(lm.get("disburseAmount"))
    admin_fee = max(amount - disburse, 0)
    principal = max(disburse, 0)
    total_amount = max(cmp_mod.to_int(lm.get("repayment")), 0)
    paid_amount = cmp_mod.to_int(lm.get("paidAmount")) if src_status in (17, 18, 19) else 0
    paid_ts = cmp_mod.to_int(lm.get("paidTime"))
    paid_time = paid_ts * 1000 if paid_ts > 0 else None
    paid_off_date = unix_to_date(paid_ts) if paid_ts > 0 else None
    disburse_ts = cmp_mod.to_int(lm.get("disburseTime"))
    due_ts = cmp_mod.to_int(lm.get("dueDate"))
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


def loan_output_path(app_output: Path) -> Path:
    name = app_output.name
    if "application" in name:
        return app_output.with_name(name.replace("application", "loan"))
    return app_output.with_name(app_output.stem + "_loan" + app_output.suffix)


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
    p = argparse.ArgumentParser(description="Generate INSERT SQL for missing LM application (+ loan) rows")
    p.add_argument("--env", default=str(HERE.parent / ".env"))
    p.add_argument("--list-file", default="")
    p.add_argument("--application-no", nargs="*", default=[])
    p.add_argument("--stdin", action="store_true")
    p.add_argument("--output", default="/tmp/insert_lm_application.sql")
    p.add_argument("--loan-output", default="", help="default: derive from --output (application→loan)")
    p.add_argument("--apply", action="store_true", help="execute INSERT on target (not dry-run)")
    p.add_argument("--insert-ignore", action="store_true", default=True)
    p.add_argument("--no-insert-ignore", action="store_false", dest="insert_ignore")
    p.add_argument("--no-vt", action="store_true", help="明文直写（仅调试；生产目标库勿用）")
    p.add_argument("--no-vt-cache", action="store_true", help="跳过 vt_token_cache，直接 /v2t")
    p.add_argument("--without-loan", action="store_true", help="不生成 loan INSERT")
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
    mc_mod = load_migrate_collection()

    base_cols = list(M.APPLICATION_COLS) + ["due_date", "due_date_final"]
    app_columns = resolve_columns(cfg, "application", base_cols)
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
    by_no: Dict[str, dict] = {}
    batch_size = max(1, args.batch_size)
    for i in range(0, len(keys), batch_size):
        chunk = keys[i:i + batch_size]
        by_no.update(fetch_rows(cmp_mod, cfg, chunk))

    hit_raws: List[dict] = []
    for app_no in app_nos:
        raw = by_no.get(app_no)
        if raw:
            hit_raws.append(raw)

    vt = None
    if not args.no_vt and hit_raws:
        vt = build_vt_client(
            cfg, mc_mod, use_cache=not args.no_vt_cache, dry_run=False,
        )
        print("# VT: resolving sensitive fields for {0} rows ...".format(len(hit_raws)), flush=True)
        resolve_vt_batch(vt, hit_raws)

    app_inserts: List[dict] = []
    loan_inserts: List[dict] = []
    diag: List[dict] = []
    stats = {
        "requested": len(app_nos),
        "source_hit": 0,
        "source_missing": 0,
        "skipped_vt": 0,
        "skipped_empty_bank": 0,
        "insert_application": 0,
        "insert_loan": 0,
        "skip_not_disbursed": 0,
        "skip_loan_exists": 0,
    }

    for app_no in app_nos:
        rec: Dict[str, Any] = {"application_no": app_no, "notes": []}
        raw = by_no.get(app_no)
        if not raw:
            stats["source_missing"] += 1
            rec["notes"].append("source_missing")
            diag.append(rec)
            continue
        stats["source_hit"] += 1

        raw = dict(raw)
        vt_err = tokenize_application_fields(vt, raw, no_vt=args.no_vt)
        if vt_err:
            stats["skipped_vt"] += 1
            rec["notes"].append(vt_err)
            diag.append(rec)
            continue
        if not str(raw.get("bank_account_number") or "").strip():
            stats["skipped_empty_bank"] += 1
            rec["notes"].append("empty_bank_account")
            diag.append(rec)
            continue

        lm_extra = strip_lm_loan_fields(raw)
        app_row = row_for_insert(raw, app_columns)
        app_inserts.append(app_row)
        stats["insert_application"] += 1
        rec["notes"].append("application_ok")

        if not args.without_loan:
            loan_row = build_lm_loan_row(cmp_mod, lm_extra, app_no)
            if loan_row is None:
                stats["skip_not_disbursed"] += 1
                rec["notes"].append("loan_skip_not_disbursed")
            elif args.apply and target_has_loan(cmp_mod, cfg, app_no):
                stats["skip_loan_exists"] += 1
                rec["notes"].append("loan_exists")
            else:
                loan_inserts.append({c: loan_row.get(c) for c in loan_columns})
                stats["insert_loan"] += 1
                rec["notes"].append("loan_ok")
        diag.append(rec)

    out_path = Path(args.output)
    app_lines = [
        "-- LM application backfill (VT={0}): {1} rows / requested {2}".format(
            not args.no_vt, len(app_inserts), len(app_nos),
        ),
        "-- generated by scripts/generate_lm_application_inserts.py",
        "SET NAMES utf8mb4;",
        "",
    ]
    for row in app_inserts:
        app_lines.append(format_insert("application", app_columns, row, args.insert_ignore))
    app_lines.append("")
    out_path.write_text("\n".join(app_lines), encoding="utf-8")

    loan_path = Path(args.loan_output) if args.loan_output else loan_output_path(out_path)
    if not args.without_loan:
        loan_lines = [
            "-- LM loan backfill: {0} rows (disburseTime<>0)".format(len(loan_inserts)),
            "SET NAMES utf8mb4;",
            "",
        ]
        for row in loan_inserts:
            loan_lines.append(format_insert("loan", loan_columns, row, args.insert_ignore))
        loan_lines.append("")
        loan_path.write_text("\n".join(loan_lines), encoding="utf-8")

    diag_path = Path(args.diag_file)
    with diag_path.open("w", encoding="utf-8") as fp:
        for row in diag:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")

    print("stats={0}".format(stats))
    print("application sql -> {0} ({1})".format(out_path, len(app_inserts)))
    if not args.without_loan:
        print("loan sql -> {0} ({1})".format(loan_path, len(loan_inserts)))
    print("diag -> {0}".format(diag_path))
    if not has_lm_core(cfg):
        print("NOTE: LM_CORE_MYSQL_* unset; submited_time/reviewed_time/last_paid_time use 0")
    if args.no_vt:
        print("WARN: --no-vt 明文写入，与生产 Flink/VT 目标不一致")

    if args.apply:
        if app_inserts:
            n = _insert_batch(cfg, "application", app_columns, app_inserts)
            print("inserted application rows={0}".format(n))
        if loan_inserts:
            n = _insert_batch(cfg, "loan", loan_columns, loan_inserts)
            print("inserted loan rows={0}".format(n))
    else:
        print("dry-run; add --apply to insert, or mysql < insert_*.sql on target")

    print("elapsed={0:.1f}s".format(time.time() - t0))
    return 0 if app_inserts else (1 if stats["source_missing"] else 0)


if __name__ == "__main__":
    raise SystemExit(main())
