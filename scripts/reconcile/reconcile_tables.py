#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""nigeria-flink-sync 表级对账：以源库业务表为期望，比对目标库并出 plan/apply。

设计对齐旧仓库 reconcile_tables.py：
  - 阶段：load-target / plan / apply / all
  - 目标切片：user_id/group_user_id > USER_ID_OFFSET（默认 1亿）
  - app 范围默认：567,568,569,571,572,573
  - 期望行直接查源表（user / user_order / …），映射对齐 Flink SQL；不读 *_sync_staging 宽表
  - VT 未命中（token 空）→ 跳过并记日志
  - insert=INSERT；update=按主键 UPDATE

Usage:
  ./scripts/reconcile/reconcile.sh --table user --phase plan
  ./scripts/reconcile/reconcile.sh --table application --phase all --apply
  ./scripts/reconcile/reconcile.sh --all-tables --phase plan --since-date 2026-01-01
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import env_util
import mapping as M
import source_queries

SUPPORTED_TABLES = (
    "user", "user_info", "user_bankcard", "user_product", "application", "loan",
)

USER_PK = ("mobile", "app_id", "closed_time")
USER_INFO_PK = ("user_id",)
BANKCARD_PK = ("group_user_id", "bank_account_number")
PRODUCT_PK = ("group_user_id", "product_id")
APPLICATION_PK = ("mobile", "group_user_id", "sn")
LOAN_PK = ("application_no", "period", "roll_sequence")

DEFAULT_SINCE_DATE = "2026-01-01"

AMOUNT_CLAMP_COLS = frozenset({
    "credit_limit", "loan_amount", "principal", "total_amount", "disbursed_amount",
    "interest", "admin_fee", "penalty_amount", "reduction_amount", "paid_amount",
    "credit_amount", "unpaid_amount", "locked_amount", "available_amount",
})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def since_date_to_ms(since_date: str) -> int:
    return int(datetime.strptime(since_date, "%Y-%m-%d").timestamp()) * 1000


def parse_int_list(raw: str, default: Tuple[int, ...]) -> Tuple[int, ...]:
    if not str(raw or "").strip():
        return default
    out: List[int] = []
    for part in str(raw).split(","):
        part = part.strip()
        if part:
            out.append(int(part))
    return tuple(out) if out else default


def plan_date_tag(plan_date: Optional[str] = None) -> str:
    s = (plan_date or "").strip().replace("-", "")
    if not s:
        return datetime.now().strftime("%Y%m%d")
    if len(s) != 8 or not s.isdigit():
        raise ValueError("plan_date must be YYYYMMDD or YYYY-MM-DD, got %r" % plan_date)
    return s


def default_paths(table: str, plan_date: Optional[str] = None) -> Dict[str, str]:
    d = plan_date_tag(plan_date)
    return {
        "target_cache": "/tmp/flink_reconcile_{0}_target.jsonl".format(table),
        "plan_file": "/tmp/flink_reconcile_{0}_plan_{1}.jsonl".format(table, d),
    }


def _now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class ReconcileLogger:
    def __init__(self, log_dir: Path, table: str) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        self.main_path = log_dir / "reconcile_{0}.log".format(table)
        self.vt_skip_path = log_dir / "vt_skip_{0}.jsonl".format(table)
        self.apply_path = log_dir / "apply_{0}.jsonl".format(table)
        self._main_fp = self.main_path.open("a", encoding="utf-8")
        self._lock = threading.Lock()

    def close(self) -> None:
        with self._lock:
            self._main_fp.close()

    def log(self, msg: str) -> None:
        line = "[{0}] {1}".format(_now_ts(), msg)
        print(line, flush=True)
        with self._lock:
            self._main_fp.write(line + "\n")
            self._main_fp.flush()

    def vt_skip(self, record: dict) -> None:
        record = dict(record)
        record["ts"] = _now_ts()
        with self._lock:
            with self.vt_skip_path.open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def apply_audit(self, record: dict) -> None:
        record = dict(record)
        record["ts"] = _now_ts()
        with self._lock:
            with self.apply_path.open("a", encoding="utf-8") as fp:
                fp.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def write_jsonl(path: Path, rows: Iterable[dict]) -> int:
    n = 0
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
            n += 1
    return n


def read_jsonl(path: Path) -> List[dict]:
    out: List[dict] = []
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def quote_cols(cols: Sequence[str]) -> str:
    return ", ".join("`{0}`".format(c) for c in cols)


def normalize_date_value(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d")
    if hasattr(val, "year") and hasattr(val, "month") and hasattr(val, "day"):
        try:
            return "{0:04d}-{1:02d}-{2:02d}".format(int(val.year), int(val.month), int(val.day))
        except Exception:
            pass
    s = str(val).strip()
    if not s or s.lower() in ("none", "null"):
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    return s


def normalize_json(val: Any) -> Optional[str]:
    if val is None or val == "":
        return None
    try:
        if isinstance(val, (dict, list)):
            obj = val
        else:
            obj = json.loads(str(val))
        return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except Exception:
        s = str(val).strip()
        return s if s else None


def normalize_cell(col: str, val: Any) -> Any:
    if val is None:
        return None
    if col in ("info", "schemes", "product_scheme_param", "repayment_plan"):
        return normalize_json(val)
    if col in (
        "created_time", "submited_time", "reviewed_time", "disbursed_time",
        "last_paid_time", "paid_off_time", "lock_expire_time", "reg_time",
        "closed_time", "test_flag", "paid_time",
    ):
        try:
            return int(val) if val not in (None, "") else None
        except (TypeError, ValueError):
            return 0 if col != "paid_time" else None
    if col in ("start_date", "due_date", "due_date_final", "paid_off_date"):
        return normalize_date_value(val)
    if col in (
        "is_open", "is_default", "credit_amount", "unpaid_amount",
        "locked_amount", "available_amount", "is_test", "is_first_apply",
        "is_auto_apply", "term", "periods", "repayment_method", "status",
        "loan_amount", "principal", "total_amount", "disbursed_amount",
        "period", "roll_sequence", "interest", "admin_fee",
        "penalty_amount", "reduction_amount", "paid_amount", "credit_limit",
    ):
        try:
            return int(val)
        except (TypeError, ValueError):
            return 0
    if col in ("app_id", "group_user_id", "info_user_id", "user_id", "id"):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, datetime):
        return val.strftime("%Y-%m-%d %H:%M:%S")
    return val


def row_diff(expected: dict, actual: dict, compare_cols: Sequence[str]) -> List[str]:
    diffs: List[str] = []
    for c in compare_cols:
        if normalize_cell(c, expected.get(c)) != normalize_cell(c, actual.get(c)):
            diffs.append(c)
    return diffs


def user_key(row: dict) -> Tuple[str, int, int]:
    return (
        str(row["mobile"]),
        int(row.get("app_id") or 0),
        int(row.get("closed_time") if row.get("closed_time") is not None else 0),
    )


def bankcard_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["bank_account_number"]))


def product_key(row: dict) -> Tuple[int, str]:
    return (int(row["group_user_id"]), str(row["product_id"]))


def application_key(row: dict) -> Tuple[str, int, str]:
    return (str(row["mobile"]), int(row["group_user_id"]), str(row["sn"]))


def loan_key(row: dict) -> Tuple[str, int, int]:
    return (
        str(row["application_no"]),
        int(row.get("period") if row.get("period") is not None else 1),
        int(row.get("roll_sequence") if row.get("roll_sequence") is not None else 0),
    )


def application_no_prefix(app_id: int) -> str:
    return "ng{0:04d}-".format(int(app_id))


def app_no_in_include(application_no: str, include_apps: Tuple[int, ...]) -> bool:
    if not include_apps:
        return True
    app_no = str(application_no or "").strip().lower()
    for aid in include_apps:
        if app_no.startswith(application_no_prefix(aid).lower()):
            return True
    return False


def clamp_amounts(row: dict) -> dict:
    for c in AMOUNT_CLAMP_COLS:
        if c not in row:
            continue
        v = row.get(c)
        if v is None or isinstance(v, bool):
            continue
        try:
            if float(v) < 0:
                row[c] = 0
        except (TypeError, ValueError):
            continue
    return row


# ---------------------------------------------------------------------------
# target load
# ---------------------------------------------------------------------------

def _table_columns(conn, table: str) -> Set[str]:
    with conn.cursor() as cur:
        cur.execute("SHOW COLUMNS FROM `{0}`".format(table))
        return {r["Field"] for r in cur.fetchall()}


def resolve_columns(cfg: Dict[str, Any], table: str, base_cols: Sequence[str]) -> List[str]:
    """目标库实际存在的列 ∩ 期望列；application 可带 coupon_code。"""
    cols = list(base_cols)
    conn = env_util.connect_target(cfg)
    try:
        existing = _table_columns(conn, table)
    finally:
        env_util.close_conn(conn)
    if table == "application" and "coupon_code" in existing and "coupon_code" not in cols:
        # 插在 status 前，与 Flink sink 一致
        if "status" in cols:
            i = cols.index("status")
            cols.insert(i, "coupon_code")
        else:
            cols.append("coupon_code")
    return [c for c in cols if c in existing]


def load_target_rows(
    cfg: Dict[str, Any],
    table: str,
    columns: Sequence[str],
    min_user_id: int,
    include_apps: Tuple[int, ...],
    since_ms: int,
    page_size: int,
    logger: ReconcileLogger,
) -> List[dict]:
    """按迁移切片加载目标行。user* 用 user_id/group_user_id > offset；application/loan 再按 app。"""
    conn = env_util.connect_target(cfg)
    rows: List[dict] = []
    try:
        with conn.cursor() as cur:
            cols_sql = quote_cols(columns)
            if table in ("user", "user_info"):
                id_col = "user_id"
                sql = (
                    "SELECT {cols} FROM `{tbl}` WHERE `{id}` > %s "
                    "ORDER BY `{id}` ASC LIMIT %s OFFSET %s"
                ).format(cols=cols_sql, tbl=table, id=id_col)
                offset = 0
                while True:
                    cur.execute(sql, (min_user_id, page_size, offset))
                    batch = cur.fetchall()
                    if not batch:
                        break
                    for r in batch:
                        if table == "user" and include_apps:
                            try:
                                if int(r.get("app_id") or 0) not in include_apps:
                                    continue
                            except (TypeError, ValueError):
                                continue
                        rows.append(dict(r))
                    if len(batch) < page_size:
                        break
                    offset += page_size
                    if offset % (page_size * 10) == 0:
                        logger.log("load target {0} progress rows={1}".format(table, len(rows)))
            elif table in ("user_bankcard", "user_product"):
                sql = (
                    "SELECT {cols} FROM `{tbl}` WHERE `group_user_id` > %s "
                    "ORDER BY `group_user_id` ASC LIMIT %s OFFSET %s"
                ).format(cols=cols_sql, tbl=table)
                offset = 0
                while True:
                    cur.execute(sql, (min_user_id, page_size, offset))
                    batch = cur.fetchall()
                    if not batch:
                        break
                    rows.extend(dict(r) for r in batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
            elif table == "application":
                # created_time 毫秒；app_id IN include
                placeholders = ",".join(["%s"] * len(include_apps)) if include_apps else ""
                where = ["`group_user_id` > %s", "`created_time` >= %s"]
                params: List[Any] = [min_user_id, since_ms]
                if include_apps:
                    where.append("`app_id` IN ({0})".format(placeholders))
                    params.extend(include_apps)
                sql = (
                    "SELECT {cols} FROM `application` WHERE {w} "
                    "ORDER BY `group_user_id` ASC, `sn` ASC LIMIT %s OFFSET %s"
                ).format(cols=cols_sql, w=" AND ".join(where))
                offset = 0
                while True:
                    cur.execute(sql, tuple(params) + (page_size, offset))
                    batch = cur.fetchall()
                    if not batch:
                        break
                    rows.extend(dict(r) for r in batch)
                    if len(batch) < page_size:
                        break
                    offset += page_size
                    if offset % (page_size * 5) == 0:
                        logger.log("load target application progress rows={0}".format(len(rows)))
            elif table == "loan":
                # 全表按 created_time 拉后内存滤 app 前缀（loan 无 app_id）
                sql = (
                    "SELECT {cols} FROM `loan` WHERE `created_time` >= %s "
                    "ORDER BY `application_no` ASC, `period` ASC, `roll_sequence` ASC "
                    "LIMIT %s OFFSET %s"
                ).format(cols=cols_sql)
                offset = 0
                skipped_app = 0
                while True:
                    cur.execute(sql, (since_ms, page_size, offset))
                    batch = cur.fetchall()
                    if not batch:
                        break
                    for r in batch:
                        if include_apps and not app_no_in_include(r.get("application_no"), include_apps):
                            skipped_app += 1
                            continue
                        rows.append(dict(r))
                    if len(batch) < page_size:
                        break
                    offset += page_size
                logger.log("load target loan skipped_not_include_app={0}".format(skipped_app))
            else:
                raise ValueError("unknown table {0}".format(table))
    finally:
        env_util.close_conn(conn)
    logger.log("load target {0} rows={1}".format(table, len(rows)))
    return rows


def load_or_build_target_cache(
    cfg: Dict[str, Any],
    cache_path: Path,
    table: str,
    columns: Sequence[str],
    min_user_id: int,
    include_apps: Tuple[int, ...],
    since_ms: int,
    page_size: int,
    logger: ReconcileLogger,
    from_cache: bool,
    key_fn: Callable[[dict], Any],
) -> Dict[Any, dict]:
    if from_cache and cache_path.is_file():
        logger.log("load target from cache {0}".format(cache_path))
        t0 = time.time()
        by_key: Dict[Any, dict] = {}
        for row in read_jsonl(cache_path):
            by_key[key_fn(row)] = row
        logger.log(
            "cache loaded rows={0} elapsed={1:.1f}s".format(len(by_key), time.time() - t0)
        )
        return by_key

    rows = load_target_rows(
        cfg, table, columns, min_user_id, include_apps, since_ms, page_size, logger,
    )
    by_key = {key_fn(r): r for r in rows}
    logger.log("write target cache {0}".format(cache_path))
    n = write_jsonl(cache_path, by_key.values())
    logger.log("cache written rows={0}".format(n))
    return by_key


# ---------------------------------------------------------------------------
# plan from source tables
# ---------------------------------------------------------------------------

def plan_table(
    cfg: Dict[str, Any],
    table: str,
    columns: Sequence[str],
    target_by_key: Dict[Any, dict],
    since_date: str,
    include_apps: Tuple[int, ...],
    exclude_loan_created: Tuple[int, ...],
    source_batch: int,
    logger: ReconcileLogger,
) -> Tuple[List[dict], Dict[str, int]]:
    offset = int(cfg["user_id_offset"])
    since_ms = since_date_to_ms(since_date)
    stats = {
        "source": 0, "ok": 0, "insert": 0, "update": 0,
        "vt_skip": 0, "skipped_filter": 0,
    }
    plan: List[dict] = []

    if table == "user":
        compare, key_fn = [c for c in columns if c not in USER_PK], user_key
        builder = lambda r: M.expected_user(r, offset)
    elif table == "user_info":
        compare = [c for c in columns if c not in USER_INFO_PK]
        key_fn = lambda r: int(r["user_id"])
        builder = lambda r: M.expected_user_info(r, offset)
    elif table == "user_bankcard":
        compare = [c for c in columns if c not in BANKCARD_PK and c != "id"]
        key_fn = bankcard_key
        builder = lambda r: M.expected_user_bankcard(r, offset)
    elif table == "user_product":
        compare, key_fn = [c for c in columns if c not in PRODUCT_PK], product_key
        builder = lambda r: M.expected_user_product(r, offset)
    elif table == "application":
        compare, key_fn = [c for c in columns if c not in APPLICATION_PK], application_key
        builder = lambda r: M.expected_application(r, offset)
    elif table == "loan":
        compare, key_fn = [c for c in columns if c not in LOAN_PK], loan_key
        builder = M.expected_loan
    else:
        raise ValueError(table)

    # user_info / bankcard / product：源侧无时间列时仍扫全量指定 app；其余表按 since 过滤
    use_since = since_ms if table in ("user", "application", "loan") else None
    exclude_created_set = set(exclude_loan_created)

    for src in source_queries.iter_source_rows(
        cfg, table, include_apps, use_since, source_batch,
    ):
        stats["source"] += 1
        if table == "loan":
            ct = int(src.get("created_time_ms") or 0)
            if ct in exclude_created_set:
                stats["skipped_filter"] += 1
                continue

        expected = builder(src)
        if expected is None:
            stats["vt_skip"] += 1
            logger.vt_skip({"table": table, "src_id": src.get("id") or src.get("user_id")})
            continue

        if table == "application" and "coupon_code" in columns:
            expected["coupon_code"] = ""

        k = key_fn(expected)
        actual = target_by_key.get(k)
        if actual is None:
            stats["insert"] += 1
            plan.append({"action": "insert", "key": list(k) if isinstance(k, tuple) else k, "row": expected})
        else:
            if table == "user_bankcard" and actual.get("id") is not None:
                expected["id"] = actual["id"]
            diffs = row_diff(expected, actual, compare)
            if not diffs:
                stats["ok"] += 1
            else:
                stats["update"] += 1
                plan.append({
                    "action": "update",
                    "key": list(k) if isinstance(k, tuple) else k,
                    "diff_cols": diffs,
                    "row": expected,
                })
        if stats["source"] % 50000 == 0:
            logger.log(
                "plan progress source={0} insert={1} update={2} ok={3} vt_skip={4}".format(
                    stats["source"], stats["insert"], stats["update"],
                    stats["ok"], stats["vt_skip"],
                )
            )

    return plan, stats


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------

def _insert_batch(cfg: Dict[str, Any], table: str, columns: Sequence[str], rows: List[dict]) -> int:
    if not rows:
        return 0
    cols = list(columns)
    if table == "user_bankcard":
        snow = env_util.get_snowflake(cfg)
        for r in rows:
            if r.get("id") in (None, "", 0):
                r["id"] = snow.next_id()
    placeholders = ", ".join(["%s"] * len(cols))
    sql = "INSERT INTO `{0}` ({1}) VALUES ({2})".format(table, quote_cols(cols), placeholders)
    conn = env_util.connect_target(cfg)
    try:
        with conn.cursor() as cur:
            data = []
            for r in rows:
                clamp_amounts(r)
                data.append(tuple(r.get(c) for c in cols))
            cur.executemany(sql, data)
        return len(rows)
    finally:
        env_util.close_conn(conn)


def _update_batch(
    cfg: Dict[str, Any],
    table: str,
    pk_cols: Sequence[str],
    update_cols: Sequence[str],
    rows: List[dict],
) -> int:
    if not rows or not update_cols:
        return 0
    set_sql = ", ".join("`{0}`=%s".format(c) for c in update_cols)
    where_sql = " AND ".join("`{0}`=%s".format(c) for c in pk_cols)
    sql = "UPDATE `{0}` SET {1} WHERE {2}".format(table, set_sql, where_sql)
    conn = env_util.connect_target(cfg)
    affected = 0
    try:
        with conn.cursor() as cur:
            for r in rows:
                clamp_amounts(r)
                params = [r.get(c) for c in update_cols] + [r.get(c) for c in pk_cols]
                cur.execute(sql, params)
                affected += cur.rowcount
        return affected
    finally:
        env_util.close_conn(conn)


def apply_plan(
    cfg: Dict[str, Any],
    table: str,
    columns: Sequence[str],
    pk_cols: Sequence[str],
    update_cols: Sequence[str],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
    logger: ReconcileLogger,
) -> Dict[str, int]:
    stats = {"insert": 0, "update": 0, "applied": 0, "batches": 0}
    inserts = [clamp_amounts(dict(p["row"])) for p in plan if p.get("action") == "insert"]
    updates = [clamp_amounts(dict(p["row"])) for p in plan if p.get("action") == "update"]
    stats["insert"] = len(inserts)
    stats["update"] = len(updates)
    if dry_run:
        logger.log("apply DRY_RUN insert={0} update={1}".format(len(inserts), len(updates)))
        return stats

    def chunks(lst: List[dict], n: int) -> List[List[dict]]:
        return [lst[i:i + n] for i in range(0, len(lst), n)]

    batch_size = max(1, batch_size)
    apply_workers = max(1, apply_workers)

    with ThreadPoolExecutor(max_workers=apply_workers) as ex:
        futs = []
        for i, batch in enumerate(chunks(inserts, batch_size), 1):
            futs.append(("insert", i, ex.submit(_insert_batch, cfg, table, columns, batch)))
        for i, batch in enumerate(chunks(updates, batch_size), 1):
            futs.append(("update", i, ex.submit(_update_batch, cfg, table, pk_cols, update_cols, batch)))
        for kind, i, fut in futs:
            n = fut.result()
            stats["applied"] += n
            stats["batches"] += 1
            logger.log("apply {0} {1} batch={2} affected={3}".format(table, kind, i, n))
            logger.apply_audit({"table": table, "kind": kind, "batch": i, "affected": n})
    return stats


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

TABLE_META = {
    "user": (M.USER_COLS, USER_PK, user_key),
    "user_info": (M.USER_INFO_COLS, USER_INFO_PK, lambda r: int(r["user_id"])),
    "user_bankcard": (M.USER_BANKCARD_COLS, BANKCARD_PK, bankcard_key),
    "user_product": (M.USER_PRODUCT_COLS, PRODUCT_PK, product_key),
    "application": (M.APPLICATION_COLS, APPLICATION_PK, application_key),
    "loan": (M.LOAN_COLS, LOAN_PK, loan_key),
}


def run_reconcile(args: argparse.Namespace, cfg: Dict[str, Any], table: str) -> int:
    base_cols, pk_cols, key_fn = TABLE_META[table]
    columns = resolve_columns(cfg, table, base_cols)
    compare_cols = [c for c in columns if c not in pk_cols]
    if table == "user_bankcard":
        compare_cols = [c for c in compare_cols if c != "id"]

    include_apps = parse_int_list(
        getattr(args, "include_app_ids", ""), M.DEFAULT_INCLUDE_APP_IDS,
    )
    exclude_loan_created = parse_int_list(
        getattr(args, "exclude_loan_created_ms", ""), M.DEFAULT_EXCLUDE_LOAN_CREATED_MS,
    )
    min_user_id = int(getattr(args, "min_target_user_id", None) or cfg["user_id_offset"])
    since_ms = since_date_to_ms(args.since_date)

    log_dir = Path(args.log_dir)
    logger = ReconcileLogger(log_dir, table)
    cache_path = Path(args.target_cache)
    plan_path = Path(args.plan_file)
    phase = args.phase
    dry_run = not args.apply

    logger.log(
        "start table={0} phase={1} since={2} min_user_id={3} include_apps={4} cols={5}".format(
            table, phase, args.since_date, min_user_id, include_apps, len(columns),
        )
    )

    target_by_key: Dict[Any, dict] = {}
    if phase in ("load-target", "plan", "all"):
        target_by_key = load_or_build_target_cache(
            cfg, cache_path, table, columns, min_user_id, include_apps, since_ms,
            args.page_size, logger,
            from_cache=args.from_cache and phase != "load-target",
            key_fn=key_fn,
        )
        if phase == "load-target":
            logger.log("phase load-target done")
            logger.close()
            return 0

    plan: List[dict] = []
    if phase in ("plan", "all"):
        t0 = time.time()
        plan, plan_stats = plan_table(
            cfg, table, columns, target_by_key, args.since_date,
            include_apps, exclude_loan_created, args.source_batch, logger,
        )
        n = write_jsonl(plan_path, plan)
        logger.log(
            "plan done file={0} rows={1} stats={2} elapsed={3:.1f}s".format(
                plan_path, n, plan_stats, time.time() - t0,
            )
        )

    if phase == "apply":
        if not plan_path.is_file():
            logger.log("ERROR plan file missing: {0}".format(plan_path))
            logger.close()
            return 1
        plan = read_jsonl(plan_path)
        logger.log("loaded plan rows={0}".format(len(plan)))

    if phase == "apply" or (phase == "all" and args.apply):
        if not plan:
            logger.log("apply skipped: empty plan")
        else:
            apply_stats = apply_plan(
                cfg, table, columns, pk_cols, compare_cols, plan,
                args.apply_batch, args.apply_workers, dry_run, logger,
            )
            logger.log("apply stats={0}".format(apply_stats))
    elif phase == "all" and not args.apply:
        logger.log("apply skipped (no --apply / DRY_RUN)")

    logger.close()
    return 0


def run_all_tables(args: argparse.Namespace, cfg: Dict[str, Any]) -> int:
    start = getattr(args, "start_table", None) or SUPPORTED_TABLES[0]
    Path(args.log_dir).mkdir(parents=True, exist_ok=True)
    master = ReconcileLogger(Path(args.log_dir), "all")
    started = False
    overall_t0 = time.time()
    master.log("reconcile_all start since={0} apply={1} start_table={2}".format(
        args.since_date, bool(args.apply), start,
    ))
    for table in SUPPORTED_TABLES:
        if not started:
            if table != start:
                master.log("skip table={0}".format(table))
                continue
            started = True
        t0 = time.time()
        master.log("========== BEGIN table={0} ==========".format(table))
        table_args = argparse.Namespace(**vars(args))
        table_args.table = table
        table_args.phase = args.phase if args.phase != "load-target" else "all"
        if args.phase == "load-target":
            table_args.phase = "load-target"
        paths = default_paths(table, getattr(args, "plan_date", None))
        table_args.target_cache = paths["target_cache"]
        table_args.plan_file = paths["plan_file"]
        rc = run_reconcile(table_args, cfg, table)
        master.log("========== DONE table={0} rc={1} elapsed={2}s ==========".format(
            table, rc, int(time.time() - t0),
        ))
        if rc != 0:
            master.close()
            return rc
    master.log("reconcile_all finished OK elapsed={0}s".format(int(time.time() - overall_t0)))
    master.close()
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Flink sync staging↔target reconcile")
    p.add_argument("--env", default=str(env_util.PROJECT_ROOT / ".env"))
    p.add_argument("--table", default="", choices=SUPPORTED_TABLES + ("",))
    p.add_argument("--all-tables", action="store_true")
    p.add_argument("--start-table", default="user", choices=SUPPORTED_TABLES)
    p.add_argument("--phase", default="plan", choices=("load-target", "plan", "apply", "all"))
    p.add_argument("--apply", action="store_true", help="apply 阶段写库（默认仅出 plan）")
    p.add_argument("--since-date", default=DEFAULT_SINCE_DATE)
    p.add_argument(
        "--min-target-user-id",
        type=int,
        default=0,
        help="目标库 user_id/group_user_id 下限（默认=USER_ID_OFFSET，即本迁移切片）",
    )
    p.add_argument("--target-cache", default="")
    p.add_argument("--plan-file", default="")
    p.add_argument("--plan-date", default="")
    p.add_argument("--log-dir", default="/tmp/flink_reconcile_logs")
    p.add_argument("--from-cache", action="store_true")
    p.add_argument("--page-size", type=int, default=100000)
    p.add_argument("--source-batch", type=int, default=20000)
    p.add_argument("--apply-batch", type=int, default=1000)
    p.add_argument("--apply-workers", type=int, default=8)
    p.add_argument(
        "--include-app-ids",
        default="",
        help="application/loan/user 限定 app_id，默认 {0}".format(
            ",".join(map(str, M.DEFAULT_INCLUDE_APP_IDS)),
        ),
    )
    p.add_argument(
        "--exclude-loan-created-ms",
        default="",
        help="loan 排除 created_time(ms)，默认 {0}".format(
            ",".join(map(str, M.DEFAULT_EXCLUDE_LOAN_CREATED_MS)),
        ),
    )
    args = p.parse_args(argv)

    env_path = Path(args.env)
    if not env_path.is_file():
        print("env not found: {0}".format(env_path), file=sys.stderr)
        return 1
    try:
        cfg = env_util.load_env(env_path)
    except Exception as exc:
        print("load env failed: {0}".format(exc), file=sys.stderr)
        return 1

    if not args.min_target_user_id:
        args.min_target_user_id = int(cfg["user_id_offset"])

    if args.all_tables:
        return run_all_tables(args, cfg)

    if not args.table:
        print("需要 --table 或 --all-tables", file=sys.stderr)
        return 2

    paths = default_paths(args.table, getattr(args, "plan_date", None))
    if not args.target_cache:
        args.target_cache = paths["target_cache"]
    if not args.plan_file:
        args.plan_file = paths["plan_file"]

    return run_reconcile(args, cfg, args.table)


if __name__ == "__main__":
    raise SystemExit(main())
