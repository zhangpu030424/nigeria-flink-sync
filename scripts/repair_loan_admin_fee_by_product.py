#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""按 product_id 批量修正目标库 loan.admin_fee。

流程：
  1. 目标库 application 按 product_id 查出 application_no
  2. 目标库 loan JOIN application 按 product_id 载入（~10 批 SQL，非 583 万次 IN）
  3. 源库 ng_loan_market 按 product_id 并行拉取 amount/disburseAmount
  4. 内存计算修复计划：admin_fee = GREATEST(amount - disburseAmount, 0)
  5. 多线程分批 UPDATE 目标库 loan（带进度条）

查数阶段（application / loan / 源库）同样多连接并行；目标 loan 与源库可并行拉取。
step1 默认只 COUNT application，避免 583 万行占内存。

Usage:
  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env ./ng_migration.env --scan --fetch-workers 8

  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env ./ng_migration.env --build-plan \\
    --plan-file /tmp/fix_loan_admin_fee_plan.jsonl \\
    --fetch-workers 8 --fetch-chunk 500

  python3 scripts/repair_loan_admin_fee_by_product.py \\
    --env ./ng_migration.env --apply \\
    --plan-file /tmp/fix_loan_admin_fee_plan.jsonl \\
    --batch-size 100 --apply-workers 4
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, TypeVar

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

APP_NO_RE = re.compile(r"^ng(\d+)-(.+)$", re.IGNORECASE)
FETCH_CHUNK = 500


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


def connect_mysql(
    cfg: Dict[str, str],
    *,
    host_key: Sequence[str],
    port_key: Sequence[str],
    user_key: Sequence[str],
    password_key: Sequence[str],
    database_key: Sequence[str],
    default_db: str,
    for_apply: bool = False,
):
    def _pick(keys: Sequence[str], default: str = "") -> str:
        for key in keys:
            val = cfg.get(key)
            if val not in (None, ""):
                return val
        return default

    host = _pick(host_key, "127.0.0.1")
    port = int(_pick(port_key, "3306"))
    user = _pick(user_key, "root")
    password = _pick(password_key, "")
    database = _pick(database_key, default_db)
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


def connect_target(cfg: Dict[str, str], for_apply: bool = False):
    return connect_mysql(
        cfg,
        host_key=("TARGET_MYSQL_HOST", "TARGET_HOST"),
        port_key=("TARGET_MYSQL_PORT", "TARGET_PORT"),
        user_key=("TARGET_MYSQL_USER", "TARGET_USER"),
        password_key=("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD"),
        database_key=("TARGET_MYSQL_DATABASE", "TARGET_DB"),
        default_db="ng",
        for_apply=for_apply,
    )


def connect_source(cfg: Dict[str, str]):
    return connect_mysql(
        cfg,
        host_key=("LM_MYSQL_HOST", "SOURCE_MYSQL_HOST"),
        port_key=("LM_MYSQL_PORT", "SOURCE_MYSQL_PORT"),
        user_key=("LM_MYSQL_USER", "SOURCE_MYSQL_USER"),
        password_key=("LM_MYSQL_PASSWORD", "SOURCE_MYSQL_PASSWORD"),
        database_key=("LM_MYSQL_DATABASE",),
        default_db="ng_loan_market",
        for_apply=False,
    )


def chunks(items: Sequence[Any], size: int) -> Iterable[List[Any]]:
    n = max(1, size)
    for i in range(0, len(items), n):
        yield list(items[i:i + n])


def parse_application_no(application_no: str) -> Tuple[int, str]:
    """ng0502-169617902712032877 -> (502, 169617902712032877)"""
    m = APP_NO_RE.match(str(application_no or "").strip())
    if not m:
        raise ValueError("bad application_no: %r" % application_no)
    return int(m.group(1)), m.group(2)


def market_application_no(application_no: str) -> str:
    return parse_application_no(application_no)[1]


T = TypeVar("T")


def default_fetch_workers() -> int:
    cpu = os.cpu_count() or 4
    return max(4, min(16, cpu))


def parallel_map(
    items: Sequence[T],
    workers: int,
    fn: Callable[[T], Any],
    label: str = "fetch",
    progress: Optional["Progress"] = None,
) -> List[Any]:
    if not items:
        return []
    workers = max(1, min(workers, len(items)))
    if workers == 1:
        results = [fn(x) for x in items]
        if progress:
            progress.increment(len(items))
        return results
    out: List[Any] = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix=label) as ex:
        futs = [ex.submit(fn, item) for item in items]
        for fut in as_completed(futs):
            out.append(fut.result())
            if progress:
                progress.increment(1)
    return out


class Progress:
    """终端单行进度条（不依赖 tqdm）。"""

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

    def finish(self, note: str = "") -> None:
        with self._lock:
            self.done = self.total
            if note:
                self.extra = note
            self._render()
            sys.stdout.write("\n")
            sys.stdout.flush()


def product_id_batches(product_ids: Sequence[int], fetch_workers: int) -> List[List[int]]:
    """按 product_id 切批：81 个 product 拆成 ~10 批，避免 583 万次 IN 查询。"""
    ids = list(product_ids)
    if not ids:
        return []
    per_batch = max(1, min(10, (len(ids) + max(1, fetch_workers) - 1) // max(1, fetch_workers)))
    return list(chunks(ids, per_batch))


def _fetch_applications_chunk(cfg: Dict[str, str], product_ids: Sequence[int]) -> List[dict]:
    if not product_ids:
        return []
    ph = ",".join(["%s"] * len(product_ids))
    sql = """
    SELECT
        application_no,
        CAST(product_id AS UNSIGNED) AS product_id,
        CAST(app_id AS UNSIGNED) AS app_id
    FROM application
    WHERE CAST(product_id AS UNSIGNED) IN ({product_ph})
    """.format(product_ph=ph)
    conn = connect_target(cfg, for_apply=False)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, list(product_ids))
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_applications_by_product(
    cfg: Dict[str, str],
    product_ids: Sequence[int],
    fetch_workers: int,
) -> List[dict]:
    ids = list(product_ids)
    if not ids:
        return []
    id_chunks = product_id_batches(ids, fetch_workers)
    prog = Progress("step1 application", len(id_chunks))
    parts = parallel_map(
        id_chunks,
        max(1, min(fetch_workers, len(id_chunks))),
        lambda part: _fetch_applications_chunk(cfg, part),
        label="fetch-app",
        progress=prog,
    )
    prog.finish()
    rows: List[dict] = []
    for part in parts:
        rows.extend(part)
    return rows


def count_applications_by_product(cfg: Dict[str, str], product_ids: Sequence[int]) -> int:
    ph = ",".join(["%s"] * len(product_ids))
    sql = """
    SELECT COUNT(*) AS cnt
    FROM application
    WHERE CAST(product_id AS UNSIGNED) IN ({product_ph})
    """.format(product_ph=ph)
    conn = connect_target(cfg, for_apply=False)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, list(product_ids))
            row = cur.fetchone()
            return int((row or {}).get("cnt") or 0)
    finally:
        conn.close()


LOAN_BY_PRODUCT_SQL = """
SELECT
    l.application_no,
    l.period,
    l.roll_sequence,
    l.loan_no,
    l.admin_fee AS old_admin_fee,
    l.principal,
    CAST(a.product_id AS UNSIGNED) AS product_id,
    CAST(a.app_id AS UNSIGNED) AS app_id
FROM loan l
INNER JOIN application a ON l.application_no = a.application_no
WHERE CAST(a.product_id AS UNSIGNED) IN ({ph})
"""

SOURCE_BY_PRODUCT_SQL = """
SELECT
    CAST(a.appId AS UNSIGNED) AS app_id,
    a.applicationNo AS market_no,
    CAST(a.productId AS UNSIGNED) AS product_id,
    CAST(COALESCE(a.amount, 0) AS SIGNED) AS market_amount,
    CAST(COALESCE(a.disburseAmount, 0) AS SIGNED) AS market_disburse_amount,
    CAST(GREATEST(COALESCE(a.amount, 0) - COALESCE(a.disburseAmount, 0), 0) AS SIGNED) AS new_admin_fee
FROM application a
WHERE CAST(a.productId AS UNSIGNED) IN ({ph})
  AND a.disburseTime <> 0
"""


def _fetch_loans_by_product_chunk(cfg: Dict[str, str], product_ids: Sequence[int]) -> List[dict]:
    if not product_ids:
        return []
    ph = ",".join(["%s"] * len(product_ids))
    conn = connect_target(cfg, for_apply=False)
    try:
        with conn.cursor() as cur:
            cur.execute(LOAN_BY_PRODUCT_SQL.format(ph=ph), list(product_ids))
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_loans_by_product_ids(
    cfg: Dict[str, str],
    product_ids: Sequence[int],
    fetch_workers: int,
    progress: Optional[Progress] = None,
) -> List[dict]:
    batches = product_id_batches(product_ids, fetch_workers)
    if not batches:
        return []
    parts = parallel_map(
        batches,
        max(1, min(fetch_workers, len(batches))),
        lambda part: _fetch_loans_by_product_chunk(cfg, part),
        label="fetch-loan",
        progress=progress,
    )
    rows: List[dict] = []
    for part in parts:
        rows.extend(part)
    if progress:
        progress.finish("rows=%s" % len(rows))
    return rows


def _fetch_source_by_product_chunk(cfg: Dict[str, str], product_ids: Sequence[int]) -> List[dict]:
    if not product_ids:
        return []
    ph = ",".join(["%s"] * len(product_ids))
    conn = connect_source(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(SOURCE_BY_PRODUCT_SQL.format(ph=ph), list(product_ids))
            return list(cur.fetchall())
    finally:
        conn.close()


def fetch_source_by_product_ids(
    cfg: Dict[str, str],
    product_ids: Sequence[int],
    fetch_workers: int,
    progress: Optional[Progress] = None,
) -> Dict[Tuple[int, str], dict]:
    batches = product_id_batches(product_ids, fetch_workers)
    if not batches:
        return {}
    parts = parallel_map(
        batches,
        max(1, min(fetch_workers, len(batches))),
        lambda part: _fetch_source_by_product_chunk(cfg, part),
        label="fetch-src",
        progress=progress,
    )
    out: Dict[Tuple[int, str], dict] = {}
    for rows in parts:
        for row in rows:
            key = (int(row["app_id"]), str(row["market_no"]))
            out[key] = row
    if progress:
        progress.finish("hits=%s" % len(out))
    return out


def build_plan(
    application_count: int,
    loans: List[dict],
    source_by_key: Dict[Tuple[int, str], dict],
) -> Tuple[List[dict], Dict[str, int]]:
    rows: List[dict] = []
    stats = {
        "applications": application_count,
        "target_loans": len(loans),
        "source_rows": len(source_by_key),
        "need_update": 0,
        "already_ok": 0,
        "no_source_match": 0,
        "bad_application_no": 0,
        "app_id_mismatch": 0,
    }

    prog = Progress("step4 build plan", len(loans) or 1)
    report_every = max(1, min(50000, len(loans) // 100)) if loans else 1

    for i, loan in enumerate(loans):
        app_no = str(loan["application_no"])
        try:
            parsed_app_id, market_no = parse_application_no(app_no)
        except ValueError:
            stats["bad_application_no"] += 1
            if (i + 1) % report_every == 0:
                prog.set_extra("need_update=%s" % stats["need_update"])
                prog.increment(report_every)
            continue

        loan_app_id = int(loan.get("app_id") or parsed_app_id)
        src = source_by_key.get((parsed_app_id, market_no))
        if not src:
            src = source_by_key.get((loan_app_id, market_no))
            if not src:
                stats["no_source_match"] += 1
                if (i + 1) % report_every == 0:
                    prog.set_extra("need_update=%s" % stats["need_update"])
                    prog.increment(report_every)
                continue

        if int(src["app_id"]) != parsed_app_id:
            stats["app_id_mismatch"] += 1

        old_fee = int(loan["old_admin_fee"] or 0)
        new_fee = int(src["new_admin_fee"] or 0)
        if old_fee == new_fee:
            stats["already_ok"] += 1
        else:
            stats["need_update"] += 1
            rows.append({
                "application_no": app_no,
                "market_no": market_no,
                "period": int(loan["period"]),
                "roll_sequence": int(loan["roll_sequence"]),
                "loan_no": loan.get("loan_no"),
                "product_id": int(
                    loan.get("product_id") or src.get("product_id") or parsed_app_id
                ),
                "old_admin_fee": old_fee,
                "new_admin_fee": new_fee,
                "principal": int(loan.get("principal") or 0),
                "market_amount": int(src.get("market_amount") or 0),
                "market_disburse_amount": int(src.get("market_disburse_amount") or 0),
            })

        if (i + 1) % report_every == 0:
            prog.set_extra("need_update=%s" % stats["need_update"])
            prog.increment(report_every)

    remain = len(loans) % report_every
    if remain:
        prog.set_extra("need_update=%s" % stats["need_update"])
        prog.increment(remain)
    prog.finish("plan_rows=%s" % len(rows))
    stats["plan_rows"] = len(rows)
    return rows, stats


def load_and_build_plan(
    cfg: Dict[str, str],
    product_ids: Sequence[int],
    fetch_workers: int,
    *,
    load_applications: bool = False,
) -> Tuple[List[dict], Dict[str, int]]:
    t0 = time.time()
    if load_applications:
        print("step1 load application rows by product_id ...", flush=True)
        applications = fetch_applications_by_product(cfg, product_ids, fetch_workers)
        app_count = len(applications)
    else:
        print("step1 count application by product_id ...", flush=True)
        app_count = count_applications_by_product(cfg, product_ids)
    print(" applications=%s elapsed=%.1fs" % (app_count, time.time() - t0), flush=True)

    batches = product_id_batches(product_ids, fetch_workers)
    print(
        "step2+3 fetch target loans + source by product_id "
        "(batches=%s workers=%s, not %s IN queries) ..."
        % (len(batches), fetch_workers, app_count),
        flush=True,
    )
    t1 = time.time()
    prog_loan = Progress("step2 target loan", len(batches))
    prog_src = Progress("step3 source market", len(batches))
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="fetch-pair") as ex:
        fut_loans = ex.submit(
            fetch_loans_by_product_ids, cfg, product_ids, fetch_workers, prog_loan,
        )
        fut_source = ex.submit(
            fetch_source_by_product_ids, cfg, product_ids, fetch_workers, prog_src,
        )
        loans = fut_loans.result()
        source_by_key = fut_source.result()
    print(
        " loans=%s source_hits=%s elapsed=%.1fs"
        % (len(loans), len(source_by_key), time.time() - t1),
        flush=True,
    )

    return build_plan(app_count, loans, source_by_key)


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


_print_lock = threading.Lock()


def _log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def apply_batch_worker(cfg: Dict[str, str], batch_no: int, batch: List[dict]) -> Tuple[int, int, int]:
    conn = connect_target(cfg, for_apply=True)
    try:
        def _run():
            return apply_batch(conn, batch)

        n = exec_with_retry(conn, _run, "batch %s" % batch_no) or 0
        return batch_no, int(n), len(batch) - int(n)
    finally:
        conn.close()


def _verify_plan_chunk(cfg: Dict[str, str], batch: List[dict]) -> Dict[str, int]:
    stats = {"ok": 0, "mismatch": 0, "missing": 0}
    mismatches: List[dict] = []
    conn = connect_target(cfg, for_apply=False)
    try:
        with conn.cursor() as cur:
            for row in batch:
                cur.execute(
                    """
                    SELECT admin_fee FROM loan
                    WHERE application_no = %s AND period = %s AND roll_sequence = %s
                    """,
                    (
                        str(row["application_no"]),
                        int(row["period"]),
                        int(row["roll_sequence"]),
                    ),
                )
                got = cur.fetchone()
                exp = int(row["new_admin_fee"])
                if not got:
                    stats["missing"] += 1
                    if len(mismatches) < 3:
                        mismatches.append({**row, "actual_admin_fee": None})
                    continue
                actual = int(got["admin_fee"] or 0)
                if actual == exp:
                    stats["ok"] += 1
                else:
                    stats["mismatch"] += 1
                    if len(mismatches) < 3:
                        mismatches.append({**row, "actual_admin_fee": actual})
    finally:
        conn.close()
    stats["_mismatches"] = mismatches
    return stats


def scan(
    cfg: Dict[str, str],
    product_ids: Sequence[int],
    fetch_workers: int,
    sample: int,
) -> Dict[str, Any]:
    rows, stats = load_and_build_plan(cfg, product_ids, fetch_workers)
    print(
        "applications=%s target_loans=%s source_rows=%s need_update=%s already_ok=%s "
        "no_source_match=%s bad_application_no=%s app_id_mismatch=%s product_ids=%s"
        % (
            stats.get("applications"),
            stats.get("target_loans"),
            stats.get("source_rows"),
            stats.get("need_update"),
            stats.get("already_ok"),
            stats.get("no_source_match"),
            stats.get("bad_application_no"),
            stats.get("app_id_mismatch"),
            len(product_ids),
        ),
        flush=True,
    )
    for row in rows[:sample]:
        print(
            " sample loan_no=%s app=%s market_no=%s p%s r%s product_id=%s "
            "admin_fee %s -> %s (amount=%s disburseAmount=%s principal=%s)"
            % (
                row.get("loan_no"),
                row.get("application_no"),
                row.get("market_no"),
                row.get("period"),
                row.get("roll_sequence"),
                row.get("product_id"),
                row.get("old_admin_fee"),
                row.get("new_admin_fee"),
                row.get("market_amount"),
                row.get("market_disburse_amount"),
                row.get("principal"),
            ),
            flush=True,
        )
    return stats


def apply_plan(
    cfg: Dict[str, str],
    plan: List[dict],
    batch_size: int,
    apply_workers: int,
    dry_run: bool,
) -> Dict[str, int]:
    if dry_run:
        print("dry-run rows=%s" % len(plan), flush=True)
        for row in plan[:10]:
            print(
                "  %s admin_fee %s -> %s (amount=%s disburseAmount=%s)"
                % (
                    row.get("loan_no"),
                    row.get("old_admin_fee"),
                    row.get("new_admin_fee"),
                    row.get("market_amount"),
                    row.get("market_disburse_amount"),
                ),
                flush=True,
            )
        return {"dry_run": len(plan)}

    batches = list(chunks(plan, max(1, batch_size)))
    total_batches = len(batches)
    stats = {"updated": 0, "skipped": 0, "batches": total_batches}
    workers = max(1, apply_workers)

    print(
        "apply start rows=%s batches=%s batch_size=%s workers=%s"
        % (len(plan), total_batches, batch_size, workers),
        flush=True,
    )

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(apply_batch_worker, cfg, i + 1, batch): i + 1
            for i, batch in enumerate(batches)
        }
        prog = Progress("apply", total_batches)
        for fut in as_completed(futs):
            bno, updated, skipped = fut.result()
            stats["updated"] += updated
            stats["skipped"] += skipped
            prog.set_extra("updated=%s skipped=%s" % (stats["updated"], stats["skipped"]))
            prog.increment(1)
        prog.finish()
    return stats


def verify_plan(
    cfg: Dict[str, str],
    plan: List[dict],
    fetch_workers: int,
    fetch_chunk: int,
    sample: int = 10,
) -> Dict[str, int]:
    stats = {"ok": 0, "mismatch": 0, "missing": 0}
    mismatches: List[dict] = []
    batches = list(chunks(plan, max(1, fetch_chunk)))
    workers = max(1, min(fetch_workers, len(batches) or 1))
    prog = Progress("verify", len(batches) or 1)
    parts = parallel_map(
        batches,
        workers,
        lambda batch: _verify_plan_chunk(cfg, batch),
        label="verify",
        progress=prog,
    )
    prog.finish()
    for part in parts:
        stats["ok"] += int(part.get("ok") or 0)
        stats["mismatch"] += int(part.get("mismatch") or 0)
        stats["missing"] += int(part.get("missing") or 0)
        for m in part.get("_mismatches") or []:
            if len(mismatches) < sample:
                mismatches.append({**m, "reason": "missing" if m.get("actual_admin_fee") is None else "mismatch"})
    print(
        "verify ok=%s mismatch=%s missing=%s total=%s workers=%s"
        % (stats["ok"], stats["mismatch"], stats["missing"], len(plan), workers),
        flush=True,
    )
    for m in mismatches:
        print(
            "  %s expect=%s actual=%s (old=%s amount=%s disburseAmount=%s)"
            % (
                m.get("loan_no"),
                m.get("new_admin_fee"),
                m.get("actual_admin_fee"),
                m.get("old_admin_fee"),
                m.get("market_amount"),
                m.get("market_disburse_amount"),
            ),
            flush=True,
        )
    return stats


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fix loan.admin_fee from ng_loan_market amount - disburseAmount")
    p.add_argument("--env", default=str(REPO / ".env"))
    p.add_argument("--scan", action="store_true", help="count + sample only")
    p.add_argument("--build-plan", action="store_true")
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verify", action="store_true", help="对照 plan 验收 admin_fee")
    p.add_argument("--plan-file", default="/tmp/fix_loan_admin_fee_plan.jsonl")
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--apply-workers", type=int, default=4, help="多线程 apply 并发数")
    p.add_argument(
        "--fetch-workers",
        type=int,
        default=default_fetch_workers(),
        help="查目标库/源库并行连接数（默认 min(16, cpu)）",
    )
    p.add_argument("--fetch-chunk", type=int, default=FETCH_CHUNK, help="verify 分批大小（查库已改为按 product_id）")
    p.add_argument("--sample", type=int, default=10)
    return p.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if not args.scan and not args.build_plan and not args.apply and not args.dry_run and not args.verify:
        print("specify --scan, --build-plan, --apply, and/or --verify", file=sys.stderr)
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
        fetch_workers = max(1, args.fetch_workers)
        if args.scan:
            scan(
                cfg, PRODUCT_IDS,
                fetch_workers=fetch_workers,
                sample=args.sample,
            )
        if args.build_plan:
            plan, stats = load_and_build_plan(
                cfg, PRODUCT_IDS, fetch_workers,
            )
            write_jsonl(plan_path, plan)
            print(
                "plan written file=%s rows=%s stats=%s fetch_workers=%s elapsed=%.1fs"
                % (plan_path, len(plan), stats, fetch_workers, time.time() - t0),
                flush=True,
            )

    if args.apply or args.dry_run or args.verify:
        if not plan_path.is_file():
            print("missing plan: %s (run --build-plan first)" % plan_path, file=sys.stderr)
            return 2
        plan = read_jsonl(plan_path)
        print("loaded plan rows=%s" % len(plan), flush=True)
        if args.verify:
            stats = verify_plan(
                cfg, plan,
                fetch_workers=max(1, args.fetch_workers),
                fetch_chunk=args.fetch_chunk,
                sample=args.sample,
            )
            print("verify stats=%s elapsed=%.1fs" % (stats, time.time() - t0), flush=True)
            return 1 if stats.get("mismatch") or stats.get("missing") else 0
        stats = apply_plan(
            cfg, plan, args.batch_size, args.apply_workers, dry_run=bool(args.dry_run),
        )
        print("apply stats=%s elapsed=%.1fs" % (stats, time.time() - t0), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
