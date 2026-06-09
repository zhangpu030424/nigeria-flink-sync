#!/usr/bin/env python3
"""
批量调用 VT /v2t，填充 vt_token_cache（status 0 → 1）。
多线程并发：每轮认领一批 → 拆成 N 份并行 HTTP → 写回 MySQL。

用法:
  ./scripts/vt-preload.sh
  ./scripts/vt-preload.sh --workers 8 --batch-size 8000 --http-batch-size 2000
  ./scripts/vt-preload.sh --vt-type gaid_idfa --retry-failed
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

# status=9 表示本脚本已认领、VT 进行中（崩溃后可 --reset-processing 重置为 0）
STATUS_PENDING = 0
STATUS_OK = 1
STATUS_FAIL = 2
STATUS_PROCESSING = 9

_print_lock = threading.Lock()
_db_lock = threading.Lock()


def log(msg: str) -> None:
    with _print_lock:
        print(msg, flush=True)


def load_dotenv(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


def mysql_query(
    host: str, port: str, user: str, password: str, database: str, sql: str,
) -> List[str]:
    env = os.environ.copy()
    env["MYSQL_PWD"] = password
    cmd = ["mysql", "-h", host, "-P", port, "-u", user, database, "-N", "-B", "-e", sql]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "mysql failed")
    return [ln for ln in proc.stdout.splitlines() if ln.strip()]


def mysql_exec(
    host: str, port: str, user: str, password: str, database: str, sql: str,
) -> None:
    with _db_lock:
        env = os.environ.copy()
        env["MYSQL_PWD"] = password
        cmd = ["mysql", "-h", host, "-P", port, "-u", user, database, "-e", sql]
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "mysql exec failed")


def escape_sql(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def parse_tokens(body: str, expected: int) -> Tuple[List[str], List[Optional[str]]]:
    try:
        data = json.loads(body)
        tokens = data.get("tokens") or []
        masking = data.get("masking") or []
    except json.JSONDecodeError:
        tokens = re.findall(r'"((?:[^"\\]|\\.)*)"', body.split('"tokens"', 1)[-1])
        masking = []
    if len(tokens) != expected:
        raise RuntimeError(f"token count mismatch: sent={expected} got={len(tokens)}")
    mask_list: List[Optional[str]] = list(masking) if masking else [None] * expected
    if len(mask_list) < expected:
        mask_list.extend([None] * (expected - len(mask_list)))
    return tokens, mask_list[:expected]


def call_v2t(base_url: str, values: List[str], timeout_sec: int) -> Tuple[List[str], List[Optional[str]]]:
    url = base_url.rstrip("/") + "/v2t"
    payload = json.dumps(values, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")
    return parse_tokens(body, len(values))


def status_clause(retry_failed: bool) -> str:
    return f"status IN ({STATUS_PENDING},{STATUS_FAIL})" if retry_failed else f"status = {STATUS_PENDING}"


def fetch_pending_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, batch_size: int, retry_failed: bool,
) -> List[Tuple[int, str]]:
    sql = f"""
SELECT id, raw_value
FROM vt_token_cache
WHERE vt_type = '{escape_sql(vt_type)}'
  AND {status_clause(retry_failed)}
  AND raw_value IS NOT NULL AND raw_value <> ''
ORDER BY id
LIMIT {int(batch_size)};
"""
    rows = mysql_query(host, port, user, password, database, sql)
    result: List[Tuple[int, str]] = []
    for row in rows:
        parts = row.split("\t", 1)
        if len(parts) == 2:
            result.append((int(parts[0]), parts[1]))
    return result


def mark_processing(
    host: str, port: str, user: str, password: str, database: str, ids: List[int],
) -> None:
    if not ids:
        return
    id_list = ",".join(str(i) for i in ids)
    sql = f"UPDATE vt_token_cache SET status={STATUS_PROCESSING} WHERE id IN ({id_list});"
    mysql_exec(host, port, user, password, database, sql)


def mark_success(
    host: str, port: str, user: str, password: str, database: str,
    updates: List[Tuple[int, str, Optional[str]]],
) -> None:
    if not updates:
        return
    parts = []
    for row_id, token, masking in updates:
        mask_sql = "NULL" if masking is None else f"'{escape_sql(masking)}'"
        parts.append(
            f"UPDATE vt_token_cache SET status={STATUS_OK}, token='{escape_sql(token)}', "
            f"masking={mask_sql}, last_error=NULL "
            f"WHERE id={row_id};"
        )
    mysql_exec(host, port, user, password, database, "".join(parts))


def mark_failed(
    host: str, port: str, user: str, password: str, database: str,
    ids: List[int], error: str,
) -> None:
    if not ids:
        return
    err = escape_sql(error[:500])
    id_list = ",".join(str(i) for i in ids)
    sql = (
        f"UPDATE vt_token_cache SET status={STATUS_FAIL}, retry_count=retry_count+1, "
        f"last_error='{err}' WHERE id IN ({id_list});"
    )
    mysql_exec(host, port, user, password, database, sql)


def reset_processing(
    host: str, port: str, user: str, password: str, database: str, vt_type: Optional[str],
) -> None:
    type_clause = "" if not vt_type else f" AND vt_type='{escape_sql(vt_type)}'"
    sql = f"UPDATE vt_token_cache SET status={STATUS_PENDING} WHERE status={STATUS_PROCESSING}{type_clause};"
    mysql_exec(host, port, user, password, database, sql)


def count_by_status(
    host: str, port: str, user: str, password: str, database: str, vt_type: str,
) -> None:
    sql = f"""
SELECT status, COUNT(*) FROM vt_token_cache
WHERE vt_type='{escape_sql(vt_type)}' GROUP BY status ORDER BY status;
"""
    rows = mysql_query(host, port, user, password, database, sql)
    log(f"  [{vt_type}] 状态统计:")
    for row in rows:
        log(f"    {row}")


def chunk_list(items: List[Tuple[int, str]], chunk_size: int) -> List[List[Tuple[int, str]]]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def process_http_chunk(
    chunk: List[Tuple[int, str]],
    worker_id: int,
    round_no: int,
    vt_type: str,
    base_url: str,
    timeout_sec: int,
    max_retries: int,
    host: str,
    port: str,
    user: str,
    password: str,
    database: str,
    dry_run: bool,
) -> Tuple[int, int, bool]:
    """返回 (成功条数, 失败条数, 是否整 chunk 失败)"""
    ids = [c[0] for c in chunk]
    values = [c[1] for c in chunk]
    label = f"[{vt_type}] round={round_no} worker={worker_id} size={len(values)} id={ids[0]}..{ids[-1]}"

    if dry_run:
        log(f"  {label} [dry-run]")
        return len(values), 0, False

    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        t0 = time.time()
        try:
            tokens, maskings = call_v2t(base_url, values, timeout_sec)
            updates = [(ids[i], tokens[i], maskings[i]) for i in range(len(ids))]
            mark_success(host, port, user, password, database, updates)
            cost = time.time() - t0
            log(f"  {label} OK {cost:.1f}s")
            return len(values), 0, False
        except (urllib.error.URLError, RuntimeError, TimeoutError) as e:
            last_err = str(e)
            log(f"  {label} 失败 {attempt}/{max_retries}: {last_err}")
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    mark_failed(host, port, user, password, database, ids, last_err)
    return 0, len(values), True


def process_vt_type(
    vt_type: str,
    host: str,
    port: str,
    user: str,
    password: str,
    database: str,
    base_url: str,
    claim_batch_size: int,
    http_batch_size: int,
    workers: int,
    timeout_sec: int,
    max_retries: int,
    max_rounds: int,
    retry_failed: bool,
    dry_run: bool,
) -> int:
    log(f"\n===== vt_type={vt_type} workers={workers} claim={claim_batch_size} http={http_batch_size} =====")
    count_by_status(host, port, user, password, database, vt_type)

    round_no = 0
    total_ok = 0
    total_fail = 0
    exit_code = 0

    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            break

        pending = fetch_pending_batch(
            host, port, user, password, database,
            vt_type, claim_batch_size, retry_failed,
        )
        if not pending:
            log(f"[{vt_type}] 无待处理记录。")
            break

        round_no += 1
        ids = [p[0] for p in pending]
        if not dry_run:
            mark_processing(host, port, user, password, database, ids)

        chunks = chunk_list(pending, http_batch_size)
        log(f"[{vt_type}] 第 {round_no} 轮: 认领 {len(pending)} 条 → {len(chunks)} 个 HTTP 任务 (workers={workers})")

        round_ok = 0
        round_fail = 0
        chunk_failed = False

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    process_http_chunk,
                    chunk, idx + 1, round_no, vt_type,
                    base_url, timeout_sec, max_retries,
                    host, port, user, password, database, dry_run,
                ): idx
                for idx, chunk in enumerate(chunks)
            }
            for fut in as_completed(futures):
                ok, fail, hard_fail = fut.result()
                round_ok += ok
                round_fail += fail
                if hard_fail:
                    chunk_failed = True

        total_ok += round_ok
        total_fail += round_fail
        log(f"[{vt_type}] 第 {round_no} 轮完成: ok={round_ok} fail={round_fail} 累计 ok={total_ok}")

        if chunk_failed:
            log(f"[{vt_type}] 本轮有 chunk 失败，可用 --retry-failed 重试", file=sys.stderr)
            exit_code = 2

    count_by_status(host, port, user, password, database, vt_type)
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="VT 字典预加载 /v2t（多线程）")
    parser.add_argument("--batch-size", type=int, default=None,
                        help="每轮从 DB 认领条数（默认 VT_PRELOAD_BATCH_SIZE）")
    parser.add_argument("--http-batch-size", type=int, default=None,
                        help="单次 HTTP /v2t 条数（默认 VT_PRELOAD_HTTP_BATCH 或 batch/workers）")
    parser.add_argument("--workers", type=int, default=None,
                        help="并发线程数（默认 VT_PRELOAD_WORKERS=4）")
    parser.add_argument("--max-rounds", "--max-batches", type=int, default=0, dest="max_rounds",
                        help="0=直到处理完（每轮=认领一批并并发 HTTP）")
    parser.add_argument("--vt-type", default="all",
                        help="mobile|gaid_idfa|bank_account|id_number|id2|all")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--reset-processing", action="store_true",
                        help="将 status=9 重置为 0 后退出")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(root, ".env"))

    host = os.environ.get("SOURCE_MYSQL_HOST", "")
    port = os.environ.get("SOURCE_MYSQL_PORT", "3306")
    user = os.environ.get("SOURCE_MYSQL_USER", "")
    password = os.environ.get("SOURCE_MYSQL_PASSWORD", "")
    database = os.environ.get("SOURCE_MYSQL_DATABASE", "nigeria_backend")
    base_url = os.environ.get("VT_BASE_URL", "http://101.47.27.225")

    workers = args.workers or int(os.environ.get("VT_PRELOAD_WORKERS", "4"))
    claim_batch = args.batch_size or int(os.environ.get("VT_PRELOAD_BATCH_SIZE", "10000"))
    http_batch = args.http_batch_size or int(os.environ.get("VT_PRELOAD_HTTP_BATCH", "0"))
    if http_batch <= 0:
        http_batch = max(500, claim_batch // max(workers, 1))
    timeout_sec = int(os.environ.get("VT_BATCH_TIMEOUT_SEC", "300"))
    max_retries = int(os.environ.get("VT_BATCH_MAX_RETRIES", "3"))

    if not all([host, user, password, database]):
        print("缺少 SOURCE_MYSQL_* 配置", file=sys.stderr)
        return 1

    all_types = ["mobile", "gaid_idfa", "bank_account", "id_number", "id2"]
    vt_types = all_types if args.vt_type == "all" else [args.vt_type]

    if args.reset_processing:
        reset_processing(host, port, user, password, database,
                         None if args.vt_type == "all" else args.vt_type)
        log("已重置 status=9 → 0")
        return 0

    # 启动时自动回收上次中断的 processing 行
    reset_processing(host, port, user, password, database,
                     None if args.vt_type == "all" else args.vt_type)

    log(f"VT 预加载: types={vt_types}, workers={workers}, claim_batch={claim_batch}, "
        f"http_batch={http_batch}, url={base_url}")

    exit_code = 0
    for vt_type in vt_types:
        code = process_vt_type(
            vt_type, host, port, user, password, database, base_url,
            claim_batch, http_batch, workers, timeout_sec, max_retries,
            args.max_rounds, args.retry_failed, args.dry_run,
        )
        if code != 0:
            exit_code = code

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
