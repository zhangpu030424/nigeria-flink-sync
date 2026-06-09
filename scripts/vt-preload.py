#!/usr/bin/env python3
"""
批量调用 VT /v2t，填充 vt_token_cache（status 0 → 1）。
多线程并发：每轮认领一批 → 拆成 N 份并行 HTTP → 写回 MySQL。

用法:
  ./scripts/vt-preload.sh
  ./scripts/vt-preload.sh --workers 20 --batch-size 1000000 --http-batch-size 50000
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
from typing import Dict, List, Optional, Tuple

# status=9 表示本脚本已认领、VT 进行中（崩溃后可 --reset-processing 重置为 0）
STATUS_PENDING = 0
STATUS_OK = 1
STATUS_FAIL = 2
STATUS_PROCESSING = 9

_print_lock = threading.Lock()
_db_lock = threading.Lock()


def log(msg: str, *, err: bool = False) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with _print_lock:
        stream = sys.stderr if err else sys.stdout
        print(f"[{ts}] {msg}", file=stream, flush=True)


class ProgressTracker:
    """线程安全进度计数：已处理 / 剩余 / 吞吐。"""

    def __init__(self, vt_type: str, initial_pending: int) -> None:
        self.vt_type = vt_type
        self.initial_pending = initial_pending
        self.ok = 0
        self.fail = 0
        self.start_time = time.time()
        self._lock = threading.Lock()

    def add(self, ok: int = 0, fail: int = 0) -> Dict[str, float]:
        with self._lock:
            self.ok += ok
            self.fail += fail
            done = self.ok + self.fail
            remain = max(0, self.initial_pending - done)
            pct = (done / self.initial_pending * 100.0) if self.initial_pending else 100.0
            elapsed = max(time.time() - self.start_time, 0.001)
            rate_per_min = self.ok / elapsed * 60.0
            return {
                "done": done,
                "remain": remain,
                "pct": pct,
                "ok": self.ok,
                "fail": self.fail,
                "rate": rate_per_min,
                "elapsed": elapsed,
            }

    def summary(self) -> str:
        s = self.add(0, 0)
        return (
            f"进度 {s['done']}/{self.initial_pending} ({s['pct']:.1f}%) "
            f"剩余 {s['remain']} | 成功 {s['ok']} 失败 {s['fail']} | "
            f"约 {s['rate']:.0f} 条/min | 已运行 {s['elapsed']:.0f}s"
        )


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


def call_v2t(
    base_url: str, values: List[str], timeout_sec: int,
) -> Tuple[List[str], List[Optional[str]], float, float]:
    """返回 (tokens, maskings, http_sec, parse_sec)。"""
    url = base_url.rstrip("/") + "/v2t"
    payload = json.dumps(values, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    t_http = time.time()
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")
    http_sec = time.time() - t_http

    t_parse = time.time()
    tokens, maskings = parse_tokens(body, len(values))
    parse_sec = time.time() - t_parse
    return tokens, maskings, http_sec, parse_sec


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


def count_pending(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, retry_failed: bool,
) -> int:
    sql = f"""
SELECT COUNT(*) FROM vt_token_cache
WHERE vt_type='{escape_sql(vt_type)}'
  AND {status_clause(retry_failed)}
  AND raw_value IS NOT NULL AND raw_value <> '';
"""
    rows = mysql_query(host, port, user, password, database, sql)
    return int(rows[0]) if rows else 0


def count_by_status(
    host: str, port: str, user: str, password: str, database: str, vt_type: str,
) -> Dict[int, int]:
    sql = f"""
SELECT status, COUNT(*) FROM vt_token_cache
WHERE vt_type='{escape_sql(vt_type)}' GROUP BY status ORDER BY status;
"""
    rows = mysql_query(host, port, user, password, database, sql)
    stats: Dict[int, int] = {}
    log(f"[{vt_type}] 状态统计:")
    for row in rows:
        parts = row.split("\t", 1)
        if len(parts) == 2:
            stats[int(parts[0])] = int(parts[1])
            label = {0: "待处理", 1: "已完成", 2: "失败", 9: "进行中"}.get(int(parts[0]), "其他")
            log(f"  status={parts[0]} ({label}): {parts[1]}")
    return stats


def chunk_list(items: List[Tuple[int, str]], chunk_size: int) -> List[List[Tuple[int, str]]]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def process_http_chunk(
    chunk: List[Tuple[int, str]],
    worker_id: int,
    round_no: int,
    chunk_no: int,
    chunk_total: int,
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
    progress: ProgressTracker,
) -> Tuple[int, int, bool]:
    """返回 (成功条数, 失败条数, 是否整 chunk 失败)"""
    ids = [c[0] for c in chunk]
    values = [c[1] for c in chunk]
    size = len(values)
    prefix = (
        f"[{vt_type}] 第{round_no}轮 worker={worker_id} "
        f"任务{chunk_no}/{chunk_total} 本批{size}条 id={ids[0]}..{ids[-1]}"
    )

    if dry_run:
        snap = progress.add(ok=size)
        log(
            f"{prefix} | [dry-run] | "
            f"进度 {snap['done']}/{progress.initial_pending} ({snap['pct']:.1f}%) "
            f"剩余 {snap['remain']}"
        )
        return size, 0, False

    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        t_total = time.time()
        try:
            tokens, maskings, http_sec, parse_sec = call_v2t(base_url, values, timeout_sec)
            updates = [(ids[i], tokens[i], maskings[i]) for i in range(len(ids))]

            t_db = time.time()
            mark_success(host, port, user, password, database, updates)
            db_sec = time.time() - t_db

            total_sec = time.time() - t_total
            snap = progress.add(ok=size)
            log(
                f"{prefix} | VT接口 {http_sec:.2f}s | 解析 {parse_sec:.3f}s | "
                f"入库 {db_sec:.2f}s | 合计 {total_sec:.2f}s | "
                f"进度 {snap['done']}/{progress.initial_pending} ({snap['pct']:.1f}%) "
                f"剩余 {snap['remain']} | 成功 {snap['ok']} 失败 {snap['fail']} | "
                f"约 {snap['rate']:.0f} 条/min"
            )
            return size, 0, False
        except (urllib.error.URLError, RuntimeError, TimeoutError) as e:
            last_err = str(e)
            elapsed = time.time() - t_total
            log(
                f"{prefix} | 第{attempt}/{max_retries}次失败 耗时 {elapsed:.2f}s | {last_err}",
                err=True,
            )
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    t_db = time.time()
    mark_failed(host, port, user, password, database, ids, last_err)
    db_sec = time.time() - t_db
    snap = progress.add(fail=size)
    log(
        f"{prefix} | 入库(失败标记) {db_sec:.2f}s | "
        f"进度 {snap['done']}/{progress.initial_pending} ({snap['pct']:.1f}%) "
        f"剩余 {snap['remain']} | 成功 {snap['ok']} 失败 {snap['fail']}",
        err=True,
    )
    return 0, size, True


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
    log(
        f"===== 开始 vt_type={vt_type} | workers={workers} "
        f"认领批次={claim_batch_size} HTTP批次={http_batch_size} ====="
    )
    count_by_status(host, port, user, password, database, vt_type)
    initial_pending = count_pending(
        host, port, user, password, database, vt_type, retry_failed,
    )
    if initial_pending == 0:
        log(f"[{vt_type}] 无待处理记录，跳过。")
        return 0

    log(f"[{vt_type}] 本次待处理共 {initial_pending} 条")
    progress = ProgressTracker(vt_type, initial_pending)

    round_no = 0
    exit_code = 0

    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            log(f"[{vt_type}] 已达 max_rounds={max_rounds}，停止。")
            break

        db_remain = count_pending(
            host, port, user, password, database, vt_type, retry_failed,
        )
        if db_remain == 0:
            log(f"[{vt_type}] 全部处理完成。")
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
        t_claim = time.time()
        if not dry_run:
            mark_processing(host, port, user, password, database, ids)
        claim_db_sec = time.time() - t_claim

        chunks = chunk_list(pending, http_batch_size)
        log(
            f"[{vt_type}] 第 {round_no} 轮开始 | 认领 {len(pending)} 条 "
            f"(DB剩余 {db_remain}) | 拆成 {len(chunks)} 个 HTTP 任务 | "
            f"认领入库 {claim_db_sec:.2f}s | {progress.summary()}"
        )

        round_t0 = time.time()
        chunk_failed = False

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    process_http_chunk,
                    chunk, idx + 1, round_no, idx + 1, len(chunks),
                    vt_type, base_url, timeout_sec, max_retries,
                    host, port, user, password, database, dry_run, progress,
                ): idx
                for idx, chunk in enumerate(chunks)
            }
            for fut in as_completed(futures):
                _ok, _fail, hard_fail = fut.result()
                if hard_fail:
                    chunk_failed = True

        round_sec = time.time() - round_t0
        db_remain_after = count_pending(
            host, port, user, password, database, vt_type, retry_failed,
        )
        log(
            f"[{vt_type}] 第 {round_no} 轮结束 | 耗时 {round_sec:.1f}s | "
            f"DB剩余 {db_remain_after} | {progress.summary()}"
        )

        if chunk_failed:
            log(f"[{vt_type}] 本轮有 chunk 失败，可用 --retry-failed 重试", err=True)
            exit_code = 2

    count_by_status(host, port, user, password, database, vt_type)
    log(f"[{vt_type}] 结束 | {progress.summary()}")
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

    workers = args.workers or int(os.environ.get("VT_PRELOAD_WORKERS", "8"))
    claim_batch = args.batch_size or int(os.environ.get("VT_PRELOAD_BATCH_SIZE", "100000"))
    http_batch = args.http_batch_size or int(os.environ.get("VT_PRELOAD_HTTP_BATCH", "0"))
    if http_batch <= 0:
        # 未配置时：优先单次 5 万（压测可扛），否则按认领数/workers 均分
        http_batch = min(50000, max(500, claim_batch // max(workers, 1)))
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
