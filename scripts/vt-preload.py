#!/usr/bin/env python3
"""
VT 字典预加载：批量 /v2t 写入 vt_token_cache。

默认 stream 模式（快）：
  源表查未 VT 明文 → 多线程 /v2t → INSERT UPSERT（无预灌、无认领 UPDATE）

legacy cache 模式（--mode cache）：
  读 vt_token_cache status=0 → 认领 → VT → 更新

用法:
  ./scripts/vt-preload.sh --workers 20 --http-batch-size 50000
  ./scripts/vt-preload.sh --mode cache --retry-failed
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
from typing import Dict, List, Optional, Tuple, Union

STATUS_PENDING = 0
STATUS_OK = 1
STATUS_FAIL = 2
STATUS_PROCESSING = 9

_print_lock = threading.Lock()
DB_INSERT_CHUNK = 2000
DEFAULT_HEARTBEAT_SEC = 30

# 源表与 vt_token_cache 排序规则可能不同（0900_ai_ci vs unicode_ci），比较时统一 COLLATE
COLLATE_CMP = "utf8mb4_bin"

SOURCE_NOT_VT = """
AND NOT EXISTS (
  SELECT 1 FROM vt_token_cache vt
  WHERE vt.vt_type = '{vt_type}'
    AND vt.raw_value COLLATE {collate} = src.raw_value COLLATE {collate}
    AND vt.status = {ok} AND vt.token IS NOT NULL AND vt.token <> ''
)
"""

SOURCE_QUERIES: Dict[str, str] = {
    "mobile": """
SELECT src.raw_value FROM (
  SELECT DISTINCT
    CASE
      WHEN u.mobile IS NULL OR TRIM(u.mobile) = '' THEN NULL
      WHEN TRIM(u.mobile) LIKE '+%' THEN TRIM(u.mobile)
      WHEN TRIM(u.mobile) LIKE '234%' THEN CONCAT('+', TRIM(u.mobile))
      WHEN TRIM(u.mobile) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(u.mobile), 2))
      ELSE CONCAT('+234', TRIM(u.mobile))
    END AS raw_value
  FROM `user` u
) src
WHERE src.raw_value IS NOT NULL AND src.raw_value <> ''
{not_vt}
{limit_clause}
""",
    "gaid_idfa": """
SELECT src.raw_value FROM (
  SELECT DISTINCT TRIM(u.gps_adid) AS raw_value FROM `user` u
  WHERE u.gps_adid IS NOT NULL AND TRIM(u.gps_adid) <> ''
  UNION
  SELECT DISTINCT TRIM(u.idfa) FROM `user` u
  WHERE u.idfa IS NOT NULL AND TRIM(u.idfa) <> ''
  UNION
  SELECT DISTINCT TRIM(d.aaid) FROM device_ids d
  WHERE d.aaid IS NOT NULL AND TRIM(d.aaid) <> ''
  UNION
  SELECT DISTINCT TRIM(d.idfa) FROM device_ids d
  WHERE d.idfa IS NOT NULL AND TRIM(d.idfa) <> ''
) src
WHERE src.raw_value IS NOT NULL AND src.raw_value <> ''
{not_vt}
{limit_clause}
""",
    "bank_account": """
SELECT src.raw_value FROM (
  SELECT DISTINCT TRIM(b.bank_account) AS raw_value
  FROM user_bank_info b
  WHERE b.deleted = 0
    AND b.bank_account IS NOT NULL AND TRIM(b.bank_account) <> ''
) src
WHERE 1=1
{not_vt}
{limit_clause}
""",
    "id_number": """
SELECT src.raw_value FROM (
  SELECT DISTINCT TRIM(p.bvn) AS raw_value
  FROM user_personal_info p
  WHERE p.bvn IS NOT NULL AND TRIM(p.bvn) <> ''
) src
WHERE 1=1
{not_vt}
{limit_clause}
""",
}


def _not_vt_clause(vt_type: str, ok: int = STATUS_OK) -> str:
    return SOURCE_NOT_VT.format(
        vt_type=escape_sql(vt_type), ok=ok, collate=COLLATE_CMP,
    )


def _source_sql(vt_type: str, not_vt: str, limit: Optional[int]) -> str:
    clause = f"LIMIT {int(limit)}" if limit is not None else ""
    return SOURCE_QUERIES[vt_type].format(not_vt=not_vt, limit_clause=clause)

CACHE_RETRY_SQL = """
SELECT raw_value FROM vt_token_cache
WHERE vt_type = '{vt_type}' AND status = {fail}
  AND raw_value IS NOT NULL AND raw_value <> ''
ORDER BY id
LIMIT {limit}
"""


def log(msg: str, *, err: bool = False) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with _print_lock:
        stream = sys.stderr if err else sys.stdout
        print(f"[{ts}] {msg}", file=stream, flush=True)


class ProgressTracker:
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
            if self.initial_pending > 0:
                remain = max(0, self.initial_pending - done)
                pct = done / self.initial_pending * 100.0
            else:
                remain = -1
                pct = 0.0
            elapsed = max(time.time() - self.start_time, 0.001)
            return {
                "done": done,
                "remain": remain,
                "pct": pct,
                "ok": self.ok,
                "fail": self.fail,
                "rate": self.ok / elapsed * 60.0,
                "elapsed": elapsed,
            }

    def summary(self) -> str:
        s = self.add(0, 0)
        if self.initial_pending > 0:
            prog = f"进度 {s['done']}/{self.initial_pending} ({s['pct']:.1f}%) 剩余 {s['remain']}"
        else:
            prog = f"已完成 {s['done']}（流式模式未预统计总量）"
        return (
            f"{prog} | 成功 {s['ok']} 失败 {s['fail']} | "
            f"约 {s['rate']:.0f} 条/min | 已运行 {s['elapsed']:.0f}s"
        )


def load_dotenv(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


def mysql_query(host: str, port: str, user: str, password: str, database: str, sql: str) -> List[str]:
    env = os.environ.copy()
    env["MYSQL_PWD"] = password
    cmd = ["mysql", "-h", host, "-P", port, "-u", user, database, "-N", "-B", "-e", sql]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "mysql failed")
    return [ln for ln in proc.stdout.splitlines() if ln.strip()]


def _mysql_run(host: str, port: str, user: str, password: str, database: str, sql: str, *, via_stdin: bool) -> None:
    env = os.environ.copy()
    env["MYSQL_PWD"] = password
    cmd = ["mysql", "-h", host, "-P", port, "-u", user, database]
    if via_stdin:
        proc = subprocess.run(cmd, input=sql, env=env, capture_output=True, text=True)
    else:
        proc = subprocess.run([*cmd, "-e", sql], env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip() or "mysql exec failed"
        if "1142" in err:
            err += (
                "\n提示: vt-preload 需要 vt_token_cache 的 SELECT + INSERT + UPDATE 权限。"
                f"\n  GRANT SELECT, INSERT, UPDATE ON {database}.vt_token_cache TO '{user}'@'<IP>';"
                "\n  见 sql/ddl/vt_token_cache_grants.sql"
            )
        raise RuntimeError(err)


def mysql_exec(host: str, port: str, user: str, password: str, database: str, sql: str) -> None:
    via_stdin = len(sql) > 32000 or ";\n" in sql or sql.count(";") > 1
    _mysql_run(host, port, user, password, database, sql, via_stdin=via_stdin)


def escape_sql(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def check_table_ready(host: str, port: str, user: str, password: str, database: str) -> None:
    """flink_cdc 通常无 CREATE 权限，仅检查表是否已由 DBA 建好。"""
    try:
        mysql_query(host, port, user, password, database, "SELECT 1 FROM vt_token_cache LIMIT 0;")
        log("vt_token_cache 表就绪")
    except RuntimeError as e:
        err = str(e)
        if "1146" in err or "doesn't exist" in err.lower():
            raise RuntimeError(
                "vt_token_cache 表不存在。请 DBA 执行:\n"
                "  mysql -h <host> -u root -p nigeria_backend < sql/ddl/vt_token_cache.sql"
            ) from e
        if "1142" in err and "SELECT" in err:
            raise RuntimeError(
                "flink_cdc 无 vt_token_cache SELECT 权限。请 DBA 执行 sql/ddl/vt_token_cache_grants.sql"
            ) from e
        raise


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


def call_v2t(base_url: str, values: List[str], timeout_sec: int) -> Tuple[List[str], List[Optional[str]], float, float]:
    url = base_url.rstrip("/") + "/v2t"
    payload = json.dumps(values, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    t_http = time.time()
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        body = resp.read().decode("utf-8", errors="replace")
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")
    http_sec = time.time() - t_http
    t_parse = time.time()
    tokens, maskings = parse_tokens(body, len(values))
    return tokens, maskings, http_sec, time.time() - t_parse


def _sql_lit(s: Optional[str]) -> str:
    return "NULL" if s is None else f"'{escape_sql(s)}'"


def upsert_success(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, rows: List[Tuple[str, str, Optional[str]]], log_prefix: str = "",
) -> None:
    if not rows:
        return
    vt = escape_sql(vt_type)
    stmts: List[str] = []
    batches = 0
    for i in range(0, len(rows), DB_INSERT_CHUNK):
        chunk = rows[i:i + DB_INSERT_CHUNK]
        values = ",\n".join(
            f"('{vt}', '{escape_sql(raw)}', '{escape_sql(tok)}', {_sql_lit(mask)}, {STATUS_OK})"
            for raw, tok, mask in chunk
        )
        stmts.append(f"""
INSERT INTO vt_token_cache (vt_type, raw_value, token, masking, status)
VALUES {values}
ON DUPLICATE KEY UPDATE
  token = VALUES(token),
  masking = VALUES(masking),
  status = {STATUS_OK},
  last_error = NULL,
  retry_count = 0;
""")
        batches += 1
    if log_prefix:
        log(f"{log_prefix} | UPSERT 入库 {len(rows)} 条 ({batches} 批)")
    mysql_exec(host, port, user, password, database, "\n".join(stmts))


def upsert_failed(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, raw_values: List[str], error: str,
) -> None:
    if not raw_values:
        return
    vt, err = escape_sql(vt_type), escape_sql(error[:500])
    for i in range(0, len(raw_values), DB_INSERT_CHUNK):
        chunk = raw_values[i:i + DB_INSERT_CHUNK]
        values = ",\n".join(
            f"('{vt}', '{escape_sql(raw)}', {STATUS_FAIL}, 1, '{err}')"
            for raw in chunk
        )
        sql = f"""
INSERT INTO vt_token_cache (vt_type, raw_value, status, retry_count, last_error)
VALUES {values}
ON DUPLICATE KEY UPDATE
  status = {STATUS_FAIL},
  retry_count = retry_count + 1,
  last_error = VALUES(last_error);
"""
        mysql_exec(host, port, user, password, database, sql)


def fetch_stream_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, limit: int, retry_failed: bool,
) -> List[str]:
    if vt_type not in SOURCE_QUERIES:
        return []
    if retry_failed:
        sql = CACHE_RETRY_SQL.format(vt_type=escape_sql(vt_type), fail=STATUS_FAIL, limit=int(limit))
        log(f"[{vt_type}] 重试失败记录 SELECT ...")
    else:
        sql = _source_sql(vt_type, _not_vt_clause(vt_type), int(limit))
        log(f"[{vt_type}] 源表反查未 VT 明文 SELECT LIMIT {limit} ...")
    t0 = time.time()
    rows = mysql_query(host, port, user, password, database, sql)
    log(f"[{vt_type}] SELECT 完成 {time.time() - t0:.1f}s，取得 {len(rows)} 条")
    return rows


def count_stream_pending(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, retry_failed: bool,
) -> int:
    if vt_type not in SOURCE_QUERIES:
        return 0
    if retry_failed:
        sql = f"""
SELECT COUNT(*) FROM vt_token_cache
WHERE vt_type='{escape_sql(vt_type)}' AND status={STATUS_FAIL}
  AND raw_value IS NOT NULL AND raw_value <> '';
"""
        rows = mysql_query(host, port, user, password, database, sql)
        return int(rows[0]) if rows else 0
    inner = _source_sql(vt_type, _not_vt_clause(vt_type), None)
    sql = f"SELECT COUNT(*) FROM ({inner}) _cnt"
    log(f"[{vt_type}] 统计待 VT 总量（可能需 1~3 分钟）...")
    t0 = time.time()
    rows = mysql_query(host, port, user, password, database, sql)
    cnt = int(rows[0]) if rows else 0
    log(f"[{vt_type}] 待 VT 约 {cnt} 条，统计耗时 {time.time() - t0:.1f}s")
    return cnt


def chunk_items(items: List, chunk_size: int) -> List[List]:
    if chunk_size <= 0:
        return [items]
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


class RoundMonitor:
    def __init__(self, vt_type: str, round_no: int, chunk_total: int) -> None:
        self.vt_type, self.round_no, self.chunk_total = vt_type, round_no, chunk_total
        self.done_chunks = 0
        self.round_start = time.time()
        self._lock = threading.Lock()

    def chunk_done(self) -> None:
        with self._lock:
            self.done_chunks += 1


def heartbeat_loop(stop: threading.Event, monitor: RoundMonitor, progress: ProgressTracker, interval_sec: int) -> None:
    while not stop.wait(interval_sec):
        log(
            f"[{monitor.vt_type}] 第{monitor.round_no}轮 心跳 | "
            f"HTTP {monitor.done_chunks}/{monitor.chunk_total} 完成 | "
            f"本轮 {time.time() - monitor.round_start:.0f}s | {progress.summary()}"
        )


def process_stream_chunk(
    chunk: List[str], worker_id: int, round_no: int, chunk_no: int, chunk_total: int,
    vt_type: str, base_url: str, timeout_sec: int, max_retries: int,
    host: str, port: str, user: str, password: str, database: str,
    dry_run: bool, progress: ProgressTracker,
) -> Tuple[int, int, bool]:
    size = len(chunk)
    prefix = f"[{vt_type}] 第{round_no}轮 w={worker_id} {chunk_no}/{chunk_total} 本批{size}条"
    if dry_run:
        snap = progress.add(ok=size)
        log(f"{prefix} | [dry-run] | {progress.summary()}")
        return size, 0, False

    log(f"{prefix} → VT 请求开始")
    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        t0 = time.time()
        try:
            tokens, maskings, http_sec, parse_sec = call_v2t(base_url, chunk, timeout_sec)
            rows = [(chunk[i], tokens[i], maskings[i]) for i in range(size)]
            t_db = time.time()
            upsert_success(host, port, user, password, database, vt_type, rows, log_prefix=prefix)
            db_sec = time.time() - t_db
            snap = progress.add(ok=size)
            log(
                f"{prefix} | VT {http_sec:.1f}s | 解析 {parse_sec:.3f}s | UPSERT {db_sec:.1f}s | "
                f"合计 {time.time() - t0:.1f}s | {progress.summary()}"
            )
            return size, 0, False
        except (urllib.error.URLError, RuntimeError, TimeoutError) as e:
            last_err = str(e)
            log(f"{prefix} | 失败 {attempt}/{max_retries} {time.time() - t0:.1f}s | {last_err}", err=True)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    t_db = time.time()
    upsert_failed(host, port, user, password, database, vt_type, chunk, last_err)
    snap = progress.add(fail=size)
    log(f"{prefix} | 失败入库 {time.time() - t_db:.1f}s | {progress.summary()}", err=True)
    return 0, size, True


def process_vt_type_stream(
    vt_type: str, host: str, port: str, user: str, password: str, database: str,
    base_url: str, round_batch: int, http_batch_size: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int, skip_count: bool,
) -> int:
    if vt_type not in SOURCE_QUERIES:
        log(f"[{vt_type}] 无源表配置，跳过")
        return 0

    log(f"===== stream | {vt_type} | workers={workers} 每轮={round_batch} http={http_batch_size} =====")
    initial = 0 if skip_count else count_stream_pending(
        host, port, user, password, database, vt_type, retry_failed,
    )
    progress = ProgressTracker(vt_type, initial)
    round_no, exit_code = 0, 0

    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            break
        round_no += 1
        batch = fetch_stream_batch(
            host, port, user, password, database, vt_type, round_batch, retry_failed,
        )
        if not batch:
            log(f"[{vt_type}] 无更多待 VT 数据")
            break

        chunks = chunk_items(batch, http_batch_size)
        log(f"[{vt_type}] 第{round_no}轮 | {len(batch)}条 → {len(chunks)}个HTTP任务 | {progress.summary()}")

        monitor = RoundMonitor(vt_type, round_no, len(chunks))
        stop_hb = threading.Event()
        hb = None
        if heartbeat_sec > 0 and not dry_run:
            hb = threading.Thread(target=heartbeat_loop, args=(stop_hb, monitor, progress, heartbeat_sec), daemon=True)
            hb.start()

        round_t0 = time.time()
        chunk_failed = False
        try:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futs = {
                    pool.submit(
                        process_stream_chunk, ch, i + 1, round_no, i + 1, len(chunks),
                        vt_type, base_url, timeout_sec, max_retries,
                        host, port, user, password, database, dry_run, progress,
                    ): i for i, ch in enumerate(chunks)
                }
                for fut in as_completed(futs):
                    _, _, hard = fut.result()
                    monitor.chunk_done()
                    if hard:
                        chunk_failed = True
        finally:
            stop_hb.set()
            if hb:
                hb.join(timeout=1)

        log(f"[{vt_type}] 第{round_no}轮结束 {time.time() - round_t0:.1f}s | {progress.summary()}")
        if chunk_failed:
            exit_code = 2

    log(f"[{vt_type}] stream 结束 | {progress.summary()}")
    return exit_code


# ---------- legacy cache 模式（保留兼容）----------

def status_clause(retry_failed: bool) -> str:
    return f"status IN ({STATUS_PENDING},{STATUS_FAIL})" if retry_failed else f"status = {STATUS_PENDING}"


def parse_id_raw_rows(rows: List[str]) -> List[Tuple[int, str]]:
    return [(int(p[0]), p[1]) for r in rows if len((p := r.split("\t", 1))) == 2]


def claim_and_fetch_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, batch_size: int, retry_failed: bool,
) -> List[Tuple[int, str]]:
    vt, limit = escape_sql(vt_type), int(batch_size)
    log(f"[{vt_type}] cache认领 UPDATE LIMIT {limit} ...")
    t0 = time.time()
    mysql_exec(host, port, user, password, database, f"""
UPDATE vt_token_cache SET status={STATUS_PROCESSING}
WHERE vt_type='{vt}' AND {status_clause(retry_failed)}
  AND raw_value IS NOT NULL AND raw_value <> ''
ORDER BY id LIMIT {limit};
""")
    log(f"[{vt_type}] 认领 UPDATE {time.time() - t0:.1f}s")
    sql = f"""
SELECT id, raw_value FROM vt_token_cache
WHERE vt_type='{vt}' AND status={STATUS_PROCESSING} ORDER BY id LIMIT {limit};
"""
    return parse_id_raw_rows(mysql_query(host, port, user, password, database, sql))


def mark_success_cache(
    host: str, port: str, user: str, password: str, database: str,
    updates: List[Tuple[int, str, Optional[str]]], log_prefix: str = "",
) -> None:
    if not updates:
        return
    stmts = [
        "DROP TEMPORARY TABLE IF EXISTS _vt_preload_batch;",
        """CREATE TEMPORARY TABLE _vt_preload_batch (
            id BIGINT PRIMARY KEY, token VARCHAR(128) NOT NULL, masking VARCHAR(128) NULL
        ) ENGINE=MEMORY;""",
    ]
    for i in range(0, len(updates), DB_INSERT_CHUNK):
        chunk = updates[i:i + DB_INSERT_CHUNK]
        vals = ",\n".join(f"({rid},'{escape_sql(tok)}',{_sql_lit(mask)})" for rid, tok, mask in chunk)
        stmts.append(f"INSERT INTO _vt_preload_batch (id, token, masking) VALUES\n{vals};")
    stmts.append(f"""
UPDATE vt_token_cache v INNER JOIN _vt_preload_batch t ON v.id=t.id
SET v.status={STATUS_OK}, v.token=t.token, v.masking=t.masking, v.last_error=NULL;
DROP TEMPORARY TABLE IF EXISTS _vt_preload_batch;
""")
    if log_prefix:
        log(f"{log_prefix} | cache JOIN UPDATE {len(updates)} 条")
    mysql_exec(host, port, user, password, database, "\n".join(stmts))


def process_cache_chunk(
    chunk: List[Tuple[int, str]], worker_id: int, round_no: int, chunk_no: int, chunk_total: int,
    vt_type: str, base_url: str, timeout_sec: int, max_retries: int,
    host: str, port: str, user: str, password: str, database: str,
    dry_run: bool, progress: ProgressTracker,
) -> Tuple[int, int, bool]:
    ids, values = [c[0] for c in chunk], [c[1] for c in chunk]
    prefix = f"[{vt_type}] cache 第{round_no}轮 w={worker_id} {chunk_no}/{chunk_total} {len(values)}条"
    if dry_run:
        progress.add(ok=len(values))
        return len(values), 0, False
    log(f"{prefix} → VT 开始")
    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        t0 = time.time()
        try:
            tokens, maskings, http_sec, _ = call_v2t(base_url, values, timeout_sec)
            updates = [(ids[i], tokens[i], maskings[i]) for i in range(len(ids))]
            t_db = time.time()
            mark_success_cache(host, port, user, password, database, updates, prefix)
            progress.add(ok=len(values))
            log(f"{prefix} | VT {http_sec:.1f}s | 入库 {time.time() - t_db:.1f}s | 合计 {time.time() - t0:.1f}s | {progress.summary()}")
            return len(values), 0, False
        except (urllib.error.URLError, RuntimeError, TimeoutError) as e:
            last_err = str(e)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)
    progress.add(fail=len(values))
    return 0, len(values), True


def reset_processing(host: str, port: str, user: str, password: str, database: str, vt_type: Optional[str]) -> None:
    tc = "" if not vt_type else f" AND vt_type='{escape_sql(vt_type)}'"
    mysql_exec(host, port, user, password, database,
               f"UPDATE vt_token_cache SET status={STATUS_PENDING} WHERE status={STATUS_PROCESSING}{tc};")


def process_vt_type_cache(
    vt_type: str, host: str, port: str, user: str, password: str, database: str,
    base_url: str, claim_batch: int, http_batch: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int,
) -> int:
    sql = f"""
SELECT COUNT(*) FROM vt_token_cache WHERE vt_type='{escape_sql(vt_type)}' AND {status_clause(retry_failed)};
"""
    pending = int(mysql_query(host, port, user, password, database, sql)[0])
    if pending == 0:
        log(f"[{vt_type}] cache 无待处理")
        return 0
    progress = ProgressTracker(vt_type, pending)
    round_no, exit_code = 0, 0
    while True:
        if max_rounds > 0 and round_no >= max_rounds:
            break
        round_no += 1
        batch = claim_and_fetch_batch(host, port, user, password, database, vt_type, claim_batch, retry_failed)
        if not batch:
            break
        chunks = chunk_items(batch, http_batch)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = [pool.submit(
                process_cache_chunk, ch, i + 1, round_no, i + 1, len(chunks),
                vt_type, base_url, timeout_sec, max_retries,
                host, port, user, password, database, dry_run, progress,
            ) for i, ch in enumerate(chunks)]
            for fut in as_completed(futs):
                if fut.result()[2]:
                    exit_code = 2
    log(f"[{vt_type}] cache 结束 | {progress.summary()}")
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="VT 预加载 /v2t")
    parser.add_argument("--mode", choices=["stream", "cache"], default=None,
                        help="stream=源表直查+UPSERT(默认,快); cache=旧认领模式")
    parser.add_argument("--batch-size", type=int, default=None, help="每轮拉取条数")
    parser.add_argument("--http-batch-size", type=int, default=None, help="单次 /v2t 条数")
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--max-rounds", "--max-batches", type=int, default=0, dest="max_rounds")
    parser.add_argument("--vt-type", default="all")
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--reset-processing", action="store_true")
    parser.add_argument("--skip-count", action="store_true", help="跳过启动时慢 COUNT")
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
    mode = args.mode or os.environ.get("VT_PRELOAD_MODE", "stream")
    workers = args.workers or int(os.environ.get("VT_PRELOAD_WORKERS", "20"))
    round_batch = args.batch_size or int(os.environ.get("VT_PRELOAD_BATCH_SIZE", "1000000"))
    http_batch = args.http_batch_size or int(os.environ.get("VT_PRELOAD_HTTP_BATCH", "50000"))
    timeout_sec = int(os.environ.get("VT_BATCH_TIMEOUT_SEC", "300"))
    max_retries = int(os.environ.get("VT_BATCH_MAX_RETRIES", "3"))
    heartbeat_sec = int(os.environ.get("VT_PRELOAD_HEARTBEAT_SEC", str(DEFAULT_HEARTBEAT_SEC)))

    if not all([host, user, password, database]):
        print("缺少 SOURCE_MYSQL_* 配置", file=sys.stderr)
        return 1

    all_types = ["mobile", "gaid_idfa", "bank_account", "id_number"]
    vt_types = all_types if args.vt_type == "all" else [args.vt_type]

    if args.reset_processing:
        reset_processing(host, port, user, password, database, None if args.vt_type == "all" else args.vt_type)
        log("已重置 status=9 → 0")
        return 0

    check_table_ready(host, port, user, password, database)

    if mode == "cache":
        log("cache 模式: 重置 status=9 ...")
        reset_processing(host, port, user, password, database, None if args.vt_type == "all" else args.vt_type)

    log(
        f"VT 预加载 mode={mode} | types={vt_types} | workers={workers} | "
        f"每轮={round_batch} | http={http_batch} | url={base_url}"
    )
    if mode == "stream":
        log("stream: 无需 init_all 预灌；源表反查 → VT → UPSERT 一次写入")

    exit_code = 0
    proc = process_vt_type_stream if mode == "stream" else process_vt_type_cache
    for vt_type in vt_types:
        kw: Dict[str, Union[int, bool, str]] = dict(
            vt_type=vt_type, host=host, port=port, user=user, password=password,
            database=database, base_url=base_url, timeout_sec=timeout_sec,
            max_retries=max_retries, max_rounds=args.max_rounds,
            retry_failed=args.retry_failed, dry_run=args.dry_run,
            heartbeat_sec=heartbeat_sec,
        )
        if mode == "stream":
            kw.update(round_batch=round_batch, http_batch_size=http_batch, workers=workers, skip_count=args.skip_count)
        else:
            kw.update(claim_batch=round_batch, http_batch=http_batch, workers=workers)
        code = proc(**kw)  # type: ignore[arg-type]
        if code != 0:
            exit_code = code
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
