#!/usr/bin/env python3
"""
VT 字典预加载：批量 /v2t 写入 vt_token_cache。

默认 fast 模式（目标 20万/分钟）：
  认领 status=0 → 4路×5万 并行 /v2t（HTTP长连接）→ 异步入库 UPSERT
  需 init_all 预灌；勿用 stream 源表反查（极慢）

mode=stream：源表 NOT EXISTS 反查（极慢，仅无预灌时用）
mode=cache：旧认领 UPDATE 模式

用法:
  ./scripts/vt-preload.sh --workers 4 --http-batch-size 50000
  ./scripts/vt-preload.sh --mode stream   # 无 init 时慢路径
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, Union

STATUS_PENDING = 0
STATUS_OK = 1
STATUS_FAIL = 2
STATUS_PROCESSING = 9

_print_lock = threading.Lock()
_db_write_lock = threading.Lock()
# 单次 UPSERT 行数（过大则按此拆分）；50k 一批对齐 Flink /v2t
DB_UPSERT_CHUNK = 50000
DEFAULT_HEARTBEAT_SEC = 30
TARGET_RATE_PER_MIN = 200_000

# vt_token_cache.vt_type TINYINT（见 sql/ddl/vt_type_codes.sql）
VT_TYPE_CODE: Dict[str, int] = {
    "mobile": 1,
    "gaid_idfa": 2,
    "bank_account": 3,
    "id_number": 4,
    "emergency_contact": 5,
    "id2": 6,
}


def vt_type_db(name: str) -> int:
    code = VT_TYPE_CODE.get(name)
    if code is None:
        raise KeyError(f"unknown vt_type: {name}")
    return code

# 源表与 vt_token_cache 排序规则可能不同（0900_ai_ci vs unicode_ci），比较时统一 COLLATE
COLLATE_CMP = "utf8mb4_bin"

SOURCE_NOT_VT = """
AND NOT EXISTS (
  SELECT 1 FROM vt_token_cache vt
  WHERE vt.vt_type = {vt_type}
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
    "emergency_contact": """
SELECT src.raw_value FROM (
  SELECT DISTINCT
    CASE
      WHEN ec.contact_number IS NULL OR TRIM(ec.contact_number) = '' THEN NULL
      WHEN TRIM(ec.contact_number) LIKE '+%' THEN TRIM(ec.contact_number)
      WHEN TRIM(ec.contact_number) LIKE '234%' THEN CONCAT('+', TRIM(ec.contact_number))
      WHEN TRIM(ec.contact_number) LIKE '0%' THEN CONCAT('+234', SUBSTRING(TRIM(ec.contact_number), 2))
      ELSE CONCAT('+234', TRIM(ec.contact_number))
    END AS raw_value
  FROM user_emergency_contact ec
) src
WHERE src.raw_value IS NOT NULL AND src.raw_value <> ''
{not_vt}
{limit_clause}
""",
}


def _not_vt_clause(vt_type: str, ok: int = STATUS_OK) -> str:
    return SOURCE_NOT_VT.format(
        vt_type=vt_type_db(vt_type), ok=ok, collate=COLLATE_CMP,
    )


def _source_sql(vt_type: str, not_vt: str, limit: Optional[int]) -> str:
    clause = f"LIMIT {int(limit)}" if limit is not None else ""
    return SOURCE_QUERIES[vt_type].format(not_vt=not_vt, limit_clause=clause)

CACHE_PENDING_ID_SQL = """
SELECT id, raw_value FROM vt_token_cache
WHERE vt_type = {vt_type} AND status = {status}
  AND raw_value IS NOT NULL AND raw_value <> ''
ORDER BY id
LIMIT {limit}
"""

# 写库策略（均需先 SELECT id）:
#   update_id=按主键批量 UPDATE（默认，仅需 UPDATE 权限，flink_cdc 可用）
#   delete_insert=DELETE+INSERT（需 DELETE 权限）
#   upsert=ON DUPLICATE KEY UPDATE（stream 无 id 时用）
WRITE_UPDATE_ID = "update_id"
WRITE_DELETE_INSERT = "delete_insert"
WRITE_UPSERT = "upsert"
WRITE_MODES = (WRITE_UPDATE_ID, WRITE_DELETE_INSERT, WRITE_UPSERT)
DB_ID_CHUNK = 3000
DB_DELETE_CHUNK = 5000


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
            hint = (
                f"\n  GRANT SELECT, INSERT, UPDATE ON {database}.vt_token_cache TO '{user}'@'<IP>';"
            )
            if "DELETE" in err:
                hint += (
                    "\n  delete_insert 模式还需 DELETE；或改用 VT_PRELOAD_WRITE_MODE=update_id（默认）"
                )
            err += "\n提示: vt-preload 权限不足。" + hint + "\n  见 sql/ddl/vt_token_cache_grants.sql"
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


class V2tHttpClient:
    """线程内复用 HTTP 连接，对齐 Flink VtBatchClient 长连接行为。"""

    def __init__(self, base_url: str, timeout_sec: int) -> None:
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlparse(self.base_url)
        self._host = parsed.hostname or "localhost"
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._https = parsed.scheme == "https"
        self._timeout = timeout_sec
        self._conn: Optional[http.client.HTTPConnection] = None

    def _connection(self) -> http.client.HTTPConnection:
        if self._conn is not None:
            return self._conn
        if self._https:
            import ssl  # noqa: PLC0415
            ctx = ssl.create_default_context()
            self._conn = http.client.HTTPSConnection(
                self._host, self._port, timeout=self._timeout, context=ctx,
            )
        else:
            self._conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
        return self._conn

    def call_v2t(self, values: List[str]) -> Tuple[List[str], List[Optional[str]], float, float]:
        path = urllib.parse.urlparse(self.base_url).path.rstrip("/") + "/v2t"
        payload = json.dumps(values, ensure_ascii=False).encode("utf-8")
        t_http = time.time()
        last_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                conn = self._connection()
                conn.request("POST", path, body=payload, headers={"Content-Type": "application/json"})
                resp = conn.getresponse()
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {body[:500]}")
                http_sec = time.time() - t_http
                t_parse = time.time()
                tokens, maskings = parse_tokens(body, len(values))
                return tokens, maskings, http_sec, time.time() - t_parse
            except (http.client.HTTPException, OSError, RuntimeError) as e:
                last_err = e
                try:
                    if self._conn:
                        self._conn.close()
                except Exception:
                    pass
                self._conn = None
        raise RuntimeError(str(last_err))


_vt_tls = threading.local()


def call_v2t(base_url: str, values: List[str], timeout_sec: int) -> Tuple[List[str], List[Optional[str]], float, float]:
    if not hasattr(_vt_tls, "clients"):
        _vt_tls.clients = {}
    client = _vt_tls.clients.get(base_url)
    if client is None:
        client = V2tHttpClient(base_url, timeout_sec)
        _vt_tls.clients[base_url] = client
    return client.call_v2t(values)


class AsyncUpsertWriter:
    """VT 与入库流水线：HTTP 返回后立即入队，后台线程写 MySQL。"""

    _STOP = object()

    def __init__(
        self, host: str, port: str, user: str, password: str, database: str,
        vt_type: str, workers: int = 2, write_mode: str = WRITE_UPDATE_ID,
    ) -> None:
        self._db = (host, port, user, password, database)
        self.vt_type = vt_type
        self.write_mode = write_mode
        self._q: queue.Queue[Any] = queue.Queue(maxsize=32)
        self._threads = [
            threading.Thread(target=self._loop, name=f"upsert-{i}", daemon=True)
            for i in range(max(1, workers))
        ]
        self._started = False
        self._errors: List[str] = []
        self._err_lock = threading.Lock()

    def start(self) -> None:
        if self._started:
            return
        for t in self._threads:
            t.start()
        self._started = True

    def _loop(self) -> None:
        while True:
            item = self._q.get()
            try:
                if item is self._STOP:
                    break
                kind = item[0]
                host, port, user, password, database = self._db
                if kind == "ok":
                    if item[1] in (WRITE_UPDATE_ID, WRITE_DELETE_INSERT):
                        fn = delete_insert_success if item[1] == WRITE_DELETE_INSERT else update_success_by_id
                        fn(host, port, user, password, database, self.vt_type, item[2])
                    else:
                        upsert_success(host, port, user, password, database, self.vt_type, item[2])
                elif kind == "fail":
                    if item[1] in (WRITE_UPDATE_ID, WRITE_DELETE_INSERT):
                        mark_failed_by_ids(host, port, user, password, database, item[2], item[3])
                    else:
                        upsert_failed(host, port, user, password, database, self.vt_type, item[2], item[3])
            except Exception as e:
                with self._err_lock:
                    self._errors.append(str(e))
                log(f"[{self.vt_type}] 异步入库失败: {e}", err=True)
            finally:
                self._q.task_done()

    def enqueue_ok(self, write_mode: str, rows: Any) -> None:
        self._q.put(("ok", write_mode, rows))

    def enqueue_fail(self, write_mode: str, payload: Any, err: str) -> None:
        self._q.put(("fail", write_mode, payload, err))

    def drain(self) -> None:
        self._q.join()
        with self._err_lock:
            if self._errors:
                sample = "\n".join(self._errors[:3])
                raise RuntimeError(f"异步入库失败 {len(self._errors)} 次，示例:\n{sample}")

    def stop(self) -> None:
        self.drain()
        for _ in self._threads:
            self._q.put(self._STOP)
        for t in self._threads:
            t.join(timeout=30)


def _sql_lit(s: Optional[str]) -> str:
    return "NULL" if s is None else f"'{escape_sql(s)}'"


def parse_id_raw_rows(rows: List[str]) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for row in rows:
        parts = row.split("\t", 1)
        if len(parts) == 2:
            out.append((int(parts[0]), parts[1]))
    return out


def update_success_by_id(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, rows: List[Tuple[int, str, str, Optional[str]]],
) -> None:
    """按主键 id 批量 UPDATE token（无需 DELETE，flink_cdc 仅需 UPDATE）。"""
    if not rows:
        return
    for i in range(0, len(rows), DB_UPSERT_CHUNK):
        chunk = rows[i:i + DB_UPSERT_CHUNK]
        for j in range(0, len(chunk), DB_ID_CHUNK):
            sub = chunk[j:j + DB_ID_CHUNK]
            token_cases = " ".join(
                f"WHEN {rid} THEN '{escape_sql(tok)}'" for rid, _raw, tok, _mask in sub
            )
            mask_parts = []
            for rid, _raw, _tok, mask in sub:
                if mask is None:
                    mask_parts.append(f"WHEN {rid} THEN NULL")
                else:
                    mask_parts.append(f"WHEN {rid} THEN '{escape_sql(mask)}'")
            mask_cases = " ".join(mask_parts)
            id_list = ",".join(str(rid) for rid, *_ in sub)
            sql = f"""
UPDATE vt_token_cache SET
  status = {STATUS_OK},
  last_error = NULL,
  retry_count = 0,
  token = CASE id {token_cases} END,
  masking = CASE id {mask_cases} END
WHERE id IN ({id_list});
"""
            with _db_write_lock:
                mysql_exec(host, port, user, password, database, sql)


def delete_insert_success(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, rows: List[Tuple[int, str, str, Optional[str]]],
) -> None:
    """DELETE 旧 status=0 行 + INSERT 新 token 行，比 UPDATE/UPSERT 更轻。"""
    if not rows:
        return
    vt = vt_type_db(vt_type)
    for i in range(0, len(rows), DB_UPSERT_CHUNK):
        chunk = rows[i:i + DB_UPSERT_CHUNK]
        stmts: List[str] = []
        for j in range(0, len(chunk), DB_DELETE_CHUNK):
            sub = chunk[j:j + DB_DELETE_CHUNK]
            ids = ",".join(str(r[0]) for r in sub)
            stmts.append(f"DELETE FROM vt_token_cache WHERE id IN ({ids});")
        values = ",\n".join(
            f"({vt}, '{escape_sql(raw)}', '{escape_sql(tok)}', {_sql_lit(mask)}, {STATUS_OK})"
            for _id, raw, tok, mask in chunk
        )
        stmts.append(f"""
INSERT INTO vt_token_cache (vt_type, raw_value, token, masking, status)
VALUES {values};
""")
        with _db_write_lock:
            mysql_exec(host, port, user, password, database, "\n".join(stmts))


def mark_failed_by_ids(
    host: str, port: str, user: str, password: str, database: str,
    ids: List[int], error: str,
) -> None:
    if not ids:
        return
    err = escape_sql(error[:500])
    for i in range(0, len(ids), DB_DELETE_CHUNK):
        chunk = ids[i:i + DB_DELETE_CHUNK]
        id_list = ",".join(str(x) for x in chunk)
        sql = (
            f"UPDATE vt_token_cache SET status={STATUS_FAIL}, retry_count=retry_count+1, "
            f"last_error='{err}' WHERE id IN ({id_list});"
        )
        with _db_write_lock:
            mysql_exec(host, port, user, password, database, sql)


def upsert_success(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, rows: List[Tuple[str, str, Optional[str]]], log_prefix: str = "",
) -> None:
    if not rows:
        return
    vt = vt_type_db(vt_type)
    for i in range(0, len(rows), DB_UPSERT_CHUNK):
        chunk = rows[i:i + DB_UPSERT_CHUNK]
        values = ",\n".join(
            f"({vt}, '{escape_sql(raw)}', '{escape_sql(tok)}', {_sql_lit(mask)}, {STATUS_OK})"
            for raw, tok, mask in chunk
        )
        sql = f"""
INSERT INTO vt_token_cache (vt_type, raw_value, token, masking, status)
VALUES {values}
ON DUPLICATE KEY UPDATE
  token = VALUES(token),
  masking = VALUES(masking),
  status = {STATUS_OK},
  last_error = NULL,
  retry_count = 0;
"""
        with _db_write_lock:
            mysql_exec(host, port, user, password, database, sql)


def upsert_failed(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, raw_values: List[str], error: str,
) -> None:
    if not raw_values:
        return
    vt, err = vt_type_db(vt_type), escape_sql(error[:500])
    for i in range(0, len(raw_values), DB_UPSERT_CHUNK):
        chunk = raw_values[i:i + DB_UPSERT_CHUNK]
        values = ",\n".join(
            f"({vt}, '{escape_sql(raw)}', {STATUS_FAIL}, 1, '{err}')"
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


def fetch_fast_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, limit: int, retry_failed: bool,
) -> List[Tuple[int, str]]:
    """只 SELECT id+raw_value（无认领 UPDATE），VT 后 DELETE+INSERT。"""
    vt, lim = vt_type_db(vt_type), int(limit)
    status = STATUS_FAIL if retry_failed else STATUS_PENDING
    sql = CACHE_PENDING_ID_SQL.format(vt_type=vt, status=status, limit=lim)
    log(f"[{vt_type}] fast: SELECT id,raw status={status} LIMIT {lim} ...")
    t0 = time.time()
    rows = parse_id_raw_rows(mysql_query(host, port, user, password, database, sql))
    log(f"[{vt_type}] SELECT {time.time() - t0:.2f}s，{len(rows)} 条")
    return rows


def count_token_stats(
    host: str, port: str, user: str, password: str, database: str, vt_type: str,
) -> Dict[int, int]:
    sql = f"""
SELECT status, COUNT(*) FROM vt_token_cache
WHERE vt_type={vt_type_db(vt_type)} GROUP BY status ORDER BY status;
"""
    rows = mysql_query(host, port, user, password, database, sql)
    stats: Dict[int, int] = {}
    for row in rows:
        parts = row.split("\t", 1)
        if len(parts) == 2:
            stats[int(parts[0])] = int(parts[1])
    return stats


def count_ok_with_token(
    host: str, port: str, user: str, password: str, database: str, vt_type: str,
) -> int:
    sql = f"""
SELECT COUNT(*) FROM vt_token_cache
WHERE vt_type={vt_type_db(vt_type)} AND status={STATUS_OK}
  AND token IS NOT NULL AND token <> '';
"""
    rows = mysql_query(host, port, user, password, database, sql)
    return int(rows[0]) if rows else 0


def count_fast_pending(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, retry_failed: bool,
) -> int:
    status = STATUS_FAIL if retry_failed else STATUS_PENDING
    sql = f"""
SELECT COUNT(*) FROM vt_token_cache
WHERE vt_type={vt_type_db(vt_type)} AND status={status}
  AND raw_value IS NOT NULL AND raw_value <> '';
"""
    rows = mysql_query(host, port, user, password, database, sql)
    return int(rows[0]) if rows else 0


def fetch_stream_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, limit: int, retry_failed: bool,
) -> List[Tuple[int, str]]:
    if vt_type not in SOURCE_QUERIES:
        return []
    if retry_failed:
        return fetch_fast_batch(host, port, user, password, database, vt_type, limit, True)
    sql = _source_sql(vt_type, _not_vt_clause(vt_type), int(limit))
    log(f"[{vt_type}] stream慢路径: 源表 NOT EXISTS 反查 LIMIT {limit}（大表可能数分钟）...")
    t0 = time.time()
    raws = mysql_query(host, port, user, password, database, sql)
    log(f"[{vt_type}] SELECT 完成 {time.time() - t0:.1f}s，取得 {len(raws)} 条")
    return [(0, r) for r in raws]


def count_stream_pending(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, retry_failed: bool,
) -> int:
    if vt_type not in SOURCE_QUERIES:
        return 0
    if retry_failed:
        sql = f"""
SELECT COUNT(*) FROM vt_token_cache
WHERE vt_type={vt_type_db(vt_type)} AND status={STATUS_FAIL}
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
    chunk: List[Tuple[int, str]], worker_id: int, round_no: int, chunk_no: int, chunk_total: int,
    vt_type: str, base_url: str, timeout_sec: int, max_retries: int,
    host: str, port: str, user: str, password: str, database: str,
    dry_run: bool, progress: ProgressTracker,
    writer: Optional[AsyncUpsertWriter] = None,
    write_mode: str = WRITE_UPDATE_ID,
) -> Tuple[int, int, bool]:
    size = len(chunk)
    ids = [c[0] for c in chunk]
    values = [c[1] for c in chunk]
    prefix = f"[{vt_type}] r{round_no} w{worker_id} {chunk_no}/{chunk_total} n={size}"
    if dry_run:
        progress.add(ok=size)
        log(f"{prefix} | dry-run | {progress.summary()}")
        return size, 0, False

    last_err = "unknown"
    for attempt in range(1, max_retries + 1):
        t0 = time.time()
        try:
            tokens, maskings, http_sec, parse_sec = call_v2t(base_url, values, timeout_sec)
            if write_mode in (WRITE_UPDATE_ID, WRITE_DELETE_INSERT):
                ok_rows = [
                    (ids[i], values[i], tokens[i], maskings[i]) for i in range(size)
                ]
                if writer is not None:
                    writer.enqueue_ok(write_mode, ok_rows)
                elif write_mode == WRITE_DELETE_INSERT:
                    delete_insert_success(host, port, user, password, database, vt_type, ok_rows)
                else:
                    update_success_by_id(host, port, user, password, database, vt_type, ok_rows)
            else:
                ok_rows = [(values[i], tokens[i], maskings[i]) for i in range(size)]
                if writer is not None:
                    writer.enqueue_ok(write_mode, ok_rows)
                else:
                    upsert_success(host, port, user, password, database, vt_type, ok_rows)
            snap = progress.add(ok=size)
            rate = snap["rate"]
            flag = "✓" if rate >= TARGET_RATE_PER_MIN * 0.8 else "△"
            wlabel = {"update_id": "UPD-id", "delete_insert": "DEL+INS", "upsert": "UPSERT"}.get(
                write_mode, write_mode,
            )
            log(
                f"{prefix} | VT {http_sec:.1f}s 解析{parse_sec:.3f}s "
                f"{'入队' if writer else wlabel} | {time.time() - t0:.1f}s | "
                f"{flag} {rate:.0f}/min 目标{TARGET_RATE_PER_MIN} | {progress.summary()}"
            )
            return size, 0, False
        except (urllib.error.URLError, RuntimeError, TimeoutError, http.client.HTTPException, OSError) as e:
            last_err = str(e)
            log(f"{prefix} | 失败 {attempt}/{max_retries} | {last_err}", err=True)
            if attempt < max_retries:
                time.sleep(0.5 * attempt)

    if write_mode in (WRITE_UPDATE_ID, WRITE_DELETE_INSERT):
        fail_payload = ids
        if writer is not None:
            writer.enqueue_fail(write_mode, fail_payload, last_err)
        else:
            mark_failed_by_ids(host, port, user, password, database, ids, last_err)
    else:
        if writer is not None:
            writer.enqueue_fail(write_mode, values, last_err)
        else:
            upsert_failed(host, port, user, password, database, vt_type, values, last_err)
    progress.add(fail=size)
    return 0, size, True


def _run_vt_rounds(
    vt_type: str, mode_label: str,
    host: str, port: str, user: str, password: str, database: str,
    base_url: str, round_batch: int, http_batch_size: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int, progress: ProgressTracker,
    fetch_fn, write_workers: int, async_write: bool, write_mode: str,
) -> int:
    log(
        f"===== {mode_label} | {vt_type} | VT并发={workers} "
        f"每轮={round_batch} 批={http_batch_size} 目标={TARGET_RATE_PER_MIN}/min ====="
    )
    writer: Optional[AsyncUpsertWriter] = None
    if async_write and not dry_run:
        writer = AsyncUpsertWriter(host, port, user, password, database, vt_type, write_workers, write_mode)
        writer.start()
        wlabel = {"update_id": "UPDATE-by-id", "delete_insert": "DELETE+INSERT", "upsert": "UPSERT"}.get(
            write_mode, write_mode,
        )
        log(f"[{vt_type}] 异步入库 {write_workers} 线程 | {wlabel}（VT 与写库流水线）")

    round_no, exit_code = 0, 0
    try:
        while True:
            if max_rounds > 0 and round_no >= max_rounds:
                break
            round_no += 1
            batch = fetch_fn(host, port, user, password, database, vt_type, round_batch, retry_failed)
            if not batch:
                log(f"[{vt_type}] 无更多待 VT 数据")
                break
            chunks = chunk_items(batch, http_batch_size)
            log(f"[{vt_type}] 第{round_no}轮 | {len(batch)}条 → {len(chunks)}路VT | {progress.summary()}")
            monitor = RoundMonitor(vt_type, round_no, len(chunks))
            stop_hb = threading.Event()
            hb = None
            if heartbeat_sec > 0 and not dry_run:
                hb = threading.Thread(
                    target=heartbeat_loop, args=(stop_hb, monitor, progress, heartbeat_sec), daemon=True,
                )
                hb.start()
            round_t0 = time.time()
            chunk_failed = False
            try:
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    futs = {
                        pool.submit(
                            process_stream_chunk, ch, i + 1, round_no, i + 1, len(chunks),
                            vt_type, base_url, timeout_sec, max_retries,
                        host, port, user, password, database, dry_run, progress, writer, write_mode,
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
            if writer is not None:
                writer.drain()
            ok_tokens = count_ok_with_token(host, port, user, password, database, vt_type)
            round_rate = len(batch) / max(time.time() - round_t0, 0.001) * 60
            log(
                f"[{vt_type}] 第{round_no}轮 {time.time() - round_t0:.1f}s "
                f"本轮≈{round_rate:.0f}/min | DB已有token={ok_tokens} | {progress.summary()}"
            )
            if chunk_failed:
                exit_code = 2
    finally:
        if writer is not None:
            writer.stop()
    stats = count_token_stats(host, port, user, password, database, vt_type)
    ok_tokens = count_ok_with_token(host, port, user, password, database, vt_type)
    log(f"[{vt_type}] {mode_label} 结束 | 库内 status 分布: {stats} | 有效token={ok_tokens}")
    if progress.ok > 0 and ok_tokens == 0:
        log(
            f"[{vt_type}] 严重: VT 报告成功 {progress.ok} 条但库内 token=0！"
            " 若曾用 delete_insert 可能已删未插。请执行 sql/ddl/vt_seed_mobile.sql 后改用 update_id 重跑。",
            err=True,
        )
        exit_code = 2
    else:
        log(f"[{vt_type}] {progress.summary()}")
    return exit_code


def process_vt_type_fast(
    vt_type: str, host: str, port: str, user: str, password: str, database: str,
    base_url: str, round_batch: int, http_batch_size: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int, skip_count: bool,
    write_workers: int, async_write: bool, write_mode: str,
) -> int:
    initial = 0 if skip_count else count_fast_pending(
        host, port, user, password, database, vt_type, retry_failed,
    )
    if initial == 0 and not skip_count and not retry_failed:
        log(f"[{vt_type}] cache 无 status=0。请先: mysql ... < sql/ddl/vt_token_cache_init_all.sql")
        log(f"[{vt_type}] 或改用 --mode stream（慢）从源表反查")
        return 0
    progress = ProgressTracker(vt_type, initial)
    return _run_vt_rounds(
        vt_type, "fast", host, port, user, password, database, base_url,
        round_batch, http_batch_size, workers, timeout_sec, max_retries,
        max_rounds, retry_failed, dry_run, heartbeat_sec, progress, fetch_fast_batch,
        write_workers, async_write, write_mode,
    )


def process_vt_type_stream(
    vt_type: str, host: str, port: str, user: str, password: str, database: str,
    base_url: str, round_batch: int, http_batch_size: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int, skip_count: bool,
    write_workers: int, async_write: bool, write_mode: str,
) -> int:
    if vt_type not in SOURCE_QUERIES:
        log(f"[{vt_type}] 无源表配置，跳过")
        return 0
    initial = 0 if skip_count else count_stream_pending(
        host, port, user, password, database, vt_type, retry_failed,
    )
    progress = ProgressTracker(vt_type, initial)
    return _run_vt_rounds(
        vt_type, "stream", host, port, user, password, database, base_url,
        round_batch, http_batch_size, workers, timeout_sec, max_retries,
        max_rounds, retry_failed, dry_run, heartbeat_sec, progress, fetch_stream_batch,
        write_workers, async_write, WRITE_UPSERT,
    )


# ---------- legacy cache 模式（保留兼容）----------

def status_clause(retry_failed: bool) -> str:
    return f"status IN ({STATUS_PENDING},{STATUS_FAIL})" if retry_failed else f"status = {STATUS_PENDING}"


def claim_and_fetch_batch(
    host: str, port: str, user: str, password: str, database: str,
    vt_type: str, batch_size: int, retry_failed: bool,
) -> List[Tuple[int, str]]:
    vt, limit = vt_type_db(vt_type), int(batch_size)
    log(f"[{vt_type}] cache认领 UPDATE LIMIT {limit} ...")
    t0 = time.time()
    mysql_exec(host, port, user, password, database, f"""
UPDATE vt_token_cache SET status={STATUS_PROCESSING}
WHERE vt_type={vt} AND {status_clause(retry_failed)}
  AND raw_value IS NOT NULL AND raw_value <> ''
ORDER BY id LIMIT {limit};
""")
    log(f"[{vt_type}] 认领 UPDATE {time.time() - t0:.1f}s")
    sql = f"""
SELECT id, raw_value FROM vt_token_cache
WHERE vt_type={vt} AND status={STATUS_PROCESSING} ORDER BY id LIMIT {limit};
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
    tc = "" if not vt_type else f" AND vt_type={vt_type_db(vt_type)}"
    mysql_exec(host, port, user, password, database,
               f"UPDATE vt_token_cache SET status={STATUS_PENDING} WHERE status={STATUS_PROCESSING}{tc};")


def process_vt_type_cache(
    vt_type: str, host: str, port: str, user: str, password: str, database: str,
    base_url: str, claim_batch: int, http_batch: int, workers: int,
    timeout_sec: int, max_retries: int, max_rounds: int, retry_failed: bool,
    dry_run: bool, heartbeat_sec: int,
) -> int:
    sql = f"""
SELECT COUNT(*) FROM vt_token_cache WHERE vt_type={vt_type_db(vt_type)} AND {status_clause(retry_failed)};
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
    parser.add_argument("--mode", choices=["fast", "stream", "cache"], default=None,
                        help="fast=读cache status=0(默认,对齐Flink); stream=源表反查(慢); cache=认领")
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
    mode = args.mode or os.environ.get("VT_PRELOAD_MODE", "fast")
    # 默认 4 路×5万/请求 ≈20万/分钟；勿盲目 20 路打满 VT 服务
    workers = args.workers or int(os.environ.get("VT_PRELOAD_WORKERS", "4"))
    http_batch = args.http_batch_size or int(os.environ.get("VT_PRELOAD_HTTP_BATCH", "50000"))
    round_batch = args.batch_size or int(os.environ.get("VT_PRELOAD_BATCH_SIZE", "0"))
    if round_batch <= 0:
        round_batch = workers * http_batch
    timeout_sec = int(os.environ.get("VT_BATCH_TIMEOUT_SEC", "300"))
    max_retries = int(os.environ.get("VT_BATCH_MAX_RETRIES", "3"))
    heartbeat_sec = int(os.environ.get("VT_PRELOAD_HEARTBEAT_SEC", str(DEFAULT_HEARTBEAT_SEC)))
    write_workers = int(os.environ.get("VT_PRELOAD_WRITE_WORKERS", "2"))
    async_write = os.environ.get("VT_PRELOAD_ASYNC_WRITE", "1").strip() not in ("0", "false", "no")
    write_mode = os.environ.get("VT_PRELOAD_WRITE_MODE", WRITE_UPDATE_ID)
    if write_mode not in WRITE_MODES:
        write_mode = WRITE_UPDATE_ID
    # delete_insert 先删后插，异步入库失败会导致数据丢失；强制同步写或改 update_id
    if write_mode == WRITE_DELETE_INSERT:
        log(
            "警告: delete_insert 已弃用（无 DELETE 权限时删成功插失败会丢数据）。"
            " 自动切换为 update_id。",
            err=True,
        )
        write_mode = WRITE_UPDATE_ID
        async_write = True

    if not all([host, user, password, database]):
        print("缺少 SOURCE_MYSQL_* 配置", file=sys.stderr)
        return 1

    all_types = ["mobile", "gaid_idfa", "bank_account", "id_number", "emergency_contact"]
    vt_types = all_types if args.vt_type == "all" else [args.vt_type]

    if args.reset_processing:
        reset_processing(host, port, user, password, database, None if args.vt_type == "all" else args.vt_type)
        log("已重置 status=9 → 0")
        return 0

    check_table_ready(host, port, user, password, database)

    if mode in ("fast", "stream"):
        log("重置中断认领 status=9 → 0 ...")
        reset_processing(host, port, user, password, database,
                         None if args.vt_type == "all" else args.vt_type)

    if mode == "cache":
        log("cache 模式: 重置 status=9 ...")
        reset_processing(host, port, user, password, database, None if args.vt_type == "all" else args.vt_type)

    log(
        f"VT 预加载 mode={mode} | types={vt_types} | workers={workers} | "
        f"每轮={round_batch} | http={http_batch} | url={base_url}"
    )
    if mode == "fast":
        wlabel = {"update_id": "SELECT+UPDATE-by-id", "delete_insert": "SELECT+DELETE+INSERT"}.get(
            write_mode, "UPSERT",
        )
        log(
            f"fast: SELECT status=0 → {workers}路VT×{http_batch} → 异步{wlabel} | "
            f"目标 {TARGET_RATE_PER_MIN}/min"
        )
    elif mode == "stream":
        log("stream慢路径: 源表 NOT EXISTS 反查，大表慎用")

    proc_map = {"fast": process_vt_type_fast, "stream": process_vt_type_stream, "cache": process_vt_type_cache}
    proc = proc_map[mode]
    exit_code = 0
    for vt_type in vt_types:
        kw: Dict[str, Union[int, bool, str]] = dict(
            vt_type=vt_type, host=host, port=port, user=user, password=password,
            database=database, base_url=base_url, timeout_sec=timeout_sec,
            max_retries=max_retries, max_rounds=args.max_rounds,
            retry_failed=args.retry_failed, dry_run=args.dry_run,
            heartbeat_sec=heartbeat_sec,
        )
        if mode == "cache":
            kw.update(claim_batch=round_batch, http_batch=http_batch, workers=workers)
        else:
            wm = WRITE_UPSERT if mode == "stream" else write_mode
            kw.update(
                round_batch=round_batch, http_batch_size=http_batch, workers=workers,
                skip_count=args.skip_count, write_workers=write_workers,
                async_write=async_write, write_mode=wm,
            )
        code = proc(**kw)  # type: ignore[arg-type]
        if code != 0:
            exit_code = code
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
