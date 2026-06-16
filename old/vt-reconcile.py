#!/usr/bin/env python3
"""
VT 对账（旧系统）：vt_token_cache 已 VT 记录重调 /v2t 与库内 token 比对，不入库。

与 old/vt-preload 一致：
  - SOURCE_MYSQL_*、VT_BASE_URL
  - vt_type 为 ENUM 字符串（mobile/id_number/...）
  - 默认单次索引扫 id+HEX(raw)+token（避免两阶段重复读行）
  - 可选 PyMySQL 长连接：pip install pymysql（或 VT_RECONCILE_MYSQL=pymysql）
  - 大表分片：--db-shards 4（单 type 时按 id 区间并行扫库）

用法:
  export VT_RECONCILE_INDEX=idx_reconcile
  ./old/vt-reconcile.sh mobile --skip-count --db-shards 4
  部署: 拷到 /data/vt-reconcile.py 与 vt-preload 共用 .env
"""
from __future__ import annotations

import argparse
import binascii
import http.client
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

STATUS_OK = 1
RAW_VALUE_MAX_LEN = 128
DEFAULT_HEARTBEAT_SEC = 30
# MySQL 每页条数（与 HTTP batch 解耦；5 万条单次查询通常比 1 万×两阶段更快）
DEFAULT_PAGE_SIZE = 50_000
RAW_BY_IDS_CHUNK = 5000
MYSQL_RETRYABLE_MARKERS = ("2013", "2006", "Lost connection", "server has gone away")

ALL_VT_TYPES = ["mobile", "gaid_idfa", "bank_account", "id_number", "emergency_contact"]

_print_lock = threading.Lock()
_log_lock = threading.Lock()

CACHE_RECONCILE_SQL = """
SELECT {select_cols}
FROM vt_token_cache{index_hint}
WHERE status = {ok}
  AND id > {last_id}
  {id_max_filter}
  AND raw_value IS NOT NULL AND raw_value <> ''
  AND CHAR_LENGTH(raw_value) <= {max_len}
  {token_filter}
  {vt_type_filter}
ORDER BY id
LIMIT {page_limit}
"""

CACHE_RECONCILE_SLIM_SQL = """
SELECT id, HEX(raw_value), IFNULL(token, ''){mask_suffix}
FROM vt_token_cache{index_hint}
WHERE status = {ok}
  AND id > {last_id}
  {id_max_filter}
  AND raw_value IS NOT NULL AND raw_value <> ''
  AND CHAR_LENGTH(raw_value) <= {max_len}
  {token_filter}
  {vt_type_filter}
ORDER BY id
LIMIT {page_limit}
"""

COUNT_RECONCILE_SQL = """
SELECT COUNT(*) FROM vt_token_cache{index_hint}
WHERE status = {ok}
  AND raw_value IS NOT NULL AND raw_value <> ''
  AND CHAR_LENGTH(raw_value) <= {max_len}
  {token_filter}
  {vt_type_filter}
"""

ID_BOUNDS_SQL = """
SELECT MIN(id), MAX(id) FROM vt_token_cache{index_hint}
WHERE status = {ok}
  AND raw_value IS NOT NULL AND raw_value <> ''
  AND CHAR_LENGTH(raw_value) <= {max_len}
  {token_filter}
  {vt_type_filter}
"""

CACHE_RECONCILE_LIGHT_SQL = """
SELECT id, IFNULL(token, ''){mask_suffix}
FROM vt_token_cache{index_hint}
WHERE status = {ok}
  AND id > {last_id}
  {id_max_filter}
  AND raw_value IS NOT NULL AND raw_value <> ''
  AND CHAR_LENGTH(raw_value) <= {max_len}
  {token_filter}
  {vt_type_filter}
ORDER BY id
LIMIT {page_limit}
"""


def _token_filter(include_empty_token: bool) -> str:
    if include_empty_token:
        return ""
    return "AND token IS NOT NULL AND TRIM(token) <> ''"


def _vt_type_filter(vt_type_name: Optional[str]) -> str:
    if not vt_type_name:
        return ""
    return f"AND vt_type = '{escape_sql(vt_type_name)}'"


def _id_max_filter(id_max: Optional[int]) -> str:
    if id_max is None:
        return ""
    return f"AND id <= {int(id_max)}"


def _index_hint() -> str:
    name = os.environ.get("VT_RECONCILE_INDEX", "").strip()
    if not name or name.lower() in ("0", "false", "no", "none"):
        return ""
    return f" FORCE INDEX ({name})"


def _select_cols(fetch_masking: bool) -> str:
    base = "id, vt_type, HEX(raw_value) AS raw_hex, IFNULL(token, '')"
    return f"{base}, IFNULL(masking, '')" if fetch_masking else base


def _sql_common(
    vt_type_name: Optional[str],
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    id_max: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "index_hint": _index_hint(),
        "ok": STATUS_OK,
        "last_id": int(last_id),
        "id_max_filter": _id_max_filter(id_max),
        "page_limit": int(page_limit),
        "max_len": RAW_VALUE_MAX_LEN,
        "token_filter": _token_filter(include_empty_token),
        "vt_type_filter": _vt_type_filter(vt_type_name),
    }


def _cursor_sql(
    vt_type_name: Optional[str],
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    fetch_masking: bool,
    id_max: Optional[int] = None,
) -> str:
    return CACHE_RECONCILE_SQL.format(
        select_cols=_select_cols(fetch_masking),
        **_sql_common(vt_type_name, include_empty_token, last_id, page_limit, id_max=id_max),
    )


def _slim_sql(
    vt_type_name: Optional[str],
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    fetch_masking: bool,
    id_max: Optional[int] = None,
) -> str:
    mask_suffix = ", IFNULL(masking, '')" if fetch_masking else ""
    return CACHE_RECONCILE_SLIM_SQL.format(
        mask_suffix=mask_suffix,
        **_sql_common(vt_type_name, include_empty_token, last_id, page_limit, id_max=id_max),
    )


def _count_sql(vt_type_name: Optional[str], include_empty_token: bool) -> str:
    return COUNT_RECONCILE_SQL.format(
        index_hint=_index_hint(),
        ok=STATUS_OK,
        max_len=RAW_VALUE_MAX_LEN,
        token_filter=_token_filter(include_empty_token),
        vt_type_filter=_vt_type_filter(vt_type_name),
    )


def _id_bounds_sql(vt_type_name: Optional[str], include_empty_token: bool) -> str:
    return ID_BOUNDS_SQL.format(
        index_hint=_index_hint(),
        ok=STATUS_OK,
        max_len=RAW_VALUE_MAX_LEN,
        token_filter=_token_filter(include_empty_token),
        vt_type_filter=_vt_type_filter(vt_type_name),
    )


def _light_sql(
    vt_type_name: Optional[str],
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    fetch_masking: bool,
    id_max: Optional[int] = None,
) -> str:
    mask_suffix = ", IFNULL(masking, '')" if fetch_masking else ""
    return CACHE_RECONCILE_LIGHT_SQL.format(
        mask_suffix=mask_suffix,
        **_sql_common(vt_type_name, include_empty_token, last_id, page_limit, id_max=id_max),
    )


def parse_slim_rows(
    rows: List[str],
    vt_type_name: str,
    *,
    expect_masking: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    dropped = 0
    min_cols = 4 if expect_masking else 3
    for row in rows:
        parts = row.split("\t")
        if len(parts) < min_cols:
            dropped += 1
            continue
        try:
            rid = int(parts[0])
            raw_hex = parts[1].strip()
            raw = decode_raw_hex(raw_hex)
            out.append({
                "id": rid,
                "vt_type": vt_type_name,
                "vt_type_name": vt_type_name,
                "raw_hex": raw_hex,
                "raw": raw,
                "db_token": parts[2],
                "db_masking": parts[3] if expect_masking else "",
            })
        except (ValueError, UnicodeDecodeError, binascii.Error):
            dropped += 1
    return out, dropped


def parse_light_rows(
    rows: List[str],
    vt_type_name: str,
    *,
    expect_masking: bool,
) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    dropped = 0
    min_cols = 3 if expect_masking else 2
    for row in rows:
        parts = row.split("\t")
        if len(parts) < min_cols:
            dropped += 1
            continue
        try:
            out.append({
                "id": int(parts[0]),
                "vt_type_name": vt_type_name,
                "db_token": parts[1],
                "db_masking": parts[2] if expect_masking else "",
            })
        except ValueError:
            dropped += 1
    return out, dropped


def _fetch_raw_by_ids(
    host: str, port: str, user: str, password: str, database: str,
    ids: List[int],
    *,
    mysql_retries: int,
) -> Dict[int, str]:
    """按主键取 raw_value（不做 HEX，主键点查快）。"""
    id_to_raw: Dict[int, str] = {}
    if not ids:
        return id_to_raw
    chunks = [ids[i:i + RAW_BY_IDS_CHUNK] for i in range(0, len(ids), RAW_BY_IDS_CHUNK)]
    workers = int(os.environ.get("VT_RECONCILE_RAW_WORKERS", "4"))

    def _one_chunk(chunk: List[int]) -> Dict[int, str]:
        out: Dict[int, str] = {}
        in_list = ",".join(str(int(x)) for x in chunk)
        sql = f"SELECT id, raw_value FROM vt_token_cache WHERE id IN ({in_list})"
        for line in mysql_query(host, port, user, password, database, sql, max_retries=mysql_retries):
            parts = line.split("\t", 1)
            if len(parts) != 2:
                continue
            try:
                out[int(parts[0])] = parts[1]
            except ValueError:
                continue
        return out

    if workers <= 1 or len(chunks) <= 1:
        for chunk in chunks:
            id_to_raw.update(_one_chunk(chunk))
        return id_to_raw

    with ThreadPoolExecutor(max_workers=min(workers, len(chunks))) as pool:
        for part in pool.map(_one_chunk, chunks):
            id_to_raw.update(part)
    return id_to_raw


def _fetch_page_two_phase(
    host: str, port: str, user: str, password: str, database: str,
    vt_type_name: str,
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    fetch_masking: bool,
    mysql_retries: int,
    id_max: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], int, float]:
    t0 = time.time()
    light_lines = mysql_query(
        host, port, user, password, database,
        _light_sql(
            vt_type_name, include_empty_token, last_id, page_limit,
            fetch_masking=fetch_masking, id_max=id_max,
        ),
        max_retries=mysql_retries,
    )
    t1 = time.time()
    light_rows, dropped = parse_light_rows(
        light_lines, vt_type_name, expect_masking=fetch_masking,
    )
    if not light_rows:
        return [], dropped, time.time() - t0

    id_to_raw = _fetch_raw_by_ids(
        host, port, user, password, database,
        [r["id"] for r in light_rows],
        mysql_retries=mysql_retries,
    )
    t2 = time.time()

    out: List[Dict[str, Any]] = []
    for lr in light_rows:
        raw = id_to_raw.get(lr["id"])
        if raw is None:
            dropped += 1
            continue
        try:
            raw_hex = binascii.hexlify(raw.encode("utf-8", errors="surrogateescape")).decode("ascii")
        except Exception:
            dropped += 1
            continue
        out.append({
            "id": lr["id"],
            "vt_type": vt_type_name,
            "vt_type_name": vt_type_name,
            "raw_hex": raw_hex,
            "raw": raw,
            "db_token": lr["db_token"],
            "db_masking": lr["db_masking"],
        })

    n_raw_batches = (len(light_rows) + RAW_BY_IDS_CHUNK - 1) // RAW_BY_IDS_CHUNK
    log(
        f"{vt_type_name} MySQL 两阶段: id+token {len(light_rows)} 条 {t1 - t0:.1f}s | "
        f"raw {n_raw_batches}批×{RAW_BY_IDS_CHUNK} {t2 - t1:.1f}s",
    )
    return out, dropped, time.time() - t0


def escape_sql(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def log(msg: str, *, err: bool = False) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with _print_lock:
        stream = sys.stderr if err else sys.stdout
        print(f"[{ts}] {msg}", file=stream, flush=True)


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


def load_env_files() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    paths: List[str] = []
    explicit = os.environ.get("VT_PRELOAD_ENV", "").strip()
    if explicit:
        paths.append(explicit)
    paths.append(os.path.join(script_dir, ".env"))
    if os.path.basename(script_dir) == "old":
        paths.append(os.path.join(os.path.dirname(script_dir), ".env"))
    for path in paths:
        load_dotenv(path)


_mysql_tls = threading.local()
_pymysql_checked = False
_pymysql_ok = False


def _pymysql_available() -> bool:
    global _pymysql_checked, _pymysql_ok
    if not _pymysql_checked:
        try:
            import pymysql  # noqa: F401, PLC0415
            _pymysql_ok = True
        except ImportError:
            _pymysql_ok = False
        _pymysql_checked = True
    return _pymysql_ok


def _mysql_init_sql() -> str:
    return os.environ.get(
        "VT_RECONCILE_MYSQL_INIT",
        "SET SESSION net_read_timeout=7200, net_write_timeout=7200, wait_timeout=7200",
    )


def _pymysql_query(
    host: str, port: str, user: str, password: str, database: str, sql: str,
    *, max_retries: int,
) -> List[str]:
    import pymysql  # noqa: PLC0415

    last_err = ""
    for attempt in range(max(1, max_retries)):
        conn = getattr(_mysql_tls, "conn", None)
        try:
            if conn is None or not getattr(conn, "open", False):
                conn = pymysql.connect(
                    host=host,
                    port=int(port),
                    user=user,
                    password=password,
                    database=database,
                    charset="utf8mb4",
                    connect_timeout=60,
                    read_timeout=7200,
                    write_timeout=7200,
                    autocommit=True,
                )
                _mysql_tls.conn = conn
                with conn.cursor() as cur:
                    cur.execute(_mysql_init_sql())
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            out: List[str] = []
            for row in rows:
                cells = []
                for cell in row:
                    if cell is None:
                        cells.append("")
                    elif isinstance(cell, (bytes, bytearray)):
                        cells.append(cell.decode("utf-8", errors="surrogateescape"))
                    else:
                        cells.append(str(cell))
                out.append("\t".join(cells))
            return out
        except Exception as e:
            last_err = str(e)
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass
            _mysql_tls.conn = None
            if not any(m in last_err for m in MYSQL_RETRYABLE_MARKERS):
                raise RuntimeError(last_err) from e
            if attempt + 1 < max_retries:
                wait = min(2 ** attempt, 30)
                log(f"MySQL 断连，{wait}s 后重试 ({attempt + 1}/{max_retries}): {last_err[:200]}", err=True)
                time.sleep(wait)
    raise RuntimeError(last_err)


def _subprocess_mysql_query(
    host: str, port: str, user: str, password: str, database: str, sql: str,
    *, max_retries: int,
) -> List[str]:
    env = os.environ.copy()
    env["MYSQL_PWD"] = password
    init_cmd = _mysql_init_sql()
    base_cmd = [
        "mysql", "-h", host, "-P", port, "-u", user, database,
        "-N", "-B", "--connect-timeout=60", "--init-command", init_cmd,
    ]
    last_err = ""
    for attempt in range(max(1, max_retries)):
        if len(sql) > 32000:
            proc = subprocess.run([*base_cmd], input=sql, env=env, capture_output=True, text=True)
        else:
            proc = subprocess.run([*base_cmd, "-e", sql], env=env, capture_output=True, text=True)
        if proc.returncode == 0:
            return [ln for ln in proc.stdout.splitlines() if ln.strip()]
        last_err = proc.stderr.strip() or proc.stdout.strip() or "mysql failed"
        if not any(m in last_err for m in MYSQL_RETRYABLE_MARKERS):
            raise RuntimeError(last_err)
        if attempt + 1 < max_retries:
            wait = min(2 ** attempt, 30)
            log(f"MySQL 断连，{wait}s 后重试 ({attempt + 1}/{max_retries}): {last_err[:200]}", err=True)
            time.sleep(wait)
    raise RuntimeError(last_err)


def mysql_query(
    host: str, port: str, user: str, password: str, database: str, sql: str,
    *, max_retries: int = 3,
) -> List[str]:
    backend = os.environ.get("VT_RECONCILE_MYSQL", "auto").strip().lower()
    if backend in ("cli", "subprocess", "mysql"):
        return _subprocess_mysql_query(
            host, port, user, password, database, sql, max_retries=max_retries,
        )
    if backend == "pymysql" or (backend == "auto" and _pymysql_available()):
        try:
            return _pymysql_query(
                host, port, user, password, database, sql, max_retries=max_retries,
            )
        except Exception:
            if backend == "pymysql":
                raise
    return _subprocess_mysql_query(
        host, port, user, password, database, sql, max_retries=max_retries,
    )


def get_id_bounds(
    host: str, port: str, user: str, password: str, database: str,
    vt_type_name: str,
    include_empty_token: bool,
    *,
    mysql_retries: int,
) -> Tuple[int, int]:
    rows = mysql_query(
        host, port, user, password, database,
        _id_bounds_sql(vt_type_name, include_empty_token),
        max_retries=mysql_retries,
    )
    if not rows:
        return 0, 0
    parts = rows[0].split("\t")
    if len(parts) < 2 or not parts[0] or parts[0] == "NULL":
        return 0, 0
    return int(parts[0]), int(parts[1])


def decode_raw_hex(hex_str: str) -> str:
    return bytes.fromhex(hex_str).decode("utf-8", errors="surrogateescape")


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
        for _attempt in range(2):
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


class ReconcileStats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total = 0
        self.match = 0
        self.changed = 0
        self.new_token = 0
        self.lost_token = 0
        self.masking_only = 0
        self.api_error = 0
        self.start_time = time.time()

    def add(self, **kwargs: int) -> None:
        with self._lock:
            for k, v in kwargs.items():
                setattr(self, k, getattr(self, k) + v)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            elapsed = max(time.time() - self.start_time, 0.001)
            return {
                "total": self.total,
                "match": self.match,
                "changed": self.changed,
                "new_token": self.new_token,
                "lost_token": self.lost_token,
                "masking_only": self.masking_only,
                "api_error": self.api_error,
                "elapsed": elapsed,
                "rate": self.total / elapsed * 60.0,
            }


def _norm_token(s: Optional[str]) -> str:
    return (s or "").strip()


def _norm_mask(s: Optional[str]) -> str:
    return (s or "").strip()


def parse_cache_rows(rows: List[str], *, expect_masking: bool) -> Tuple[List[Dict[str, Any]], int]:
    out: List[Dict[str, Any]] = []
    dropped = 0
    for row in rows:
        parts = row.split("\t")
        min_cols = 5 if expect_masking else 4
        if len(parts) < min_cols:
            dropped += 1
            continue
        try:
            rid = int(parts[0])
            vt_name = parts[1].strip()
            raw_hex = parts[2].strip()
            raw = decode_raw_hex(raw_hex)
            out.append({
                "id": rid,
                "vt_type": vt_name,
                "vt_type_name": vt_name,
                "raw_hex": raw_hex,
                "raw": raw,
                "db_token": parts[3],
                "db_masking": parts[4] if expect_masking else "",
            })
        except (ValueError, UnicodeDecodeError, binascii.Error):
            dropped += 1
            continue
    return out, dropped


def write_detail_log(
    log_file: str,
    category: str,
    row: Dict[str, Any],
    api_token: str,
    api_masking: Optional[str],
) -> None:
    line = (
        f"{time.strftime('%Y-%m-%d %H:%M:%S')}\t{category}\t"
        f"id={row['id']}\tvt_type={row['vt_type_name']}\t"
        f"raw_hex={row['raw_hex']}\t"
        f"db_token={row['db_token']}\tapi_token={api_token}\t"
        f"db_masking={row['db_masking']}\tapi_masking={_norm_mask(api_masking)}"
    )
    with _log_lock:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")


def reconcile_batch(
    batch: List[Dict[str, Any]],
    *,
    base_url: str,
    timeout_sec: int,
    max_retries: int,
    stats: ReconcileStats,
    log_file: str,
    log_masking_only: bool,
) -> None:
    values = [r["raw"] for r in batch]
    last_err: Optional[Exception] = None
    tokens: List[str] = []
    maskings: List[Optional[str]] = []
    for attempt in range(max_retries):
        try:
            tokens, maskings, _, _ = call_v2t(base_url, values, timeout_sec)
            last_err = None
            break
        except Exception as e:
            last_err = e
            time.sleep(min(2 ** attempt, 8))
    if last_err is not None:
        stats.add(total=len(batch), api_error=len(batch))
        with _log_lock:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(
                    f"{time.strftime('%Y-%m-%d %H:%M:%S')}\tAPI_ERROR\t"
                    f"batch_size={len(batch)}\terror={last_err}\n"
                )
        log(f"批次 /v2t 失败 size={len(batch)}: {last_err}", err=True)
        return

    for row, api_token, api_mask in zip(batch, tokens, maskings):
        db_t = _norm_token(row["db_token"])
        api_t = _norm_token(api_token)
        db_m = _norm_mask(row["db_masking"])
        api_m = _norm_mask(api_mask)

        stats.add(total=1)

        if not db_t and api_t:
            stats.add(new_token=1)
            write_detail_log(log_file, "NEW", row, api_t, api_mask)
            continue
        if db_t and not api_t:
            stats.add(lost_token=1)
            write_detail_log(log_file, "LOST", row, api_t, api_mask)
            continue
        if db_t != api_t:
            stats.add(changed=1)
            write_detail_log(log_file, "CHANGED", row, api_t, api_mask)
            continue
        if log_masking_only and db_m != api_m:
            stats.add(masking_only=1)
            write_detail_log(log_file, "MASKING_ONLY", row, api_t, api_mask)
            continue
        stats.add(match=1)


def _run_http_batches(
    batches: List[List[Dict[str, Any]]],
    *,
    pool: ThreadPoolExecutor,
    base_url: str,
    timeout_sec: int,
    max_retries: int,
    stats: ReconcileStats,
    log_file: str,
    log_masking_only: bool,
) -> None:
    if not batches:
        return
    futures = [
        pool.submit(
            reconcile_batch,
            batch,
            base_url=base_url,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            stats=stats,
            log_file=log_file,
            log_masking_only=log_masking_only,
        )
        for batch in batches
    ]
    for fut in as_completed(futures):
        fut.result()


def _fetch_page(
    host: str, port: str, user: str, password: str, database: str,
    vt_type_name: str,
    include_empty_token: bool,
    last_id: int,
    page_limit: int,
    *,
    fetch_masking: bool,
    mysql_retries: int,
    fetch_mode: str,
    id_max: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], int, float]:
    if fetch_mode == "two_phase":
        return _fetch_page_two_phase(
            host, port, user, password, database,
            vt_type_name, include_empty_token, last_id, page_limit,
            fetch_masking=fetch_masking,
            mysql_retries=mysql_retries,
            id_max=id_max,
        )
    if fetch_mode == "full":
        sql = _cursor_sql(
            vt_type_name, include_empty_token, last_id, page_limit,
            fetch_masking=fetch_masking, id_max=id_max,
        )
        parser = lambda lines: parse_cache_rows(lines, expect_masking=fetch_masking)
    else:
        sql = _slim_sql(
            vt_type_name, include_empty_token, last_id, page_limit,
            fetch_masking=fetch_masking, id_max=id_max,
        )
        parser = lambda lines: parse_slim_rows(
            lines, vt_type_name, expect_masking=fetch_masking,
        )
    t0 = time.time()
    rows, dropped = parser(
        mysql_query(host, port, user, password, database, sql, max_retries=mysql_retries),
    )
    return rows, dropped, time.time() - t0


def reconcile_type(
    vt_type_name: str,
    *,
    host: str, port: str, user: str, password: str, database: str,
    base_url: str,
    http_batch_size: int,
    workers: int,
    timeout_sec: int,
    max_retries: int,
    include_empty_token: bool,
    limit: Optional[int],
    page_size: int,
    prefetch: bool,
    log_file: str,
    log_masking_only: bool,
    heartbeat_sec: int,
    skip_count: bool,
    mysql_retries: int,
    fetch_mode: str,
    id_min: Optional[int] = None,
    id_max: Optional[int] = None,
    shard_label: str = "",
) -> ReconcileStats:
    tag = f"{vt_type_name}{shard_label}"
    fetch_masking = log_masking_only
    total_hint = -1
    if not skip_count and not shard_label:
        total_hint = int(mysql_query(
            host, port, user, password, database, _count_sql(vt_type_name, include_empty_token),
            max_retries=mysql_retries,
        )[0])
        log(f"=== {tag} 待对账约 {total_hint} 条 ===")
    else:
        if shard_label:
            log(f"=== {tag} 开始对账 id∈[{id_min},{id_max}] ===")
        else:
            log(f"=== {tag} 开始对账（已跳过 COUNT）===")

    stats = ReconcileStats()
    last_id = max(0, int(id_min) - 1) if id_min is not None else 0
    page_no = 0
    last_heartbeat = time.time()
    idx_hint = _index_hint()
    prefetch_max_fetch = float(os.environ.get("VT_RECONCILE_PREFETCH_MAX_FETCH_SEC", "180"))
    mysql_backend = os.environ.get("VT_RECONCILE_MYSQL", "auto").strip().lower()
    if mysql_backend == "auto":
        mysql_note = "PyMySQL" if _pymysql_available() else "mysql-cli"
    elif mysql_backend == "pymysql":
        mysql_note = "PyMySQL"
    else:
        mysql_note = "mysql-cli"

    log(
        f"{tag} 流式对账：MySQL 每页 {page_size} 条 | 拉取={fetch_mode} | 连接={mysql_note} | "
        f"HTTP 批 {http_batch_size} | 预取下一页={'开' if prefetch else '关'} | 索引{idx_hint or '自动'}",
    )

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        prefetch_pool: Optional[ThreadPoolExecutor] = (
            ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"prefetch-{vt_type_name}")
            if prefetch else None
        )
        pending_fetch = None

        try:
            while True:
                s = stats.snapshot()
                if limit is not None and limit > 0 and s["total"] >= limit:
                    break

                page_limit = page_size
                if limit is not None and limit > 0:
                    page_limit = min(page_limit, limit - s["total"])
                    if page_limit <= 0:
                        break

                if pending_fetch is not None:
                    rows, dropped, fetch_sec = pending_fetch.result()
                    pending_fetch = None
                else:
                    log(f"{tag} 拉取第 {page_no + 1} 页 (id>{last_id}, limit={page_limit}) ...")
                    rows, dropped, fetch_sec = _fetch_page(
                        host, port, user, password, database,
                        vt_type_name, include_empty_token, last_id, page_limit,
                        fetch_masking=fetch_masking,
                        mysql_retries=mysql_retries,
                        fetch_mode=fetch_mode,
                        id_max=id_max,
                    )

                if dropped:
                    log(f"{tag} 本页跳过无法解析行 {dropped} 条")
                if not rows:
                    break

                page_no += 1
                last_id = rows[-1]["id"]
                log(f"{tag} 第 {page_no} 页 {len(rows)} 条，MySQL {fetch_sec:.1f}s，/v2t 对账 ...")

                if (
                    prefetch_pool is not None
                    and len(rows) >= page_limit
                    and fetch_sec <= prefetch_max_fetch
                ):
                    s2 = stats.snapshot()
                    next_limit = page_limit
                    if limit is not None and limit > 0:
                        next_limit = min(page_limit, limit - s2["total"] - len(rows))
                        if next_limit <= 0:
                            next_limit = 0
                    if next_limit > 0:
                        nid = last_id
                        nlim = next_limit
                        pending_fetch = prefetch_pool.submit(
                            _fetch_page,
                            host, port, user, password, database,
                            vt_type_name, include_empty_token, nid, nlim,
                            fetch_masking=fetch_masking,
                            mysql_retries=mysql_retries,
                            fetch_mode=fetch_mode,
                            id_max=id_max,
                        )

                batches = [rows[i:i + http_batch_size] for i in range(0, len(rows), http_batch_size)]
                _run_http_batches(
                    batches,
                    pool=pool,
                    base_url=base_url,
                    timeout_sec=timeout_sec,
                    max_retries=max_retries,
                    stats=stats,
                    log_file=log_file,
                    log_masking_only=log_masking_only,
                )

                now = time.time()
                if now - last_heartbeat >= heartbeat_sec:
                    s = stats.snapshot()
                    if total_hint > 0:
                        pct = min(100.0, s["total"] / total_hint * 100.0)
                        prog = f"进度 {s['total']}/{total_hint} ({pct:.1f}%)"
                    else:
                        prog = f"已检 {s['total']} 条"
                    log(
                        f"{tag} {prog} | 页 {page_no} | "
                        f"一致 {s['match']} 变化 {s['changed']} 新增 {s['new_token']} 丢失 {s['lost_token']} "
                        f"仅mask {s['masking_only']} API失败 {s['api_error']} | "
                        f"约 {s['rate']:.0f} 条/min",
                    )
                    last_heartbeat = now

                if len(rows) < page_limit:
                    break
                if id_max is not None and last_id >= id_max:
                    break
        finally:
            if prefetch_pool is not None:
                prefetch_pool.shutdown(wait=False)

    s = stats.snapshot()
    log(
        f"=== {tag} 对账完成 === "
        f"总计 {s['total']} | 一致 {s['match']} | 变化 {s['changed']} | "
        f"新增 {s['new_token']} | 丢失 {s['lost_token']} | "
        f"仅 masking 不同 {s['masking_only']} | API 失败 {s['api_error']} | "
        f"耗时 {s['elapsed']:.0f}s",
    )
    return stats


def reconcile_type_sharded(
    vt_type_name: str,
    *,
    db_shards: int,
    **kwargs: Any,
) -> ReconcileStats:
    host = kwargs["host"]
    port = kwargs["port"]
    user = kwargs["user"]
    password = kwargs["password"]
    database = kwargs["database"]
    include_empty_token = kwargs["include_empty_token"]
    mysql_retries = kwargs["mysql_retries"]

    id_min, id_max = get_id_bounds(
        host, port, user, password, database,
        vt_type_name, include_empty_token,
        mysql_retries=mysql_retries,
    )
    if id_max <= 0:
        log(f"{vt_type_name} 无待对账数据（id 区间为空）")
        return ReconcileStats()

    span = max(1, (id_max - id_min + 1) // db_shards)
    ranges: List[Tuple[int, int]] = []
    for i in range(db_shards):
        lo = id_min + i * span
        hi = id_min + (i + 1) * span - 1 if i < db_shards - 1 else id_max
        if lo > id_max:
            break
        ranges.append((lo, min(hi, id_max)))

    log(
        f"{vt_type_name} 按 id 分 {len(ranges)} 片并行扫库 "
        f"(全局 id {id_min}..{id_max})",
    )

    def _run_shard(idx: int, lo: int, hi: int) -> ReconcileStats:
        label = f"[shard {idx + 1}/{len(ranges)}]"
        return reconcile_type(
            vt_type_name,
            id_min=lo,
            id_max=hi,
            shard_label=label,
            **kwargs,
        )

    grand = ReconcileStats()
    with ThreadPoolExecutor(max_workers=len(ranges), thread_name_prefix=f"db-shard-{vt_type_name}") as pool:
        futures = [pool.submit(_run_shard, i, lo, hi) for i, (lo, hi) in enumerate(ranges)]
        for fut in as_completed(futures):
            grand = merge_stats(grand, fut.result())
    return grand


def merge_stats(a: ReconcileStats, b: ReconcileStats) -> ReconcileStats:
    out = ReconcileStats()
    for field in ("total", "match", "changed", "new_token", "lost_token", "masking_only", "api_error"):
        out.add(**{field: getattr(a, field) + getattr(b, field)})
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="VT 对账（旧系统 ENUM）：重跑 /v2t 与 vt_token_cache 比对，不入库")
    parser.add_argument("--vt-type", default="all",
                        help="mobile|bank_account|id_number|gaid_idfa|emergency_contact|all")
    parser.add_argument("--workers", type=int, default=None, help="每 type 并行 HTTP 批次数")
    parser.add_argument("--http-batch-size", type=int, default=None, help="单次 /v2t 条数")
    parser.add_argument("--page-size", type=int, default=None, help="MySQL 每页条数（默认5万）")
    parser.add_argument("--batch-size", type=int, default=None, help="同 --page-size（兼容旧参数）")
    parser.add_argument("--limit", type=int, default=None, help="最多对账条数（测试用）")
    parser.add_argument(
        "--two-phase", action="store_true",
        help="两阶段拉取（先 id+token 再按主键取 raw；通常比默认慢）",
    )
    parser.add_argument(
        "--full-query", action="store_true",
        help="单条 SQL 含 vt_type 列（调试用；默认 slim 单次查询）",
    )
    parser.add_argument("--single-query", action="store_true", help="同默认 slim 单次查询（兼容旧参数）")
    parser.add_argument("--no-prefetch", action="store_true", help="关闭 MySQL 下一页预取")
    parser.add_argument(
        "--db-shards", type=int, default=None,
        help="单 type 时按 id 区间并行扫库（如 4；也可用 VT_RECONCILE_DB_SHARDS）",
    )
    parser.add_argument("--include-empty-token", action="store_true", help="含 status=1 但 token 为空的行")
    parser.add_argument("--log-file", default=None, help="差异明细日志路径")
    parser.add_argument("--log-masking-only", action="store_true", help="token 一致但 masking 不同时也写日志")
    parser.add_argument("--heartbeat-sec", type=int, default=None)
    parser.add_argument("--skip-count", action="store_true", help="跳过启动 COUNT（大表可省几十秒）")
    parser.add_argument(
        "--parallel-types", action="store_true",
        help="all 时多 type 并行（默认串行，避免 5 路同时扫库）",
    )
    parser.add_argument(
        "--sequential-types", action="store_true",
        help="同默认；与 --parallel-types 互斥",
    )
    args = parser.parse_args()

    load_env_files()

    host = os.environ.get("SOURCE_MYSQL_HOST", "")
    port = os.environ.get("SOURCE_MYSQL_PORT", "3306")
    user = os.environ.get("SOURCE_MYSQL_USER", "")
    password = os.environ.get("SOURCE_MYSQL_PASSWORD", "")
    database = os.environ.get("SOURCE_MYSQL_DATABASE", "nigeria_backend")
    base_url = os.environ.get("VT_BASE_URL", "http://101.47.27.225")

    if not all([host, user, password, database]):
        print("缺少 SOURCE_MYSQL_* 配置", file=sys.stderr)
        return 1

    script_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(script_dir, "logs")
    http_batch = args.http_batch_size or int(os.environ.get("VT_PRELOAD_HTTP_BATCH", "50000"))
    page_size = (
        args.page_size or args.batch_size
        or int(os.environ.get("VT_RECONCILE_PAGE_SIZE", str(DEFAULT_PAGE_SIZE)))
    )
    if args.two_phase:
        fetch_mode = "two_phase"
    elif args.full_query:
        fetch_mode = "full"
    else:
        fetch_mode = os.environ.get("VT_RECONCILE_FETCH_MODE", "single").strip()
    if fetch_mode not in ("single", "two_phase", "full"):
        fetch_mode = "single"
    db_shards = args.db_shards or int(os.environ.get("VT_RECONCILE_DB_SHARDS", "1"))
    if db_shards < 1:
        db_shards = 1
    prefetch = not args.no_prefetch and os.environ.get("VT_RECONCILE_PREFETCH", "1").strip() not in (
        "0", "false", "no",
    )
    timeout_sec = int(os.environ.get("VT_BATCH_TIMEOUT_SEC", "300"))
    max_retries = int(os.environ.get("VT_BATCH_MAX_RETRIES", "3"))
    mysql_retries = int(os.environ.get("VT_RECONCILE_MYSQL_RETRIES", "5"))
    heartbeat_sec = args.heartbeat_sec or int(os.environ.get("VT_PRELOAD_HEARTBEAT_SEC", str(DEFAULT_HEARTBEAT_SEC)))

    workers = args.workers or int(os.environ.get("VT_PRELOAD_WORKERS", "4"))
    os.makedirs(log_dir, exist_ok=True)
    log_file = args.log_file or os.path.join(
        log_dir, f"vt-reconcile-{time.strftime('%Y%m%d-%H%M%S')}.log",
    )
    with open(log_file, "w", encoding="utf-8") as f:
        f.write(
            f"# VT reconcile (old/ENUM) {time.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"# vt_type={args.vt_type} url={base_url} db={database}\n"
            f"# category: CHANGED=token变化 NEW=库无token接口有 LOST=库有token接口无\n"
        )

    vt_arg = args.vt_type.strip()
    if vt_arg == "all":
        types = list(ALL_VT_TYPES)
    elif vt_arg in ALL_VT_TYPES:
        types = [vt_arg]
    else:
        log(f"未知 vt_type: {vt_arg}，可选: {', '.join(ALL_VT_TYPES)}", err=True)
        return 2

    log(
        f"VT 对账开始（旧系统） | db={database} | types={','.join(types)} | "
        f"workers={workers} | http_batch={http_batch} | page_size={page_size} | "
        f"fetch={fetch_mode} | db_shards={db_shards} | url={base_url} | 明细={log_file}",
    )
    log("只读比对，不会写入 vt_token_cache")
    if _pymysql_available():
        log("MySQL 使用 PyMySQL 长连接（省掉每页 subprocess 开销）")
    else:
        log("提示: pip install pymysql 可加速 MySQL 拉取（或继续用 mysql 客户端）")
    if _index_hint():
        log(f"MySQL 使用 {_index_hint().strip()}（需已执行 sql/ddl/vt_token_cache_reconcile_index.sql）")
    else:
        log("提示: 对账慢可在源库加索引 sql/ddl/vt_token_cache_reconcile_index.sql 后设 VT_RECONCILE_INDEX=idx_reconcile")

    run_start = time.time()
    grand = ReconcileStats()
    parallel_types = (
        len(types) > 1
        and args.parallel_types
        and not args.sequential_types
    ) or (
        len(types) > 1
        and os.environ.get("VT_RECONCILE_TYPE_PARALLEL", "").strip() in ("1", "true", "yes")
        and not args.sequential_types
    )

    common_kw = dict(
        host=host, port=port, user=user, password=password, database=database,
        base_url=base_url,
        http_batch_size=http_batch,
        workers=workers,
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        include_empty_token=args.include_empty_token,
        limit=args.limit,
        page_size=page_size,
        prefetch=prefetch,
        log_file=log_file,
        log_masking_only=args.log_masking_only,
        heartbeat_sec=heartbeat_sec,
        skip_count=args.skip_count,
        mysql_retries=mysql_retries,
        fetch_mode=fetch_mode,
    )

    if len(types) > 1 and not parallel_types:
        log("多 type 默认串行扫库（要并行加 --parallel-types 或 VT_RECONCILE_TYPE_PARALLEL=1）")
    if db_shards > 1 and len(types) > 1:
        log("db-shards 仅对单 type 生效；all 时请先按 type 分别跑", err=True)
        db_shards = 1

    def _run_one_type(vt_name: str) -> ReconcileStats:
        if db_shards > 1:
            return reconcile_type_sharded(vt_name, db_shards=db_shards, **common_kw)
        return reconcile_type(vt_name, **common_kw)

    if parallel_types:
        log(f"多类型并行：{len(types)} 个 type 各一线程，每 type HTTP workers={workers}")

        def _run_type(vt_name: str) -> Tuple[str, ReconcileStats]:
            return vt_name, _run_one_type(vt_name)

        with ThreadPoolExecutor(max_workers=len(types), thread_name_prefix="vt-type") as type_pool:
            futures = {type_pool.submit(_run_type, vt_name): vt_name for vt_name in types}
            for fut in as_completed(futures):
                vt_name, st = fut.result()
                grand = merge_stats(grand, st)
                log(f"类型 {vt_name} 对账线程已结束")
    else:
        for vt_name in types:
            grand = merge_stats(grand, _run_one_type(vt_name))

    wall_elapsed = time.time() - run_start
    gs = grand.snapshot()
    gs["elapsed"] = wall_elapsed
    if wall_elapsed > 0:
        gs["rate"] = gs["total"] / wall_elapsed * 60.0

    summary = (
        f"\n========== VT 对账汇总 ==========\n"
        f"总计:     {gs['total']}\n"
        f"一致:     {gs['match']}\n"
        f"变化:     {gs['changed']}\n"
        f"新增:     {gs['new_token']}\n"
        f"丢失:     {gs['lost_token']}\n"
        f"仅masking: {gs['masking_only']}\n"
        f"API失败:  {gs['api_error']}\n"
        f"耗时:     {gs['elapsed']:.0f}s\n"
        f"吞吐:     约 {gs['rate']:.0f} 条/min\n"
        f"明细日志: {log_file}\n"
        f"================================="
    )
    log(summary)

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(summary + "\n")

    if gs["changed"] > 0 or gs["new_token"] > 0 or gs["lost_token"] > 0 or gs["api_error"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
