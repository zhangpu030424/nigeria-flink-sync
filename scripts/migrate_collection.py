#!/usr/bin/env python3
"""
催收数据迁移：nigeria_admin / nigeria_backend -> cms

设计目标：
1. 源库只读
2. 目标库 UPSERT，可重跑
3. 运行时探测目标表列，避免因字段轻微差异直接失败
4. 支持 dry-run / verify / limit / batch
5. MySQL 断连（2013/2006/2003）自动重试

说明：
- 需求来自 README_migrate_collection.md 与飞书文档
- 当前主源库为 nigeria_admin；用户补充画像来自 nigeria_backend
- cases/dispatch 的 case_no = repayment_plan.repayment_plan_order_no
- level_code：T0->D0，T-N->S-N，TN->SN
- product_id：与 02_sync_user_product_incr.sql 同一套 P*/L* 映射
- cases 迁移前预加载已放款 user_order + 对应用户姓名/画像/紧急联系人到内存
- cases/dispatch：按主键区间多线程分片读 + 分批写（--workers）
- 复杂表（cases / traces / dispatch）按“能迁多少迁多少”策略构造行，
  如果目标库少字段会自动跳过；如果源库列缺失会明确报错
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor, SSDictCursor


TABLE_KEYS = ("companys", "members", "cases", "traces", "dispatch")
# case_traces 暂不迁移：--table all 默认跳过 traces
DEFAULT_TABLES = ("companys", "members", "cases", "dispatch")
DEFAULT_ENV = "/opt/nigeria-flink-sync/.env"
DEFAULT_BID = "ng01"
DEFAULT_BATCH = 1000
DEFAULT_WORKERS = 8
DEFAULT_VT_URL = "http://101.47.23.241:9505"
DEFAULT_PASSWORD = "ng01123456."
# htpasswd -bnBC 10 "" ng01123456. 预生成；无 bcrypt/htpasswd 时兜底
DEFAULT_PASSWORD_HASH = "$2y$10$j6w2VR3rPMu69vTjHxfss.N5AVpj5e4fn0Yx.Ec1.mxLsuDIJeET6"
DEFAULT_MYSQL_RETRIES = 5
# 2003 连不上 / 2006 Gone away / 2013 Lost connection / 2014 Commands out of sync
MYSQL_RETRY_ERRNOS = {2003, 2006, 2013, 2014}
# 源 user_emergency_contact.contact_relationship:
#   0 Cousin / 1 Colleague / 2 Friend / 3 Wife/Husband /
#   4 Sister/Brother / 5 Other / 6 parents
# 目标 cms:
#   0 本人 / 1 父母 / 2 配偶 / 3 子女 / 4 兄弟姐妹 / 5 朋友 / 6 同事 / 7 其他
RELATION_MAP = {
    0: 7,  # Cousin -> 其他
    1: 6,  # Colleague -> 同事
    2: 5,  # Friend -> 朋友
    3: 2,  # Wife/Husband -> 配偶
    4: 4,  # Sister/Brother -> 兄弟姐妹
    5: 7,  # Other -> 其他
    6: 1,  # parents -> 父母
}
# 与 02_sync_user_product_incr.sql 一致
PRODUCT_ID_MAP = {
    "P1": "648",
    "P2": "6481",
    "P3": "6482",
    "P4": "6483",
    "P5": "6484",
    "P6": "6485",
    "L1": "649",
    "L2": "650",
    "L3": "651",
    "L4": "652",
    "L5": "653",
    "L6": "654",
    "L7": "6551",
    "L8": "6561",
    "L9": "6571",
    "L10": "6581",
    "L11": "6591",
    "L12": "6601",
    "L13": "6611",
    "L14": "6621",
    "L15": "6631",
    "L16": "6641",
    "L17": "6651",
    "L18": "6661",
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%F %T')}] {msg}", flush=True)


def map_level_code(raw: Any) -> str:
    """催收等级：T0->D0，T-N->S-N，TN->SN；已是 S/D 前缀则原样。"""
    s = ("" if raw is None else str(raw)).strip()
    if not s:
        return ""
    upper = s.upper()
    if upper == "T0":
        return "D0"
    m = re.fullmatch(r"T-(\d+)", upper)
    if m:
        return f"S-{m.group(1)}"
    m = re.fullmatch(r"T(\d+)", upper)
    if m:
        return f"S{m.group(1)}"
    return s[:32]


def map_product_id(raw: Any) -> str:
    """产品 ID：与 Flink user_product 同步同一套映射；未命中则原样。"""
    s = ("" if raw is None else str(raw)).strip()
    if not s:
        return ""
    return PRODUCT_ID_MAP.get(s.upper(), s)[:64]


def is_retryable_mysql(exc: BaseException) -> bool:
    if isinstance(exc, pymysql.err.OperationalError):
        code = exc.args[0] if exc.args else None
        return code in MYSQL_RETRY_ERRNOS
    if isinstance(exc, pymysql.err.InterfaceError):
        # 连接已断时常表现为 InterfaceError(0, '')
        return True
    return False


def mysql_retry(op_name: str, fn, *, retries: int = DEFAULT_MYSQL_RETRIES):
    last: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except (pymysql.MySQLError, OSError) as e:
            last = e
            if not is_retryable_mysql(e) and not isinstance(e, OSError):
                raise
            if attempt >= retries:
                break
            sleep_s = min(2**attempt, 60)
            code = e.args[0] if getattr(e, "args", None) else type(e).__name__
            log(f"MySQL {code} on {op_name}, retry {attempt}/{retries} after {sleep_s}s")
            time.sleep(sleep_s)
    assert last is not None
    raise last


def load_dotenv(path: str) -> None:
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def env_first(*keys: str, default: Optional[str] = None) -> Optional[str]:
    for k in keys:
        v = os.environ.get(k)
        if v not in (None, ""):
            return v
    return default


def parse_tables(raw: str) -> List[str]:
    if raw == "all":
        return list(DEFAULT_TABLES)
    items = [x.strip() for x in raw.split(",") if x.strip()]
    bad = [x for x in items if x not in TABLE_KEYS]
    if bad:
        raise SystemExit(f"未知 --table: {','.join(bad)}")
    return items


def to_ts_seconds(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, dt.datetime):
        return int(value.timestamp())
    if isinstance(value, dt.date):
        return int(dt.datetime.combine(value, dt.time.min).timestamp())
    s = str(value).strip()
    if not s:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S"):
        try:
            return int(dt.datetime.strptime(s, fmt).timestamp())
        except ValueError:
            pass
    try:
        return int(float(s))
    except ValueError:
        return 0


def to_date_str(value: Any) -> str:
    if value is None:
        return "1970-01-01"
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return "1970-01-01"
    return s[:10]


def to_date_or_none(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date().isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    return s[:10]


def unsigned_amount(*values: int) -> int:
    """目标金额列为 bigint unsigned，禁止负数。"""
    total = 0
    for v in values:
        total += int(v or 0)
    return total if total > 0 else 0


def digits_only(value: Any) -> str:
    return re.sub(r"\D+", "", "" if value is None else str(value))


def normalize_mobile_local10(value: Any) -> str:
    s = digits_only(value)
    if not s:
        return ""
    if s.startswith("234") and len(s) >= 13:
        return s[-10:]
    if s.startswith("0") and len(s) >= 11:
        return s[-10:]
    if len(s) >= 10:
        return s[-10:]
    return s


def to_vt_mobile(value: Any) -> str:
    """与 source_all_sync_staging 一致：查出来的电话规范化成 +234... 再调 VT。"""
    s = ("" if value is None else str(value)).strip()
    if not s:
        return ""
    if s.startswith("+"):
        s = s[1:]
    if s.startswith("234"):
        return "+" + s
    if s.startswith("0"):
        return "+234" + s[1:]
    return "+234" + s


def ensure_json(value: Any) -> str:
    if value in (None, "", []):
        return "[]"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def build_loan_no(order_no: str, current_period: Any) -> str:
    order_no = str(order_no or "").strip()
    if not order_no:
        return ""
    try:
        period = int(current_period or 1)
    except (TypeError, ValueError):
        period = 1
    return f"ng-{order_no}-{period:02d}000"


def as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value).strip()))
        except (TypeError, ValueError):
            return None


def as_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def to_fen(value: Any) -> int:
    """源金额为主单位，目标统一按分写入。"""
    if value in (None, ""):
        return 0
    try:
        return int(round(float(value) * 100))
    except (TypeError, ValueError):
        return 0


def to_int(value: Any, default: int = 0) -> int:
    try:
        s = str(value).strip()
        if not s:
            return default
        return int(float(s))
    except (TypeError, ValueError):
        return default


def hold_days_from(assignment_date: Any) -> int:
    if assignment_date is None:
        return 0
    if isinstance(assignment_date, dt.datetime):
        d = assignment_date.date()
    elif isinstance(assignment_date, dt.date):
        d = assignment_date
    else:
        s = str(assignment_date).strip()[:10]
        if not s:
            return 0
        try:
            d = dt.datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return 0
    return max((dt.date.today() - d).days, 0)


def json_obj(value: Any) -> str:
    if value in (None, "", {}):
        return "{}"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def password_hash() -> str:
    override = os.environ.get("COLLECTION_DEFAULT_PASSWORD_HASH", "").strip()
    if override:
        return override

    # 优先纯 Python bcrypt
    try:
        import bcrypt  # type: ignore

        hashed = bcrypt.hashpw(DEFAULT_PASSWORD.encode("utf-8"), bcrypt.gensalt(rounds=10))
        return hashed.decode("utf-8")
    except Exception:
        pass

    # 其次 htpasswd（包名 apache2-utils）
    cmd = ["htpasswd", "-bnBC", "10", "", DEFAULT_PASSWORD]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode == 0:
            return proc.stdout.strip().lstrip(":")
    except FileNotFoundError:
        pass

    # 最后兜底：默认口令预生成 hash，保证迁移不被环境卡住
    return DEFAULT_PASSWORD_HASH


@dataclass
class DbConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


class DB:
    def __init__(self, cfg: DbConfig, *, readonly: bool = False) -> None:
        self.cfg = cfg
        self.readonly = readonly

    def connect(self, *, stream: bool = False, retries: int = DEFAULT_MYSQL_RETRIES):
        label = f"{self.cfg.host}:{self.cfg.port}/{self.cfg.database}"

        def _open():
            conn = pymysql.connect(
                host=self.cfg.host,
                port=self.cfg.port,
                user=self.cfg.user,
                password=self.cfg.password,
                database=self.cfg.database,
                charset="utf8mb4",
                autocommit=False,
                cursorclass=SSDictCursor if stream else DictCursor,
                read_timeout=3600,
                write_timeout=3600,
                connect_timeout=30,
            )
            with conn.cursor() as cur:
                cur.execute("SET NAMES utf8mb4")
                if self.readonly:
                    cur.execute("SET SESSION TRANSACTION READ ONLY")
            return conn

        return mysql_retry(f"connect {label}", _open, retries=retries)


class VtClient:
    def __init__(self, base_url: str, *, dry_run: bool) -> None:
        self.base_url = base_url.rstrip("/")
        self.dry_run = dry_run
        self.cache: Dict[str, str] = {}
        self._lock = threading.Lock()

    def _v2t_url(self) -> str:
        path = urllib.parse.urlparse(self.base_url).path.rstrip("/") + "/v2t"
        return f"{urllib.parse.urlparse(self.base_url).scheme}://{urllib.parse.urlparse(self.base_url).netloc}{path}"

    def tokenize(self, raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return ""
        got = self.tokenize_many([raw])
        return got[0] if got else ""

    def tokenize_many(self, values: Sequence[str]) -> List[str]:
        """批量 /v2t；已缓存的不重复请求。返回与输入等长。"""
        cleaned = [(v or "").strip() for v in values]
        out: List[str] = [""] * len(cleaned)
        miss_idx: List[int] = []
        miss_vals: List[str] = []
        with self._lock:
            for i, raw in enumerate(cleaned):
                if not raw:
                    continue
                hit = self.cache.get(raw)
                if hit is not None:
                    out[i] = hit
                else:
                    miss_idx.append(i)
                    miss_vals.append(raw)
        if not miss_vals:
            return out

        if self.dry_run:
            with self._lock:
                for i, raw in zip(miss_idx, miss_vals):
                    token = f"dry_{base64.urlsafe_b64encode(raw.encode()).decode()[:20]}"
                    self.cache[raw] = token
                    out[i] = token
            return out

        # VT 一次最多送一批，避免 body 过大
        url = self._v2t_url()
        batch_size = 200
        for start in range(0, len(miss_vals), batch_size):
            chunk_vals = miss_vals[start : start + batch_size]
            chunk_idx = miss_idx[start : start + batch_size]
            body = json.dumps(chunk_vals, ensure_ascii=False).encode("utf-8")
            req = urllib.request.Request(
                url,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", errors="replace")[:500]
                raise RuntimeError(f"VT /v2t 调用失败: HTTP {e.code}: {detail}") from e
            except urllib.error.URLError as e:
                raise RuntimeError(f"VT /v2t 调用失败: {e}") from e
            tokens = data.get("tokens") or []
            if len(tokens) != len(chunk_vals):
                raise RuntimeError(f"VT /v2t 返回数量不匹配: in={len(chunk_vals)} out={len(tokens)} data={data}")
            with self._lock:
                for i, raw, tok in zip(chunk_idx, chunk_vals, tokens):
                    if not tok:
                        raise RuntimeError(f"VT /v2t 返回空 token: raw={raw} data={data}")
                    token = str(tok)
                    self.cache.setdefault(raw, token)
                    out[i] = self.cache[raw]
        return out


class Migrator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.bid = args.bid
        self.src = DB(self._src_cfg(), readonly=True)
        self.tgt = DB(self._tgt_cfg(), readonly=False)
        self.backend = DB(self._backend_cfg(), readonly=True)
        self.vt = VtClient(args.vt_base_url, dry_run=args.dry_run)
        self._target_columns: Dict[str, List[str]] = {}
        self._target_pks: Dict[str, List[str]] = {}
        self._source_tables: Optional[set[str]] = None
        self._adapter: Optional[str] = None
        self._member_password = password_hash()
        self.mysql_retries = int(getattr(args, "mysql_retries", DEFAULT_MYSQL_RETRIES) or DEFAULT_MYSQL_RETRIES)
        self.workers = max(1, int(getattr(args, "workers", DEFAULT_WORKERS) or DEFAULT_WORKERS))
        # order_no -> user_order 宽字段（disburse_time 非空约 5 万单）
        self._order_cache: Optional[Dict[str, Dict[str, Any]]] = None
        self._name_by_user_id: Dict[Any, str] = {}
        self._name_cache: Dict[str, str] = {}  # local10 phone -> name
        self._emerg_cache: Dict[Any, str] = {}
        self._cust_cache: Dict[Any, str] = {}
        self._user_aux_ready = False
        self._aux_lock = threading.Lock()
        self._progress_lock = threading.Lock()

    def _src_cfg(self) -> DbConfig:
        return DbConfig(
            host=env_first("COLLECTION_SOURCE_MYSQL_HOST", "SOURCE_MYSQL_HOST", default="127.0.0.1") or "127.0.0.1",
            port=int(env_first("COLLECTION_SOURCE_MYSQL_PORT", "SOURCE_MYSQL_PORT", default="3306") or "3306"),
            user=env_first("COLLECTION_SOURCE_MYSQL_USER", "SOURCE_MYSQL_USER", default="root") or "root",
            password=env_first("COLLECTION_SOURCE_MYSQL_PASSWORD", "SOURCE_MYSQL_PASSWORD", default="") or "",
            database=env_first("COLLECTION_SOURCE_MYSQL_DATABASE", default="nigeria_admin") or "nigeria_admin",
        )

    def _tgt_cfg(self) -> DbConfig:
        return DbConfig(
            host=env_first("COLLECTION_TARGET_MYSQL_HOST", "TARGET_MYSQL_HOST", default="127.0.0.1") or "127.0.0.1",
            port=int(env_first("COLLECTION_TARGET_MYSQL_PORT", "TARGET_MYSQL_PORT", default="3306") or "3306"),
            user=env_first("COLLECTION_TARGET_MYSQL_USER", "TARGET_MYSQL_USER", default="root") or "root",
            password=env_first("COLLECTION_TARGET_MYSQL_PASSWORD", "TARGET_MYSQL_PASSWORD", default="") or "",
            database=env_first("COLLECTION_TARGET_MYSQL_DATABASE", "TARGET_MYSQL_DATABASE", default="cms") or "cms",
        )

    def _backend_cfg(self) -> DbConfig:
        return DbConfig(
            host=env_first("BACKEND_MYSQL_HOST", "SOURCE_MYSQL_HOST", default="127.0.0.1") or "127.0.0.1",
            port=int(env_first("BACKEND_MYSQL_PORT", "SOURCE_MYSQL_PORT", default="3306") or "3306"),
            user=env_first("BACKEND_MYSQL_USER", "SOURCE_MYSQL_USER", default="root") or "root",
            password=env_first("BACKEND_MYSQL_PASSWORD", "SOURCE_MYSQL_PASSWORD", default="") or "",
            database=env_first("BACKEND_MYSQL_DATABASE", default="nigeria_backend") or "nigeria_backend",
        )

    def source_tables(self) -> set[str]:
        if self._source_tables is not None:
            return self._source_tables
        conn = self.src.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW TABLES")
                self._source_tables = {list(r.values())[0] for r in cur.fetchall()}
        finally:
            conn.close()
        return self._source_tables

    def adapter(self) -> str:
        if self._adapter:
            return self._adapter
        tables = self.source_tables()
        if "repayment_plan" in tables:
            self._adapter = "plan"
        else:
            raise RuntimeError("未识别到催收源表：缺少 repayment_plan")
        log(f"source adapter={self._adapter}")
        return self._adapter

    def target_columns(self, table: str) -> List[str]:
        if table in self._target_columns:
            return self._target_columns[table]
        conn = self.tgt.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                    ORDER BY ORDINAL_POSITION
                    """,
                    (self.tgt.cfg.database, table),
                )
                cols = [r["COLUMN_NAME"] for r in cur.fetchall()]
                if not cols:
                    raise RuntimeError(f"目标表不存在或无列: {self.tgt.cfg.database}.{table}")
                self._target_columns[table] = cols
        finally:
            conn.close()
        return self._target_columns[table]

    def target_pks(self, table: str) -> List[str]:
        if table in self._target_pks:
            return self._target_pks[table]
        conn = self.tgt.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY'
                    ORDER BY ORDINAL_POSITION
                    """,
                    (self.tgt.cfg.database, table),
                )
                self._target_pks[table] = [r["COLUMN_NAME"] for r in cur.fetchall()]
        finally:
            conn.close()
        return self._target_pks[table]

    def stream(self, conn, sql: str, params: Sequence[Any] = ()) -> Iterator[Dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(self.args.batch)
                if not rows:
                    break
                for row in rows:
                    yield row

    def fetch_all(self, db: DB, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        def _do():
            conn = db.connect(retries=self.mysql_retries)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return list(cur.fetchall())
            finally:
                conn.close()

        return mysql_retry("fetch_all", _do, retries=self.mysql_retries)

    def fetch_one(self, db: DB, sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
        rows = self.fetch_all(db, sql, params)
        return rows[0] if rows else None

    def upsert(self, table: str, rows: List[Dict[str, Any]]) -> Tuple[int, int]:
        if not rows:
            return 0, 0
        tgt_cols = self.target_columns(table)
        pks = set(self.target_pks(table))
        cols = [c for c in tgt_cols if c in rows[0]]
        update_cols = [c for c in cols if c not in pks]
        if not cols:
            raise RuntimeError(f"{table} 无可写入列")

        if self.args.dry_run:
            return len(rows), 0

        placeholders = ",".join(["%s"] * len(cols))
        col_sql = ",".join(f"`{c}`" for c in cols)
        update_sql = ",".join(f"`{c}`=VALUES(`{c}`)" for c in update_cols) or ",".join(
            f"`{c}`=`{c}`" for c in cols[:1]
        )
        sql = (
            f"INSERT INTO `{table}` ({col_sql}) VALUES ({placeholders}) "
            f"ON DUPLICATE KEY UPDATE {update_sql}"
        )
        vals = [tuple(r.get(c) for c in cols) for r in rows]

        def _do():
            conn = self.tgt.connect(retries=self.mysql_retries)
            try:
                with conn.cursor() as cur:
                    cur.executemany(sql, vals)
                    rowcount = cur.rowcount
                conn.commit()
                return len(rows), rowcount
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise
            finally:
                conn.close()

        return mysql_retry(f"upsert {table}", _do, retries=self.mysql_retries)

    def run(self) -> None:
        if self.args.verify:
            self.verify_counts()
            return

        for key in parse_tables(self.args.table):
            self._run_table(key)

    def _run_table(self, key: str) -> None:
        fn = getattr(self, f"migrate_{key}")
        last: Optional[BaseException] = None
        for attempt in range(1, self.mysql_retries + 1):
            try:
                fn()
                return
            except (pymysql.MySQLError, OSError) as e:
                last = e
                if not is_retryable_mysql(e) and not isinstance(e, OSError):
                    raise
                if attempt >= self.mysql_retries:
                    break
                sleep_s = min(2**attempt, 60)
                code = e.args[0] if getattr(e, "args", None) else type(e).__name__
                log(
                    f"{key}: MySQL {code}，整表重跑 {attempt}/{self.mysql_retries} "
                    f"(UPSERT 可重入)，{sleep_s}s 后重试"
                )
                time.sleep(sleep_s)
        assert last is not None
        raise last

    def verify_counts(self) -> None:
        mappings = {
            "companys": "collection_company",
            "members": "collection_staff",
            "cases": "repayment_plan",
            "traces": "collection_follow_status_log",
            "dispatch": "collection_assign_log",
        }
        target_map = {
            "companys": "companys",
            "members": "members",
            "cases": "cases",
            "traces": "case_traces",
            "dispatch": "dispatch_logs",
        }
        src_conn = self.src.connect()
        tgt_conn = self.tgt.connect()
        try:
            for key in parse_tables(self.args.table):
                s = mappings[key]
                t = target_map[key]
                with src_conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) AS c FROM `{s}`")
                    src_cnt = cur.fetchone()["c"]
                with tgt_conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) AS c FROM `{t}`")
                    tgt_cnt = cur.fetchone()["c"]
                log(f"verify {key}: source={src_cnt} target={tgt_cnt}")
        finally:
            src_conn.close()
            tgt_conn.close()

    def migrate_companys(self) -> None:
        log("migrate companys")
        sql = """
        SELECT id, company_name, status, created_at, updated_at
        FROM collection_company
        ORDER BY id
        """
        if self.args.limit:
            sql += f" LIMIT {int(self.args.limit)}"
        rows_out: List[Dict[str, Any]] = []
        conn = self.src.connect(stream=True, retries=self.mysql_retries)
        total = 0
        try:
            for row in self.stream(conn, sql):
                rows_out.append(
                    {
                        "company_id": int(row["id"]) + 10000,
                        "bid": self.bid,
                        "provider": "",
                        "name": row.get("company_name") or "",
                        "source": "",
                        "enabled": 1 if int(row.get("status") or 0) == 1 else 0,
                        "config": None,
                        "created_time": to_ts_seconds(row.get("created_at")),
                        "updated_time": to_ts_seconds(row.get("updated_at")),
                        "deleted_time": 0,
                    }
                )
                if len(rows_out) >= self.args.batch:
                    n, _ = self.upsert("companys", rows_out)
                    total += n
                    rows_out = []
            if rows_out:
                n, _ = self.upsert("companys", rows_out)
                total += n
        finally:
            conn.close()
        log(f"companys done: {total}")

    def migrate_members(self) -> None:
        log("migrate members")
        rows = self.fetch_all(
            self.src,
            """
            SELECT
              s.*,
              l.name AS level_name
            FROM collection_staff s
            LEFT JOIN collection_level l ON l.id = s.collection_level_id
            ORDER BY s.id
            """ + (f" LIMIT {int(self.args.limit)}" if self.args.limit else ""),
        )
        out: List[Dict[str, Any]] = []
        total = 0
        for r in rows:
            staff_type = int(r.get("staff_type") or 0)
            status = int(r.get("account_status") or 0)
            name = (r.get("staff_code") or "").strip()
            mobile = normalize_mobile_local10(r.get("mobile"))
            account = name or mobile
            out.append(
                {
                    "member_id": int(r["id"]) + 10000,
                    "bid": self.bid,
                    "company_id": int(r.get("company_id") or 0) + 10000 if r.get("company_id") is not None else 0,
                    "group_id": None,
                    "name": name,
                    "account": account,
                    "password": self._member_password,
                    "role": "leader" if staff_type == 1 else "collector",
                    "type": "manual",
                    "level_code": map_level_code(r.get("level_name")) or None,
                    "level_range": None,
                    "product_ids": None,
                    "app_ids": None,
                    "term": None,
                    "cust_group": "[]",
                    "risk_level": "[]",
                    "disabled_time": to_ts_seconds(r.get("updated_at")) if status in (0, 2) else 0,
                    "dispatch": 1 if status == 1 else 0,
                    "authorize": 0,
                    "created_time": to_ts_seconds(r.get("created_at")),
                    "updated_time": to_ts_seconds(r.get("updated_at")),
                    "deleted_time": 0,
                }
            )
            if len(out) >= self.args.batch:
                n, _ = self.upsert("members", out)
                total += n
                out = []
        if out:
            n, _ = self.upsert("members", out)
            total += n
        log(f"members done: {total}")

    def _ensure_order_cache(self) -> None:
        """一次性把已放款订单 + 用户画像灌进内存，避免 cases 逐单查 backend。"""
        if self._order_cache is not None:
            return
        log("preload backend user_order (disburse_time IS NOT NULL) ...")
        sql = """
        SELECT
          o.order_no,
          o.app_code,
          o.user_id,
          o.disburse_time,
          o.order_time,
          o.last_repayment_time,
          CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)) AS received_amount,
          CAST(NULLIF(TRIM(o.repaid_amount), '') AS DECIMAL(20, 2)) AS repaid_amount,
          CAST(NULLIF(TRIM(o.remaining_repayment), '') AS DECIMAL(20, 2)) AS remaining_repayment,
          DATE(COALESCE(o.disburse_time, o.order_time)) AS disburse_date,
          UNIX_TIMESTAMP(COALESCE(o.disburse_time, o.order_time)) AS disburse_ts,
          DATE(o.last_repayment_time) AS due_date,
          UNIX_TIMESTAMP(o.last_repayment_time) AS due_ts,
          p.bvn,
          b.bank_code,
          b.bank_account,
          ui.current_period,
          CONCAT_WS(' ', NULLIF(TRIM(p.first_name), ''), NULLIF(TRIM(p.sur_name), '')) AS full_name,
          RIGHT(REPLACE(REPLACE(TRIM(u.mobile), '+', ''), ' ', ''), 10) AS user_mobile10,
          DATE_FORMAT(p.date_of_birth, '%%Y-%%m-%%d') AS birthday,
          p.gender,
          p.education_level AS education,
          p.marriage AS marital,
          p.living_address_state AS province,
          p.living_address_city AS city,
          NULLIF(TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ',
                             COALESCE(p.living_address_second_line, ''))), '') AS detail,
          wr.occupation AS profession,
          NULLIF(TRIM(wr.company_name), '') AS company
        FROM user_order o
        LEFT JOIN `user` u ON u.id = o.user_id
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_personal_info GROUP BY user_id
        ) px ON px.user_id = o.user_id
        LEFT JOIN user_personal_info p ON p.id = px.mid
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_bank_info WHERE deleted = 0 GROUP BY user_id
        ) bx ON bx.user_id = o.user_id
        LEFT JOIN user_bank_info b ON b.id = bx.mid
        LEFT JOIN (
            SELECT user_order_id, MAX(id) AS mid FROM user_order_installment GROUP BY user_order_id
        ) ux ON ux.user_order_id = o.id
        LEFT JOIN user_order_installment ui ON ui.id = ux.mid
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_work_related GROUP BY user_id
        ) wx ON wx.user_id = o.user_id
        LEFT JOIN user_work_related wr ON wr.id = wx.mid
        WHERE o.disburse_time IS NOT NULL
          AND UNIX_TIMESTAMP(o.disburse_time) > 0
        """
        cache: Dict[str, Dict[str, Any]] = {}
        conn = self.backend.connect(stream=True, retries=self.mysql_retries)
        try:
            for r in self.stream(conn, sql):
                ono = str(r.get("order_no") or "").strip()
                if ono:
                    cache[ono] = r
        finally:
            conn.close()
        self._order_cache = cache
        log(f"order cache ready: {len(cache)} rows")
        self._preload_user_aux_from_orders(cache)

    def _preload_user_aux_from_orders(self, orders: Dict[str, Dict[str, Any]]) -> None:
        """从订单缓存的 user_id 灌姓名/画像，并批量加载紧急联系人。"""
        if self._user_aux_ready:
            return
        user_ids = set()
        for r in orders.values():
            uid = r.get("user_id")
            if uid is None:
                continue
            user_ids.add(uid)
            name = (r.get("full_name") or "").strip()
            if name:
                self._name_by_user_id[uid] = name
            m10 = (r.get("user_mobile10") or "").strip()
            if m10 and name:
                self._name_cache[m10] = name
            if uid not in self._cust_cache:
                info = {
                    "gender": as_int(r.get("gender")),
                    "address": {
                        "city": as_str(r.get("city")),
                        "detail": as_str(r.get("detail")),
                        "provice": as_str(r.get("province")),
                        "village": "",
                        "district": "",
                    },
                    "company": as_str(r.get("company")),
                    "marital": as_int(r.get("marital")),
                    "birthday": as_str(r.get("birthday")),
                    "education": as_int(r.get("education")),
                    "profession": as_int(r.get("profession")),
                }
                self._cust_cache[uid] = json_obj(info)
        log(
            f"user profile cache ready: users={len(user_ids)} "
            f"names={len(self._name_by_user_id)} phones={len(self._name_cache)} "
            f"cust={len(self._cust_cache)}"
        )
        log(f"preload emergency contacts for {len(user_ids)} users (chunked + batch VT) ...")
        by_user: Dict[Any, List[Dict[str, Any]]] = {}
        uid_list = list(user_ids)
        chunk_size = 500
        t0 = time.time()
        for i in range(0, len(uid_list), chunk_size):
            chunk = uid_list[i : i + chunk_size]
            placeholders = ",".join(["%s"] * len(chunk))
            emerg_sql = f"""
            SELECT user_id, contact_name, contact_number, contact_relationship AS relation, id
            FROM user_emergency_contact
            WHERE user_id IN ({placeholders})
            ORDER BY user_id ASC, id DESC
            """
            rows = self.fetch_all(self.backend, emerg_sql, chunk)
            for r in rows:
                uid = r.get("user_id")
                if uid is None:
                    continue
                bucket = by_user.setdefault(uid, [])
                if len(bucket) >= 10:
                    continue
                bucket.append(r)
            if i == 0 or (i // chunk_size) % 5 == 0:
                log(
                    f"emerg sql progress: users={min(i + chunk_size, len(uid_list))}/{len(uid_list)} "
                    f"rows_kept={sum(len(v) for v in by_user.values())} "
                    f"elapsed={int(time.time() - t0)}s"
                )

        # 先收集手机号，批量 VT，再拼 JSON
        unique_mobiles: List[str] = []
        seen_m: set = set()
        for uid, rows in by_user.items():
            for r in rows:
                vm = to_vt_mobile(r.get("contact_number"))
                r["_vt_mobile"] = vm
                if vm and vm not in seen_m:
                    seen_m.add(vm)
                    unique_mobiles.append(vm)
        log(f"emerg VT batch: unique_mobiles={len(unique_mobiles)}")
        for j in range(0, len(unique_mobiles), 1000):
            part = unique_mobiles[j : j + 1000]
            self.vt.tokenize_many(part)
            log(
                f"emerg VT progress: {min(j + 1000, len(unique_mobiles))}/{len(unique_mobiles)} "
                f"elapsed={int(time.time() - t0)}s"
            )

        for uid, rows in by_user.items():
            out = []
            seen = set()
            for r in rows:
                vt_mobile = r.get("_vt_mobile") or ""
                if not vt_mobile:
                    continue
                key = ((r.get("contact_name") or "").strip(), vt_mobile)
                if key in seen:
                    continue
                seen.add(key)
                rel = RELATION_MAP.get(int(r.get("relation") or 0))
                item = {
                    "name": (r.get("contact_name") or "").strip(),
                    "mobile": self.vt.tokenize(vt_mobile),  # 已在 cache
                }
                if rel is not None:
                    item["relation"] = rel
                out.append(item)
            self._emerg_cache[uid] = ensure_json(out)
        # 没有紧急联系人的用户也占位，避免运行时再打库
        for uid in user_ids:
            if uid not in self._emerg_cache:
                self._emerg_cache[uid] = "[]"
        self._user_aux_ready = True
        log(f"emerg cache ready: users={len(self._emerg_cache)} elapsed={int(time.time() - t0)}s")

    def _backend_user_bundle(self, order_no: str) -> Dict[str, Any]:
        order_no = (order_no or "").strip()
        if not order_no:
            return {}
        self._ensure_order_cache()
        assert self._order_cache is not None
        hit = self._order_cache.get(order_no)
        if hit is not None:
            return hit
        # 未放款 / 缓存外：回退单次查询
        return self._fetch_backend_user_bundle(order_no)

    def _fetch_backend_user_bundle(self, order_no: str) -> Dict[str, Any]:
        sql = """
        SELECT
          o.order_no,
          o.app_code,
          o.user_id,
          o.disburse_time,
          o.order_time,
          o.last_repayment_time,
          CAST(NULLIF(TRIM(o.received), '') AS DECIMAL(20, 2)) AS received_amount,
          CAST(NULLIF(TRIM(o.repaid_amount), '') AS DECIMAL(20, 2)) AS repaid_amount,
          CAST(NULLIF(TRIM(o.remaining_repayment), '') AS DECIMAL(20, 2)) AS remaining_repayment,
          DATE(COALESCE(o.disburse_time, o.order_time)) AS disburse_date,
          UNIX_TIMESTAMP(COALESCE(o.disburse_time, o.order_time)) AS disburse_ts,
          DATE(o.last_repayment_time) AS due_date,
          UNIX_TIMESTAMP(o.last_repayment_time) AS due_ts,
          p.bvn,
          b.bank_code,
          b.bank_account,
          ui.current_period,
          CONCAT_WS(' ', NULLIF(TRIM(p.first_name), ''), NULLIF(TRIM(p.sur_name), '')) AS full_name
        FROM user_order o
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_personal_info GROUP BY user_id
        ) px ON px.user_id = o.user_id
        LEFT JOIN user_personal_info p ON p.id = px.mid
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_bank_info WHERE deleted = 0 GROUP BY user_id
        ) bx ON bx.user_id = o.user_id
        LEFT JOIN user_bank_info b ON b.id = bx.mid
        LEFT JOIN (
            SELECT user_order_id, MAX(id) AS mid FROM user_order_installment GROUP BY user_order_id
        ) ux ON ux.user_order_id = o.id
        LEFT JOIN user_order_installment ui ON ui.id = ux.mid
        WHERE o.order_no = %s
        LIMIT 1
        """
        return self.fetch_one(self.backend, sql, (order_no,)) or {}

    def _resolve_name(self, user_id: Any, phone: Any) -> str:
        if user_id is not None:
            with self._aux_lock:
                hit = self._name_by_user_id.get(user_id)
            if hit:
                return hit
            # 订单宽表里的 full_name
            # （已在 preload 写入 _name_by_user_id）
        return self._backend_name_by_phone(phone)

    def _backend_name_by_phone(self, phone: Any) -> str:
        local10 = normalize_mobile_local10(phone)
        if not local10:
            return ""
        with self._aux_lock:
            if local10 in self._name_cache:
                return self._name_cache[local10]
        # 用户画像已预热后仍未命中：不再打库（避免拖慢），返回空
        if self._user_aux_ready:
            with self._aux_lock:
                self._name_cache[local10] = ""
            return ""
        sql = """
        SELECT CONCAT_WS(' ', NULLIF(TRIM(p.first_name), ''), NULLIF(TRIM(p.sur_name), '')) AS full_name
        FROM `user` u
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_personal_info GROUP BY user_id
        ) px ON px.user_id = u.id
        LEFT JOIN user_personal_info p ON p.id = px.mid
        WHERE RIGHT(REPLACE(REPLACE(TRIM(u.mobile), '+', ''), ' ', ''), 10) = %s
        ORDER BY u.id DESC
        LIMIT 1
        """
        row = self.fetch_one(self.backend, sql, (local10,))
        name = ((row or {}).get("full_name") or "").strip()
        with self._aux_lock:
            self._name_cache[local10] = name
        return name

    def _emerg_contacts(self, user_id: Any) -> str:
        if not user_id:
            return "[]"
        with self._aux_lock:
            if user_id in self._emerg_cache:
                return self._emerg_cache[user_id]
        if self._user_aux_ready:
            with self._aux_lock:
                self._emerg_cache[user_id] = "[]"
            return "[]"
        sql = """
        SELECT contact_name, contact_number, contact_relationship AS relation
        FROM user_emergency_contact
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT 10
        """
        rows = self.fetch_all(self.backend, sql, (user_id,))
        out = []
        seen = set()
        for r in rows:
            vt_mobile = to_vt_mobile(r.get("contact_number"))
            if not vt_mobile:
                continue
            key = ((r.get("contact_name") or "").strip(), vt_mobile)
            if key in seen:
                continue
            seen.add(key)
            rel = RELATION_MAP.get(int(r.get("relation") or 0))
            item = {
                "name": (r.get("contact_name") or "").strip(),
                "mobile": self.vt.tokenize(vt_mobile),
            }
            if rel is not None:
                item["relation"] = rel
            out.append(item)
        result = ensure_json(out)
        with self._aux_lock:
            self._emerg_cache[user_id] = result
        return result

    def _cust_info(self, user_id: Any) -> str:
        """按 cms.cases.cust_info 样例结构组装客户画像。"""
        if not user_id:
            return "{}"
        with self._aux_lock:
            if user_id in self._cust_cache:
                return self._cust_cache[user_id]
        if self._user_aux_ready:
            empty = json_obj(
                {
                    "gender": None,
                    "address": {"city": "", "detail": "", "provice": "", "village": "", "district": ""},
                    "company": "",
                    "marital": None,
                    "birthday": "",
                    "education": None,
                    "profession": None,
                }
            )
            with self._aux_lock:
                self._cust_cache[user_id] = empty
            return empty
        sql = """
        SELECT
          DATE_FORMAT(p.date_of_birth, '%%Y-%%m-%%d') AS birthday,
          p.gender,
          p.education_level AS education,
          p.marriage AS marital,
          p.living_address_state AS province,
          p.living_address_city AS city,
          NULLIF(TRIM(CONCAT(COALESCE(p.living_address_first_line, ''), ' ',
                             COALESCE(p.living_address_second_line, ''))), '') AS detail,
          wr.occupation AS profession,
          NULLIF(TRIM(wr.company_name), '') AS company
        FROM user_personal_info p
        LEFT JOIN (
            SELECT user_id, MAX(id) AS mid FROM user_work_related GROUP BY user_id
        ) wx ON wx.user_id = p.user_id
        LEFT JOIN user_work_related wr ON wr.id = wx.mid
        WHERE p.id = (
            SELECT MAX(p2.id) FROM user_personal_info p2 WHERE p2.user_id = %s
        )
        LIMIT 1
        """
        row = self.fetch_one(self.backend, sql, (user_id,)) or {}
        info = {
            "gender": as_int(row.get("gender")),
            "address": {
                "city": as_str(row.get("city")),
                "detail": as_str(row.get("detail")),
                "provice": as_str(row.get("province")),
                "village": "",
                "district": "",
            },
            "company": as_str(row.get("company")),
            "marital": as_int(row.get("marital")),
            "birthday": as_str(row.get("birthday")),
            "education": as_int(row.get("education")),
            "profession": as_int(row.get("profession")),
        }
        result = json_obj(info)
        with self._aux_lock:
            self._cust_cache[user_id] = result
        return result

    @staticmethod
    def _split_id_ranges(mi: int, ma: int, n: int) -> List[Tuple[int, int]]:
        """按 id 数值均分（稀疏主键时负载不均，优先用 _balanced_id_ranges）。"""
        if mi > ma:
            return []
        n = max(1, min(n, ma - mi + 1))
        span = ma - mi + 1
        size = (span + n - 1) // n
        out: List[Tuple[int, int]] = []
        cur = mi
        while cur <= ma:
            hi = min(ma, cur + size - 1)
            out.append((cur, hi))
            cur = hi + 1
        return out

    def _balanced_id_ranges(self, table: str, *, n: int) -> Tuple[int, List[Tuple[int, int]]]:
        """按行数近似均分主键区间，避免雪花 id 跨度导致 7 个空分片、1 个扛全部。"""
        meta = self.fetch_one(
            self.src,
            f"SELECT MIN(id) AS mi, MAX(id) AS ma, COUNT(*) AS cnt FROM `{table}`",
        ) or {}
        mi = meta.get("mi")
        ma = meta.get("ma")
        cnt = int(meta.get("cnt") or 0)
        if mi is None or ma is None or cnt <= 0:
            return 0, []
        mi_i, ma_i = int(mi), int(ma)
        n = max(1, min(n, cnt))
        if n == 1:
            return cnt, [(mi_i, ma_i)]
        cuts: List[int] = [mi_i]
        for i in range(1, n):
            offset = (cnt * i) // n
            # OFFSET 在 ~5 万行可接受；取分界 id
            row = self.fetch_one(
                self.src,
                f"SELECT id FROM `{table}` ORDER BY id LIMIT 1 OFFSET %s",
                (offset,),
            )
            if row and row.get("id") is not None:
                cuts.append(int(row["id"]))
        cuts.append(ma_i + 1)  # exclusive end sentinel
        # 去重并生成闭区间 [lo, hi]
        ranges: List[Tuple[int, int]] = []
        for i in range(len(cuts) - 1):
            lo = cuts[i]
            hi_excl = cuts[i + 1]
            if hi_excl <= lo:
                continue
            ranges.append((lo, hi_excl - 1))
        if not ranges:
            return cnt, [(mi_i, ma_i)]
        # 合并可能因重复 cut 产生的空洞，保证覆盖到 ma
        ranges[-1] = (ranges[-1][0], ma_i)
        return cnt, ranges

    def _cases_select_sql(self, *, where: str = "") -> str:
        return f"""
        SELECT
          p.id,
          p.order_no,
          p.repayment_plan_order_no,
          p.app_id,
          p.app_name,
          p.product_id,
          p.product_name,
          p.phone,
          p.contract_amount,
          p.amount_due,
          p.repaid_amount,
          p.principal_due,
          p.overdue_fees,
          p.overdue_days,
          p.status AS plan_status,
          p.collection_follow_status,
          p.created_at AS plan_created_at,
          p.updated_at,
          a.collector_id,
          a.assignment_date,
          a.created_at AS assignment_created_at,
          al.current_collector_id,
          st.id AS staff_id,
          st.company_id,
          cc.company_name,
          cl.name AS level_name
        FROM repayment_plan p
        LEFT JOIN collection_level cl ON cl.id = p.collection_level_id
        LEFT JOIN (
            SELECT repayment_plan_id, MAX(id) AS mid
            FROM collection_assignment
            WHERE assignment_status = 1
            GROUP BY repayment_plan_id
        ) ax ON ax.repayment_plan_id = p.id
        LEFT JOIN collection_assignment a ON a.id = ax.mid
        LEFT JOIN (
            SELECT repayment_plan_id, MAX(id) AS mid
            FROM collection_assign_log
            GROUP BY repayment_plan_id
        ) lx ON lx.repayment_plan_id = p.id
        LEFT JOIN collection_assign_log al ON al.id = lx.mid
        LEFT JOIN collection_staff st
          ON st.staff_code = COALESCE(a.collector_id, al.current_collector_id)
        LEFT JOIN collection_company cc
          ON cc.id = st.company_id
        {where}
        ORDER BY p.id
        """

    def _build_case_row(self, r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        order_no = str(r.get("order_no") or "").strip()
        if not order_no:
            return None
        bu = self._backend_user_bundle(order_no)
        user_id = bu.get("user_id")
        phone = r.get("phone")
        vt_mobile = to_vt_mobile(phone)
        case_no = str(r.get("repayment_plan_order_no") or "").strip() or order_no
        loan_amount = unsigned_amount(to_fen(r.get("contract_amount")))
        unpaid_amount = unsigned_amount(to_fen(bu.get("remaining_repayment")))
        paid_amount = unsigned_amount(to_fen(bu.get("repaid_amount")))
        total_amount = unsigned_amount(unpaid_amount + paid_amount)
        principal = unsigned_amount(to_fen(r.get("principal_due")))
        fee = unsigned_amount(int(round(loan_amount * 0.35)))
        penalty_amount = unsigned_amount(to_fen(r.get("overdue_fees")))
        received_fen = unsigned_amount(to_fen(bu.get("received_amount")))
        disbursed_amount = received_fen if received_fen > 0 else unsigned_amount(loan_amount - fee)
        app_code = bu.get("app_code") or r.get("app_id") or 0
        application_no = f"ng0{to_int(app_code):01d}-{order_no}"[:36]
        follow_status = (r.get("collection_follow_status") or "").strip().upper()
        plan_status = to_int(r.get("plan_status"), default=-1)
        closed = plan_status == 0 or follow_status == "PAID"
        collection_raw = r.get("assignment_date") or r.get("assignment_created_at") or r.get("plan_created_at") or r.get("updated_at")
        collection_time = to_ts_seconds(r.get("assignment_created_at") or r.get("plan_created_at") or r.get("updated_at"))
        if collection_time <= 0:
            collection_time = int(time.time())
        collection_date = to_date_str(collection_raw)
        disbursed_time = to_int(bu.get("disburse_ts"))
        disbursed_date = to_date_or_none(bu.get("disburse_date"))
        due_time = to_int(bu.get("due_ts"))
        due_date = to_date_or_none(bu.get("due_date"))
        return {
            "bid": self.bid,
            "case_no": case_no[:64],
            "application_no": application_no,
            "loan_no": build_loan_no(order_no, bu.get("current_period"))[:36],
            "sn": order_no[:36],
            "company_id": int(r.get("company_id") or 0) + 10000 if r.get("company_id") else 0,
            "company_name": (r.get("company_name") or "").strip(),
            "member_id": 10000 + int(r.get("staff_id") or 0) if r.get("staff_id") else 0,
            "member_name": str(r.get("collector_id") or r.get("current_collector_id") or "")[:64],
            "level_code": map_level_code(r.get("level_name")),
            "product_id": map_product_id(r.get("product_id")),
            "product_name": str(r.get("product_name") or "")[:128],
            "app_id": to_int(r.get("app_id")),
            "app_name": str(r.get("app_name") or "")[:128],
            "status": "CLOSED" if closed else "COLLECTING",
            "term": 7,
            "cust_group": "[]",
            "name": self._resolve_name(user_id, phone)[:128],
            "mobile": (self.vt.tokenize(vt_mobile) if vt_mobile else "")[:28],
            "id_number": (self.vt.tokenize((bu.get("bvn") or "").strip()) if bu.get("bvn") else "")[:28],
            "email": "",
            "bank_code": (bu.get("bank_code") or "").strip()[:128],
            "bank_account": (self.vt.tokenize((bu.get("bank_account") or "").strip()) if bu.get("bank_account") else "")[:128],
            "loan_amount": loan_amount,
            "total_amount": total_amount,
            "principal": principal,
            "fee": fee,
            "interest": 0,
            "penalty_amount": penalty_amount,
            "tax_amount": 0,
            "rollover_amount": 0,
            "disbursed_amount": disbursed_amount,
            "disbursed_time": disbursed_time,
            "disbursed_date": disbursed_date,
            "paid_amount": paid_amount,
            "unpaid_amount": unpaid_amount,
            "overdue_days": to_int(r.get("overdue_days")),
            "hold_days": hold_days_from(r.get("assignment_date")),
            "due_time": due_time,
            "due_date": due_date,
            "closed_method": "settle" if follow_status == "PAID" else "",
            "closed_time": to_ts_seconds(r.get("updated_at")) if plan_status == 0 else 0,
            "closed_date": to_date_or_none(r.get("updated_at")) if plan_status == 0 else None,
            "collection_time": collection_time,
            "collection_date": collection_date,
            "promise_time": 0,
            "last_trace_id": 0,
            "contacts": "[]",
            "emerg_contacts": self._emerg_contacts(user_id),
            "cust_info": self._cust_info(user_id),
            "updated_time": to_ts_seconds(r.get("updated_at")),
        }

    def _migrate_cases_shard(self, lo: int, hi: int, wid: int, t0: float) -> Tuple[int, int]:
        log(f"cases w{wid} start id[{lo},{hi}]")
        sql = self._cases_select_sql(where="WHERE p.id BETWEEN %s AND %s")
        conn = self.src.connect(stream=True, retries=self.mysql_retries)
        batch: List[Dict[str, Any]] = []
        total = 0
        scanned = 0
        try:
            for r in self.stream(conn, sql, (lo, hi)):
                scanned += 1
                if scanned == 1 or scanned % 50 == 0:
                    with self._progress_lock:
                        log(
                            f"cases w{wid} scanning scanned={scanned} pending_batch={len(batch)} "
                            f"upserted={total} elapsed={int(time.time() - t0)}s"
                        )
                row = self._build_case_row(r)
                if not row:
                    continue
                batch.append(row)
                if len(batch) >= self.args.batch:
                    n, _ = self.upsert("cases", batch)
                    total += n
                    batch = []
                    with self._progress_lock:
                        log(
                            f"cases w{wid} upserted+={n} shard_total={total} "
                            f"scanned={scanned} elapsed={int(time.time() - t0)}s"
                        )
            if batch:
                n, _ = self.upsert("cases", batch)
                total += n
        finally:
            conn.close()
        log(f"cases w{wid} done id[{lo},{hi}] upserted={total} scanned={scanned}")
        return total, scanned

    def migrate_cases(self) -> None:
        log("migrate cases")
        self._ensure_order_cache()
        t0 = time.time()
        if self.args.limit:
            # limit 场景：单连接扫 + 线程池并行 enrich，再分批写
            sql = self._cases_select_sql() + f" LIMIT {int(self.args.limit)}"
            log(f"migrate cases: limit={self.args.limit} workers={self.workers}")
            self.target_columns("cases")
            self.target_pks("cases")
            conn = self.src.connect(stream=True, retries=self.mysql_retries)
            raw_buf: List[Dict[str, Any]] = []
            total = 0
            scanned = 0
            try:
                with ThreadPoolExecutor(max_workers=self.workers) as pool:
                    for r in self.stream(conn, sql):
                        raw_buf.append(r)
                        scanned += 1
                        if len(raw_buf) < max(self.args.batch, self.workers * 20):
                            continue
                        rows = [x for x in pool.map(self._build_case_row, raw_buf) if x]
                        raw_buf = []
                        for i in range(0, len(rows), self.args.batch):
                            chunk = rows[i : i + self.args.batch]
                            n, _ = self.upsert("cases", chunk)
                            total += n
                        log(f"cases progress upserted={total} scanned={scanned} elapsed={int(time.time() - t0)}s")
                    if raw_buf:
                        rows = [x for x in pool.map(self._build_case_row, raw_buf) if x]
                        for i in range(0, len(rows), self.args.batch):
                            chunk = rows[i : i + self.args.batch]
                            n, _ = self.upsert("cases", chunk)
                            total += n
            finally:
                conn.close()
            log(f"cases done: {total} (scanned={scanned}, elapsed={int(time.time() - t0)}s)")
            return

        cnt, ranges = self._balanced_id_ranges("repayment_plan", n=self.workers)
        if cnt <= 0 or not ranges:
            log("cases done: 0 (empty repayment_plan)")
            return
        log(
            f"migrate cases: rows={cnt} shards={len(ranges)} "
            f"workers={self.workers} batch={self.args.batch} ranges={ranges}"
        )
        # 预热目标表元数据，避免多线程首次探测竞态
        self.target_columns("cases")
        self.target_pks("cases")
        total = 0
        scanned = 0
        with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
            futs = [
                pool.submit(self._migrate_cases_shard, lo, hi, i + 1, t0)
                for i, (lo, hi) in enumerate(ranges)
            ]
            for fut in as_completed(futs):
                up, sc = fut.result()
                total += up
                scanned += sc
        log(f"cases done: {total} (scanned={scanned}, elapsed={int(time.time() - t0)}s)")

    def migrate_traces(self) -> None:
        # 暂不迁移 case_traces；仅在显式 --table traces 时进入这里并直接跳过
        log("migrate traces: skipped (case_traces 暂不传)")
        return

    def _build_dispatch_row(self, r: Dict[str, Any]) -> Dict[str, Any]:
        paid = 0
        total_amount = to_fen(r.get("contract_amount"))
        case_no = (
            str(r.get("repayment_plan_order_no") or "").strip()
            or str(r.get("plan_order_no") or r.get("order_no") or "").strip()
        )
        return {
            "bid": self.bid,
            "case_no": case_no,
            "total_amount": total_amount,
            "paid_amount": paid,
            "level_code": map_level_code(r.get("collection_level_name") or r.get("level_name")),
            "application_no": "",
            "executor_id": 0,
            "executor_name": (r.get("operator_name") or "").strip(),
            "original_member_id": 10000 + int(r.get("initial_staff_id") or 0) if r.get("initial_staff_id") else 0,
            "original_member_name": str(r.get("initial_collector_id") or ""),
            "member_id": 10000 + int(r.get("current_staff_id") or 0) if r.get("current_staff_id") else 0,
            "member_name": str(r.get("current_collector_id") or ""),
            "dispatch_time": to_ts_seconds(r.get("created_at")),
            "dispatch_date": to_date_str(r.get("created_at")),
        }

    def _migrate_dispatch_shard(self, lo: int, hi: int, wid: int, t0: float) -> Tuple[int, int]:
        log(f"dispatch w{wid} start id[{lo},{hi}]")
        sql = """
        SELECT
          l.*,
          p.repayment_plan_order_no,
          p.order_no AS plan_order_no,
          s1.id AS initial_staff_id,
          s2.id AS current_staff_id
        FROM collection_assign_log l
        LEFT JOIN repayment_plan p ON p.id = l.repayment_plan_id
        LEFT JOIN collection_staff s1 ON s1.staff_code = l.initial_collector_id
        LEFT JOIN collection_staff s2 ON s2.staff_code = l.current_collector_id
        WHERE l.id BETWEEN %s AND %s
        ORDER BY l.id
        """
        conn = self.src.connect(stream=True, retries=self.mysql_retries)
        batch: List[Dict[str, Any]] = []
        total = 0
        scanned = 0
        try:
            for r in self.stream(conn, sql, (lo, hi)):
                scanned += 1
                if scanned == 1 or scanned % 200 == 0:
                    with self._progress_lock:
                        log(
                            f"dispatch w{wid} scanning scanned={scanned} upserted={total} "
                            f"elapsed={int(time.time() - t0)}s"
                        )
                batch.append(self._build_dispatch_row(r))
                if len(batch) >= self.args.batch:
                    n, _ = self.upsert("dispatch_logs", batch)
                    total += n
                    batch = []
                    with self._progress_lock:
                        log(
                            f"dispatch w{wid} upserted+={n} shard_total={total} "
                            f"scanned={scanned} elapsed={int(time.time() - t0)}s"
                        )
            if batch:
                n, _ = self.upsert("dispatch_logs", batch)
                total += n
        finally:
            conn.close()
        log(f"dispatch w{wid} done id[{lo},{hi}] upserted={total} scanned={scanned}")
        return total, scanned

    def migrate_dispatch(self) -> None:
        log("migrate dispatch")
        t0 = time.time()
        if self.args.limit:
            sql = """
            SELECT
              l.*,
              p.repayment_plan_order_no,
              p.order_no AS plan_order_no,
              s1.id AS initial_staff_id,
              s2.id AS current_staff_id
            FROM collection_assign_log l
            LEFT JOIN repayment_plan p ON p.id = l.repayment_plan_id
            LEFT JOIN collection_staff s1 ON s1.staff_code = l.initial_collector_id
            LEFT JOIN collection_staff s2 ON s2.staff_code = l.current_collector_id
            ORDER BY l.id
            """ + f" LIMIT {int(self.args.limit)}"
            conn = self.src.connect(stream=True, retries=self.mysql_retries)
            batch: List[Dict[str, Any]] = []
            total = 0
            try:
                for r in self.stream(conn, sql):
                    batch.append(self._build_dispatch_row(r))
                    if len(batch) >= self.args.batch:
                        n, _ = self.upsert("dispatch_logs", batch)
                        total += n
                        batch = []
                if batch:
                    n, _ = self.upsert("dispatch_logs", batch)
                    total += n
            finally:
                conn.close()
            log(f"dispatch done: {total}")
            return

        cnt, ranges = self._balanced_id_ranges("collection_assign_log", n=self.workers)
        if cnt <= 0 or not ranges:
            log("dispatch done: 0")
            return
        log(f"migrate dispatch: rows={cnt} shards={len(ranges)} workers={self.workers} ranges={ranges}")
        self.target_columns("dispatch_logs")
        self.target_pks("dispatch_logs")
        total = 0
        scanned = 0
        with ThreadPoolExecutor(max_workers=len(ranges)) as pool:
            futs = [
                pool.submit(self._migrate_dispatch_shard, lo, hi, i + 1, t0)
                for i, (lo, hi) in enumerate(ranges)
            ]
            for fut in as_completed(futs):
                up, sc = fut.result()
                total += up
                scanned += sc
        log(f"dispatch done: {total} (scanned={scanned}, elapsed={int(time.time() - t0)}s)")



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="迁移催收历史数据到 cms")
    p.add_argument("--env", default=DEFAULT_ENV, help="env 文件路径")
    p.add_argument(
        "--table",
        default="all",
        help="companys,members,cases,dispatch 或 all；all 默认不含 traces（case_traces 暂不传）",
    )
    p.add_argument("--dry-run", action="store_true", help="只读源库，不写目标；VT 用占位 token")
    p.add_argument("--limit", type=int, default=0, help="限制处理行数")
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="批量写入行数")
    p.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help="cases/dispatch 分片并发数，默认 8",
    )
    p.add_argument("--bid", default=DEFAULT_BID, help="业务线，默认 ng01")
    p.add_argument("--verify", action="store_true", help="仅做源/目标行数比对")
    p.add_argument(
        "--mysql-retries",
        type=int,
        default=DEFAULT_MYSQL_RETRIES,
        help="MySQL 断连(2013/2006/2003)重试次数，默认 5",
    )
    p.add_argument("--vt-base-url", default=env_first("VT_BASE_URL", default=DEFAULT_VT_URL) or DEFAULT_VT_URL)
    return p


def main() -> int:
    args = build_parser().parse_args()
    load_dotenv(args.env)
    try:
        Migrator(args).run()
    except KeyboardInterrupt:
        log("用户中断")
        return 130
    except Exception as e:
        log(f"失败: {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
