#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""加载 nigeria-flink-sync .env，并提供源/目标库连接。"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SnowflakeIdGenerator:
    """对齐 udf SnowflakeIdGenerator 默认参数（可用 env 覆盖）。"""

    def __init__(
        self,
        epoch_ms: int = 1288834974657,
        datacenter_id: int = 0,
        worker_id: int = 20,
        worker_id_bits: int = 5,
        datacenter_id_bits: int = 5,
        sequence_bits: int = 12,
    ) -> None:
        self.epoch = epoch_ms
        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        self.sequence_mask = (1 << sequence_bits) - 1
        self.worker_id_shift = sequence_bits
        self.datacenter_id_shift = sequence_bits + worker_id_bits
        self.timestamp_shift = sequence_bits + worker_id_bits + datacenter_id_bits
        self.sequence = 0
        self.last_timestamp = -1
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            ts = int(time.time() * 1000)
            if ts < self.last_timestamp:
                raise RuntimeError("clock moved backwards")
            if ts == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.sequence_mask
                if self.sequence == 0:
                    while ts <= self.last_timestamp:
                        ts = int(time.time() * 1000)
            else:
                self.sequence = 0
            self.last_timestamp = ts
            return (
                ((ts - self.epoch) << self.timestamp_shift)
                | (self.datacenter_id << self.datacenter_id_shift)
                | (self.worker_id << self.worker_id_shift)
                | self.sequence
            )


_snowflake: Optional[SnowflakeIdGenerator] = None
_snowflake_lock = threading.Lock()


def get_snowflake(cfg: Optional[Dict[str, Any]] = None) -> SnowflakeIdGenerator:
    global _snowflake
    with _snowflake_lock:
        if _snowflake is None:
            c = cfg or {}
            _snowflake = SnowflakeIdGenerator(
                epoch_ms=int(c.get("SNOWFLAKE_EPOCH_MS") or os.environ.get("SNOWFLAKE_EPOCH_MS") or 1288834974657),
                datacenter_id=int(c.get("SNOWFLAKE_DATACENTER_ID") or os.environ.get("SNOWFLAKE_DATACENTER_ID") or 0),
                worker_id=int(c.get("SNOWFLAKE_WORKER_ID") or os.environ.get("SNOWFLAKE_WORKER_ID") or 20),
            )
        return _snowflake


def load_env(env_path: Optional[Path] = None) -> Dict[str, Any]:
    path = Path(env_path) if env_path else PROJECT_ROOT / ".env"
    if not path.is_file():
        raise FileNotFoundError("env not found: {0}".format(path))
    cfg: Dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        # 去掉行内注释（非引号内）
        if " #" in line:
            line = line.split(" #", 1)[0].rstrip()
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip("'\"")
        os.environ[key] = val
        cfg[key] = val

    cfg["user_id_offset"] = int(cfg.get("USER_ID_OFFSET") or os.environ.get("USER_ID_OFFSET") or 100_000_000)
    cfg["vt_token_enable"] = str(cfg.get("VT_TOKEN_ENABLE", "1")).lower() not in ("0", "false", "no")
    return cfg


def connect_source(cfg: Dict[str, Any]):
    return pymysql.connect(
        host=cfg["SOURCE_MYSQL_HOST"],
        port=int(cfg.get("SOURCE_MYSQL_PORT") or 3306),
        user=cfg["SOURCE_MYSQL_USER"],
        password=cfg["SOURCE_MYSQL_PASSWORD"],
        database=cfg.get("SOURCE_MYSQL_DATABASE") or "nigeria_backend",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=3600,
        write_timeout=3600,
        autocommit=True,
    )


def connect_target(cfg: Dict[str, Any]):
    return pymysql.connect(
        host=cfg["TARGET_MYSQL_HOST"],
        port=int(cfg.get("TARGET_MYSQL_PORT") or 3306),
        user=cfg["TARGET_MYSQL_USER"],
        password=cfg["TARGET_MYSQL_PASSWORD"],
        database=cfg["TARGET_MYSQL_DATABASE"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=3600,
        write_timeout=3600,
        autocommit=True,
    )


def close_conn(conn) -> None:
    if conn is None:
        return
    try:
        conn.close()
    except Exception:
        pass
