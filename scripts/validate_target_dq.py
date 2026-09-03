#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""目标库 ng 全量数据质量校验（对标 dq_fulltable_validate.py / dq_full_ng_report.md）。

在目标库执行 application / loan / user 规则计数，输出 Markdown 或 JSON 报告；
命中规则另导出 TSV 明细，并在报告中附预览表格。

Usage（101 内网）:
  python3 scripts/validate_target_dq.py --env ./.env -o /tmp/dq_ng_report.md --workers 8
  python3 scripts/validate_target_dq.py --env ./.env --rules G2,G3 --detail-dir /tmp/dq_details
  python3 scripts/validate_target_dq.py --env ./.env --json --detail-limit 0
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import pymysql

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
sys.path.insert(0, str(REPO / "scripts" / "reconcile"))
import env_util  # noqa: E402

# Nigeria WAT = UTC+1；部分 MySQL 未导入 tz 表时 Africa/Lagos 不可用
_SESSION_TZ_CANDIDATES = ("Africa/Lagos", "+01:00")
_TZ_LOG_LOCK = threading.Lock()
_TZ_LOGGED = False

# MySQL 可重试错误码：断连、锁等待、连接数等
_RETRYABLE_MYSQL_ERRNO = frozenset({
    1040,  # too many connections
    1159,  # timeout
    1161,  # timeout
    1205,  # lock wait timeout
    1213,  # deadlock
    2006,  # server has gone away
    2013,  # lost connection during query
})


def connect_target(cfg: Dict[str, Any], read_timeout: int) -> Any:
    return pymysql.connect(
        host=cfg["TARGET_MYSQL_HOST"],
        port=int(cfg.get("TARGET_MYSQL_PORT") or 3306),
        user=cfg["TARGET_MYSQL_USER"],
        password=cfg["TARGET_MYSQL_PASSWORD"],
        database=cfg["TARGET_MYSQL_DATABASE"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=30,
        read_timeout=read_timeout,
        write_timeout=read_timeout,
        autocommit=True,
    )


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, pymysql.err.InterfaceError):
        return True
    if isinstance(exc, pymysql.err.OperationalError) and exc.args:
        return int(exc.args[0]) in _RETRYABLE_MYSQL_ERRNO
    if isinstance(exc, OSError):
        return True
    return False


def call_with_conn_retry(
    cfg: Dict[str, Any],
    label: str,
    read_timeout: int,
    retries: int,
    fn: Callable[[Any], Any],
) -> Any:
    last_exc: Optional[BaseException] = None
    for attempt in range(retries + 1):
        conn = None
        try:
            conn = connect_target(cfg, read_timeout)
            ensure_session_tz(conn)
            return fn(conn)
        except Exception as exc:
            last_exc = exc
            if attempt >= retries or not is_retryable(exc):
                raise
            wait_s = min(2 ** attempt, 30)
            print(
                "[retry] {0} attempt={1}/{2} wait={3}s err={4}".format(
                    label, attempt + 1, retries, wait_s, exc,
                ),
                file=sys.stderr,
            )
            time.sleep(wait_s)
        finally:
            env_util.close_conn(conn)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("call_with_conn_retry: no result")


def ensure_session_tz(conn) -> str:
    """设置会话时区；返回实际使用的 tz 名/偏移（或 server_default）。"""
    global _TZ_LOGGED
    if getattr(conn, "_dq_session_tz", None):
        return conn._dq_session_tz
    for tz in _SESSION_TZ_CANDIDATES:
        try:
            with conn.cursor() as cur:
                cur.execute("SET time_zone = %s", (tz,))
            conn._dq_session_tz = tz
            with _TZ_LOG_LOCK:
                if not _TZ_LOGGED:
                    print("session time_zone={0}".format(tz), file=sys.stderr)
                    _TZ_LOGGED = True
            return tz
        except pymysql.err.OperationalError as exc:
            if exc.args and exc.args[0] == 1298:
                continue
            raise
    conn._dq_session_tz = "server_default"
    with _TZ_LOG_LOCK:
        if not _TZ_LOGGED:
            print(
                "warn: cannot SET time_zone (Africa/Lagos / +01:00); using server default",
                file=sys.stderr,
            )
            _TZ_LOGGED = True
    return "server_default"

# ---------------------------------------------------------------------------
# Status / time constants (aligned with dq_full_ng_report.md)
# ---------------------------------------------------------------------------
VALID_APP_STATUS = (
    1, 3, 5, 7, 9, 11, 13, 15, 20, 23, 24, 25, 27, 29,
)
POST_REVIEW_APP = (13, 15, 20, 23, 24, 25, 27)
DISBURSED_APP = (20, 23, 24, 25, 27)
NOT_DISBURSED_APP = (1, 3, 5, 7, 9, 11, 13)  # excludes 15 disbursing
PAID_OFF_APP = (27,)
WRITTEN_OFF_APP = (25,)
CANCEL_APP = (7, 9)
PRE_DISBURSE_APP = (1, 3, 5, 13, 15)
DISBURSED_NEED_LOAN = (20, 23, 27, 29)
UNSETTLED_APP = (5, 7, 9, 15, 27)  # complement used in G4

VALID_LOAN_STATUS = (1, 20, 23, 24, 25, 27, 29)
ACTIVE_LOAN = (20, 23, 29)
SETTLED_LOAN = (25, 27)
ROLLOVER_PAID_OFF = (25,)

MS_LOWER = 1_000_000_000_000
MS_2018 = 1514764800000  # 2018-01-01 UTC ms

APP_DETAIL_COLS = (
    "application_no, sn, mobile, bid, app_id, user_id, group_user_id, status, "
    "principal, total_amount, loan_amount, disbursed_amount, "
    "created_time, submited_time, reviewed_time, disbursed_time, paid_off_time"
)
APP_DETAIL_COLS_A = (
    "a.application_no, a.sn, a.mobile, a.bid, a.app_id, a.user_id, a.group_user_id, a.status, "
    "a.principal, a.total_amount, a.loan_amount, a.disbursed_amount, "
    "a.created_time, a.submited_time, a.reviewed_time, a.disbursed_time, a.paid_off_time"
)
LOAN_DETAIL_COLS = (
    "loan_no, application_no, period, roll_sequence, status, "
    "principal, total_amount, paid_amount, paid_time, paid_off_date, created_time"
)
LOAN_DETAIL_COLS_L = (
    "l.loan_no, l.application_no, l.period, l.roll_sequence, l.status, "
    "l.principal, l.total_amount, l.paid_amount, l.paid_time, l.paid_off_date, l.created_time"
)


def _in(values: Sequence[int]) -> str:
    return ",".join(str(int(v)) for v in values)


def _not_in(values: Sequence[int]) -> str:
    return ",".join(str(int(v)) for v in values)


def _coalesce(field: str) -> str:
    return "COALESCE({0}, 0)".format(field)


def _ms_ok(field: str) -> str:
    """B1/B2/B7: 13 位毫秒且在 [1e12, now]。"""
    return (
        "({f} > 0 AND ({f} < {lo} OR CHAR_LENGTH(CAST({f} AS CHAR)) <> 13 "
        "OR {f} > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)))"
    ).format(f=field, lo=MS_LOWER)


@dataclass(frozen=True)
class Rule:
    rule_id: str
    level: str
    section: str
    description: str
    sql: str
    detail_sql: Optional[str] = None


def build_rules(bid_clause: str) -> List[Rule]:
    """bid_clause: '' or \"AND bid = 'ng01'\" (leading space included)."""
    bc = bid_clause

    rules: List[Rule] = []

    # ----- application row rules -----
    rules += [
        Rule("A1", "ERROR", "application", "status空",
             "SELECT COUNT(*) FROM application WHERE status IS NULL{0}".format(bc)),
        Rule("A2", "WARN", "application", "status未映射",
             "SELECT COUNT(*) FROM application WHERE status NOT IN ({1}){0}".format(
                 bc, _in(VALID_APP_STATUS))),
        Rule("A3", "WARN", "application", "结清缺paid_off_time",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND {2} = 0{0}".format(
                 bc, _in(PAID_OFF_APP), _coalesce("paid_off_time"))),
        Rule("A4", "ERROR", "application", "放款态缺disbursed_time",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND {2} = 0{0}".format(
                 bc, _in(DISBURSED_APP), _coalesce("disbursed_time"))),
        Rule("A5", "ERROR", "application", "复核后缺reviewed_time",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND {2} = 0{0}".format(
                 bc, _in(POST_REVIEW_APP), _coalesce("reviewed_time"))),
        Rule("A6", "ERROR", "application", "复核前有reviewed_time",
             "SELECT COUNT(*) FROM application WHERE status NOT IN ({1}) AND {2} > 0{0}".format(
                 bc, _in(POST_REVIEW_APP), _coalesce("reviewed_time"))),
        Rule("A7", "ERROR", "application", "未结清态有paid_off_time",
             "SELECT COUNT(*) FROM application WHERE status NOT IN ({1}) AND {2} > 0{0}".format(
                 bc, _in(PAID_OFF_APP), _coalesce("paid_off_time"))),
        Rule("A8", "ERROR", "application", "未放款态有disbursed_time",
             "SELECT COUNT(*) FROM application WHERE status NOT IN ({1},15) AND {2} > 0{0}".format(
                 bc, _in(DISBURSED_APP), _coalesce("disbursed_time"))),
        Rule("A9", "ERROR", "application", "坏账有paid_off_time",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND {2} > 0{0}".format(
                 bc, _in(WRITTEN_OFF_APP), _coalesce("paid_off_time"))),
        Rule("A10", "WARN", "application", "取消态有paid_off_time",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND {2} > 0{0}".format(
                 bc, _in(CANCEL_APP), _coalesce("paid_off_time"))),
        Rule("A11", "ERROR", "application", "未放款却有结清时间",
             "SELECT COUNT(*) FROM application WHERE status NOT IN ({1},15) AND {2} > 0{0}".format(
                 bc, _in(PAID_OFF_APP + DISBURSED_APP), _coalesce("paid_off_time"))),
        Rule("A12", "ERROR", "application", "未复核却有放款时间",
             "SELECT COUNT(*) FROM application WHERE {1} = 0 AND {2} > 0{0}".format(
                 bc, _coalesce("reviewed_time"), _coalesce("disbursed_time"))),
        Rule("A13", "ERROR", "application", "未提交却有复核时间",
             "SELECT COUNT(*) FROM application WHERE {1} = 0 AND {2} > 0{0}".format(
                 bc, _coalesce("submited_time"), _coalesce("reviewed_time"))),
        Rule("A14", "ERROR", "application", "created=0但后续时间>0",
             "SELECT COUNT(*) FROM application WHERE {1} = 0 AND ("
             "{2} > 0 OR {3} > 0 OR {4} > 0 OR {5} > 0 OR {6} > 0){0}".format(
                 bc,
                 _coalesce("created_time"),
                 _coalesce("submited_time"),
                 _coalesce("reviewed_time"),
                 _coalesce("disbursed_time"),
                 _coalesce("paid_off_time"),
                 _coalesce("last_paid_time"),
             )),
        Rule("B1", "ERROR", "application", "时间<13位ms下界",
             "SELECT COUNT(*) FROM application WHERE ({1}){0}".format(
                 bc, " OR ".join("{0} > 0 AND {0} < {1}".format(f, MS_LOWER)
                                 for f in ("created_time", "submited_time", "reviewed_time",
                                           "disbursed_time", "paid_off_time", "last_paid_time")))),
        Rule("B2", "ERROR", "application", "时间超上界",
             "SELECT COUNT(*) FROM application WHERE ({1}){0}".format(
                 bc, " OR ".join(
                     "{0} > CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED)".format(f)
                     for f in ("created_time", "submited_time", "reviewed_time",
                               "disbursed_time", "paid_off_time", "last_paid_time")
                 ))),
        Rule("B3", "ERROR", "application", "submit<created",
             "SELECT COUNT(*) FROM application WHERE {1} > 0 AND created_time > 0 "
             "AND submited_time < created_time{0}".format(bc, _coalesce("submited_time"))),
        Rule("B4", "ERROR", "application", "reviewed<submit",
             "SELECT COUNT(*) FROM application WHERE {1} > 0 AND {2} > 0 "
             "AND reviewed_time < submited_time{0}".format(
                 bc, _coalesce("reviewed_time"), _coalesce("submited_time"))),
        Rule("B5", "ERROR", "application", "disbursed<reviewed",
             "SELECT COUNT(*) FROM application WHERE {1} > 0 AND {2} > 0 "
             "AND disbursed_time < reviewed_time{0}".format(
                 bc, _coalesce("disbursed_time"), _coalesce("reviewed_time"))),
        Rule("B6", "ERROR", "application", "paid_off<disbursed",
             "SELECT COUNT(*) FROM application WHERE {1} > 0 AND {2} > 0 "
             "AND paid_off_time < disbursed_time{0}".format(
                 bc, _coalesce("paid_off_time"), _coalesce("disbursed_time"))),
        Rule("B7", "ERROR", "application", "created晚于now",
             "SELECT COUNT(*) FROM application WHERE created_time > "
             "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS UNSIGNED){0}".format(bc)),
        Rule("B8", "WARN", "application", "created早于2018",
             "SELECT COUNT(*) FROM application WHERE created_time > 0 "
             "AND created_time < {1}{0}".format(bc, MS_2018)),
        Rule("C1", "ERROR", "application", "金额<0",
             "SELECT COUNT(*) FROM application WHERE loan_amount < 0 OR principal < 0 "
             "OR total_amount < 0 OR disbursed_amount < 0{0}".format(bc)),
        Rule("C2", "WARN", "application", "loan_amount应>0",
             "SELECT COUNT(*) FROM application WHERE COALESCE(loan_amount, 0) <= 0{0}".format(bc)),
        Rule("C3", "ERROR", "application", "过审/放款态principal或total≤0",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND "
             "(COALESCE(principal, 0) <= 0 OR COALESCE(total_amount, 0) <= 0){0}".format(
                 bc, _in(POST_REVIEW_APP))),
        Rule("C4", "ERROR", "application", "total<principal",
             "SELECT COUNT(*) FROM application WHERE COALESCE(total_amount, 0) < "
             "COALESCE(principal, 0){0}".format(bc)),
        Rule("C5", "ERROR", "application", "放款态disbursed_amount≤0",
             "SELECT COUNT(*) FROM application WHERE status IN ({1}) AND "
             "COALESCE(disbursed_amount, 0) <= 0{0}".format(bc, _in(DISBURSED_APP))),
        Rule("C6", "ERROR", "application", "disbursed>principal",
             "SELECT COUNT(*) FROM application WHERE COALESCE(disbursed_amount, 0) > "
             "COALESCE(principal, 0){0}".format(bc)),
        Rule("C7", "ERROR", "application", "未放款却有disbursed_amount",
             "SELECT COUNT(*) FROM application WHERE {1} = 0 AND "
             "COALESCE(disbursed_amount, 0) > 0{0}".format(bc, _coalesce("disbursed_time"))),
        Rule("E1", "ERROR", "application", "mobile空",
             "SELECT COUNT(*) FROM application WHERE mobile IS NULL OR TRIM(mobile) = ''{0}".format(bc)),
        Rule("E2", "ERROR", "application", "id_number空",
             "SELECT COUNT(*) FROM application WHERE id_number IS NULL OR TRIM(id_number) = ''{0}".format(bc)),
        Rule("E3", "ERROR", "application", "app_id<0",
             "SELECT COUNT(*) FROM application WHERE app_id < 0{0}".format(bc)),
        Rule("E4", "ERROR", "application", "product_id空",
             "SELECT COUNT(*) FROM application WHERE product_id IS NULL OR TRIM(product_id) = ''{0}".format(bc)),
        Rule("E5", "ERROR", "application", "bank_account空",
             "SELECT COUNT(*) FROM application WHERE bank_account_number IS NULL "
             "OR TRIM(bank_account_number) = ''{0}".format(bc)),
        Rule("E6", "ERROR", "application", "app_id<>0却无user_id",
             "SELECT COUNT(*) FROM application WHERE app_id <> 0 AND "
             "COALESCE(user_id, 0) = 0{0}".format(bc)),
    ]

    app_join = ""
    if bc.strip():
        app_join = " INNER JOIN application a ON a.application_no = l.application_no{0}".format(bc.replace("bid", "a.bid"))

    # ----- loan row rules -----
    rules += [
        Rule("F1", "ERROR", "loan", "period/roll非法",
             "SELECT COUNT(*) FROM loan WHERE period < 1 OR roll_sequence < 0"),
        Rule("F2", "ERROR", "loan", "loan.status非法枚举",
             "SELECT COUNT(*) FROM loan WHERE status NOT IN ({0})".format(_in(VALID_LOAN_STATUS))),
        Rule("F3", "ERROR", "loan", "loan金额<0",
             "SELECT COUNT(*) FROM loan WHERE principal < 0 OR interest < 0 OR admin_fee < 0 "
             "OR total_amount < 0 OR paid_amount < 0"),
        Rule("F4", "WARN", "loan", "total≠分项加总",
             "SELECT COUNT(*) FROM loan l{0} WHERE l.total_amount <> ("
             "COALESCE(l.principal,0)+COALESCE(l.interest,0)+COALESCE(l.admin_fee,0)"
             "+COALESCE(l.roll_fee,0)+COALESCE(l.penalty_amount,0)"
             "-COALESCE(l.reduction_amount,0))".format(app_join)),
        Rule("F5", "ERROR", "loan", "结清缺结清日/时间",
             "SELECT COUNT(*) FROM loan WHERE status IN ({0}) AND ("
             "paid_off_date IS NULL OR {1} = 0)".format(
                 _in(PAID_OFF_APP), _coalesce("paid_time"))),
        Rule("F6", "ERROR", "loan", "展期结清缺结清日/时间",
             "SELECT COUNT(*) FROM loan WHERE status IN ({0}) AND ("
             "paid_off_date IS NULL OR {1} = 0)".format(
                 _in(ROLLOVER_PAID_OFF), _coalesce("paid_time"))),
        Rule("F7", "ERROR", "loan", "在贷却有结清日",
             "SELECT COUNT(*) FROM loan WHERE status IN ({0}) AND paid_off_date IS NOT NULL".format(
                 _in(ACTIVE_LOAN))),
        Rule("F9", "ERROR", "loan", "坏账却有结清日",
             "SELECT COUNT(*) FROM loan WHERE status IN ({0}) AND paid_off_date IS NOT NULL".format(
                 _in(WRITTEN_OFF_APP))),
        Rule("F10", "ERROR", "loan", "有结清日但paid_time=0",
             "SELECT COUNT(*) FROM loan WHERE paid_off_date IS NOT NULL AND {0} = 0".format(
                 _coalesce("paid_time"))),
        Rule("F17", "ERROR", "loan", "paid_time非法ms",
             "SELECT COUNT(*) FROM loan l{0} WHERE l.paid_time IS NOT NULL AND ({1})".format(
                 app_join, _ms_ok("l.paid_time"))),
        Rule("F18", "ERROR", "loan", "loan.created非法ms",
             "SELECT COUNT(*) FROM loan l{0} WHERE ({1})".format(
                 app_join, _ms_ok("l.created_time"))),
        Rule("F12", "ERROR", "loan", "日期次序倒挂",
             "SELECT COUNT(*) FROM loan WHERE start_date > due_date "
             "OR due_date > due_date_final OR start_date > due_date_final"),
    ]

    # ----- cross table D.1 / F11 -----
    a_bc = bc.replace("bid", "a.bid") if bc else ""
    rules += [
        Rule("D.1-cancel", "WARN", "cross", "取消(app.status 7/9)却存在 loan",
             "SELECT COUNT(DISTINCT a.application_no) FROM application a "
             "INNER JOIN loan l ON l.application_no = a.application_no "
             "WHERE a.status IN ({0}){1}".format(_in(CANCEL_APP), a_bc)),
        Rule("D.1-approved", "ERROR", "cross", "APPROVED(11)却存在 loan",
             "SELECT COUNT(DISTINCT a.application_no) FROM application a "
             "INNER JOIN loan l ON l.application_no = a.application_no "
             "WHERE a.status = 11{0}".format(a_bc)),
        Rule("D.1-prend", "ERROR", "cross", "复核前/未放款态(1,3,5,15,13)却存在 loan",
             "SELECT COUNT(DISTINCT a.application_no) FROM application a "
             "INNER JOIN loan l ON l.application_no = a.application_no "
             "WHERE a.status IN ({0}){1}".format(_in(PRE_DISBURSE_APP), a_bc)),
        Rule("D.1-paidoff-mix", "ERROR", "cross", "PAID_OFF(27)含非结清 loan(status NOT IN 25,27)",
             "SELECT COUNT(DISTINCT a.application_no) FROM application a "
             "INNER JOIN loan l ON l.application_no = a.application_no "
             "WHERE a.status IN (27) AND l.status NOT IN ({0}){1}".format(
                 _in(SETTLED_LOAN), a_bc)),
        Rule("F11", "ERROR", "cross", "loan.paid_time < app.disbursed_time",
             "SELECT COUNT(*) FROM loan l INNER JOIN application a "
             "ON a.application_no = l.application_no "
             "WHERE {0} > 0 AND {1} > 0 AND l.paid_time < a.disbursed_time{2}".format(
                 _coalesce("l.paid_time"), _coalesce("a.disbursed_time"), a_bc)),
    ]

    # ----- loan sequence F14/F15/F16 -----
    rules += [
        Rule("F14", "ERROR", "sequence", "period 序列缺号(1..max 不连续)",
             "SELECT COUNT(*) FROM ("
             "SELECT application_no, MAX(period) AS mx, COUNT(DISTINCT period) AS dc "
             "FROM loan GROUP BY application_no HAVING mx <> dc) t"),
        Rule("F15", "ERROR", "sequence", "period 内 roll_sequence 缺号(非0起或不连续)",
             "SELECT COUNT(*) FROM ("
             "SELECT application_no, period, MAX(roll_sequence) AS mx, "
             "MIN(roll_sequence) AS mn, COUNT(DISTINCT roll_sequence) AS dc "
             "FROM loan GROUP BY application_no, period "
             "HAVING mx <> dc OR mn <> 0) t"),
        Rule("F16", "ERROR", "sequence", "展期(roll>0)前序 loan 非 ROLLOVER_PAID_OFF(25)",
             "SELECT COUNT(*) FROM loan l "
             "INNER JOIN loan prev ON prev.application_no = l.application_no "
             "AND prev.period = l.period AND prev.roll_sequence = l.roll_sequence - 1 "
             "WHERE l.roll_sequence > 0 AND prev.status <> 25"),
    ]

    # ----- uniqueness U1/U2/U3 -----
    rules += [
        Rule("U1", "ERROR", "unique", "user.user_id 重复",
             "SELECT COALESCE(SUM(c - 1), 0) FROM ("
             "SELECT user_id, COUNT(*) AS c FROM user GROUP BY user_id HAVING COUNT(*) > 1) t"),
        Rule("U2", "ERROR", "unique", "application.application_no 重复",
             "SELECT COALESCE(SUM(c - 1), 0) FROM ("
             "SELECT application_no, COUNT(*) AS c FROM application GROUP BY application_no "
             "HAVING COUNT(*) > 1) t"),
        Rule("U3", "ERROR", "unique", "loan.loan_no 重复",
             "SELECT COALESCE(SUM(c - 1), 0) FROM ("
             "SELECT loan_no, COUNT(*) AS c FROM loan GROUP BY loan_no HAVING COUNT(*) > 1) t"),
    ]

    # ----- reference G1-G5 -----
    rules += [
        Rule("G1", "ERROR", "reference", "user.group_user_id 不在 user_id 集合(悬空 gid 数)",
             "SELECT COUNT(*) FROM (SELECT DISTINCT group_user_id AS gid FROM user) g "
             "WHERE NOT EXISTS (SELECT 1 FROM user u WHERE u.user_id = g.gid)"),
        Rule("G2", "ERROR", "reference", "application.user_id 不在 user 表(application 数)",
             "SELECT COUNT(*) FROM application a "
             "WHERE NOT EXISTS (SELECT 1 FROM user u WHERE u.user_id = a.user_id){0}".format(a_bc)),
        Rule("G3", "ERROR", "reference",
             "app_id<>0 的申请, (mobile,app_id,user_id,group_user_id)在 user 无一致行(缺失或不一致)",
             "SELECT COUNT(*) FROM application a WHERE a.app_id <> 0{a_bc} "
             "AND NOT EXISTS (SELECT 1 FROM user u WHERE u.mobile = a.mobile "
             "AND u.app_id = a.app_id AND u.user_id = a.user_id "
             "AND u.group_user_id = a.group_user_id)".format(a_bc=a_bc)),
        Rule("G4", "ERROR", "reference",
             "已注销用户(closed_time>0)名下 application 未结清(status NOT IN 5,7,9,15,27)",
             "SELECT COUNT(*) FROM application a "
             "INNER JOIN user u ON u.mobile = a.mobile AND u.app_id = a.app_id "
             "AND u.user_id = a.user_id AND u.closed_time > 0 "
             "WHERE a.status NOT IN ({0}){1}".format(_not_in(UNSETTLED_APP), a_bc)),
        Rule("G5", "ERROR", "reference",
             "已注销用户(closed_time>0)名下 loan 未结清(status IN 20,23,29)",
             "SELECT COUNT(*) FROM loan l "
             "INNER JOIN application a ON a.application_no = l.application_no "
             "INNER JOIN user u ON u.mobile = a.mobile AND u.app_id = a.app_id "
             "AND u.user_id = a.user_id AND u.closed_time > 0 "
             "WHERE l.status IN ({0}){1}".format(_in(ACTIVE_LOAN), a_bc)),
    ]

    # ----- missing loan D.1-miss-disb -----
    rules += [
        Rule("D.1-miss-disb", "ERROR", "missing_loan",
             "已放款态(20,23,27,29)却无任何 loan",
             "SELECT COUNT(*) FROM application a "
             "LEFT JOIN loan l ON l.application_no = a.application_no "
             "WHERE a.status IN ({0}) AND l.application_no IS NULL{1}".format(
                 _in(DISBURSED_NEED_LOAN), a_bc)),
    ]

    return rules


def build_detail_sql(rule: Rule, bc: str, a_bc: str) -> Optional[str]:
    """由计数 SQL 推导明细 SELECT（无 LIMIT）。"""
    if rule.detail_sql:
        return rule.detail_sql.format(bc=bc, a_bc=a_bc)

    rid = rule.rule_id.upper()
    sql = rule.sql.strip().rstrip(";")

    overrides: Dict[str, str] = {
        "G2": (
            "SELECT a.application_no, a.sn, a.mobile, a.bid, a.app_id, a.user_id, "
            "a.group_user_id, a.status FROM application a "
            "WHERE NOT EXISTS (SELECT 1 FROM user u WHERE u.user_id = a.user_id){a_bc} "
            "ORDER BY a.user_id, a.application_no"
        ),
        "G3": (
            "SELECT a.mobile, a.app_id, a.user_id, a.group_user_id, a.application_no, a.sn, a.status "
            "FROM application a WHERE a.app_id <> 0{a_bc} AND NOT EXISTS ("
            "SELECT 1 FROM user u WHERE u.mobile = a.mobile AND u.app_id = a.app_id "
            "AND u.user_id = a.user_id AND u.group_user_id = a.group_user_id) "
            "ORDER BY a.mobile, a.app_id, a.user_id"
        ),
        "G1": (
            "SELECT g.gid AS group_user_id FROM ("
            "SELECT DISTINCT group_user_id AS gid FROM user) g "
            "WHERE NOT EXISTS (SELECT 1 FROM user u WHERE u.user_id = g.gid) "
            "ORDER BY g.gid"
        ),
        "G4": (
            "SELECT a.application_no, a.mobile, a.app_id, a.user_id, a.status, u.closed_time "
            "FROM application a INNER JOIN user u ON u.mobile = a.mobile AND u.app_id = a.app_id "
            "AND u.user_id = a.user_id AND u.closed_time > 0 "
            "WHERE a.status NOT IN ({unsettled}){a_bc} "
            "ORDER BY u.closed_time DESC, a.application_no"
        ).format(unsettled=_not_in(UNSETTLED_APP), a_bc=a_bc),
        "G5": (
            "SELECT l.loan_no, l.application_no, l.status AS loan_status, a.mobile, a.app_id, "
            "a.user_id, u.closed_time FROM loan l "
            "INNER JOIN application a ON a.application_no = l.application_no "
            "INNER JOIN user u ON u.mobile = a.mobile AND u.app_id = a.app_id "
            "AND u.user_id = a.user_id AND u.closed_time > 0 "
            "WHERE l.status IN ({active}){a_bc} ORDER BY l.application_no, l.period, l.roll_sequence"
        ).format(active=_in(ACTIVE_LOAN), a_bc=a_bc),
        "U1": (
            "SELECT user_id, COUNT(*) AS row_cnt, MIN(mobile) AS sample_mobile, "
            "MIN(app_id) AS sample_app_id FROM user GROUP BY user_id HAVING COUNT(*) > 1 "
            "ORDER BY row_cnt DESC, user_id"
        ),
        "U2": (
            "SELECT application_no, COUNT(*) AS row_cnt FROM application "
            "GROUP BY application_no HAVING COUNT(*) > 1 ORDER BY row_cnt DESC, application_no"
        ),
        "U3": (
            "SELECT loan_no, COUNT(*) AS row_cnt FROM loan "
            "GROUP BY loan_no HAVING COUNT(*) > 1 ORDER BY row_cnt DESC, loan_no"
        ),
        "D.1-CANCEL": (
            "SELECT DISTINCT a.application_no, a.status AS app_status, a.mobile, a.app_id "
            "FROM application a INNER JOIN loan l ON l.application_no = a.application_no "
            "WHERE a.status IN ({cancel}){a_bc} ORDER BY a.application_no"
        ).format(cancel=_in(CANCEL_APP), a_bc=a_bc),
        "D.1-APPROVED": (
            "SELECT DISTINCT a.application_no, a.status AS app_status, a.mobile, l.loan_no, l.status AS loan_status "
            "FROM application a INNER JOIN loan l ON l.application_no = a.application_no "
            "WHERE a.status = 11{a_bc} ORDER BY a.application_no"
        ).format(a_bc=a_bc),
        "D.1-PREND": (
            "SELECT DISTINCT a.application_no, a.status AS app_status, a.mobile, l.loan_no, l.status AS loan_status "
            "FROM application a INNER JOIN loan l ON l.application_no = a.application_no "
            "WHERE a.status IN ({pre}){a_bc} ORDER BY a.application_no"
        ).format(pre=_in(PRE_DISBURSE_APP), a_bc=a_bc),
        "D.1-PAIDOFF-MIX": (
            "SELECT a.application_no, a.status AS app_status, l.loan_no, l.period, l.roll_sequence, l.status AS loan_status "
            "FROM application a INNER JOIN loan l ON l.application_no = a.application_no "
            "WHERE a.status IN (27) AND l.status NOT IN ({settled}){a_bc} "
            "ORDER BY a.application_no, l.period, l.roll_sequence"
        ).format(settled=_in(SETTLED_LOAN), a_bc=a_bc),
        "D.1-MISS-DISB": (
            "SELECT a.application_no, a.sn, a.mobile, a.app_id, a.status, a.disbursed_time, a.disbursed_amount "
            "FROM application a LEFT JOIN loan l ON l.application_no = a.application_no "
            "WHERE a.status IN ({disb}) AND l.application_no IS NULL{a_bc} ORDER BY a.application_no"
        ).format(disb=_in(DISBURSED_NEED_LOAN), a_bc=a_bc),
        "F11": (
            "SELECT l.loan_no, l.application_no, l.paid_time, a.disbursed_time, "
            "(a.disbursed_time - l.paid_time) AS paid_before_disburse_ms "
            "FROM loan l INNER JOIN application a ON a.application_no = l.application_no "
            "WHERE COALESCE(l.paid_time, 0) > 0 AND COALESCE(a.disbursed_time, 0) > 0 "
            "AND l.paid_time < a.disbursed_time{a_bc} ORDER BY paid_before_disburse_ms DESC"
        ).format(a_bc=a_bc),
        "F14": (
            "SELECT application_no, MAX(period) AS max_period, COUNT(DISTINCT period) AS period_cnt "
            "FROM loan GROUP BY application_no HAVING max_period <> period_cnt ORDER BY application_no"
        ),
        "F15": (
            "SELECT application_no, period, MAX(roll_sequence) AS max_roll, MIN(roll_sequence) AS min_roll, "
            "COUNT(DISTINCT roll_sequence) AS roll_cnt FROM loan GROUP BY application_no, period "
            "HAVING max_roll <> roll_cnt OR min_roll <> 0 ORDER BY application_no, period"
        ),
        "F16": (
            "SELECT l.application_no, l.period, l.roll_sequence, l.loan_no, l.status AS loan_status, "
            "prev.loan_no AS prev_loan_no, prev.status AS prev_status "
            "FROM loan l INNER JOIN loan prev ON prev.application_no = l.application_no "
            "AND prev.period = l.period AND prev.roll_sequence = l.roll_sequence - 1 "
            "WHERE l.roll_sequence > 0 AND prev.status <> 25 "
            "ORDER BY l.application_no, l.period, l.roll_sequence"
        ),
    }
    if rid in overrides:
        return overrides[rid]

    sub = re.match(r"SELECT COUNT\(\*\)\s+FROM\s+\((.+)\)\s+t\s*$", sql, re.IGNORECASE | re.DOTALL)
    if sub:
        return "SELECT * FROM ({0}) t ORDER BY 1".format(sub.group(1))

    if re.search(r"SELECT COUNT\(\*\)\s+FROM\s+application\s+a\b", sql, re.IGNORECASE):
        return re.sub(
            r"SELECT COUNT\(\*\)",
            "SELECT " + APP_DETAIL_COLS_A,
            sql,
            count=1,
            flags=re.IGNORECASE,
        ) + " ORDER BY a.application_no"

    if re.search(r"SELECT COUNT\(\*\)\s+FROM\s+application\b", sql, re.IGNORECASE):
        return re.sub(
            r"SELECT COUNT\(\*\)",
            "SELECT " + APP_DETAIL_COLS,
            sql,
            count=1,
            flags=re.IGNORECASE,
        ) + " ORDER BY application_no"

    if re.search(r"SELECT COUNT\(\*\)\s+FROM\s+loan\s+l\b", sql, re.IGNORECASE):
        detail = re.sub(
            r"SELECT COUNT\(\*\)",
            "SELECT " + LOAN_DETAIL_COLS_L,
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        if " ORDER BY " not in detail.upper():
            detail += " ORDER BY l.application_no, l.period, l.roll_sequence"
        return detail

    if re.search(r"SELECT COUNT\(\*\)\s+FROM\s+loan\b", sql, re.IGNORECASE):
        detail = re.sub(
            r"SELECT COUNT\(\*\)",
            "SELECT " + LOAN_DETAIL_COLS,
            sql,
            count=1,
            flags=re.IGNORECASE,
        )
        if " ORDER BY " not in detail.upper():
            detail += " ORDER BY application_no, period, roll_sequence"
        return detail

    distinct = re.match(
        r"SELECT COUNT\(DISTINCT\s+(.+?)\)\s+FROM\s+(.+)$",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if distinct:
        cols, rest = distinct.group(1), distinct.group(2)
        return "SELECT DISTINCT {0} FROM {1} ORDER BY 1".format(cols, rest)

    return None


def _cell_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (datetime, date)):
        return val.isoformat(sep=" ")
    if isinstance(val, Decimal):
        return format(val, "f").rstrip("0").rstrip(".") if "." in format(val, "f") else str(val)
    s = str(val)
    return s.replace("\t", " ").replace("\r", " ").replace("\n", " ")


def rows_to_markdown_table(rows: Sequence[Dict[str, Any]], max_cols: int = 16) -> str:
    if not rows:
        return "_无明细_"
    cols = list(rows[0].keys())[:max_cols]
    lines = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join("---" for _ in cols) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_cell_str(row.get(c)).replace("|", "\\|") for c in cols) + " |")
    if len(rows[0].keys()) > max_cols:
        lines.append("")
        lines.append("_（仅展示前 {0} 列）_".format(max_cols))
    return "\n".join(lines)


def write_tsv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({c: _cell_str(row.get(c)) for c in cols})


def fetch_detail_rows(
    conn,
    detail_sql: str,
    limit: int,
) -> Tuple[List[Dict[str, Any]], bool]:
    """返回 (rows, truncated)。"""
    q = detail_sql.rstrip().rstrip(";")
    truncated = False
    if limit > 0:
        q = "{0} LIMIT {1}".format(q, int(limit))
    with conn.cursor() as cur:
        cur.execute(q)
        rows = list(cur.fetchall())
    if limit > 0 and len(rows) >= limit:
        truncated = True
    return rows, truncated


def export_one_rule_details(
    conn,
    rule: Rule,
    rec: Dict[str, Any],
    bc: str,
    a_bc: str,
    detail_dir: Optional[Path],
    export_limit: int,
    preview_limit: int,
) -> None:
    if rec["hits"] <= 0:
        rec["detail_rows"] = []
        return

    detail_sql = build_detail_sql(rule, bc, a_bc)
    if not detail_sql:
        rec["detail_rows"] = []
        rec["detail_note"] = "无明细 SQL"
        return

    rec["detail_sql"] = detail_sql

    if detail_dir is not None:
        cap = export_limit if export_limit > 0 else 0
        rows, truncated = fetch_detail_rows(conn, detail_sql, cap)
        rec["detail_exported"] = len(rows)
        rec["detail_truncated"] = truncated
        safe_name = rec["rule_id"].replace("/", "_").replace(".", "_")
        tsv_path = detail_dir / "{0}.tsv".format(safe_name)
        write_tsv(tsv_path, rows)
        rec["detail_file"] = str(tsv_path)
        print("detail {0}: {1} rows -> {2}{3}".format(
            rec["rule_id"],
            len(rows),
            tsv_path,
            " (truncated)" if truncated else "",
        ), file=sys.stderr)
        rec["detail_rows"] = rows[:preview_limit] if preview_limit > 0 else []
    elif preview_limit > 0:
        rows, truncated = fetch_detail_rows(conn, detail_sql, preview_limit)
        rec["detail_rows"] = rows
        rec["detail_exported"] = len(rows)
        rec["detail_truncated"] = truncated or rec["hits"] > len(rows)
    else:
        rec["detail_rows"] = []


def run_one_rule(
    cfg: Dict[str, Any],
    rule: Rule,
    bc: str,
    a_bc: str,
    detail_dir: Optional[Path],
    export_limit: int,
    preview_limit: int,
    read_timeout: int,
    retries: int,
) -> Dict[str, Any]:
    """单规则：独立连库 → COUNT →（命中则）导出明细；失败可重试。"""

    def work(conn) -> Dict[str, Any]:
        t_rule = time.time()
        hits = run_count(conn, rule.sql)
        rec: Dict[str, Any] = {
            "rule_id": rule.rule_id,
            "level": rule.level,
            "section": rule.section,
            "description": rule.description,
            "hits": hits,
        }
        if hits > 0:
            export_one_rule_details(
                conn, rule, rec, bc, a_bc, detail_dir, export_limit, preview_limit,
            )
        else:
            rec["detail_rows"] = []
        rec["elapsed_sec"] = round(time.time() - t_rule, 2)
        return rec

    return call_with_conn_retry(cfg, rule.rule_id, read_timeout, retries, work)


def run_count(conn, sql: str) -> int:
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            return 0
        val = row.get("COUNT(*)") if isinstance(row, dict) else row[0]
        if val is None and isinstance(row, dict):
            val = next(iter(row.values()))
        return int(val or 0)


def fetch_table_stats(conn) -> Dict[str, int]:
    stats: Dict[str, int] = {}
    for table in ("application", "loan", "user"):
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM `{0}`".format(table))
            stats[table] = int(cur.fetchone()["c"])
    return stats


def filter_rules(all_rules: Sequence[Rule], rule_ids: Optional[Sequence[str]]) -> List[Rule]:
    if not rule_ids:
        return list(all_rules)
    wanted = {r.strip().upper() for r in rule_ids}
    out = [r for r in all_rules if r.rule_id.upper() in wanted]
    missing = wanted - {r.rule_id.upper() for r in out}
    if missing:
        raise SystemExit("unknown rules: {0}".format(", ".join(sorted(missing))))
    return out


def render_markdown(
    results: Sequence[Dict[str, Any]],
    table_stats: Dict[str, int],
    bid: Optional[str],
    elapsed_sec: float,
) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    scope = "bid={0}".format(bid) if bid else "全库 ng"
    lines = [
        "# 全量数据质量报告 — ng",
        "",
        "生成: {0}  工具: validate_target_dq.py  范围: {1}".format(now, scope),
        "",
        "表行数: application={0:,}  loan={1:,}  user={2:,}".format(
            table_stats.get("application", 0),
            table_stats.get("loan", 0),
            table_stats.get("user", 0),
        ),
        "",
    ]

    sections: Dict[str, List[Dict[str, Any]]] = {}
    for r in results:
        sections.setdefault(r["section"], []).append(r)

    section_titles = {
        "application": "## application(行内)",
        "loan": "## loan(行内)",
        "cross": "## 跨表 有loan却不该有(D.1 / F11)",
        "sequence": "## loans 序列完整性(F14/F15/F16)",
        "unique": "## 主键唯一性(U1/U2/U3)",
        "reference": "## 引用一致性 application↔user(G1/G2/G3)",
        "missing_loan": "## 跨表 应有loan却缺失(D.1 缺失方向)",
    }

    for section, title in section_titles.items():
        rows = sections.get(section, [])
        if not rows:
            continue
        lines.append(title)
        lines.append("")
        lines.append("| 规则 | 级别 | 命中 | 语义 |")
        lines.append("|---|---|---|---|")
        for r in rows:
            flag = " ⚠️" if r["hits"] > 0 else ""
            lines.append("| {rule} | {lvl} | {hits}{flag} | {desc} |".format(
                rule=r["rule_id"], lvl=r["level"], hits=r["hits"], flag=flag, desc=r["description"],
            ))
        lines.append("")

    err_hits = sum(r["hits"] for r in results if r["level"] == "ERROR" and r["hits"] > 0)
    warn_hits = sum(r["hits"] for r in results if r["level"] == "WARN" and r["hits"] > 0)
    lines.append("---")
    lines.append("")
    lines.append("耗时: {0:.1f}s  ERROR命中规则数: {1}  WARN命中规则数: {2}".format(
        elapsed_sec, err_hits, warn_hits,
    ))
    lines.append("")

    hit_results = [r for r in results if r.get("hits", 0) > 0]
    if hit_results:
        lines.append("## 命中规则明细")
        lines.append("")
        for r in hit_results:
            lines.append("### {0} — {1}（命中 {2}）".format(
                r["rule_id"], r["description"], r["hits"],
            ))
            lines.append("")
            detail_file = r.get("detail_file")
            if detail_file:
                lines.append("明细文件: `{0}`（导出 {1} 行{2}）".format(
                    detail_file,
                    r.get("detail_exported", 0),
                    "，已截断" if r.get("detail_truncated") else "",
                ))
                lines.append("")
            elif r.get("detail_note"):
                lines.append("_{0}_".format(r["detail_note"]))
                lines.append("")

            preview = r.get("detail_rows") or []
            if preview:
                shown = len(preview)
                lines.append("预览（前 {0} 行）:".format(shown))
                lines.append("")
                lines.append(rows_to_markdown_table(preview))
                if r["hits"] > shown:
                    lines.append("")
                    lines.append("_… 共 {0} 行，完整见 TSV_".format(r["hits"]))
            else:
                lines.append("_无预览数据_")
            lines.append("")

    return "\n".join(lines)


def main() -> int:
    p = argparse.ArgumentParser(description="Validate target ng DB data quality rules")
    p.add_argument("--env", default=str(REPO / ".env"), help=".env with TARGET_MYSQL_*")
    p.add_argument("--bid", help="only validate one bid, e.g. ng01")
    p.add_argument("--rules", help="comma-separated rule ids, e.g. C7,G2,G3")
    p.add_argument("--output", "-o", help="write markdown report to file")
    p.add_argument("--json", action="store_true", help="print JSON instead of markdown")
    p.add_argument(
        "--detail-dir",
        help="export hit-rule rows as TSV per rule (default: <output_stem>_details when -o set)",
    )
    p.add_argument(
        "--detail-limit",
        type=int,
        default=0,
        help="max rows exported per rule TSV (0=全部，慎用 G2/G3 等大规则)",
    )
    p.add_argument(
        "--detail-preview",
        type=int,
        default=20,
        help="markdown/JSON 中每条命中规则预览行数 (0=不预览)",
    )
    p.add_argument("--workers", type=int, default=8, help="并发跑规则的线程数（每线程独立连库）")
    p.add_argument("--retries", type=int, default=3, help="单规则失败后的重试次数")
    p.add_argument(
        "--query-timeout",
        type=int,
        default=3600,
        help="MySQL read/write 超时秒数（单条规则查询）",
    )
    args = p.parse_args()

    cfg = env_util.load_env(Path(args.env))
    bid_clause = ""
    a_bc = ""
    if args.bid:
        bid_clause = " AND bid = '{0}'".format(args.bid.replace("'", "''"))
        a_bc = bid_clause.replace("bid", "a.bid")

    detail_dir: Optional[Path] = None
    if args.detail_dir:
        detail_dir = Path(args.detail_dir)
    elif args.output:
        detail_dir = Path(args.output).with_suffix("").parent / (Path(args.output).stem + "_details")

    all_rules = build_rules(bid_clause)
    rules = filter_rules(all_rules, args.rules.split(",") if args.rules else None)

    if detail_dir is not None:
        detail_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    table_stats = call_with_conn_retry(
        cfg,
        "table_stats",
        args.query_timeout,
        args.retries,
        fetch_table_stats,
    )

    workers = max(1, args.workers)
    print(
        "running {0} rules with {1} workers (timeout={2}s retries={3})".format(
            len(rules), workers, args.query_timeout, args.retries,
        ),
        file=sys.stderr,
    )

    results_by_id: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dq-rule") as ex:
        futs = {
            ex.submit(
                run_one_rule,
                cfg,
                rule,
                bid_clause,
                a_bc,
                detail_dir,
                args.detail_limit,
                args.detail_preview,
                args.query_timeout,
                args.retries,
            ): rule
            for rule in rules
        }
        for fut in as_completed(futs):
            rule = futs[fut]
            try:
                rec = fut.result()
            except Exception as exc:
                print("[FAIL] {0} err={1}".format(rule.rule_id, exc), file=sys.stderr)
                raise
            results_by_id[rule.rule_id] = rec
            if rec["hits"] > 0:
                print("[{0}] {1} hits={2} ({3:.1f}s)".format(
                    rec["level"], rec["rule_id"], rec["hits"], rec["elapsed_sec"],
                ), file=sys.stderr)
            else:
                print("[ok] {0} ({1:.1f}s)".format(rule.rule_id, rec["elapsed_sec"]), file=sys.stderr)

    results = [results_by_id[r.rule_id] for r in rules]

    elapsed = time.time() - t0
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "bid": args.bid,
        "table_stats": table_stats,
        "elapsed_sec": round(elapsed, 1),
        "workers": workers,
        "detail_dir": str(detail_dir) if detail_dir else None,
        "results": results,
    }

    if args.json:
        # detail_rows 可能很大；JSON 默认只保留 preview
        slim = []
        for r in results:
            item = dict(r)
            if "detail_sql" in item:
                del item["detail_sql"]
            slim.append(item)
        payload["results"] = slim
        out = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    else:
        out = render_markdown(results, table_stats, args.bid, elapsed)

    if args.output:
        Path(args.output).write_text(out, encoding="utf-8")
        print("wrote {0}".format(args.output), file=sys.stderr)
        if detail_dir:
            print("details -> {0}/".format(detail_dir), file=sys.stderr)
    else:
        print(out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
