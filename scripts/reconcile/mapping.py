#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""源表行 → 目标行映射（对齐 Flink SQL 02_sync_*_fast.sql）。"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional


# 与 Flink sink / Target.sql 对齐的写入列（不含 created_at/updated_at）
USER_COLS = [
    "user_id", "app_id", "group_user_id", "info_user_id", "mobile",
    "closed_time", "reg_device_uuid", "reg_time", "test_flag",
    "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
    "campaign_id", "ad_group_id", "advertiser_id",
]

USER_INFO_COLS = [
    "user_id", "id_number", "full_name", "password", "live_image", "id_card", "info",
]

USER_BANKCARD_COLS = ["id", "group_user_id", "bank_code", "bank_account_number", "is_default"]

USER_PRODUCT_COLS = [
    "group_user_id", "product_id", "schemes", "is_open",
    "credit_amount", "unpaid_amount", "locked_amount", "available_amount",
]

APPLICATION_COLS = [
    "application_no", "mobile", "bid", "app_id", "app_version", "user_id",
    "group_user_id", "sn", "is_test", "is_first_apply", "is_auto_apply", "id_number",
    "gaid_idfa", "device_uuid", "session_id", "bank_code", "bank_account_name",
    "bank_account_number", "product_id", "product_scheme_id", "product_calculator_version",
    "repay_calculator_version", "rollover_calculator_version",
    "product_scheme_param", "term", "periods", "repayment_method", "repayment_plan",
    "credit_limit", "loan_amount", "principal", "total_amount", "disbursed_amount",
    "created_time", "submited_time", "reviewed_time", "disbursed_time", "last_paid_time",
    "paid_off_time", "lock_expire_time", "status",
]
# coupon_code：Flink sink 写空串；若目标库有该列则在 load 时动态追加

# Flink sink_loan 当前未写 roll_fee / roll_paid_amount（目标 DDL 有默认值）
LOAN_COLS = [
    "loan_no", "application_no", "period", "roll_sequence", "start_date", "due_date",
    "due_date_final", "principal", "interest", "admin_fee",
    "penalty_amount", "reduction_amount", "total_amount", "paid_amount",
    "paid_time", "paid_off_date", "created_time", "status",
]

# 本迁移切片默认 app（目标库 / 源库 app_code）
DEFAULT_INCLUDE_APP_IDS = (567, 568, 569, 571, 572, 573)
# DK/LD 假数据时间戳，对账时默认排除
DEFAULT_EXCLUDE_LOAN_CREATED_MS = (1785340800000,)

PRODUCT_SCHEME_ID = "PROD-002-D7"
PRODUCT_CALC_VER = "48"
REPAY_CALC_VER = "50"
ROLLOVER_CALC_VER = "49"
PRODUCT_SCHEME_PARAM = "{}"
BID = "ng01"
APP_VERSION = "1"


def map_utm_source(network_name: Any, tracker_name: Any) -> Optional[str]:
    """对齐 sql/02_sync_user_fast.sql 的 CASE。"""
    raw = None
    for cand in (network_name, tracker_name):
        if cand is None:
            continue
        s = str(cand).strip()
        if s:
            raw = s
            break
    if not raw:
        return None
    low = raw.lower()
    if "unattributed" in low:
        return None
    if "organic" in low:
        return "organic"
    if "google" in low:
        return "google"
    if "tiktok" in low:
        return "tiktok"
    if "facebook" in low or "instagram" in low or "messenger" in low:
        return "facebook"
    if "kuai" in low or "kwai" in low or "kuaishou" in low:
        return "kwai"
    return low


def _as_int(val: Any, default: int = 0) -> int:
    if val is None or val == "":
        return default
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _as_opt_int(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _dt_to_ms(val: Any) -> Optional[int]:
    if val is None or val == "":
        return None
    if isinstance(val, datetime):
        return int(val.timestamp()) * 1000
    s = str(val).strip()
    if not s:
        return None
    try:
        # 已是毫秒
        n = int(float(s)) if s.replace(".", "", 1).isdigit() else None
        if n is not None:
            return n if n >= 10**12 else n * 1000
    except (TypeError, ValueError):
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return int(datetime.strptime(s[:19] if len(s) >= 19 else s[:10], fmt).timestamp()) * 1000
        except ValueError:
            continue
    return None


def _json_str(val: Any, default: str = "{}") -> str:
    if val is None or val == "":
        return default
    if isinstance(val, (dict, list)):
        return json.dumps(val, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    s = str(val).strip()
    if not s:
        return default
    try:
        obj = json.loads(s)
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        return s


def expected_user(row: dict, offset: int) -> Optional[dict]:
    token = (row.get("mobile_token") or "").strip()
    if not token:
        return None
    uid = _as_int(row.get("id")) + offset
    return {
        "user_id": uid,
        "app_id": _as_int(row.get("app_code")),
        "group_user_id": uid,
        "info_user_id": uid,
        "mobile": token,
        "closed_time": 0,
        "reg_device_uuid": (row.get("device_id") or "") or "",
        "reg_time": _as_int(row.get("reg_time"), 0),
        "test_flag": 0,
        "utm_source": map_utm_source(row.get("network_name"), row.get("tracker_name")),
        "utm_medium": row.get("campaign_tracker"),
        "utm_campaign": row.get("campaign_name"),
        "utm_content": row.get("creative_name"),
        "utm_term": row.get("adgroup_tracker"),
        "campaign_id": row.get("creative_tracker"),
        "ad_group_id": row.get("campaign_tracker"),
        "advertiser_id": row.get("adgroup_tracker"),
    }


def expected_user_info(row: dict, offset: int) -> Optional[dict]:
    bvn = (row.get("bvn_raw") or "").strip() if row.get("bvn_raw") is not None else ""
    id_token = (row.get("id_number_token") or "").strip()
    if bvn and not id_token:
        return None
    uid = _as_int(row.get("user_id")) + offset
    return {
        "user_id": uid,
        "id_number": "" if not bvn else id_token,
        "full_name": (row.get("full_name") or "") or "",
        "password": "",
        "live_image": "",
        "id_card": "",
        "info": _json_str(row.get("info_json"), "{}"),
    }


def expected_user_bankcard(row: dict, offset: int) -> Optional[dict]:
    token = (row.get("bank_account_token") or "").strip()
    if not token:
        return None
    return {
        "id": None,  # 插入时由目标库/雪花；update 保留目标 id
        "group_user_id": _as_int(row.get("user_id")) + offset,
        "bank_code": (row.get("bank_code") or "") or "",
        "bank_account_number": token,
        "is_default": _as_int(row.get("is_default"), 0),
    }


def expected_user_product(row: dict, offset: int) -> dict:
    credit = _as_int(row.get("credit_amount_minor"), 0)
    unpaid = _as_int(row.get("unpaid_amount_minor"), 0)
    pid = str(row.get("product_id") or "").strip()
    schemes = '[{"schemeId":"PROD-001-D7","amountRange":[%d]}]' % credit
    return {
        "group_user_id": _as_int(row.get("user_id")) + offset,
        "product_id": pid,
        "schemes": schemes,
        "is_open": 1,
        "credit_amount": credit,
        "unpaid_amount": unpaid,
        "locked_amount": 0,
        "available_amount": 0,
    }


def expected_application(row: dict, offset: int) -> Optional[dict]:
    mobile = (row.get("mobile_token") or "").strip()
    bank = (row.get("bank_account_token") or "").strip()
    if not mobile or not bank:
        return None
    bvn = (row.get("bvn_raw") or "").strip() if row.get("bvn_raw") is not None else ""
    id_token = (row.get("id_number_token") or "").strip()
    if bvn and not id_token:
        return None
    gaid_raw = (row.get("gaid_idfa_raw") or "").strip() if row.get("gaid_idfa_raw") is not None else ""
    gaid_token = (row.get("gaid_idfa_token") or "").strip()
    uid = _as_int(row.get("user_id")) + offset
    order_ms = _dt_to_ms(row.get("order_time")) or 0
    return {
        "application_no": row.get("application_no"),
        "mobile": mobile,
        "bid": BID,
        "app_id": _as_int(row.get("app_code")),
        "app_version": APP_VERSION,
        "user_id": uid,
        "group_user_id": uid,
        "sn": row.get("sn"),
        "is_test": 0,
        "is_first_apply": _as_int(row.get("re_loan"), 0),
        "is_auto_apply": 0,
        "id_number": "" if not bvn else id_token,
        "gaid_idfa": None if not gaid_raw else (gaid_token or None),
        "device_uuid": (row.get("device_uuid") or "") or "",
        "session_id": row.get("session_id"),
        "bank_code": (row.get("bank_code") or "") or "",
        "bank_account_name": (row.get("bank_account_name") or "") or "",
        "bank_account_number": bank,
        "product_id": str(row.get("product_id") or "").strip(),
        "product_scheme_id": PRODUCT_SCHEME_ID,
        "product_calculator_version": PRODUCT_CALC_VER,
        "repay_calculator_version": REPAY_CALC_VER,
        "rollover_calculator_version": ROLLOVER_CALC_VER,
        "product_scheme_param": PRODUCT_SCHEME_PARAM,
        "term": _as_int(row.get("period_days"), 7),
        "periods": _as_int(row.get("period_count"), 1),
        "repayment_method": 1,
        "repayment_plan": _json_str(row.get("repayment_plan_json"), "{}"),
        "credit_limit": _as_int(row.get("credit_limit_minor"), 0),
        "loan_amount": _as_int(row.get("loan_amount_minor"), 0),
        "principal": _as_int(row.get("principal_minor"), 0),
        "total_amount": _as_int(row.get("total_amount_minor"), 0),
        "disbursed_amount": _as_int(row.get("disbursed_amount_minor"), 0),
        "created_time": order_ms,
        "submited_time": order_ms,
        "reviewed_time": _dt_to_ms(row.get("reviewed_time")),
        "disbursed_time": _dt_to_ms(row.get("disburse_time")),
        "last_paid_time": _dt_to_ms(row.get("last_paid_time")),
        "paid_off_time": _dt_to_ms(row.get("settled_time")),
        "lock_expire_time": order_ms + 7 * 86400 * 1000,
        "status": _as_int(row.get("risk_status"), 1),
    }


def expected_loan(row: dict) -> dict:
    return {
        "loan_no": row.get("loan_no"),
        "application_no": row.get("application_no"),
        "period": _as_int(row.get("period"), 1),
        "roll_sequence": _as_int(row.get("roll_sequence"), 0),
        "start_date": row.get("start_date"),
        "due_date": row.get("due_date"),
        "due_date_final": row.get("due_date_final"),
        "principal": _as_int(row.get("principal_minor"), 0),
        "interest": _as_int(row.get("interest_minor"), 0),
        "admin_fee": _as_int(row.get("admin_fee_minor"), 0),
        "penalty_amount": _as_int(row.get("penalty_amount_minor"), 0),
        "reduction_amount": _as_int(row.get("reduction_amount_minor"), 0),
        "total_amount": _as_int(row.get("total_amount_minor"), 0),
        "paid_amount": _as_int(row.get("paid_amount_minor"), 0),
        "paid_time": _as_opt_int(row.get("paid_time_ms")),
        "paid_off_date": row.get("paid_off_date"),
        "created_time": max(_as_int(row.get("created_time_ms"), 0), 0),
        "status": _as_int(row.get("risk_status"), 20),
    }
