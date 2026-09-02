#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 G2/G3 校验 TSV 补目标库缺失或不一致的 user 行。

G2: application.user_id 不在 user 表（MISSING）
G3: (mobile, app_id, user_id, group_user_id) 四元组在 user 无一致行（MISSING + INCONSISTENT）

源库：仅用 TSV 里的 **user_id** 查 user（G2/G3 两文件都有该列）
  user_id >= USER_ID_OFFSET → nigeria_backend.user，WHERE id = user_id - offset
  user_id <  offset          → ng_loan_market.user，WHERE id = user_id
                              若 user 无行，回退 user_data.userId + application.deviceId

目标库写入：TSV 的 mobile 已是 VT token（tk_*），**直接使用，不再查 VT**；
app_id / user_id / group_user_id 亦以 TSV 为准。

默认 **仅 INSERT**（PK 已存在则跳过并列入 needs_update）；加 `--upsert` 才 ON DUPLICATE KEY UPDATE。

Usage（101 内网，.env 需 SOURCE_* + LM_MYSQL_* + TARGET_*）:
  python3 scripts/backfill_missing_users_from_g2_g3.py \\
    --env ./.env \\
    --g2-file /path/G2.tsv \\
    --g3-file /path/G3.tsv

  python3 scripts/backfill_missing_users_from_g2_g3.py \\
    --env ./.env \\
    --g3-file /path/G3.tsv \\
    --apply --workers 8 --batch-size 200
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

HERE = Path(__file__).resolve().parent
RECON = HERE / "reconcile"
sys.path.insert(0, str(RECON))

import env_util  # noqa: E402
import mapping as M  # noqa: E402
from reconcile_tables import (  # noqa: E402
    USER_PK,
    _insert_batch,
    clamp_amounts,
    quote_cols,
    resolve_columns,
)

NG01_USER_SQL = """
SELECT u.id,
       u.app_code,
       u.device_id,
       u.adid,
       u.create_time,
       UNIX_TIMESTAMP(u.create_time) * 1000 AS reg_time,
       a.network_name,
       a.tracker_name,
       a.campaign_tracker,
       a.campaign_name,
       a.creative_name,
       a.adgroup_tracker,
       a.creative_tracker
FROM `user` u
LEFT JOIN adjust_latest_by_adid a
       ON u.adid IS NOT NULL AND u.adid <> '' AND a.adid = u.adid
WHERE u.id IN ({ph})
"""

LM_USER_SQL = """
SELECT u.id,
       u.`appId` AS app_id,
       u.mobile,
       u.`deviceId` AS device_id,
       u.`isCancel` AS is_cancel,
       u.created,
       u.updated
FROM `user` u
WHERE u.id IN ({ph})
"""

LM_USER_DATA_SQL = """
SELECT ud.`userId` AS id, ud.created, ud.updated
FROM `user_data` ud
INNER JOIN (
    SELECT `userId`, MAX(id) AS max_id
    FROM `user_data`
    WHERE `userId` IN ({ph})
    GROUP BY `userId`
) pick ON pick.max_id = ud.id
"""

LM_APP_DEVICE_SQL = """
SELECT `userId` AS id, MAX(`deviceId`) AS device_id
FROM `application`
WHERE `userId` IN ({ph})
GROUP BY `userId`
"""


@dataclass(frozen=True)
class UserNeed:
    user_id: int
    app_id: int
    mobile: str
    group_user_id: int
    kind: str

    def key(self) -> Tuple[int, int, str, int]:
        return (self.user_id, self.app_id, self.mobile, self.group_user_id)

    def g3_key(self) -> str:
        return "{0}|{1}|{2}|{3}".format(self.mobile, self.app_id, self.user_id, self.group_user_id)


def load_compare_connectors():
    path = HERE / "compare_orders_source_target.py"
    spec = importlib.util.spec_from_file_location("order_compare", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def read_tsv(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            rows.append({k: (v or "").strip() for k, v in row.items()})
    return rows


def pipeline_for_user_id(user_id: int, offset: int) -> str:
    return "ng01" if user_id >= offset else "lm"


def parse_needs(
    g2_file: Optional[Path],
    g3_file: Optional[Path],
    kinds: Set[str],
) -> List[UserNeed]:
    seen: Set[Tuple[int, int, str, int]] = set()
    out: List[UserNeed] = []

    def add_from_rows(rows: Iterable[dict], default_kind: str) -> None:
        for row in rows:
            kind = (row.get("kind") or default_kind).upper()
            if kind not in kinds:
                continue
            try:
                user_id = int(row["user_id"])
                app_id = int(row["app_id"])
                group_user_id = int(row["group_user_id"])
            except (KeyError, TypeError, ValueError):
                continue
            mobile = str(row.get("mobile") or "").strip()
            if not mobile or user_id <= 0 or app_id <= 0:
                continue
            key = (user_id, app_id, mobile, group_user_id)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                UserNeed(
                    user_id=user_id,
                    app_id=app_id,
                    mobile=mobile,
                    group_user_id=group_user_id,
                    kind=kind,
                )
            )

    if g2_file:
        add_from_rows(read_tsv(g2_file), "MISSING")
    if g3_file:
        add_from_rows(read_tsv(g3_file), "MISSING")
    return out


def chunks(items: Sequence[Any], size: int) -> List[List[Any]]:
    n = max(1, size)
    return [list(items[i:i + n]) for i in range(0, len(items), n)]


def ng01_source_id(user_id: int, offset: int) -> int:
    return user_id - offset


def source_raw_id(user_id: int, offset: int) -> int:
    """ng01 源表 id（日志用）；LM 与 TSV user_id 相同。"""
    if user_id >= offset:
        return ng01_source_id(user_id, offset)
    return user_id


def _dt_to_ms(val: Any) -> int:
    if val is None or val == "":
        return 0
    if isinstance(val, datetime):
        return int(val.timestamp() * 1000)
    if isinstance(val, (int, float)):
        return int(val)
    s = str(val).strip()
    if not s:
        return 0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return int(datetime.strptime(s[:19], fmt).timestamp() * 1000)
        except ValueError:
            pass
    return 0


def build_ng01_row(need: UserNeed, src: dict) -> dict:
    uid = need.user_id
    return {
        "user_id": uid,
        "app_id": need.app_id,
        "group_user_id": need.group_user_id,
        "info_user_id": uid,
        "mobile": need.mobile,
        "closed_time": 0,
        "reg_device_uuid": str(src.get("device_id") or ""),
        "reg_time": int(src.get("reg_time") or 0),
        "test_flag": 0,
        "utm_source": M.map_utm_source(src.get("network_name"), src.get("tracker_name")),
        "utm_medium": src.get("campaign_tracker"),
        "utm_campaign": src.get("campaign_name"),
        "utm_content": src.get("creative_name"),
        "utm_term": src.get("adgroup_tracker"),
        "campaign_id": src.get("creative_tracker"),
        "ad_group_id": src.get("campaign_tracker"),
        "advertiser_id": src.get("adgroup_tracker"),
    }


def build_lm_row(need: UserNeed, src: dict) -> dict:
    is_cancel = src.get("is_cancel") in (1, "1", True)
    closed_time = _dt_to_ms(src.get("updated")) if is_cancel else 0
    return {
        "user_id": need.user_id,
        "app_id": need.app_id,
        "group_user_id": need.group_user_id,
        "info_user_id": need.user_id,
        "mobile": need.mobile,
        "closed_time": closed_time,
        "reg_device_uuid": str(src.get("device_id") or ""),
        "reg_time": _dt_to_ms(src.get("created")),
        "test_flag": 0,
        "utm_source": None,
        "utm_medium": None,
        "utm_campaign": None,
        "utm_content": None,
        "utm_term": None,
        "campaign_id": None,
        "ad_group_id": None,
        "advertiser_id": None,
    }


def fetch_ng01_by_user_ids(
    cmp_mod, cfg: dict, user_ids: Sequence[int], offset: int,
) -> Dict[int, dict]:
    """按 TSV user_id 查 ng01 源；返回 map[target_user_id] -> row。"""
    if not user_ids:
        return {}
    raw_ids = sorted({ng01_source_id(uid, offset) for uid in user_ids})
    conn = cmp_mod.connect_ng01_source(cfg)
    raw_map: Dict[int, dict] = {}
    try:
        with conn.cursor() as cur:
            for batch in chunks(raw_ids, 200):
                ph = ",".join(["%s"] * len(batch))
                cur.execute(NG01_USER_SQL.format(ph=ph), list(batch))
                for row in cur.fetchall():
                    raw_map[int(row["id"])] = dict(row)
    finally:
        conn.close()
    out: Dict[int, dict] = {}
    for uid in user_ids:
        raw = raw_map.get(ng01_source_id(uid, offset))
        if raw is not None:
            out[uid] = raw
    return out


def fetch_lm_by_user_ids(cmp_mod, cfg: dict, user_ids: Sequence[int]) -> Dict[int, dict]:
    """按 TSV user_id 查 LM 源；返回 map[user_id] -> row。"""
    if not user_ids:
        return {}
    ids = sorted(set(int(u) for u in user_ids))
    conn = cmp_mod.connect_lm_source(cfg)
    out: Dict[int, dict] = {}
    try:
        with conn.cursor() as cur:
            for batch in chunks(ids, 200):
                ph = ",".join(["%s"] * len(batch))
                cur.execute(LM_USER_SQL.format(ph=ph), list(batch))
                for row in cur.fetchall():
                    out[int(row["id"])] = dict(row)
    finally:
        conn.close()
    return out


def fetch_lm_user_data_fallback(
    cmp_mod, cfg: dict, user_ids: Sequence[int],
) -> Dict[int, dict]:
    """user 表无行时，用 user_data.userId 拼 LM user 行（形状同 fetch_lm_by_user_ids）。"""
    if not user_ids:
        return {}
    ids = sorted(set(int(u) for u in user_ids))
    conn = cmp_mod.connect_lm_source(cfg)
    ud_map: Dict[int, dict] = {}
    dev_map: Dict[int, Any] = {}
    try:
        with conn.cursor() as cur:
            for batch in chunks(ids, 200):
                ph = ",".join(["%s"] * len(batch))
                cur.execute(LM_USER_DATA_SQL.format(ph=ph), list(batch))
                for row in cur.fetchall():
                    ud_map[int(row["id"])] = dict(row)
            for batch in chunks(ids, 200):
                ph = ",".join(["%s"] * len(batch))
                cur.execute(LM_APP_DEVICE_SQL.format(ph=ph), list(batch))
                for row in cur.fetchall():
                    dev_map[int(row["id"])] = row.get("device_id")
    finally:
        conn.close()
    out: Dict[int, dict] = {}
    for uid in ids:
        ud = ud_map.get(uid)
        if not ud:
            continue
        out[uid] = {
            "id": uid,
            "device_id": dev_map.get(uid) or 0,
            "is_cancel": 0,
            "created": ud.get("created"),
            "updated": ud.get("updated"),
            "_source": "user_data",
        }
    return out


def filter_existing_g3(conn, needs: Sequence[UserNeed]) -> Tuple[List[UserNeed], int]:
    """返回仍缺四元组的 need；skipped=已在目标库一致的数量。"""
    if not needs:
        return [], 0
    existing: Set[str] = set()
    with conn.cursor() as cur:
        for batch in chunks(list(needs), 100):
            parts: List[str] = []
            params: List[Any] = []
            for n in batch:
                parts.append("(mobile=%s AND app_id=%s AND user_id=%s AND group_user_id=%s)")
                params.extend([n.mobile, n.app_id, n.user_id, n.group_user_id])
            sql = (
                "SELECT mobile, app_id, user_id, group_user_id FROM `user` WHERE "
                + " OR ".join(parts)
            )
            cur.execute(sql, params)
            for row in cur.fetchall():
                existing.add(
                    "{0}|{1}|{2}|{3}".format(
                        row["mobile"], row["app_id"], row["user_id"], row["group_user_id"],
                    )
                )
    pending = [n for n in needs if n.g3_key() not in existing]
    return pending, len(needs) - len(pending)


def fetch_sources_parallel(
    cmp_mod,
    cfg: dict,
    needs: Sequence[UserNeed],
    offset: int,
    workers: int,
) -> Tuple[Dict[str, Dict[int, dict]], List[UserNeed], int]:
    """并发按 TSV user_id 查源库；src_map[pipeline][user_id] -> row。返回 lm_user_data_fallback 计数。"""
    ng01_uids = sorted({n.user_id for n in needs if n.user_id >= offset})
    lm_uids = sorted({n.user_id for n in needs if n.user_id < offset})

    src_map: Dict[str, Dict[int, dict]] = {"ng01": {}, "lm": {}}
    tasks = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        for batch in chunks(ng01_uids, 500):
            tasks.append(("ng01", ex.submit(fetch_ng01_by_user_ids, cmp_mod, cfg, batch, offset)))
        for batch in chunks(lm_uids, 500):
            tasks.append(("lm", ex.submit(fetch_lm_by_user_ids, cmp_mod, cfg, batch)))
        for pipe, fut in tasks:
            src_map[pipe].update(fut.result())

    lm_missing_uids = [uid for uid in lm_uids if uid not in src_map["lm"]]
    ud_fallback = fetch_lm_user_data_fallback(cmp_mod, cfg, lm_missing_uids)
    src_map["lm"].update(ud_fallback)

    missing_needs: List[UserNeed] = []
    for n in needs:
        pipe = pipeline_for_user_id(n.user_id, offset)
        if n.user_id not in src_map.get(pipe, {}):
            missing_needs.append(n)
    return src_map, missing_needs, len(ud_fallback)


def build_rows(
    needs: Sequence[UserNeed],
    src_map: Dict[str, Dict[int, dict]],
    offset: int,
) -> Tuple[List[dict], List[UserNeed]]:
    rows: List[dict] = []
    missing: List[UserNeed] = []
    for n in needs:
        pipe = pipeline_for_user_id(n.user_id, offset)
        src = src_map.get(pipe, {}).get(n.user_id)
        if not src:
            missing.append(n)
            continue
        if pipe == "ng01":
            rows.append(build_ng01_row(n, src))
        else:
            rows.append(build_lm_row(n, src))
    return rows, missing


def dedupe_rows_by_pk(rows: Sequence[dict]) -> List[dict]:
    seen: Set[Tuple[str, int, int]] = set()
    out: List[dict] = []
    for r in rows:
        pk = (str(r["mobile"]), int(r["app_id"]), int(r.get("closed_time") or 0))
        if pk in seen:
            continue
        seen.add(pk)
        out.append(r)
    return out


def user_row_pk(row: dict) -> Tuple[str, int, int]:
    return (str(row["mobile"]), int(row["app_id"]), int(row.get("closed_time") or 0))


def fetch_target_by_pks(conn, pks: Sequence[Tuple[str, int, int]]) -> Dict[Tuple[str, int, int], dict]:
    """按 user 主键 (mobile, app_id, closed_time) 查目标库已有行。"""
    out: Dict[Tuple[str, int, int], dict] = {}
    if not pks:
        return out
    uniq = list(dict.fromkeys(pks))
    with conn.cursor() as cur:
        for batch in chunks(uniq, 100):
            parts: List[str] = []
            params: List[Any] = []
            for mobile, app_id, closed_time in batch:
                parts.append("(mobile=%s AND app_id=%s AND closed_time=%s)")
                params.extend([mobile, app_id, closed_time])
            sql = (
                "SELECT mobile, app_id, closed_time, user_id, group_user_id, info_user_id, reg_time "
                "FROM `user` WHERE " + " OR ".join(parts)
            )
            cur.execute(sql, params)
            for row in cur.fetchall():
                pk = (str(row["mobile"]), int(row["app_id"]), int(row["closed_time"]))
                out[pk] = dict(row)
    return out


def classify_insert_vs_update(
    conn,
    rows: Sequence[dict],
) -> Tuple[List[dict], List[dict]]:
    """PK 不存在 → insert；PK 已存在但 user_id/group_user_id 与 plan 不同 → needs_update。"""
    pks = [user_row_pk(r) for r in rows]
    existing = fetch_target_by_pks(conn, pks)
    insert_rows: List[dict] = []
    update_rows: List[dict] = []
    for r in rows:
        pk = user_row_pk(r)
        tgt = existing.get(pk)
        if tgt is None:
            insert_rows.append(r)
            continue
        if int(tgt["user_id"]) == int(r["user_id"]) and int(tgt["group_user_id"]) == int(r["group_user_id"]):
            continue
        update_rows.append(
            {
                "mobile": pk[0],
                "app_id": pk[1],
                "closed_time": pk[2],
                "target_user_id": int(tgt["user_id"]),
                "target_group_user_id": int(tgt["group_user_id"]),
                "target_info_user_id": int(tgt.get("info_user_id") or tgt["user_id"]),
                "planned_user_id": int(r["user_id"]),
                "planned_group_user_id": int(r["group_user_id"]),
                "planned_info_user_id": int(r.get("info_user_id") or r["user_id"]),
                "planned_row": r,
            }
        )
    return insert_rows, update_rows


def write_jsonl(path: Path, items: Iterable[Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")


def apply_inserts(
    cfg: dict,
    columns: Sequence[str],
    rows: List[dict],
    batch_size: int,
    workers: int,
) -> int:
    if not rows:
        return 0
    affected = 0
    batches = chunks(rows, batch_size)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = [
            ex.submit(_insert_batch, cfg, "user", columns, batch)
            for batch in batches
        ]
        for fut in as_completed(futs):
            affected += fut.result()
    return affected


def apply_upserts(
    cfg: dict,
    columns: Sequence[str],
    rows: List[dict],
    batch_size: int,
    workers: int,
) -> int:
    """INSERT；PK(mobile,app_id,closed_time) 冲突则 UPDATE（修 G3 INCONSISTENT）。"""
    if not rows:
        return 0
    cols = list(columns)
    pk_set = set(USER_PK)
    update_cols = [c for c in cols if c not in pk_set]
    placeholders = ", ".join(["%s"] * len(cols))
    set_sql = ", ".join("`{0}`=VALUES(`{0}`)".format(c) for c in update_cols)
    sql = "INSERT INTO `user` ({0}) VALUES ({1}) ON DUPLICATE KEY UPDATE {2}".format(
        quote_cols(cols), placeholders, set_sql,
    )

    def _upsert_batch(batch: List[dict]) -> int:
        conn = env_util.connect_target(cfg)
        try:
            with conn.cursor() as cur:
                data = []
                for r in batch:
                    clamp_amounts(r)
                    data.append(tuple(r.get(c) for c in cols))
                cur.executemany(sql, data)
            return len(batch)
        finally:
            env_util.close_conn(conn)

    affected = 0
    batches = chunks(rows, batch_size)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futs = [ex.submit(_upsert_batch, batch) for batch in batches]
        for fut in as_completed(futs):
            affected += fut.result()
    return affected


def main() -> int:
    p = argparse.ArgumentParser(description="Backfill target.user from G2/G3 TSV via source DB")
    p.add_argument("--env", default=str(RECON.parent.parent / ".env"), help=".env path")
    p.add_argument("--g2-file", help="G2.tsv path (MISSING user_id)")
    p.add_argument("--g3-file", help="G3.tsv path (MISSING + INCONSISTENT)")
    p.add_argument(
        "--kinds",
        default="MISSING,INCONSISTENT",
        help="comma-separated: MISSING,INCONSISTENT (default both)",
    )
    p.add_argument("--apply", action="store_true", help="insert into target (default dry-run)")
    p.add_argument("--workers", type=int, default=8, help="parallel workers for fetch/insert")
    p.add_argument("--batch-size", type=int, default=200, help="insert batch size")
    p.add_argument("--plan-file", help="write rows to INSERT (JSONL)")
    p.add_argument(
        "--update-file",
        help="write rows that need UPDATE (PK exists, user_id/group_user_id mismatch) JSONL",
    )
    p.add_argument(
        "--upsert",
        action="store_true",
        help="apply 时用 INSERT..ON DUPLICATE KEY UPDATE（默认仅 INSERT，冲突行写入 --update-file）",
    )

    args = p.parse_args()

    if not args.g2_file and not args.g3_file:
        print("need --g2-file and/or --g3-file", file=sys.stderr)
        return 2

    kinds = {k.strip().upper() for k in args.kinds.split(",") if k.strip()}
    cfg = env_util.load_env(Path(args.env))
    offset = int(cfg["user_id_offset"])
    cmp_mod = load_compare_connectors()

    needs = parse_needs(
        Path(args.g2_file) if args.g2_file else None,
        Path(args.g3_file) if args.g3_file else None,
        kinds,
    )
    if not needs:
        print("no user tuples from tsv", file=sys.stderr)
        return 1

    conn = env_util.connect_target(cfg)
    try:
        pending, skipped_ok = filter_existing_g3(conn, needs)
    finally:
        env_util.close_conn(conn)

    stats: Dict[str, Any] = {
        "tsv_tuples": len(needs),
        "already_ok": skipped_ok,
        "pending": len(pending),
        "by_kind": {},
        "by_pipeline": {},
        "lm_user_data_fallback": 0,
        "source_missing": 0,
        "planned_total": 0,
        "planned_insert": 0,
        "needs_update": 0,
        "applied_insert": 0,
    }
    for n in pending:
        stats["by_kind"][n.kind] = stats["by_kind"].get(n.kind, 0) + 1
        pipe = pipeline_for_user_id(n.user_id, offset)
        stats["by_pipeline"][pipe] = stats["by_pipeline"].get(pipe, 0) + 1

    t0 = time.time()
    src_map, src_missing, ud_fb = fetch_sources_parallel(
        cmp_mod, cfg, pending, offset, args.workers,
    )
    rows, still_missing = build_rows(pending, src_map, offset)
    rows = dedupe_rows_by_pk(rows)
    stats["lm_user_data_fallback"] = ud_fb
    stats["source_missing"] = len(still_missing)
    stats["planned_total"] = len(rows)

    conn = env_util.connect_target(cfg)
    try:
        insert_rows, update_rows = classify_insert_vs_update(conn, rows)
    finally:
        env_util.close_conn(conn)

    stats["planned_insert"] = len(insert_rows)
    stats["needs_update"] = len(update_rows)

    plan_path = Path(args.plan_file) if args.plan_file else None
    update_path = Path(args.update_file) if args.update_file else None
    if plan_path:
        write_jsonl(plan_path, insert_rows)
    if update_path:
        write_jsonl(update_path, update_rows)
    elif update_rows:
        default_update = Path("/tmp/user_backfill_needs_update.jsonl")
        write_jsonl(default_update, update_rows)
        stats["needs_update_file"] = str(default_update)

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if update_rows[:3]:
        print("needs_update sample:", file=sys.stderr)
        for item in update_rows[:3]:
            print(
                "  mobile={mobile} app_id={app_id} target_uid={tu} planned_uid={pu}".format(
                    mobile=item["mobile"],
                    app_id=item["app_id"],
                    tu=item["target_user_id"],
                    pu=item["planned_user_id"],
                ),
                file=sys.stderr,
            )
    if still_missing[:5]:
        print("source_missing sample:", file=sys.stderr)
        for n in still_missing[:5]:
            print(
                "  pipeline={0} user_id={1} app_id={2} source_id={3} mobile={4}".format(
                    pipeline_for_user_id(n.user_id, offset),
                    n.user_id,
                    n.app_id,
                    source_raw_id(n.user_id, offset),
                    n.mobile,
                ),
                file=sys.stderr,
            )

    if not args.apply:
        print(
            "dry-run: would INSERT {0}, needs UPDATE {1}; pass --apply to insert only".format(
                len(insert_rows), len(update_rows),
            )
        )
        print("elapsed_sec={0:.1f}".format(time.time() - t0))
        return 0

    columns = resolve_columns(cfg, "user", M.USER_COLS)
    if args.upsert:
        applied = apply_upserts(cfg, columns, insert_rows + [
            u["planned_row"] for u in update_rows
        ], args.batch_size, args.workers)
        print(json.dumps({"applied_upsert": applied, "elapsed_sec": round(time.time() - t0, 1)}))
    else:
        applied = apply_inserts(cfg, columns, insert_rows, args.batch_size, args.workers)
        stats["applied_insert"] = applied
        print(json.dumps({
            "applied_insert": applied,
            "skipped_needs_update": len(update_rows),
            "elapsed_sec": round(time.time() - t0, 1),
        }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
