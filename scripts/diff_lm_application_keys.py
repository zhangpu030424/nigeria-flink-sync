#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LM 贷超 application_no 与目标对比：整库各 1 条流式 SQL 落盘 → 本地 sort → merge diff。

默认 **不按 app_id 拆分**（仅 2 条连接：LM + 目标各扫一遍 application_no）。
比对键即可找 570 条差，不必 SELECT *（全字段体积大数十倍）。

目录（--work-dir）:
  lm/all.keys / target/all.keys
  diff/only_lm.txt / diff/only_target.txt
  repair_plan.md

Usage:
  python3 scripts/diff_lm_application_keys.py --env ./.env --phase all --work-dir /data/lm_application_diff
  python3 scripts/diff_lm_application_keys.py --env ./.env --phase export --skip-existing
  python3 scripts/diff_lm_application_keys.py --env ./.env --phase compare

  # 旧模式：按 app_id 多连接（易 Connection closed，一般不推荐）
  python3 scripts/diff_lm_application_keys.py --env ./.env --phase export --split-by-app --workers 4
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

import pymysql
from pymysql.cursors import DictCursor, SSCursor

HERE = Path(__file__).resolve().parent
REPO = HERE.parent

EXCLUDE_APP_IDS = (567, 568, 569, 571, 572, 573)
DEFAULT_WORK_DIR = Path("/tmp/lm_application_diff")


def load_env(path: Path) -> dict:
    cfg: dict = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if " #" in line:
            line = line.split(" #", 1)[0].rstrip()
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip().strip("'\"")
    return cfg


def connect(cfg: dict, prefix: str, default_db: str):
    def pick(*keys: str, default: str = "") -> str:
        for key in keys:
            val = cfg.get(key)
            if val not in (None, ""):
                return val
        return default

    if prefix == "LM":
        host = pick("LM_MYSQL_HOST")
        port = pick("LM_MYSQL_PORT", default="3306")
        user = pick("LM_MYSQL_USER", "SOURCE_MYSQL_USER")
        password = pick("LM_MYSQL_PASSWORD", "SOURCE_MYSQL_PASSWORD")
        database = pick("LM_MYSQL_DATABASE", default=default_db)
    else:
        host = pick("TARGET_MYSQL_HOST", "TARGET_HOST")
        port = pick("TARGET_MYSQL_PORT", "TARGET_PORT", default="3306")
        user = pick("TARGET_MYSQL_USER", "TARGET_USER")
        password = pick("TARGET_MYSQL_PASSWORD", "TARGET_PASSWORD")
        database = pick("TARGET_MYSQL_DATABASE", "TARGET_DB", default=default_db)

    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        cursorclass=DictCursor,
        connect_timeout=120,
        read_timeout=86400,
        write_timeout=86400,
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SET SESSION wait_timeout=28800, "
            "net_read_timeout=86400, net_write_timeout=86400"
        )
    return conn


def exclude_ph() -> str:
    return ",".join(["%s"] * len(EXCLUDE_APP_IDS))


def work_paths(work_dir: Path) -> dict:
    return {
        "root": work_dir,
        "meta": work_dir / "meta",
        "lm": work_dir / "lm",
        "target": work_dir / "target",
        "diff": work_dir / "diff",
        "diff_app": work_dir / "diff" / "by_app",
        "app_ids": work_dir / "meta" / "app_ids.txt",
        "counts": work_dir / "meta" / "counts.tsv",
        "only_lm": work_dir / "diff" / "only_lm.txt",
        "only_target": work_dir / "diff" / "only_target.txt",
        "plan": work_dir / "repair_plan.md",
        "lm_bulk": work_dir / "lm" / "all.keys",
        "tgt_bulk": work_dir / "target" / "all.keys",
    }


def ensure_dirs(paths: dict) -> None:
    for k in ("meta", "lm", "target", "diff", "diff_app"):
        paths[k].mkdir(parents=True, exist_ok=True)


def lm_key_sql() -> str:
    return (
        "CONCAT('ng', LPAD(appId, 4, '0'), '-', applicationNo)"
    )


def discover_apps(cfg: dict, paths: dict, progress_every: int) -> Dict[int, int]:
    """LM 流式扫 appId，写 app_ids.txt，返回 app_id -> lm_cnt。"""
    conn = connect(cfg, "LM", "ng_loan_market")
    where = "appId NOT IN (" + exclude_ph() + ")"
    params: list = list(EXCLUDE_APP_IDS)
    counts: Dict[int, int] = {}
    sql = "SELECT appId FROM application WHERE " + where
    print("# discover: LM stream appId ...", flush=True)
    n = 0
    cur = conn.cursor(SSCursor)
    try:
        cur.execute(sql, params)
        while True:
            batch = cur.fetchmany(100000)
            if not batch:
                break
            for row in batch:
                aid = int(row[0])
                counts[aid] = counts.get(aid, 0) + 1
                n += 1
                if progress_every > 0 and n % progress_every == 0:
                    print("# discover: {0} rows".format(n), flush=True)
    finally:
        cur.close()
        conn.close()

    apps = sorted(counts.keys())
    paths["app_ids"].write_text("\n".join(str(a) for a in apps) + "\n", encoding="utf-8")
    print("# discover: apps={0} lm_rows={1}".format(len(apps), n), flush=True)
    return counts


def load_app_ids(paths: dict) -> List[int]:
    if not paths["app_ids"].is_file():
        raise SystemExit("missing {0}; run --phase discover first".format(paths["app_ids"]))
    return _read_app_ids_file(paths["app_ids"])


def _read_app_ids_file(path: Path) -> List[int]:
    out: List[int] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(int(line))
    return out


def ensure_app_ids(
    cfg: dict, paths: dict, progress_every: int, auto_discover: bool = True
) -> List[int]:
    if paths["app_ids"].is_file():
        return _read_app_ids_file(paths["app_ids"])
    if not auto_discover:
        raise SystemExit("missing {0}; run --phase discover first".format(paths["app_ids"]))
    print(
        "# {0} not found; running discover ...".format(paths["app_ids"]),
        flush=True,
    )
    discover_apps(cfg, paths, progress_every)
    return _read_app_ids_file(paths["app_ids"])


def sort_key_file(path: Path, label: str) -> None:
    """本地 sort，避免 SQL ORDER BY 在大表上长时间无输出。"""
    if not path.is_file() or path.stat().st_size == 0:
        return
    t0 = time.time()
    subprocess.run(
        ["sort", "--parallel=2", "-T", "/tmp", "-o", str(path), str(path)],
        check=True,
    )
    print(
        "# {0}: sorted {1} ({2:.1f}s)".format(label, path.name, time.time() - t0),
        flush=True,
    )


def export_lm_app(cfg: dict, app_id: int, out_file: Path, progress_every: int) -> int:
    conn = connect(cfg, "LM", "ng_loan_market")
    # 不要 ORDER BY：否则 InnoDB 先排序千万行，客户端长时间收不到行
    sql = (
        "SELECT {0} AS k FROM application WHERE appId = %s "
        "AND applicationNo IS NOT NULL AND TRIM(applicationNo) <> ''"
    ).format(lm_key_sql())
    n = 0
    t0 = time.time()
    print("# LM app_id={0}: pull start -> {1}".format(app_id, out_file.name), flush=True)
    cur = conn.cursor(SSCursor)
    try:
        with out_file.open("w", encoding="utf-8") as fp:
            cur.execute(sql, (app_id,))
            while True:
                rows = cur.fetchmany(20000)
                if not rows:
                    break
                for row in rows:
                    fp.write(str(row[0]) + "\n")
                    n += 1
                if progress_every > 0 and n % progress_every == 0:
                    print(
                        "# LM app_id={0}: {1} rows ({2:.0f}s)".format(
                            app_id, n, time.time() - t0,
                        ),
                        flush=True,
                    )
    finally:
        cur.close()
        conn.close()
    print(
        "# LM app_id={0}: pull done rows={1} ({2:.1f}s)".format(
            app_id, n, time.time() - t0,
        ),
        flush=True,
    )
    sort_key_file(out_file, "LM app_id={0}".format(app_id))
    return n


def export_target_app(cfg: dict, app_id: int, out_file: Path, progress_every: int) -> int:
    conn = connect(cfg, "TARGET", "ng")
    sql = (
        "SELECT application_no AS k FROM application WHERE app_id = %s "
        "AND application_no IS NOT NULL AND TRIM(application_no) <> ''"
    )
    n = 0
    t0 = time.time()
    print("# TARGET app_id={0}: pull start -> {1}".format(app_id, out_file.name), flush=True)
    cur = conn.cursor(SSCursor)
    try:
        with out_file.open("w", encoding="utf-8") as fp:
            cur.execute(sql, (app_id,))
            while True:
                rows = cur.fetchmany(20000)
                if not rows:
                    break
                for row in rows:
                    fp.write(str(row[0]) + "\n")
                    n += 1
                if progress_every > 0 and n % progress_every == 0:
                    print(
                        "# TARGET app_id={0}: {1} rows ({2:.0f}s)".format(
                            app_id, n, time.time() - t0,
                        ),
                        flush=True,
                    )
    finally:
        cur.close()
        conn.close()
    print(
        "# TARGET app_id={0}: pull done rows={1} ({2:.1f}s)".format(
            app_id, n, time.time() - t0,
        ),
        flush=True,
    )
    sort_key_file(out_file, "TARGET app_id={0}".format(app_id))
    return n


def export_lm_bulk(cfg: dict, out_file: Path, progress_every: int) -> int:
    conn = connect(cfg, "LM", "ng_loan_market")
    where = "appId NOT IN (" + exclude_ph() + ")"
    params: list = list(EXCLUDE_APP_IDS)
    sql = (
        "SELECT {0} AS k FROM application WHERE {1} "
        "AND applicationNo IS NOT NULL AND TRIM(applicationNo) <> ''"
    ).format(lm_key_sql(), where)
    n = 0
    t0 = time.time()
    print("# LM bulk: pull -> {0}".format(out_file), flush=True)
    cur = conn.cursor(SSCursor)
    try:
        with out_file.open("w", encoding="utf-8") as fp:
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(50000)
                if not rows:
                    break
                for row in rows:
                    fp.write(str(row[0]) + "\n")
                    n += 1
                if progress_every > 0 and n % progress_every == 0:
                    print(
                        "# LM bulk: {0} rows ({1:.0f}s)".format(n, time.time() - t0),
                        flush=True,
                    )
    finally:
        cur.close()
        conn.close()
    print("# LM bulk: done rows={0} ({1:.1f}s)".format(n, time.time() - t0), flush=True)
    sort_key_file(out_file, "LM bulk")
    return n


def export_target_bulk(cfg: dict, out_file: Path, progress_every: int) -> int:
    conn = connect(cfg, "TARGET", "ng")
    where = "app_id NOT IN (" + exclude_ph() + ")"
    params: list = list(EXCLUDE_APP_IDS)
    sql = (
        "SELECT application_no AS k FROM application WHERE {0} "
        "AND application_no IS NOT NULL AND TRIM(application_no) <> ''"
    ).format(where)
    n = 0
    t0 = time.time()
    print("# TARGET bulk: pull -> {0}".format(out_file), flush=True)
    cur = conn.cursor(SSCursor)
    try:
        with out_file.open("w", encoding="utf-8") as fp:
            cur.execute(sql, params)
            while True:
                rows = cur.fetchmany(50000)
                if not rows:
                    break
                for row in rows:
                    fp.write(str(row[0]) + "\n")
                    n += 1
                if progress_every > 0 and n % progress_every == 0:
                    print(
                        "# TARGET bulk: {0} rows ({1:.0f}s)".format(n, time.time() - t0),
                        flush=True,
                    )
    finally:
        cur.close()
        conn.close()
    print("# TARGET bulk: done rows={0} ({1:.1f}s)".format(n, time.time() - t0), flush=True)
    sort_key_file(out_file, "TARGET bulk")
    return n


def parallel_export_bulk(
    cfg: dict,
    paths: dict,
    skip_existing: bool,
    progress_every: int,
) -> Tuple[int, int]:
    lm_f = paths["lm_bulk"]
    tg_f = paths["tgt_bulk"]
    need_lm = not (skip_existing and lm_f.is_file() and lm_f.stat().st_size > 0)
    need_tgt = not (skip_existing and tg_f.is_file() and tg_f.stat().st_size > 0)
    if not need_lm and not need_tgt:
        print("# bulk export: both all.keys exist (--skip-existing)", flush=True)
        return -1, -1
    lm_n = tgt_n = -1
    print("# bulk export: 2 connections (LM || TARGET)", flush=True)
    with ThreadPoolExecutor(max_workers=2) as pool:
        f_lm = pool.submit(export_lm_bulk, cfg, lm_f, progress_every) if need_lm else None
        f_tg = pool.submit(export_target_bulk, cfg, tg_f, progress_every) if need_tgt else None
        if f_lm is not None:
            lm_n = f_lm.result()
        if f_tg is not None:
            tgt_n = f_tg.result()
    return lm_n, tgt_n


def parallel_export(
    label: str,
    fn,
    cfg: dict,
    app_ids: Sequence[int],
    out_dir: Path,
    workers: int,
    skip_existing: bool,
    progress_every: int,
) -> Dict[int, int]:
    counts: Dict[int, int] = {}
    todo = []
    for aid in app_ids:
        dest = out_dir / "{0}.keys".format(aid)
        if skip_existing and dest.is_file() and dest.stat().st_size > 0:
            counts[aid] = -1  # 已存在，行数见 meta/counts.tsv
            continue
        todo.append((aid, dest))

    if not todo:
        print("# {0}: all {1} files exist (--skip-existing)".format(label, len(app_ids)), flush=True)
        return counts

    print("# {0}: export {1} apps, workers={2}".format(label, len(todo), workers), flush=True)
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(fn, cfg, aid, dest, progress_every): aid for aid, dest in todo
        }
        done = 0
        for fut in as_completed(futs):
            aid = futs[fut]
            counts[aid] = fut.result()
            done += 1
            print(
                "# {0}: finished app_id={1} rows={2} ({3}/{4} apps)".format(
                    label, aid, counts[aid], done, len(todo),
                ),
                flush=True,
            )
    return counts


def export_app_both(
    cfg: dict,
    app_id: int,
    lm_file: Path,
    tgt_file: Path,
    skip_existing: bool,
    progress_every: int,
) -> Tuple[int, int]:
    """单 app：LM + 目标两个独立连接，并行流式导出到本地 .keys。"""
    need_lm = not (
        skip_existing and lm_file.is_file() and lm_file.stat().st_size > 0
    )
    need_tgt = not (
        skip_existing and tgt_file.is_file() and tgt_file.stat().st_size > 0
    )
    if not need_lm and not need_tgt:
        return -1, -1

    print("# export pair start app_id={0}".format(app_id), flush=True)
    t0 = time.time()
    lm_n = 0
    tgt_n = 0
    with ThreadPoolExecutor(max_workers=2) as inner:
        futs = []
        if need_lm:
            futs.append(
                ("lm", inner.submit(export_lm_app, cfg, app_id, lm_file, progress_every)),
            )
        if need_tgt:
            futs.append(
                (
                    "tgt",
                    inner.submit(export_target_app, cfg, app_id, tgt_file, progress_every),
                ),
            )
        for kind, fut in futs:
            n = fut.result()
            if kind == "lm":
                lm_n = n
            else:
                tgt_n = n
    print(
        "# export pair done app_id={0} lm={1} tgt={2} ({3:.1f}s)".format(
            app_id, lm_n, tgt_n, time.time() - t0,
        ),
        flush=True,
    )
    return lm_n, tgt_n


def parallel_export_both(
    cfg: dict,
    app_ids: Sequence[int],
    paths: dict,
    workers: int,
    skip_existing: bool,
    progress_every: int,
) -> Tuple[Dict[int, int], Dict[int, int]]:
    lm_counts: Dict[int, int] = {}
    tgt_counts: Dict[int, int] = {}
    todo: List[int] = []

    for aid in app_ids:
        lm_f = paths["lm"] / "{0}.keys".format(aid)
        tg_f = paths["target"] / "{0}.keys".format(aid)
        if (
            skip_existing
            and lm_f.is_file()
            and lm_f.stat().st_size > 0
            and tg_f.is_file()
            and tg_f.stat().st_size > 0
        ):
            lm_counts[aid] = tgt_counts[aid] = -1
            continue
        todo.append(aid)

    if not todo:
        print(
            "# export: all {0} app pairs on disk (--skip-existing)".format(len(app_ids)),
            flush=True,
        )
        return lm_counts, tgt_counts

    print(
        "# export: {0} apps, workers={1} (each app: LM||TARGET parallel pull)".format(
            len(todo), workers,
        ),
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futs = {
            pool.submit(
                export_app_both,
                cfg,
                aid,
                paths["lm"] / "{0}.keys".format(aid),
                paths["target"] / "{0}.keys".format(aid),
                skip_existing,
                progress_every,
            ): aid
            for aid in todo
        }
        done = 0
        for fut in as_completed(futs):
            aid = futs[fut]
            lm_n, tgt_n = fut.result()
            lm_counts[aid] = lm_n
            tgt_counts[aid] = tgt_n
            done += 1
            print(
                "# export: {0}/{1} apps complete (last app_id={2})".format(
                    done, len(todo), aid,
                ),
                flush=True,
            )
    return lm_counts, tgt_counts


def iter_lines(path: Path) -> Iterator[str]:
    with path.open(encoding="utf-8") as fp:
        for line in fp:
            s = line.strip()
            if s:
                yield s


def merge_diff_files(
    lm_file: Path,
    tgt_file: Path,
    only_lm_fp,
    only_tgt_fp,
    only_lm_app_fp,
) -> Tuple[int, int, int]:
    """两文件均已 ORDER BY，流式 merge，O(1) 内存。"""
    lm_it = iter_lines(lm_file) if lm_file.is_file() else iter(())
    tgt_it = iter_lines(tgt_file) if tgt_file.is_file() else iter(())
    lm_next = next(lm_it, None)
    tgt_next = next(tgt_it, None)
    only_lm = only_tgt = matched = 0

    while lm_next is not None or tgt_next is not None:
        if lm_next is None:
            only_tgt += 1
            only_tgt_fp.write(tgt_next + "\n")
            tgt_next = next(tgt_it, None)
            continue
        if tgt_next is None:
            only_lm += 1
            only_lm_fp.write(lm_next + "\n")
            only_lm_app_fp.write(lm_next + "\n")
            lm_next = next(lm_it, None)
            continue
        if lm_next == tgt_next:
            matched += 1
            lm_next = next(lm_it, None)
            tgt_next = next(tgt_it, None)
        elif lm_next < tgt_next:
            only_lm += 1
            only_lm_fp.write(lm_next + "\n")
            only_lm_app_fp.write(lm_next + "\n")
            lm_next = next(lm_it, None)
        else:
            only_tgt += 1
            only_tgt_fp.write(tgt_next + "\n")
            tgt_next = next(tgt_it, None)

    return only_lm, only_tgt, matched


def compare_bulk(paths: dict) -> dict:
    summary = {
        "only_lm": 0,
        "only_target": 0,
        "matched": 0,
        "by_app": {},
        "mode": "bulk",
    }
    lm_f = paths["lm_bulk"]
    tg_f = paths["tgt_bulk"]
    if not lm_f.is_file() or not tg_f.is_file():
        raise SystemExit(
            "missing {0} or {1}; run --phase export first".format(lm_f, tg_f)
        )
    print("# compare: merge lm/all.keys vs target/all.keys ...", flush=True)
    t0 = time.time()
    with paths["only_lm"].open("w", encoding="utf-8") as olm, paths[
        "only_target"
    ].open("w", encoding="utf-8") as otg, open(os.devnull, "w", encoding="utf-8") as devnull:
        ol, ot, m = merge_diff_files(lm_f, tg_f, olm, otg, devnull)
    summary["only_lm"] = ol
    summary["only_target"] = ot
    summary["matched"] = m
    (paths["meta"] / "compare_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        "# compare total only_lm={0} only_target={1} matched={2} ({3:.1f}s)".format(
            ol, ot, m, time.time() - t0,
        ),
        flush=True,
    )
    return summary


def compare_all(paths: dict, app_ids: Sequence[int]) -> dict:
    summary = {
        "only_lm": 0,
        "only_target": 0,
        "matched": 0,
        "by_app": {},
    }
    with paths["only_lm"].open("w", encoding="utf-8") as olm, paths[
        "only_target"
    ].open("w", encoding="utf-8") as otg:
        for aid in app_ids:
            lm_f = paths["lm"] / "{0}.keys".format(aid)
            tg_f = paths["target"] / "{0}.keys".format(aid)
            app_diff = paths["diff_app"] / "{0}.only_lm.txt".format(aid)
            with app_diff.open("w", encoding="utf-8") as oa:
                ol, ot, m = merge_diff_files(lm_f, tg_f, olm, otg, oa)
            if ol or ot:
                summary["by_app"][aid] = {"only_lm": ol, "only_target": ot, "matched": m}
            summary["only_lm"] += ol
            summary["only_target"] += ot
            summary["matched"] += m
            if ol or ot:
                print(
                    "# compare app_id={0} only_lm={1} only_target={2}".format(aid, ol, ot),
                    flush=True,
                )
    (paths["meta"] / "compare_summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    print(
        "# compare total only_lm={0} only_target={1} matched={2}".format(
            summary["only_lm"], summary["only_target"], summary["matched"],
        ),
        flush=True,
    )
    return summary


def target_count_app(cfg: dict, app_id: int) -> int:
    conn = connect(cfg, "TARGET", "ng")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(1) AS c FROM application WHERE app_id = %s",
                (app_id,),
            )
            return int(cur.fetchone()["c"])
    finally:
        conn.close()


def write_counts_tsv(
    paths: dict,
    app_ids: Sequence[int],
    lm_counts: Dict[int, int],
    tgt_counts: Dict[int, int],
) -> None:
    lines = ["app_id\tlm_keys\ttgt_keys\tdelta_lm_tgt"]
    for aid in app_ids:
        lc = lm_counts.get(aid, 0)
        tc = tgt_counts.get(aid, 0)
        lines.append("{0}\t{1}\t{2}\t{3}".format(aid, lc, tc, lc - tc))
    paths["counts"].write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_repair_plan(paths: dict, summary: dict, work_dir: Path) -> None:
    only_lm = int(summary.get("only_lm") or 0)
    only_tgt = int(summary.get("only_target") or 0)
    by_app = summary.get("by_app") or {}

    lines = [
        "# LM application → 目标 ng.application 差集修复计划",
        "",
        "工作目录: `{0}`".format(work_dir),
        "",
        "## 结果摘要",
        "",
        "- 目标缺失 (only_lm): **{0}** 条 → 见 `diff/only_lm.txt`".format(only_lm),
        "- 目标多出 (only_target): **{0}** 条 → 见 `diff/only_target.txt`".format(only_tgt),
        "- 按 app 缺失: 见 `diff/by_app/<app_id>.only_lm.txt`",
        "",
        "## 根因（本仓库）",
        "",
        "- 目标 `application` 主链路来自 **nigeria_backend** Flink 同步（app 567/568/571/572/573）。",
        "- **LM `ng_loan_market.application` 无整表同步 Job**；贷超单需单独补 `application` + `loan`。",
        "",
        "## 修复步骤（建议顺序）",
        "",
        "1. **核对单号**",
        "   ```bash",
        "   python3 scripts/compare_orders_source_target.py --env ./.env \\",
        "     --list-file {0}/diff/only_lm.txt".format(work_dir),
        "   ```",
        "",
        "2. **补 application 行**",
        "   - 仓库暂无 LM→target.application 全字段自动 INSERT 脚本。",
        "   - 按 LM 源行映射字段写入目标（application_no = `ng`+LPAD(appId,4)+`-`+applicationNo）。",
        "   - 可先从 LM 拉明细:",
        "     `SELECT * FROM application WHERE appId=? AND applicationNo=?`",
        "",
        "3. **补 loan（已放款）**",
        "   ```bash",
        "   python3 scripts/backfill_lm_orders_by_application_no.py --env ./.env \\",
        "     --list-file {0}/diff/only_lm.txt --apply".format(work_dir),
        "   ```",
        "   脚本**只插 loan**；若 `target.application` 仍缺失会打 WARN。",
        "",
        "4. **复跑对比**",
        "   ```bash",
        "   python3 scripts/diff_lm_application_keys.py --env ./.env --phase compare \\",
        "     --work-dir {0}".format(work_dir),
        "   ```",
        "",
        "## 有差的 app_id",
        "",
    ]
    if not by_app:
        lines.append("（无 per-app 差集，或尚未 compare）")
    else:
        lines.append("| app_id | only_lm | only_target |")
        lines.append("|--------|---------|-------------|")
        for aid in sorted(by_app.keys(), key=lambda x: -by_app[x].get("only_lm", 0)):
            row = by_app[aid]
            lines.append(
                "| {0} | {1} | {2} |".format(
                    aid, row.get("only_lm", 0), row.get("only_target", 0),
                )
            )

    paths["plan"].write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("# wrote {0}".format(paths["plan"]), flush=True)


def phase_all(cfg: dict, paths: dict, args: argparse.Namespace) -> int:
    ensure_dirs(paths)
    if args.split_by_app:
        lm_counts = discover_apps(cfg, paths, args.progress_every)
        app_ids = load_app_ids(paths)
        print("# phase export (split by app_id)", flush=True)
        lm_export, tgt_counts = parallel_export_both(
            cfg,
            app_ids,
            paths,
            args.workers,
            args.skip_existing,
            args.progress_every,
        )
        for aid, c in lm_export.items():
            if c >= 0:
                lm_counts[aid] = c
        write_counts_tsv(paths, app_ids, lm_counts, tgt_counts)
        summary = compare_all(paths, app_ids)
    else:
        print("# phase export (bulk: 2 connections)", flush=True)
        lm_n, tgt_n = parallel_export_bulk(
            cfg, paths, args.skip_existing, args.progress_every,
        )
        (paths["meta"] / "bulk_counts.json").write_text(
            json.dumps({"lm_rows": lm_n, "tgt_rows": tgt_n}, indent=2) + "\n",
            encoding="utf-8",
        )
        summary = compare_bulk(paths)
    write_repair_plan(paths, summary, paths["root"])
    return 0 if summary["only_lm"] == 0 and summary["only_target"] == 0 else 1


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description="LM vs target application keys: export, diff, plan")
    p.add_argument("--env", default=str(REPO / ".env"))
    p.add_argument(
        "--phase",
        choices=(
            "all",
            "discover",
            "export",
            "export-lm",
            "export-target",
            "compare",
            "plan",
        ),
        default="all",
    )
    p.add_argument("--work-dir", type=Path, default=DEFAULT_WORK_DIR)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--skip-existing", action="store_true", help="跳过已存在的 .keys 文件")
    p.add_argument("--progress-every", type=int, default=500_000)
    p.add_argument(
        "--split-by-app",
        action="store_true",
        help="按 app_id 多连接导出（慢、易断连；默认整库 bulk）",
    )
    args = p.parse_args(argv)

    env_path = Path(args.env).resolve()
    if not env_path.is_file():
        print("missing env: {0}".format(env_path), file=sys.stderr)
        return 2

    cfg = load_env(env_path)
    paths = work_paths(args.work_dir.resolve())
    ensure_dirs(paths)

    if args.phase == "all":
        return phase_all(cfg, paths, args)

    if args.phase == "discover":
        discover_apps(cfg, paths, args.progress_every)
        return 0

    if args.phase == "export":
        if args.split_by_app:
            app_ids = ensure_app_ids(cfg, paths, args.progress_every)
            parallel_export_both(
                cfg,
                app_ids,
                paths,
                args.workers,
                args.skip_existing,
                args.progress_every,
            )
        else:
            parallel_export_bulk(cfg, paths, args.skip_existing, args.progress_every)
        return 0

    if args.phase == "export-lm":
        app_ids = ensure_app_ids(cfg, paths, args.progress_every)
        parallel_export(
            "export-lm",
            export_lm_app,
            cfg,
            app_ids,
            paths["lm"],
            args.workers,
            args.skip_existing,
            args.progress_every,
        )
        return 0

    if args.phase == "export-target":
        app_ids = ensure_app_ids(cfg, paths, args.progress_every)
        parallel_export(
            "export-target",
            export_target_app,
            cfg,
            app_ids,
            paths["target"],
            args.workers,
            args.skip_existing,
            args.progress_every,
        )
        return 0

    if args.phase == "compare":
        if args.split_by_app or not paths["lm_bulk"].is_file():
            app_ids = ensure_app_ids(cfg, paths, args.progress_every)
            summary = compare_all(paths, app_ids)
        else:
            summary = compare_bulk(paths)
        write_repair_plan(paths, summary, paths["root"])
        return 0 if summary["only_lm"] == 0 else 1

    if args.phase == "plan":
        sp = paths["meta"] / "compare_summary.json"
        if sp.is_file():
            summary = json.loads(sp.read_text(encoding="utf-8"))
        else:
            summary = {"only_lm": 0, "only_target": 0, "by_app": {}}
        write_repair_plan(paths, summary, paths["root"])
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
