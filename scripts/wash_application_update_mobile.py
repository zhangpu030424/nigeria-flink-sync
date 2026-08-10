#!/usr/bin/env python3
"""根据 group_user_id + sn 回查目标库 application.mobile，重写 UPDATE 的 WHERE mobile。

用法:
  python3 scripts/wash_application_update_mobile.py \\
    --input docs/新建\\ 文本文档.txt \\
    --output /tmp/application_updates_washed.sql \\
    --env .env

默认只生成洗后的 SQL，不执行更新。加 --apply 才会在目标库执行。
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

try:
    import pymysql
except ImportError:
    print("需要 pymysql: pip install pymysql", file=sys.stderr)
    sys.exit(1)

UPDATE_RE = re.compile(
    r"UPDATE\s+`?application`?\s+SET\s+(?P<set>.+?)\s+"
    r"WHERE\s+`?mobile`?\s*=\s*'(?P<mobile>[^']*)'\s+"
    r"AND\s+`?group_user_id`?\s*=\s*(?P<guid>\d+)\s+"
    r"AND\s+`?sn`?\s*=\s*'(?P<sn>[^']*)'\s*;?",
    re.IGNORECASE | re.DOTALL,
)


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


def env_first(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.environ.get(k)
        if v not in (None, ""):
            return v
    return default


def parse_updates(text: str) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for m in UPDATE_RE.finditer(text):
        rows.append(
            {
                "set": re.sub(r"\s+", " ", m.group("set").strip()),
                "mobile": m.group("mobile"),
                "group_user_id": m.group("guid"),
                "sn": m.group("sn"),
                "raw": m.group(0).rstrip(),
            }
        )
    return rows


def fetch_mobiles(
    conn, pairs: List[Tuple[int, str]]
) -> Dict[Tuple[int, str], Optional[str]]:
    """(group_user_id, sn) -> mobile；未命中为 None。"""
    out: Dict[Tuple[int, str], Optional[str]] = {p: None for p in pairs}
    if not pairs:
        return out
    # 分批 IN 查询
    batch = 200
    with conn.cursor() as cur:
        for i in range(0, len(pairs), batch):
            chunk = pairs[i : i + batch]
            placeholders = ",".join(["(%s,%s)"] * len(chunk))
            params: List[object] = []
            for guid, sn in chunk:
                params.extend([guid, sn])
            sql = (
                "SELECT group_user_id, sn, mobile FROM application "
                f"WHERE (group_user_id, sn) IN ({placeholders})"
            )
            cur.execute(sql, params)
            for guid, sn, mobile in cur.fetchall():
                out[(int(guid), str(sn))] = str(mobile) if mobile is not None else None
    return out


def build_update(set_clause: str, mobile: str, guid: int, sn: str) -> str:
    return (
        f"UPDATE `application` SET {set_clause} "
        f"WHERE `mobile` = '{mobile}' AND `group_user_id` = {guid} AND `sn` = '{sn}';"
    )


def main() -> int:
    p = argparse.ArgumentParser(description="按 group_user_id+sn 洗 application UPDATE 的 mobile")
    p.add_argument("--input", "-i", required=True, help="原始 UPDATE SQL 文本")
    p.add_argument(
        "--output",
        "-o",
        default="",
        help="洗后 SQL 输出路径（默认 stdout）",
    )
    p.add_argument("--report", default="", help="差异报告路径（默认 stderr 摘要）")
    p.add_argument("--env", default=".env", help="含 TARGET_MYSQL_* 的 env")
    p.add_argument(
        "--apply",
        action="store_true",
        help="用洗后的 SQL 在目标库执行（默认只生成文件）",
    )
    p.add_argument("--dry-run-apply", action="store_true", help="打印将执行条数，不真正 UPDATE")
    args = p.parse_args()

    load_dotenv(args.env)
    host = env_first("TARGET_MYSQL_HOST")
    port = int(env_first("TARGET_MYSQL_PORT", default="3306") or "3306")
    user = env_first("TARGET_MYSQL_USER")
    password = env_first("TARGET_MYSQL_PASSWORD")
    database = env_first("TARGET_MYSQL_DATABASE", default="ng")
    if not host or not user:
        print("缺少 TARGET_MYSQL_HOST / TARGET_MYSQL_USER（检查 --env）", file=sys.stderr)
        return 1

    with open(args.input, encoding="utf-8") as f:
        text = f.read()
    rows = parse_updates(text)
    if not rows:
        print("未解析到任何 UPDATE application 语句", file=sys.stderr)
        return 1

    pairs = sorted({(int(r["group_user_id"]), r["sn"]) for r in rows})
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        connect_timeout=15,
        read_timeout=120,
        write_timeout=120,
    )
    try:
        mobile_map = fetch_mobiles(conn, pairs)
    finally:
        if not args.apply:
            conn.close()

    washed: List[str] = []
    missing: List[str] = []
    changed = 0
    same = 0
    report_lines: List[str] = []

    for r in rows:
        key = (int(r["group_user_id"]), r["sn"])
        real = mobile_map.get(key)
        if not real:
            missing.append(f"MISS group_user_id={key[0]} sn={key[1]} old_mobile={r['mobile']}")
            # 未命中：去掉 mobile 条件，仅用 group_user_id+sn（若目标确无该行仍影响 0）
            sql = (
                f"UPDATE `application` SET {r['set']} "
                f"WHERE `group_user_id` = {key[0]} AND `sn` = '{key[1]}';"
            )
            washed.append(sql)
            report_lines.append(f"MISS\t{key[0]}\t{key[1]}\t{r['mobile']}\t")
            continue
        if real != r["mobile"]:
            changed += 1
            report_lines.append(f"CHANGE\t{key[0]}\t{key[1]}\t{r['mobile']}\t{real}")
        else:
            same += 1
            report_lines.append(f"SAME\t{key[0]}\t{key[1]}\t{r['mobile']}\t{real}")
        washed.append(build_update(r["set"], real, key[0], key[1]))

    out_text = "\n".join(washed) + "\n"
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(out_text)
        print(f"washed SQL -> {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(out_text)

    summary = (
        f"total={len(rows)} unique_keys={len(pairs)} "
        f"changed_mobile={changed} same={same} miss={len(missing)}"
    )
    print(summary, file=sys.stderr)
    if args.report:
        with open(args.report, "w", encoding="utf-8") as f:
            f.write("status\tgroup_user_id\tsn\told_mobile\tnew_mobile\n")
            f.write("\n".join(report_lines) + "\n")
            f.write(f"# {summary}\n")
        print(f"report -> {args.report}", file=sys.stderr)
    for line in missing[:20]:
        print(line, file=sys.stderr)
    if len(missing) > 20:
        print(f"... and {len(missing) - 20} more MISS", file=sys.stderr)

    if args.apply:
        if args.dry_run_apply:
            print(f"dry-run-apply: would execute {len(washed)} statements", file=sys.stderr)
            conn.close()
            return 0
        ok = 0
        affected = 0
        try:
            with conn.cursor() as cur:
                for sql in washed:
                    cur.execute(sql)
                    affected += cur.rowcount
                    ok += 1
            conn.commit()
            print(f"applied={ok} affected_rows={affected}", file=sys.stderr)
        except Exception as e:
            conn.rollback()
            print(f"apply failed: {e}", file=sys.stderr)
            return 1
        finally:
            conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
