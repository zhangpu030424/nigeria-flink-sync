#!/usr/bin/env python3
"""从超大 mysqldump 中抽出表名含 location 的表（一次顺序扫描）。"""
from __future__ import annotations

import re
import sys
from pathlib import Path

DUMP = Path("/Volumes/新加卷 1/Ng/NgDataBases-new/nigeria_backend.sql")
OUT_DIR = Path("/Volumes/新加卷 1/Ng/NgDataBases-new/location_extract")
TABLE_RE = re.compile(rb"^-- Table structure for table `([^`]+)`")
LOC_RE = re.compile(r"location", re.I)

# 扫到文件末尾才停；匹配到的表写独立 sql
def main() -> int:
    if not DUMP.exists():
        print(f"ERR: dump not found: {DUMP}", file=sys.stderr)
        return 1
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    catalog = OUT_DIR / "_all_tables.txt"
    catalog.write_text("", encoding="utf-8")

    n_tables = 0
    extracted: list[str] = []
    extracting = False
    out_fp = None
    last_name = ""

    print(f"scan {DUMP} ({DUMP.stat().st_size / 1024**3:.1f}G)", flush=True)
    with DUMP.open("rb") as f:
        for line in f:
            m = TABLE_RE.match(line)
            if m:
                name = m.group(1).decode("utf-8", "replace")
                n_tables += 1
                last_name = name
                with catalog.open("a", encoding="utf-8") as cf:
                    cf.write(name + "\n")
                if out_fp:
                    out_fp.close()
                    out_fp = None
                extracting = bool(LOC_RE.search(name))
                if extracting:
                    dest = OUT_DIR / f"{name}.sql"
                    out_fp = dest.open("wb")
                    extracted.append(name)
                    print(f"EXTRACT {name} -> {dest}", flush=True)
                elif n_tables % 5 == 0:
                    print(f"seen {n_tables} last={name}", flush=True)
            if extracting and out_fp:
                out_fp.write(line)

    if out_fp:
        out_fp.close()
    print(f"DONE tables={n_tables} last={last_name} extracted={extracted}", flush=True)
    for name in extracted:
        p = OUT_DIR / f"{name}.sql"
        print(f"  {name}: {p.stat().st_size} bytes", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
