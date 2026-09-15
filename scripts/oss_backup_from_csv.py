#!/usr/bin/env python3
"""
从阿里云控制台导出的 Bucket CSV 批量下载 OSS 到本地。
Windows / Mac / Linux 均可直接: python scripts/oss_backup_from_csv.py

依赖: 官方 ossutil（Windows 用 ossutil64.exe，加入 PATH 或配置 OSSUTIL_PATH）

配置: scripts/oss_backup.env（见 oss_backup.env.example）
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ENV = SCRIPT_DIR / "oss_backup.env"
SCRIPT_BUILD = "20260914-skip-archive"

# 即使 CSV 存储类型列异常，也默认跳过这两个印度归档备份桶
DEFAULT_EXCLUDE_BUCKETS = (
    "loan-core-data-india-sg-bak,india-filess-new-bak"
)

ENDPOINTS = {
    "oss-cn-beijing": "https://oss-cn-beijing.aliyuncs.com",
    "oss-cn-hongkong": "https://oss-cn-hongkong.aliyuncs.com",
    "oss-ap-southeast-1": "https://oss-ap-southeast-1.aliyuncs.com",
    "oss-ap-southeast-5": "https://oss-ap-southeast-5.aliyuncs.com",
    "oss-ap-southeast-7": "https://oss-ap-southeast-7.aliyuncs.com",
    "oss-eu-central-1": "https://oss-eu-central-1.aliyuncs.com",
    "oss-eu-west-1": "https://oss-eu-west-1.aliyuncs.com",
    "oss-me-east-1": "https://oss-me-east-1.aliyuncs.com",
}

DEFAULT_EXCLUDE_REGEX = (
    r"(get-your|restore-your|we-have-your|your-loan-records|read-inside|"
    r"contact-discord|production-loan-data-back)"
)


def is_archive_storage(storage: str) -> bool:
    s = storage.lower()
    if "standard" in s and "archive" not in s:
        return False
    markers = (
        "archive",
        "归档",
        "cold",
        "冷归档",
        "deep",
        "深度",
    )
    return any(m in s for m in markers)


def load_env_file(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        # 配置文件优先（避免 Windows 里残留的空 SKIP_ARCHIVE 等环境变量盖掉 .env）
        os.environ[key] = val


def endpoint_for_region(region: str) -> str:
    if region in ENDPOINTS:
        return ENDPOINTS[region]
    if region.startswith("oss-"):
        return f"https://{region}.aliyuncs.com"
    raise ValueError(f"unknown region: {region}")


def find_ossutil() -> Path:
    explicit = os.environ.get("OSSUTIL_PATH", "").strip()
    if explicit:
        p = Path(explicit)
        if p.is_file():
            return p
        raise SystemExit(f"ERR: OSSUTIL_PATH 不存在: {p}")

    for name in ("ossutil64", "ossutil64.exe", "ossutil", "ossutil.exe"):
        found = shutil.which(name)
        if found:
            return Path(found)

    raise SystemExit(
        "ERR: 未找到 ossutil。Windows 请下载 ossutil64.exe 并加入 PATH，或设置 OSSUTIL_PATH"
    )


def env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    val = raw.strip()
    if not val:
        return default
    return val.lower() not in ("0", "false", "no", "off", "n")


def parse_buckets(
    csv_path: Path, cfg: dict
) -> tuple[list[tuple[str, str, int, str]], int]:
    min_bytes = int(cfg["min_bytes"])
    exclude = re.compile(cfg["exclude_regex"], re.I)
    prefixes = [p.strip().lower() for p in cfg["exclude_prefixes"].split(",") if p.strip()]
    only = {x.strip() for x in cfg["only_buckets"].split(",") if x.strip()}
    exclude_buckets = {
        x.strip()
        for x in cfg["exclude_buckets"].split(",")
        if x.strip()
    }

    def skip_prefix(name: str) -> bool:
        low = name.lower()
        return any(low.startswith(p) for p in prefixes)

    rows = list(csv.DictReader(csv_path.open(encoding="utf-8-sig")))
    planned: list[tuple[str, str, int, str]] = []
    for r in rows:
        bucket = (r.get("存储桶名称") or "").strip()
        region = (r.get("地域") or "").strip()
        storage = (r.get("存储类型") or "").strip()
        try:
            nbytes = int((r.get("容量（Byte）") or "0").strip() or 0)
        except ValueError:
            nbytes = 0
        if not bucket or not region:
            continue
        if only and bucket not in only:
            continue
        if skip_prefix(bucket):
            continue
        if exclude.search(bucket):
            continue
        if bucket in exclude_buckets:
            continue
        if nbytes < min_bytes:
            continue
        if cfg.get("skip_archive") and is_archive_storage(storage):
            continue
        planned.append((bucket, region, nbytes, storage))

    planned.sort(key=lambda x: -x[2])
    return planned, len(rows)


def load_config(env_path: Path) -> dict:
    load_env_file(env_path)
    root = os.environ.get("OSS_BACKUP_ROOT", "").strip()
    if not root:
        raise SystemExit(
            "ERR: 请设置 OSS_BACKUP_ROOT\n"
            "  1) copy scripts\\oss_backup.env.example scripts\\oss_backup.env\n"
            "  2) 编辑 OSS_BACKUP_ROOT，例如 D:\\OSS-backup 或 D:/OSS-backup"
        )
    csv_default = Path.home() / "Downloads" / "buckets_20260914.csv"
    csv_path = Path(os.environ.get("BUCKETS_CSV", str(csv_default)))
    return {
        "root": Path(root),
        "csv_path": csv_path,
        "min_bytes": os.environ.get("MIN_BYTES", "1048576"),
        "exclude_regex": os.environ.get("EXCLUDE_REGEX", DEFAULT_EXCLUDE_REGEX),
        "exclude_prefixes": os.environ.get("EXCLUDE_PREFIXES", "tanzania,th"),
        "only_buckets": os.environ.get("ONLY_BUCKETS", ""),
        "exclude_buckets": os.environ.get(
            "EXCLUDE_BUCKETS", DEFAULT_EXCLUDE_BUCKETS
        ),
        "skip_archive": env_flag("SKIP_ARCHIVE", True),
        "dry_run": os.environ.get("DRY_RUN", "0") == "1",
        "jobs": os.environ.get("JOBS", "5"),
        "ak": os.environ.get("OSS_ACCESS_KEY_ID", "").strip(),
        "sk": os.environ.get("OSS_ACCESS_KEY_SECRET", "").strip(),
        "env_path": env_path,
    }


def run_bucket(
    ossutil: Path,
    bucket: str,
    region: str,
    nbytes: int,
    storage: str,
    cfg: dict,
) -> int:
    root: Path = cfg["root"]
    log_dir = root / "_logs"
    manifest = root / "_manifest.tsv"
    dest = root / region / bucket
    ep = endpoint_for_region(region)
    log_file = log_dir / f"{bucket}.log"
    report_dir = log_dir / f"ossutil-report-{bucket}"

    dest.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%F %T")
    with manifest.open("a", encoding="utf-8") as mf:
        mf.write(f"[{ts}] START {bucket} -> {dest} endpoint={ep} bytes={nbytes}\n")

    cmd = [
        str(ossutil),
        "cp",
        "-r",
        f"oss://{bucket}/",
        str(dest) + os.sep,
        "--endpoint",
        ep,
        "--jobs",
        str(cfg["jobs"]),
        "--update",
        "--output-dir",
        str(report_dir),
    ]
    if cfg["ak"] and cfg["sk"]:
        cmd.extend(["-i", cfg["ak"], "-k", cfg["sk"]])

    if cfg["dry_run"]:
        print("DRY_RUN:", " ".join(cmd))
        return 0

    print(f"[{ts}] sync {bucket} -> {dest}")
    with log_file.open("a", encoding="utf-8") as lf:
        proc = subprocess.run(cmd, stdout=lf, stderr=subprocess.STDOUT)
    status = "OK" if proc.returncode == 0 else "FAIL"
    ts2 = datetime.now().strftime("%F %T")
    line = f"[{ts2}] {status} {bucket}"
    if proc.returncode != 0:
        line += f" (see {log_file})"
    print(line)
    with manifest.open("a", encoding="utf-8") as mf:
        mf.write(line + "\n")
    return proc.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description="OSS bucket backup from CSV (ossutil)")
    parser.add_argument("--list", action="store_true", help="只列出将要备份的 bucket")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(os.environ.get("OSS_BACKUP_ENV", str(DEFAULT_ENV))),
        help="配置文件路径（默认 scripts/oss_backup.env）",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    csv_path: Path = cfg["csv_path"]
    if not csv_path.is_file():
        raise SystemExit(f"ERR: CSV not found: {csv_path}")

    planned, total_rows = parse_buckets(csv_path, cfg)
    total_gib = sum(x[2] for x in planned) / 1024**3
    print(
        f"# build={SCRIPT_BUILD} script={Path(__file__).resolve()}",
        file=sys.stderr,
    )
    print(
        f"# SKIP_ARCHIVE={1 if cfg['skip_archive'] else 0} "
        f"EXCLUDE_BUCKETS={cfg['exclude_buckets']}",
        file=sys.stderr,
    )
    print(
        f"# planned={len(planned)} skip={total_rows - len(planned)} total_GiB={total_gib:.1f}",
        file=sys.stderr,
    )

    if args.list:
        for bucket, region, nbytes, storage in planned:
            gib = nbytes / 1024**3
            print(f"{bucket:40} {region:22} {gib:12.2f} GiB  {storage}")
        if cfg["skip_archive"]:
            print()
            print("（SKIP_ARCHIVE=1：已跳过 CSV 中 Archive/冷归档 桶，需解冻后设 SKIP_ARCHIVE=0 或 ONLY_BUCKETS=桶名）")
        print()
        print(f"备份根目录: {cfg['root']}")
        print(f"配置文件: {cfg['env_path']}")
        print("执行备份: python scripts/oss_backup_from_csv.py")
        return 0

    ossutil = find_ossutil()
    rc = 0
    for bucket, region, nbytes, storage in planned:
        if run_bucket(ossutil, bucket, region, nbytes, storage, cfg) != 0:
            rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
