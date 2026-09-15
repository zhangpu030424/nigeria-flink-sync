#!/usr/bin/env bash
# Mac/Linux 薄封装；Windows 请用 python scripts/oss_backup_from_csv.py 或 .bat
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec python3 "${SCRIPT_DIR}/oss_backup_from_csv.py" "$@"
