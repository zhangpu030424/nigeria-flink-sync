#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/run-sql.sh sql/02_sync_application_fast.sql
