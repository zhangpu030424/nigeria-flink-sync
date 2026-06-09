#!/usr/bin/env bash
# user Job 快捷入口 → sync-job-auto.sh user
exec "$(dirname "$0")/sync-job-auto.sh" user "$@"
