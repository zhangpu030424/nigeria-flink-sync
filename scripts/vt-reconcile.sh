#!/usr/bin/env bash
# 转发到旧系统 VT 对账脚本（nigeria_backend / ENUM vt_type）
exec "$(dirname "$0")/../old/vt-reconcile.sh" "$@"
