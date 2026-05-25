#!/usr/bin/env bash
# Shared shell fragments sourced by deploy/activate.sh + deploy/local-deploy.sh.
# Source this file via:  source "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
# (both scripts live in the same directory).
#
# Putting these here so a change in one script does NOT silently drift
# from the other.

# Rsync excludes used by every stage:
#   .venv/      — dev .venv in source tree must not overwrite the
#                 deployed venv (activate.sh's venv-python sanity
#                 check would fail if it did)
#   target/     — cargo build artifacts; the deployed data-plane
#                 binary lands via a separate `install` call
#   __pycache__/  — Python bytecode caches; harmless but noisy
RSYNC_EXCLUDES=(--exclude='.venv/' --exclude='target/' --exclude='__pycache__/')
