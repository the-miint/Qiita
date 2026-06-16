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
#   build.env   — deploy-written build stamp under the control-plane
#                 rsync target; excluded so a `--delete` rsync never
#                 wipes it. activate.sh (re)writes it every deploy, so
#                 the write no longer has to be ordered after the rsync.
RSYNC_EXCLUDES=(--exclude='.venv/' --exclude='target/' --exclude='__pycache__/' --exclude='build.env')
