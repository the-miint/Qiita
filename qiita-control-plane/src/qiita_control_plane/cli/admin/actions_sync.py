"""qiita-admin CLI — actions sync subcommand (direct-DB upsert).

Split out of the former single-file ``cli.admin`` module; behavior unchanged.
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
from pydantic import ValidationError

from qiita_control_plane.actions import (
    DuplicateActionError,
    load_actions,
    sync_actions,
)

from ._helpers import _DB_CONNECT_TIMEOUT_SECONDS

# ---------------------------------------------------------------------------
# actions sync — direct-DB upsert of YAML-authoritative columns
# ---------------------------------------------------------------------------


async def _sync_actions(database_url: str, workflows_dir: Path) -> dict:
    """Load every action YAML under workflows_dir, then upsert into
    qiita.action inside one transaction. Returns a dict with counts of
    inserted, updated, and total actions found."""
    actions = load_actions(workflows_dir)
    if not actions:
        return {"found": 0, "inserted": 0, "updated": 0}
    try:
        conn = await asyncpg.connect(database_url, timeout=_DB_CONNECT_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001 — show full reason, including OS errors
        raise RuntimeError(
            f"could not connect to DATABASE_URL: {type(exc).__name__}: {exc}"
        ) from exc
    try:
        result = await sync_actions(conn, actions)
    finally:
        await conn.close()
    return {"found": len(actions), **result}


def _handle_actions_sync(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("error: DATABASE_URL not set", file=sys.stderr)
        return 2
    try:
        result = asyncio.run(_sync_actions(database_url, args.workflows_dir))
    except (FileNotFoundError, DuplicateActionError, ValidationError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2))
    return 0
