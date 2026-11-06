"""`qiita-admin backfill check-stored-preflights`: list every stored run pre-flight
that this build's kl-run-preflight cannot open. Writes nothing.

Opening a stored pre-flight applies the library's pending schema patches, and a
patch may refuse a pre-flight's existing values, so running this from a new build
before it is deployed shows which stored pre-flights the deploy would make
unreadable.

A one-off, kept in its own module so its removal touches `cli/admin/__init__.py`
only at the two lines that register the parser.
"""

from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from typing import NamedTuple

import asyncpg

from ...preflight import open_blob
from ._helpers import open_admin_pool, requires_database_url

# Blobs are fetched one at a time, so the scan never holds them all in memory
_STORED_PREFLIGHTS_SQL = (
    "SELECT idx, sequencing_run_idx, run_preflight_filename FROM qiita.sequenced_pool"
    " WHERE run_preflight_blob IS NOT NULL ORDER BY idx"
)
_PREFLIGHT_BLOB_SQL = "SELECT run_preflight_blob FROM qiita.sequenced_pool WHERE idx = $1"


class UnopenablePreflight(NamedTuple):
    """A stored pre-flight that fails to open, with the library's reason."""

    sequenced_pool_idx: int
    sequencing_run_idx: int
    filename: str
    error: str


class PreflightCheck(NamedTuple):
    """How many stored pre-flights were opened, and those that failed."""

    checked: int
    unopenable: list[UnopenablePreflight]


def add_check_stored_preflights_parser(backfill_sub: argparse._SubParsersAction) -> None:
    p = backfill_sub.add_parser(
        "check-stored-preflights",
        help="List stored run pre-flights this build cannot open",
        description=(
            "Open every run pre-flight stored on a sequenced pool with this build's"
            " kl-run-preflight, which applies its pending schema patches, and list each"
            " one that fails, with the reason. Run it from the new build before"
            " deploying it: a pre-flight listed here becomes unreadable once that build"
            " is live. Writes nothing; exits 1 if any pre-flight fails to open. Needs"
            " DATABASE_URL."
        ),
    )
    p.set_defaults(handler=_handle)


async def find_unopenable_preflights(pool: asyncpg.Pool) -> PreflightCheck:
    """Open each stored pre-flight and report those that fail."""
    rows = await pool.fetch(_STORED_PREFLIGHTS_SQL)
    unopenable = []
    for row in rows:
        blob = await pool.fetchval(_PREFLIGHT_BLOB_SQL, row["idx"])
        try:
            with open_blob(blob):
                pass
        except (ValueError, sqlite3.DatabaseError) as exc:
            unopenable.append(
                UnopenablePreflight(
                    row["idx"],
                    row["sequencing_run_idx"],
                    row["run_preflight_filename"],
                    f"{type(exc).__name__}: {exc}",
                )
            )
    check = PreflightCheck(checked=len(rows), unopenable=unopenable)
    return check


async def _check(database_url: str) -> PreflightCheck:
    pool = await open_admin_pool(database_url)
    try:
        check = await find_unopenable_preflights(pool)
    finally:
        await pool.close()
    return check


@requires_database_url
def _handle(args: argparse.Namespace, parser: argparse.ArgumentParser, database_url: str) -> int:
    try:
        check = asyncio.run(_check(database_url))
    except RuntimeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(f"stored pre-flights checked: {check.checked}")
    print(f"unopenable: {len(check.unopenable)}")
    for u in check.unopenable:
        print(
            f"  sequenced_pool {u.sequenced_pool_idx} (sequencing_run {u.sequencing_run_idx},"
            f" {u.filename}): {u.error}"
        )
    exit_code = 1 if check.unopenable else 0
    return exit_code
