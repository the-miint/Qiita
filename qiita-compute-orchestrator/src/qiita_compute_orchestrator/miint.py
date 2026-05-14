"""Shared miint extension install + DuckDB connection helpers.

Lives at the sibling level (NOT inside `jobs/`) because `jobs/` is the
native-job package — every non-dunder file there must export an
`Inputs` model and an `execute` coroutine (enforced by
`scan_native_jobs`). Shared helpers go alongside, not inside.

Two callers:
- `qiita_compute_orchestrator.backends.local.LocalBackend` (container
  step path: hash, load).
- `qiita_compute_orchestrator.jobs.fastq_to_parquet` (native step
  path; reads + transforms FASTQ via DuckDB+miint).
Both use `_ensure_miint_installed()` to lazily install miint once per
process (concurrency-safe via asyncio.Lock), then `_open_conn()` to
materialize a fresh DuckDB connection with the right config — the
caller `LOAD miint;`s the extension on its own connection.

`MIINT_EXTENSION_REPO` env override exists for the team mirror at
https://ftp.microbio.me/pub/miint; without it, install pulls from
DuckDB community-extensions. The override implies
`allow_unsigned_extensions=true` (the mirror's signing chain is the
team's own, not DuckDB's).
"""

from __future__ import annotations

import asyncio
import os

import duckdb

_miint_install_lock = asyncio.Lock()
_miint_installed = False

_MIINT_EXT_REPO = os.environ.get("MIINT_EXTENSION_REPO")


def _open_conn() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection. Unsigned-extensions config is enabled
    only when MIINT_EXTENSION_REPO points at a non-default repo (the
    team mirror), since that path serves community-signed binaries."""
    if _MIINT_EXT_REPO is not None:
        return duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    return duckdb.connect(":memory:")


async def _ensure_miint_installed() -> None:
    """Install miint once per process, concurrency-safe."""
    global _miint_installed
    if _miint_installed:
        return
    async with _miint_install_lock:
        if _miint_installed:
            return
        with _open_conn() as conn:
            if _MIINT_EXT_REPO is not None:
                conn.execute(f"FORCE INSTALL miint FROM '{_MIINT_EXT_REPO}';")
            else:
                conn.execute("INSTALL miint FROM community;")
        _miint_installed = True
