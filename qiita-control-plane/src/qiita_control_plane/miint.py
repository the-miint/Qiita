"""DuckDB + miint connection helper for the control-plane CLI.

`qiita reference load` parses FASTA with miint's `read_fastx` and chunks in
DuckDB (no Python parser), so it needs a DuckDB connection with the miint
extension installed and loaded. The connect-config + install/load statements are
shared with the orchestrator via `qiita_common.duckdb_miint` (single source for
the `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY` env contract).

Unlike the cluster paths (CO service, native jobs, the probe), this CLI runs
from **arbitrary client hosts** that have no deploy-staged extension_directory,
so it can't be LOAD-only. It installs into its own cache — but a plain
`INSTALL` (a no-op once the cache is warm), once per process (thread-safe),
then LOADs. The retired `FORCE INSTALL` re-downloaded on every invocation; this
downloads at most once per host. The concurrency model is **synchronous** (the
CLI runs the upload stream inside `asyncio.to_thread`, with a `threading.Lock`
guarding the one-time install) where the orchestrator's is async.
"""

from __future__ import annotations

import threading

import duckdb
from qiita_common.duckdb_miint import (
    miint_connect_config,
    miint_install_sql,
    miint_load_sql,
)

_install_lock = threading.Lock()
_installed = False


def _connect() -> duckdb.DuckDBPyConnection:
    # miint_connect_config() is always non-empty (allow_unsigned is always set,
    # since miint installs from a mirror), so always pass it.
    return duckdb.connect(":memory:", config=miint_connect_config())


def connect_with_miint() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the miint extension loaded.

    Client-side: a plain `INSTALL` once per process (thread-safe, a no-op on a
    warm cache so it never re-downloads), then LOAD on the fresh connection. The
    caller owns the connection and must close it."""
    global _installed
    conn = _connect()
    if not _installed:
        with _install_lock:
            if not _installed:
                conn.execute(miint_install_sql())
                _installed = True
    conn.execute(miint_load_sql())
    return conn
