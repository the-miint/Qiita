"""DuckDB + miint connection helper for the control-plane CLI.

`qiita reference load` parses FASTA with miint's `read_fastx` and chunks in
DuckDB (no Python parser), so it needs a DuckDB connection with the miint
extension installed and loaded. The connect-config + install-statement
resolution is shared with the orchestrator via `qiita_common.duckdb_miint`
(single source for the `MIINT_EXTENSION_REPO` / `MIINT_EXTENSION_DIRECTORY` env
contract); only the concurrency model differs — this is **synchronous** (the
CLI runs the upload stream inside `asyncio.to_thread`, with a `threading.Lock`
guarding the one-time install) where the orchestrator's is async.
"""

from __future__ import annotations

import threading

import duckdb
from qiita_common.duckdb_miint import miint_connect_config, miint_install_sql

_install_lock = threading.Lock()
_installed = False


def _connect() -> duckdb.DuckDBPyConnection:
    config = miint_connect_config()
    return duckdb.connect(":memory:", config=config) if config else duckdb.connect(":memory:")


def connect_with_miint() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with the miint extension loaded.

    Installs miint once per process (thread-safe), then LOADs it on the fresh
    connection. The caller owns the connection and must close it."""
    global _installed
    conn = _connect()
    if not _installed:
        with _install_lock:
            if not _installed:
                conn.execute(miint_install_sql())
                _installed = True
    conn.execute("LOAD miint;")
    return conn
