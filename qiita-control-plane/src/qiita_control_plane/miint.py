"""DuckDB + miint connection helpers for the control plane.

Two connect paths, because the control plane runs miint in two very different
places and they have opposite requirements:

* `connect_with_miint()` — CLIENT side (`qiita reference load`). Runs from
  **arbitrary client hosts** that have no deploy-staged extension_directory, so
  it can't be LOAD-only: it INSTALLs into its own cache (a plain `INSTALL`, a
  no-op once the cache is warm, once per process, thread-safe) then LOADs.
* `connect_with_miint_staged()` — SERVICE side (the CP runner, in-process under
  systemd). LOAD-only from the deploy-staged `MIINT_EXTENSION_DIRECTORY`,
  exactly like the cluster paths (CO service, native jobs, the probe).

The distinction is load-bearing, not stylistic. `INSTALL` resolves DuckDB's
extension directory, which defaults to `$HOME/.duckdb/extensions` when
`MIINT_EXTENSION_DIRECTORY` is unset — and the service accounts have no usable
`$HOME` (`qiita-api` / `qiita-orch` are `/dev/null`), so a service-side INSTALL
dies with `IO Error: Can't find the home directory at '/dev/null'`. Service-side
code must therefore never reach the INSTALL path. Note this is a property of
where the code RUNS, not of which CLI it belongs to: `qiita-admin` subcommands
are run as `qiita-api` on the deploy host (see `deploy/verify.sh`), so an
admin-CLI path that INSTALLs is only safe while it stays off the service
accounts. This mirrors the reasoning
already spelled out in `qiita_common.duckdb_miint.miint_load_sql`: cluster
runtime LOADs from a pre-staged directory so no node "depends on mirror
reachability, or needs a writable `$HOME`". The control plane is no different.

The connect-config + install/load statements are shared with the orchestrator
via `qiita_common.duckdb_miint` (single source for the `MIINT_EXTENSION_REPO` /
`MIINT_EXTENSION_DIRECTORY` env contract).
"""

from __future__ import annotations

import threading
from pathlib import Path

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


def connect_with_miint_staged() -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with miint LOADed from the
    deploy-staged extension directory. The SERVICE-side counterpart to
    `connect_with_miint()` — use this from anything running inside the CP
    service (the runner), never the INSTALL-based client helper.

    LOAD-only by design: no mirror round-trip on a request path, and no
    dependence on a writable `$HOME` the service account does not have (see the
    module docstring). Requires `MIINT_EXTENSION_DIRECTORY`, which must be the
    same staged directory the compute orchestrator and data plane use.

    Fails loudly and specifically when the directory is unset or missing: the
    alternative is DuckDB's `Can't find the home directory at '/dev/null'`,
    which names neither the variable to set nor the service that needs it. The
    caller owns the connection and must close it."""
    # Validate the value DuckDB will actually receive, not a second read of the
    # environment: miint_connect_config() is the one that reaches `connect()`.
    config = miint_connect_config()
    ext_dir = config.get("extension_directory")
    if not ext_dir:
        raise RuntimeError(
            "MIINT_EXTENSION_DIRECTORY is not set for the control-plane service. "
            "Service-side miint is LOAD-only from the deploy-staged extension "
            "directory (the CP service account has no writable $HOME, so INSTALL "
            "cannot resolve one). Set it in the control plane's env file to the "
            "same path the compute orchestrator and data plane use."
        )
    if not Path(ext_dir).is_dir():
        raise RuntimeError(
            f"MIINT_EXTENSION_DIRECTORY={ext_dir!r} is not a directory. It must point "
            "at the deploy-staged miint extension directory, byte-identical to the "
            "compute orchestrator's and data plane's."
        )
    conn = duckdb.connect(":memory:", config=config)
    conn.execute(miint_load_sql())
    return conn
