"""Shared integration-test plumbing for the data-plane subprocess.

Both `conftest.py` and `test_system_gg2_backbone.py` need the same env-var
overrides (so CI can point them at a host-provisioned Postgres on macOS where
Docker isn't available) and the same host-OS-aware paths when spawning the
data plane binary. They live here so the two files cannot drift.

The Postgres URL resolver moved to `qiita_control_plane.testing.postgres` so
the control-plane test suite can use it too; this module re-exports
`postgres_url` for backwards compatibility with `test_system_gg2_backbone.py`.
"""

import os
import sys
from pathlib import Path

from qiita_control_plane.testing.postgres import resolve_postgres_url as postgres_url

# Pin sslmode=disable for the same reason POSTGRES_URL_DEFAULT does in
# qiita_control_plane.testing.postgres — see the comment there.
DUCKLAKE_CONNSTR_DEFAULT = (
    "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita sslmode=disable"
)
LIB_PATH_ENV = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"


def ducklake_catalog_connstr() -> str:
    return os.environ.get("DUCKLAKE_CATALOG_CONNSTR", DUCKLAKE_CONNSTR_DEFAULT)


def find_duckdb_lib_dir(data_plane_dir: Path) -> Path | None:
    """Locate libduckdb downloaded by the Rust build (DUCKDB_DOWNLOAD_LIB=1).

    Path is target/duckdb-download/<rust-triple>/<duckdb-version>/. Both vary
    per host and per dependency bump, so glob and match the libduckdb file
    name for the current OS so a cross-built directory cannot shadow it.
    """
    base = data_plane_dir / "target" / "duckdb-download"
    if not base.exists():
        return None
    libname = "libduckdb.dylib" if sys.platform == "darwin" else "libduckdb.so"
    for candidate in sorted(base.glob("*/*"), reverse=True):
        if (candidate / libname).is_file():
            return candidate
    return None


__all__ = [
    "DUCKLAKE_CONNSTR_DEFAULT",
    "LIB_PATH_ENV",
    "ducklake_catalog_connstr",
    "find_duckdb_lib_dir",
    "postgres_url",
]
