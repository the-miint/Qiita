"""Shared integration-test plumbing for Postgres and the data plane subprocess.

Both `conftest.py` and `test_system_gg2_backbone.py` need the same env-var
overrides (so CI can point them at a host-provisioned Postgres on macOS where
Docker isn't available) and the same host-OS-aware paths when spawning the
data plane binary. They live here so the two files cannot drift.
"""

import os
import sys
from pathlib import Path

POSTGRES_URL_DEFAULT = "postgresql://qiita:qiita@localhost:5433/qiita_test"
DUCKLAKE_CONNSTR_DEFAULT = (
    "dbname=qiita_ducklake host=localhost port=5433 user=qiita password=qiita"
)
LIB_PATH_ENV = "DYLD_LIBRARY_PATH" if sys.platform == "darwin" else "LD_LIBRARY_PATH"


def postgres_url() -> str:
    return os.environ.get("QIITA_TEST_POSTGRES_URL", POSTGRES_URL_DEFAULT)


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
