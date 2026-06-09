"""Shared fixtures for the control-plane test suite.

Imports the postgres / sessions / jwks / fasta fixtures from
qiita_control_plane.testing so DB-bound tests under tests/auth/ and
tests/routes/ can use them. Pure-unit tests at this directory's top level
ignore the imported fixtures and don't trigger the postgres_pool fixture
unless they request it.
"""

from qiita_common.duckdb_miint import setup_miint_test_env

# The `qiita reference load` CLI parses FASTA with miint's read_fastx, so its
# unit tests (tests/cli/test_user_reference.py) install + load the miint
# extension. Point it at the team mirror + a per-component private extension dir
# before any test connects (shared with the orchestrator's conftest).
setup_miint_test_env("control-plane")

from pathlib import Path  # noqa: E402

import pytest  # noqa: E402

from qiita_control_plane.testing.jwks import jwks_harness  # noqa: E402, F401
from qiita_control_plane.testing.postgres import (  # noqa: E402, F401
    _run_db_migrations,
    postgres_pool,
    postgres_url,
)
from qiita_control_plane.testing.sessions import (  # noqa: E402, F401
    compute_worker_service_account,
    human_admin_session,
    regular_user_session,
    wet_lab_admin_session,
)


@pytest.fixture(scope="session")
def migrations_dir() -> Path:
    """Path to qiita-control-plane/db/migrations from this conftest.

    Resolved from `__file__` because conftest is loaded directly from disk by
    pytest, never copied into site-packages."""
    return Path(__file__).resolve().parent.parent / "db" / "migrations"
