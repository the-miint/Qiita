"""Shared fixtures for the control-plane test suite.

Imports the postgres / sessions / jwks / fasta fixtures from
qiita_control_plane.testing so DB-bound tests under tests/auth/ and
tests/routes/ can use them. Pure-unit tests at this directory's top level
ignore the imported fixtures and don't trigger the postgres_pool fixture
unless they request it.
"""

from pathlib import Path

import pytest

from qiita_control_plane.testing.jwks import jwks_harness  # noqa: F401
from qiita_control_plane.testing.postgres import (  # noqa: F401
    _run_db_migrations,
    postgres_pool,
    postgres_url,
)
from qiita_control_plane.testing.sessions import (  # noqa: F401
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
