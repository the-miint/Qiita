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
import pytest_asyncio  # noqa: E402

from qiita_control_plane.notify.sweeper import _OWED_SET_WHERE  # noqa: E402
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


@pytest_asyncio.fixture(autouse=True)
async def _no_leaked_owed_tickets(request):
    """Fail the test that LEAKS an owed work ticket, not the innocent test that
    later trips over it.

    The test database is isolated per xdist *worker*, not per test, and the notify
    sweeper's owed-set query (`sweeper._OWED_SET_WHERE`) scans all of it. So a test
    that seeds a terminal ticket and doesn't clean it up makes the sweeper email that
    ticket's originator — and the failure surfaces as `assert 5 == 1` in
    test_notify_sweeper, a file the leaker never touched, and only when the runner's
    core count happens to co-locate the two. That is a genuinely hostile debug.

    Autouse fixtures finalize last, so this runs after the test's own teardown. It is
    a no-op for the vast majority of tests, which never touch the DB: it only queries
    when a test actually built one.

    It compares against a BASELINE taken before the test, rather than asserting the
    owed set is empty. The worker database is reused across pytest sessions, so an
    absolute check would blame each test for whatever an earlier, buggy run left
    behind — a tripwire that fires on someone else's mess is worse than none.
    """
    if "postgres_pool" not in request.fixturenames:
        yield  # a pure-unit test: no database was ever built
        return

    async def _owed(pool) -> set[int]:
        rows = await pool.fetch(
            f"SELECT work_ticket_idx FROM qiita.work_ticket WHERE {_OWED_SET_WHERE}"
        )
        return {r["work_ticket_idx"] for r in rows}

    # Session-scoped, so it is still alive at teardown — and already instantiated,
    # since the test requested it. Nothing is created just to run this check.
    pool = request.getfixturevalue("postgres_pool")
    before = await _owed(pool)
    yield
    leaked = sorted(await _owed(pool) - before)
    if leaked:
        raise AssertionError(
            f"{request.node.nodeid} leaked {len(leaked)} owed work ticket(s) {leaked} into "
            "the shared worker database. They are terminal with notified_at IS NULL — the "
            "notify sweeper's owed set — and the sweeper scans the whole database, so they "
            "will make it email their originators and fail an unrelated test. Delete them "
            "in your fixture's teardown."
        )
