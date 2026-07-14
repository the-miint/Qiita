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

import sqlite3  # noqa: E402
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
    ticket's originator, and the failure lands on whichever test asserts against a
    global sweep tally — in a file the leaker never touched, and only when the
    scheduler happens to co-locate the two on one worker. That is a hostile debug:
    the symptom names the victim, never the culprit. This names the culprit.

    A function-scoped autouse fixture is set up before the test's other function-scoped
    fixtures, so it finalizes after them — including the one that seeded the tickets.
    It is a no-op for the tests that never touch the DB: it only queries when a test
    actually built one.

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


_CASE5_CSV = Path(__file__).resolve().parent / "cli" / "data" / "good_pacbio_absquantv11.csv"


@pytest.fixture
def build_case5_preflight(tmp_path):
    """Build a real kl-run-preflight SQLite from the pinned case-5 fixture.

    ONE builder, shared by every tier that needs a PacBio pre-flight: the ingest
    CLI's reader tests, the server-side `preflight` reader tests (incl. the
    CLI-vs-route parity pin), and the pool-roster route test. They must all parse
    the SAME bytes, or the parity pin proves nothing.

    Uses run_preflight's own CSV loader, so the seam is exercised against the true
    schema and the real `get_pacbio_sample_info`. The fixture leaves the biosample +
    project **bioproject** accessions NULL (they are populated upstream in
    production); `get_pacbio_sample_info` REQUIRES both and raises otherwise, so
    `populate_accessions=True` sets them via plain sqlite (run_preflight's
    `save_db_file` is avoided — it blocks in this harness). biosample -> BIO_<name>;
    the single project's bioproject -> PRJNA<external_project_id>.

    `sheet_type` overrides the run's legacy sheet type, for building a well-formed
    blob the PacBio reader must decline (it keys on that field).
    """
    from run_preflight.legacy.api import migrate_legacy_csv_to_db_file

    def _build(
        *, populate_accessions: bool = True, sheet_type: str | None = None, name: str = "case5.db"
    ) -> Path:
        db = tmp_path / name
        migrate_legacy_csv_to_db_file(str(_CASE5_CSV), str(db))
        if populate_accessions or sheet_type is not None:
            conn = sqlite3.connect(db)
            if populate_accessions:
                conn.execute("UPDATE input_sample SET biosample_accession = 'BIO_' || sample_name")
                conn.execute(
                    "UPDATE project SET bioproject_accession = 'PRJNA' || external_project_id"
                )
            if sheet_type is not None:
                # Retarget only THIS run's format row: a blanket UPDATE collides on
                # the (legacy_sheet_type, legacy_version) unique constraint.
                conn.execute(
                    "UPDATE legacy_samplesheet_format SET legacy_sheet_type = ?"
                    " WHERE legacy_format_idx ="
                    " (SELECT legacy_format_idx FROM processing_run LIMIT 1)",
                    (sheet_type,),
                )
            conn.commit()
            conn.close()
        return db

    return _build
