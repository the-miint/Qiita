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
from qiita_common.models.biosample import MAX_ROSTER_TUBE_TEXT_LENGTH  # noqa: E402
from run_preflight import (  # noqa: E402
    load_db_file,
    save_db_file,
    set_input_sample_identifier,
    set_project_identifier,
)
from run_preflight.constants import (  # noqa: E402
    InputSampleIdentifierKind,
    ProjectIdentifierKind,
)
from run_preflight.db import get_input_sample_project_info  # noqa: E402

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
from qiita_control_plane.testing.terminology import created_terminologies  # noqa: E402, F401


@pytest.fixture(scope="session", autouse=True)
def _stage_miint_extension():
    """Stage miint once into the per-component extension dir, so the LOAD-only
    paths in this suite find it — the unit-tier mirror of what the deploy stages
    and what the orchestrator's and integration conftests already do.

    **Not optional, and not merely a speed-up.** `connect_with_miint_staged()` is
    the service-side helper and is LOAD-only by design, so nothing it drives will
    ever install. What made that work here was another module in the same session
    happening to INSTALL first (the `reference load` CLI tests use the client
    helper) into a directory that caches across runs — so a warm developer machine
    passed while CI, which always starts cold, failed every real-miint test in the
    tier. Order-dependent either way, since test order is randomized.

    Plain INSTALL, not the deploy's FORCE, so the stable temp dir keeps caching:
    the first run downloads from the mirror and later ones are instant.
    """
    import duckdb  # noqa: PLC0415
    from qiita_common.duckdb_miint import (  # noqa: PLC0415
        miint_connect_config,
        miint_install_sql,
        miint_load_sql,
    )

    with duckdb.connect(":memory:", config=miint_connect_config()) as conn:
        conn.execute(miint_install_sql())
        conn.execute(miint_load_sql())


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

_PREFLIGHT_SAMPLES_SQL = "SELECT input_sample_idx, sample_name FROM input_sample"
_PREFLIGHT_PROJECTS_SQL = "SELECT external_project_id FROM project ORDER BY external_project_id"
# Each identifier kind's test value, from (input_sample_idx, sample_name) or from
# (1-based project position, external_project_id)
_SAMPLE_IDENTIFIER_VALUES = {
    "biosample_accession": lambda idx, name: f"BIO_{name}",
    "biosample_export_entity_id": lambda idx, name: f"QB{idx}",
}
_PROJECT_IDENTIFIER_VALUES = {
    "bioproject_accession": lambda position, external_id: f"PRJNA{external_id}",
    "study_export_entity_id": lambda position, external_id: f"QS{position}",
}
_OVERLONG_TUBE_LENGTH = MAX_ROSTER_TUBE_TEXT_LENGTH + 1
_OVERLONG_TUBE_MESSAGE = f"biosample matrix_tube_id: Value should have at most {MAX_ROSTER_TUBE_TEXT_LENGTH} items after validation, not {_OVERLONG_TUBE_LENGTH}"  # noqa: E501


@pytest.fixture
def set_preflight_identifiers():
    """Set one identifier kind on every input sample and one on every project of a
    pre-flight file, through kl-run-preflight's own setters. A kind of None leaves
    that side unset; each value comes from the `_*_IDENTIFIER_VALUES` maps."""

    def _set(
        db: Path,
        *,
        biosample_kind: InputSampleIdentifierKind | None,
        project_kind: ProjectIdentifierKind | None,
    ) -> None:
        conn = load_db_file(str(db))
        try:
            # Every input sample gets the biosample identifier, found by its name
            if biosample_kind is not None:
                make_value = _SAMPLE_IDENTIFIER_VALUES[biosample_kind]
                for idx, name in conn.execute(_PREFLIGHT_SAMPLES_SQL).fetchall():
                    value = make_value(idx, name)
                    set_input_sample_identifier(conn, biosample_kind, value, sample_name=name)

            # Every project gets the study identifier, found by its external id
            if project_kind is not None:
                make_value = _PROJECT_IDENTIFIER_VALUES[project_kind]
                external_ids = [r[0] for r in conn.execute(_PREFLIGHT_PROJECTS_SQL)]
                for position, external_id in enumerate(external_ids, start=1):
                    value = make_value(position, external_id)
                    set_project_identifier(
                        conn, project_kind, value, external_project_id=external_id
                    )
            save_db_file(conn, str(db))
        finally:
            conn.close()

    return _set


@pytest.fixture
def set_overlong_matrix_tube_ids():
    """Give every input sample of a pre-flight file a matrix_tube_id one character
    longer than a roster ref allows, through kl-run-preflight's own reader and
    setter."""

    def _set(db: Path) -> None:
        conn = load_db_file(str(db))
        try:
            # Each sample gets its own value, so no two share one
            project_info = get_input_sample_project_info(conn, include_do_not_use=True)
            names = sorted({name for name, _, _ in project_info})
            for position, name in enumerate(names):
                value = str(position).zfill(_OVERLONG_TUBE_LENGTH)
                set_input_sample_identifier(conn, "matrix_tube_id", value, sample_name=name)
            save_db_file(conn, str(db))
        finally:
            conn.close()

    return _set


@pytest.fixture
def overlong_tube_id() -> str:
    """A matrix_tube_id one character longer than a roster ref allows."""
    return "1" * _OVERLONG_TUBE_LENGTH


@pytest.fixture
def overlong_tube_message() -> str:
    """The refusal a biosample ref gives for an over-long matrix_tube_id."""
    return _OVERLONG_TUBE_MESSAGE


@pytest.fixture
def build_case5_preflight(tmp_path, set_preflight_identifiers):
    """Build a real kl-run-preflight SQLite from the pinned case-5 fixture.

    ONE builder, shared by every tier that needs a PacBio pre-flight: the ingest
    CLI's reader tests, the server-side `preflight` reader tests (incl. the
    CLI-vs-route parity pin), and the pool-roster route test. They must all parse
    the SAME bytes, or the parity pin proves nothing.

    Uses run_preflight's own CSV loader, so the seam is exercised against the true
    schema and the real `get_pacbio_sample_info`, which requires at least one
    identifier on each sample's biosample and projects. The case-5 CSV sets none, so
    `populate_accessions=True` sets the biosample and bioproject accessions through
    `set_preflight_identifiers`.

    `sheet_type` overrides the run's legacy sheet type, for building a well-formed
    blob the PacBio reader must decline (it keys on that field).
    """
    from run_preflight.legacy.api import migrate_legacy_csv_to_db_file

    def _build(
        *, populate_accessions: bool = True, sheet_type: str | None = None, name: str = "case5.db"
    ) -> Path:
        db = tmp_path / name
        migrate_legacy_csv_to_db_file(str(_CASE5_CSV), str(db))
        if populate_accessions:
            set_preflight_identifiers(
                db, biosample_kind="biosample_accession", project_kind="bioproject_accession"
            )
        if sheet_type is not None:
            # Retarget only THIS run's format row: a blanket UPDATE collides on
            # the (legacy_sheet_type, legacy_version) unique constraint.
            conn = sqlite3.connect(db)
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


_AMPLICON_V1_SQLITE_GZ = (
    Path(__file__).resolve().parent / "cli" / "data" / "good_amplicon_v1.sqlite.gz"
)


@pytest.fixture
def build_amplicon_preflight(tmp_path, set_preflight_identifiers):
    """Build a real kl-run-preflight amplicon SQLite from the committed fixture.

    The amplicon twin of `build_case5_preflight`: ONE builder shared by the ingest
    CLI's reader tests and the server-side `preflight` reader tests (incl. the
    CLI-vs-runner parity pin — the runner reads this roster, no route does), so
    both parse the SAME bytes.

    Decompresses the committed migrated preflight (the artifact Qiita-MIINT consumes —
    it never migrates a classic prep template). `get_amplicon_sample_info` requires at
    least one identifier on each sample's biosample and projects, and the committed
    file sets none, so `populate_accessions=True` sets the biosample and bioproject
    accessions through `set_preflight_identifiers`, which also brings the file up to
    the library's current schema; with `populate_accessions=False` the file keeps the
    committed file's older schema.

    `barcodes_are_rc` overrides the committed file's run-level orientation (it is
    True, the EMP 515rcbc set).
    """
    import gzip

    def _build(
        *,
        populate_accessions: bool = True,
        barcodes_are_rc: bool | None = None,
        name: str = "amplicon_v1.db",
    ) -> Path:
        db = tmp_path / name
        db.write_bytes(gzip.decompress(_AMPLICON_V1_SQLITE_GZ.read_bytes()))
        if populate_accessions:
            set_preflight_identifiers(
                db, biosample_kind="biosample_accession", project_kind="bioproject_accession"
            )
        if barcodes_are_rc is not None:
            # Run-level: amplicon_run carries it; run_amplicon_sample stamps it per row.
            conn = sqlite3.connect(db)
            conn.execute("UPDATE amplicon_run SET barcodes_are_rc = ?", (int(barcodes_are_rc),))
            conn.commit()
            conn.close()
        return db

    return _build
