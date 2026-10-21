"""Full-span integration test: the batch multi-study ENA import driver threaded
end-to-end into a real DuckLake `read` table, in one process — batch driver
(real registration + a real submitted work_ticket) -> `ingest_ena_reads.execute`
(the ENA fetch and mint monkeypatched, no network) -> real `register-files`
against a real data plane and DuckLake catalog.

The one fixture study resolves two runs sharing ONE sample accession on the same
platform — the shape that exercises the full span at once: one `sequenced_pool`
covering both runs and one de-duplicated biosample.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest
import pytest_asyncio
import yaml
from conftest import cleanup_ena_study, ducklake_connect
from qiita_common.actions import ActionDefinition, WorkflowAction
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.auth_constants import SystemRole
from qiita_common.models import FailureType, WorkTicketState
from qiita_common.models.ena_import import BatchItemState
from qiita_control_plane.auth.principal import HumanUser
from qiita_control_plane.dispatch import build_dispatch_semaphore
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.ena_import.batch import (
    _process_one_study,
    build_ena_import_study_semaphore,
    create_ena_import_batch,
    fetch_batch_status,
    schedule_ena_import_batch,
)
from qiita_control_plane.ena_import.registration import EnaRunRegistrationStatus
from qiita_control_plane.repositories.sequenced_sample import (
    fetch_sequenced_pool_ena_run_roster,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal
from qiita_control_plane.testing.db_teardown import delete_principal

from qiita_control_plane.testing.unique_names import (
    unique_accession,
    unique_ena_accession,
)

_DOWNLOAD_ENA_STUDY_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "download-ena-study" / "1.0.0.yaml"
)

# Network-free resolver seam.
_QUERY_STUDY = "qiita_control_plane.ena_import.miint_resolver._query_ena_study_header"
_QUERY_RUNS = "qiita_control_plane.ena_import.miint_resolver._query_ena_runs"
_QUERY_ATTRS = "qiita_control_plane.ena_import.miint_resolver._query_ena_sample_attributes"

_RUN_COLUMNS = (
    "run_accession",
    "experiment_accession",
    "sample_accession",
    "study_accession",
    "library_layout",
    "library_strategy",
    "library_source",
    "library_selection",
    "instrument_platform",
    "fastq_ftp",
    "fastq_aspera",
    "fastq_bytes",
    "fastq_md5",
    "read_count",
    "base_count",
    "status",
)


def _fake_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    return (
        ["study_accession", "secondary_study_accession", "study_title", "status"],
        [(accession, None, f"title for {accession}", "public")],
    )


def _make_two_runs_sharing_one_sample(shared_sample_accession: str):
    """Build a `(runs, attrs)` fake-resolver pair for one study whose two runs
    share ONE `sample_accession`, both ILLUMINA (a single `sequenced_pool`), so
    the full span exercises intra-study biosample de-dup."""

    def _fake_runs(accession: str) -> tuple[list[str], list[tuple]]:
        rows = [
            (
                f"SRR-{accession}-1",
                f"SRX-{accession}-1",
                shared_sample_accession,
                accession,
                "SINGLE",
                "WGS",
                "GENOMIC",
                None,
                "ILLUMINA",
                [],
                [],
                [],
                [],
                None,
                None,
                "public",
            ),
            (
                f"SRR-{accession}-2",
                f"SRX-{accession}-2",
                shared_sample_accession,
                accession,
                "SINGLE",
                "WGS",
                "GENOMIC",
                None,
                "ILLUMINA",
                [],
                [],
                [],
                [],
                None,
                None,
                "public",
            ),
        ]
        return list(_RUN_COLUMNS), rows

    def _fake_attrs(accession: str) -> list[tuple[str, dict[str, list[str]]]]:
        return [(shared_sample_accession, {"collection date": ["2020-01-01"]})]

    return _fake_runs, _fake_attrs


def _download_ena_study_action_entries():
    """Parse the shipped download-ena-study YAML and return its `action:`
    WorkflowAction entries in declared order."""
    data = yaml.safe_load(_DOWNLOAD_ENA_STUDY_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    return [e for e in action.steps if isinstance(e, WorkflowAction)]


def _entry_by_name(name: str):
    for entry in _download_ena_study_action_entries():
        if entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in download-ena-study YAML")


def _write_ena_run_map(path: Path, roster: list[tuple[int, str]]) -> None:
    """Write the `(prep_sample_idx, ena_run_accession)` roster Parquet the runner
    materializes for the step."""
    rows = ", ".join(f"({idx}, '{acc}')" for idx, acc in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES " + rows + ") AS t(prep_sample_idx, ena_run_accession)) "
            f"TO '{path}' (FORMAT parquet)"
        )


def _write_intermediate(
    path: Path,
    rows: list[tuple[int, str, str, list[int] | None, str | None, list[int] | None]],
) -> None:
    """Write the `_stage_run_reads` intermediate shape."""
    with duckdb.connect(":memory:") as conn:
        values = ", ".join(
            "(CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
            "CAST(? AS UTINYINT[]), CAST(? AS VARCHAR), CAST(? AS UTINYINT[]))"
            for _ in rows
        )
        params: list = []
        for sidx, rid, s1, q1, s2, q2 in rows:
            params.extend([sidx, rid, s1, q1, s2, q2])
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            "AS t(sequence_index, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{path}' (FORMAT PARQUET)",
            params,
        )


def _fake_stage_run_reads_factory(by_run: dict[str, tuple[list[tuple], list[str]]]):
    """Build a `_stage_run_reads`-shaped fake keyed by run_accession."""

    def _fake(
        run_accession,
        download_method,
        intermediate_path,
        duckdb_tmp,
        memory_gb,
        threads,
    ):
        rows, warnings = by_run[run_accession]
        _write_intermediate(intermediate_path, rows)
        return len(rows), warnings

    return _fake


def _data_plane_url(data_plane) -> str:
    return f"grpc://{LOOPBACK_HOST}:{data_plane['port']}"


def _count_read_rows(data_plane, prep_sample_idxs: list[int]) -> int:
    conn = ducklake_connect(data_plane["data_path"])
    try:
        (n,) = conn.execute(
            "SELECT count(*) FROM qiita_lake.read WHERE prep_sample_idx = ANY(?)",
            [list(prep_sample_idxs)],
        ).fetchone()
        return n
    finally:
        conn.close()


async def _run_register_files(
    postgres_pool, data_plane, *, staging_dir: Path, work_ticket_idx: int
):
    """Drive the REAL runner adapter for the download-ena-study `register-files`
    entry."""
    from qiita_control_plane.runner import _run_action_primitive

    entry = _entry_by_name("register-files")
    await _run_action_primitive(
        postgres_pool,
        entry,
        {"read_staging_dir": str(staging_dir)},
        staging_dir,
        {},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )


@pytest.fixture(autouse=True)
def _patch_run_and_log(monkeypatch):
    """No-op the actual dispatch of the submitted download-ena-study ticket --
    this test drives its native step directly, not through a live orchestrator."""

    async def _noop(_app, _idx, **_kwargs):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest_asyncio.fixture
async def batch_app(postgres_pool):
    """The shared main.app, configured for direct (non-HTTP) calls into the batch
    driver."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    saved_compute_backend_client = getattr(app.state, "compute_backend_client", None)
    app.state.compute_backend_client = object()
    app.state.running_dispatches = set()
    app.state.running_ena_import_batches = set()
    app.state.dispatch_semaphore = build_dispatch_semaphore()

    yield app

    pending = list(app.state.running_dispatches) + list(app.state.running_ena_import_batches)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    app.state.compute_backend_client = saved_compute_backend_client


@pytest_asyncio.fixture
async def admin_principal(postgres_pool):
    """A real seeded wet_lab_admin principal."""
    pidx = await seed_user_principal(
        postgres_pool,
        prefix="ena-e2e-admin",
        suffix="t07",
        system_role=SystemRole.WET_LAB_ADMIN,
    )
    principal = HumanUser(
        principal_idx=pidx,
        email=f"ena-e2e-admin-{pidx}@test.local",
        system_role=SystemRole.WET_LAB_ADMIN,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    yield principal
    await delete_principal(postgres_pool, pidx)


@pytest_asyncio.fixture
async def download_ena_study_action(postgres_pool):
    """Seed the real, pinned `download-ena-study`/`1.0.0` action row so
    `submit_work_ticket_core` can resolve it."""
    steps = [
        {
            "kind": "step",
            "name": "ingest_ena_reads",
            "step_type": "singleton",
            "module": "qiita_compute_orchestrator.jobs.ingest_ena_reads",
            "inputs": ["ena_run_map", "reads_staging_root"],
            "outputs": ["read_staging_dir"],
            "baseline_resources": {"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
        }
    ]
    await postgres_pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, target_processing_kinds,"
        "  scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling,"
        "  success_status, failure_status"
        ") VALUES ($1, $2, 'sequenced_pool'::qiita.scope_target_kind,"
        "          '{}'::qiita.processing_kind[], '{}'::text[], $3::jsonb,"
        "          $4::jsonb, $5::jsonb, 1, 1, '1 minute', 'active', 'failed')",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        json.dumps({"service": False, "human_roles": ["wet_lab_admin", "system_admin"]}),
        json.dumps(
            {
                "type": "object",
                "required": ["ena_study_accession"],
                "properties": {
                    "ena_study_accession": {"type": "string", "minLength": 1},
                    "download_method": {"type": "string", "enum": ["http"]},
                },
            }
        ),
        json.dumps(steps),
    )
    yield DOWNLOAD_ENA_STUDY_ACTION_ID, DOWNLOAD_ENA_STUDY_ACTION_VERSION
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )


@pytest_asyncio.fixture
async def batch_cleanup(postgres_pool):
    batch_idxs: list[int] = []
    yield batch_idxs
    if batch_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.ena_import_batch WHERE idx = ANY($1::bigint[])",
            batch_idxs,
        )


@pytest_asyncio.fixture
async def ena_study_cleanup(postgres_pool):
    """Accessions whose registered study graph is torn down after the test.

    A test appends its accession as soon as it has one, so a failed assert
    leaves no study, sample graph or sequencing run behind for the rest of the
    session.
    """
    accessions: list[str] = []
    yield accessions
    for accession in accessions:
        await cleanup_ena_study(postgres_pool, accession)


# Fixture reads for the two runs sharing one biosample: 3 for run 1, 2 for
# run 2 -- small, deterministic, distinct counts so a mismatch between the
# two prep_samples' rows is easy to see.
_RUN1_ROWS = [
    (1, "ERR-r1a", "ACGTACGTAC", None, None, None),
    (2, "ERR-r1b", "GGGGCCCCTT", None, None, None),
    (3, "ERR-r1c", "TTTTAAAACC", None, None, None),
]
_RUN2_ROWS = [
    (1, "ERR-r2a", "AAAACCCCGG", None, None, None),
    (2, "ERR-r2b", "TGCATGCATG", None, None, None),
]


async def test_batch_driver_to_register_files_to_ducklake_full_span(
    postgres_pool,
    data_plane,
    batch_app,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    ena_study_cleanup,
    tmp_path,
    monkeypatch,
):
    """The full span: batch driver -> real registration -> real
    `ingest_ena_reads` -> real `register-files` -> real DuckLake `read`.
    Then re-runs the batch driver's per-study step a second time and
    asserts registration-level idempotency (no new study/biosample, no
    duplicate reads)."""
    accession = unique_ena_accession("PRJNA")
    ena_study_cleanup.append(accession)
    shared_sample_accession = unique_accession("SAMN")
    fake_runs, fake_attrs = _make_two_runs_sharing_one_sample(shared_sample_accession)
    monkeypatch.setattr(_QUERY_STUDY, _fake_study_header)
    monkeypatch.setattr(_QUERY_RUNS, fake_runs)
    monkeypatch.setattr(_QUERY_ATTRS, fake_attrs)

    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    await _process_one_study(
        batch_app,
        postgres_pool,
        item=items[0],
        principal=admin_principal,
    )

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, download_work_ticket_idxs, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value, item_row["failure_reason"]
    assert item_row["study_idx"] is not None
    assert len(item_row["download_work_ticket_idxs"]) == 1
    work_ticket_idx = item_row["download_work_ticket_idxs"][0]

    # --- Assert: exactly 1 study row for this accession. ---
    study_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", accession
    )
    assert study_count == 1

    # --- Assert: the two runs' shared sample_accession de-duplicated to ONE
    # biosample, linked to the one study exactly once. ---
    biosample_rows = await postgres_pool.fetch(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    assert len(biosample_rows) == 1
    biosample_idx = biosample_rows[0]["idx"]
    link_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.biosample_to_study WHERE biosample_idx = $1 AND study_idx = $2",
        biosample_idx,
        item_row["study_idx"],
    )
    assert link_count == 1

    # --- Real runner-shaped roster from the ticket's own sequenced_pool --
    # exactly what the runner's _stage_ena_run_roster would produce. ---
    ticket_row = await postgres_pool.fetchrow(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    sequenced_pool_idx = ticket_row["sequenced_pool_idx"]
    assert sequenced_pool_idx is not None
    sequencing_run_idx = await postgres_pool.fetchval(
        "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1",
        sequenced_pool_idx,
    )
    roster_rows = await fetch_sequenced_pool_ena_run_roster(
        postgres_pool, sequenced_pool_idx=sequenced_pool_idx
    )
    assert len(roster_rows) == 2
    roster = [(r["prep_sample_idx"], r["ena_run_accession"]) for r in roster_rows]
    prep_sample_idxs = sorted(psi for psi, _ in roster)

    # --- No network: the whole read_ena_sequences seam is replaced with an
    # inline DuckDB COPY, keyed by the REAL ena_run_accession the roster
    # carries. ---
    by_run = {
        roster[0][1]: (_RUN1_ROWS, []),
        roster[1][1]: (_RUN2_ROWS, []),
    }
    from qiita_compute_orchestrator import sequence_range_retry
    from qiita_compute_orchestrator.jobs import ingest_ena_reads
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

    monkeypatch.setattr(ingest_ena_reads, "_stage_run_reads", _fake_stage_run_reads_factory(by_run))

    mint_calls: list[tuple[int, int]] = []

    async def _local_mint(*, http, prep_sample_idx, count, work_ticket_idx):
        mint_calls.append((prep_sample_idx, count))
        row = await postgres_pool.fetchrow(
            "SELECT * FROM qiita.mint_sequence_range($1, $2, $3, $4)",
            prep_sample_idx,
            count,
            admin_principal.principal_idx,
            work_ticket_idx,
        )
        return MintedSequenceRange(
            prep_sample_idx=row["prep_sample_idx"],
            sequence_idx_start=row["sequence_idx_start"],
            sequence_idx_stop=row["sequence_idx_stop"],
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _local_mint)

    ena_run_map_path = tmp_path / "ena_run_map.parquet"
    _write_ena_run_map(ena_run_map_path, roster)
    inputs = ingest_ena_reads.Inputs(
        ena_run_map=ena_run_map_path,
        reads_staging_root=tmp_path / "reads-staging",
        sequenced_pool_idx=sequenced_pool_idx,
        sequencing_run_idx=sequencing_run_idx,
        work_ticket_idx=work_ticket_idx,
    )

    outputs = await ingest_ena_reads.execute(inputs, tmp_path / "ws1")
    total_reads = len(_RUN1_ROWS) + len(_RUN2_ROWS)
    assert sum(count for _, count in mint_calls) == total_reads

    # --- Drive the REAL register-files tail into the REAL data plane. ---
    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=outputs["read_staging_dir"],
        work_ticket_idx=work_ticket_idx,
    )

    # --- Assert: read row counts equal the fixture read counts, split
    # correctly across the two prep_samples. ---
    assert _count_read_rows(data_plane, prep_sample_idxs) == total_reads
    expected_counts = (len(_RUN1_ROWS), len(_RUN2_ROWS))
    for prep_idx, expected in zip(prep_sample_idxs, expected_counts, strict=True):
        assert _count_read_rows(data_plane, [prep_idx]) == expected

    # Idempotency: re-run _process_one_study for the SAME item. Registration is
    # idempotent (both runs are already sequenced_sample rows, study/biosample
    # already exist), and this second call never touches
    # ingest_ena_reads/register-files, so the DuckLake read count is unchanged.
    await _process_one_study(
        batch_app,
        postgres_pool,
        item=items[0],
        principal=admin_principal,
    )

    study_count_after = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", accession
    )
    assert study_count_after == 1
    biosample_count_after = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    assert biosample_count_after == 1
    assert _count_read_rows(data_plane, prep_sample_idxs) == total_reads


async def test_run_list_import_ingests_only_the_listed_runs(
    postgres_pool,
    data_plane,
    batch_app,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    ena_study_cleanup,
    tmp_path,
    monkeypatch,
):
    """Two of a study's three runs: the pool roster and DuckLake reads hold only those two."""
    accession = unique_ena_accession("PRJNA")
    ena_study_cleanup.append(accession)
    all_runs = [unique_ena_accession("SRR") for _ in range(3)]
    listed = all_runs[:2]
    _, template_rows = _make_two_runs_sharing_one_sample("unused")[0](accession)

    def _row(run: str) -> tuple:
        row = list(template_rows[0])
        row[0], row[1], row[2] = run, f"SRX{run[3:]}", f"SAMN{run[3:]}"
        return tuple(row)

    def fake_runs(query):
        wanted = all_runs if isinstance(query, str) else query
        return list(_RUN_COLUMNS), [_row(r) for r in all_runs if r in wanted]

    def fake_attrs(query):
        samples = [f"SAMN{r[3:]}" for r in all_runs] if isinstance(query, str) else query
        return [(s, {"collection date": ["2020-01-01"]}) for s in samples]

    monkeypatch.setattr(_QUERY_STUDY, _fake_study_header)
    monkeypatch.setattr(_QUERY_RUNS, fake_runs)
    monkeypatch.setattr(_QUERY_ATTRS, fake_attrs)
    batch_app.state.ena_import_study_semaphore = build_ena_import_study_semaphore()

    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=listed, principal=admin_principal
    )
    batch_cleanup.append(batch_idx)
    await schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)

    item_row = await postgres_pool.fetchrow(
        "SELECT state, ena_study_accession, download_work_ticket_idxs, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value, item_row["failure_reason"]
    assert item_row["ena_study_accession"] == accession
    (work_ticket_idx,) = item_row["download_work_ticket_idxs"]

    sequenced_pool_idx = await postgres_pool.fetchval(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    sequencing_run_idx = await postgres_pool.fetchval(
        "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx
    )
    roster_rows = await fetch_sequenced_pool_ena_run_roster(
        postgres_pool, sequenced_pool_idx=sequenced_pool_idx
    )
    roster = sorted((r["ena_run_accession"], r["prep_sample_idx"]) for r in roster_rows)
    assert [run for run, _ in roster] == sorted(listed)
    prep_sample_idxs = [psi for _, psi in roster]

    by_run = {roster[0][0]: (_RUN1_ROWS, []), roster[1][0]: (_RUN2_ROWS, [])}
    from qiita_compute_orchestrator import sequence_range_retry
    from qiita_compute_orchestrator.jobs import ingest_ena_reads
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

    monkeypatch.setattr(ingest_ena_reads, "_stage_run_reads", _fake_stage_run_reads_factory(by_run))

    async def _local_mint(*, http, prep_sample_idx, count, work_ticket_idx):
        row = await postgres_pool.fetchrow(
            "SELECT * FROM qiita.mint_sequence_range($1, $2, $3, $4)",
            prep_sample_idx,
            count,
            admin_principal.principal_idx,
            work_ticket_idx,
        )
        return MintedSequenceRange(
            prep_sample_idx=row["prep_sample_idx"],
            sequence_idx_start=row["sequence_idx_start"],
            sequence_idx_stop=row["sequence_idx_stop"],
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _local_mint)

    ena_run_map_path = tmp_path / "ena_run_map.parquet"
    _write_ena_run_map(ena_run_map_path, [(psi, run) for run, psi in roster])
    outputs = await ingest_ena_reads.execute(
        ingest_ena_reads.Inputs(
            ena_run_map=ena_run_map_path,
            reads_staging_root=tmp_path / "reads-staging",
            sequenced_pool_idx=sequenced_pool_idx,
            sequencing_run_idx=sequencing_run_idx,
            work_ticket_idx=work_ticket_idx,
        ),
        tmp_path / "ws1",
    )
    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=outputs["read_staging_dir"],
        work_ticket_idx=work_ticket_idx,
    )

    assert _count_read_rows(data_plane, prep_sample_idxs) == len(_RUN1_ROWS) + len(_RUN2_ROWS)
    unlisted_prep_samples = await postgres_pool.fetch(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        all_runs[2],
    )
    assert unlisted_prep_samples == []


async def test_pool_with_a_permanently_failed_run_lands_the_rest_and_reads_completed_partial(
    postgres_pool,
    data_plane,
    batch_app,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    ena_study_cleanup,
    tmp_path,
    monkeypatch,
):
    """One run has no sequence data: its pool's other run is stored and loaded, the
    failed run is recorded, and the item reads `completed_partial`."""
    accession = unique_ena_accession("PRJNA")
    ena_study_cleanup.append(accession)
    fake_runs, fake_attrs = _make_two_runs_sharing_one_sample(unique_accession("SAMN"))
    monkeypatch.setattr(_QUERY_STUDY, _fake_study_header)
    monkeypatch.setattr(_QUERY_RUNS, fake_runs)
    monkeypatch.setattr(_QUERY_ATTRS, fake_attrs)

    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)
    await _process_one_study(batch_app, postgres_pool, item=items[0], principal=admin_principal)
    (work_ticket_idx,) = await postgres_pool.fetchval(
        "SELECT download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    sequenced_pool_idx = await postgres_pool.fetchval(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    sequencing_run_idx = await postgres_pool.fetchval(
        "SELECT sequencing_run_idx FROM qiita.sequenced_pool WHERE idx = $1", sequenced_pool_idx
    )
    roster_rows = await fetch_sequenced_pool_ena_run_roster(
        postgres_pool, sequenced_pool_idx=sequenced_pool_idx
    )
    roster = sorted((r["ena_run_accession"], r["prep_sample_idx"]) for r in roster_rows)
    (bad_run, bad_psi), (good_run, good_psi) = roster

    from qiita_compute_orchestrator import sequence_range_retry
    from qiita_compute_orchestrator.jobs import ingest_ena_reads
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

    good_fake = _fake_stage_run_reads_factory({good_run: (_RUN1_ROWS, [])})

    def _stage(run_accession, *args):
        if run_accession == bad_run:
            raise duckdb.IOException(
                "read_ena_sequences: no sequence data found for the provided accession(s)"
            )
        return good_fake(run_accession, *args)

    monkeypatch.setattr(ingest_ena_reads, "_stage_run_reads", _stage)

    async def _local_mint(*, http, prep_sample_idx, count, work_ticket_idx):
        row = await postgres_pool.fetchrow(
            "SELECT * FROM qiita.mint_sequence_range($1, $2, $3, $4)",
            prep_sample_idx,
            count,
            admin_principal.principal_idx,
            work_ticket_idx,
        )
        return MintedSequenceRange(
            prep_sample_idx=row["prep_sample_idx"],
            sequence_idx_start=row["sequence_idx_start"],
            sequence_idx_stop=row["sequence_idx_stop"],
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _local_mint)

    ena_run_map_path = tmp_path / "ena_run_map.parquet"
    _write_ena_run_map(ena_run_map_path, [(psi, run) for run, psi in roster])
    outputs = await ingest_ena_reads.execute(
        ingest_ena_reads.Inputs(
            ena_run_map=ena_run_map_path,
            reads_staging_root=tmp_path / "reads-staging",
            sequenced_pool_idx=sequenced_pool_idx,
            sequencing_run_idx=sequencing_run_idx,
            work_ticket_idx=work_ticket_idx,
        ),
        tmp_path / "ws1",
    )
    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=outputs["read_staging_dir"],
        work_ticket_idx=work_ticket_idx,
    )
    from qiita_control_plane.runner import _run_action_primitive

    await _run_action_primitive(
        postgres_pool,
        _entry_by_name("persist-ena-run-outcomes"),
        {"run_outcomes": str(outputs["run_outcomes"])},
        tmp_path / "ws1",
        {"kind": "sequenced_pool", "sequenced_pool_idx": sequenced_pool_idx},
        work_ticket_idx=work_ticket_idx,
        signing_key=data_plane["secret"],
        data_plane_url=_data_plane_url(data_plane),
    )

    assert _count_read_rows(data_plane, [good_psi]) == len(_RUN1_ROWS)
    assert _count_read_rows(data_plane, [bad_psi]) == 0
    recorded = await postgres_pool.fetch(
        "SELECT ena_run_accession, download_failure_type AS failure_type,"
        "       download_failure_reason FROM qiita.sequenced_sample"
        " WHERE sequenced_pool_idx = $1 AND download_failure_type IS NOT NULL",
        sequenced_pool_idx,
    )
    assert [(r["ena_run_accession"], r["failure_type"]) for r in recorded] == [
        (bad_run, FailureType.PERMANENT)
    ]
    assert "no sequence data found" in recorded[0]["download_failure_reason"]

    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = $2 WHERE work_ticket_idx = $1",
        work_ticket_idx,
        WorkTicketState.COMPLETED.value,
    )
    (item,) = (await fetch_batch_status(postgres_pool, batch_idx=batch_idx)).items
    assert item.state == BatchItemState.COMPLETED_PARTIAL
    by_run = {r.run_accession: r for r in item.ena_runs}
    assert by_run[bad_run].status == EnaRunRegistrationStatus.DOWNLOAD_FAILED.value
    assert by_run[bad_run].failure_type == FailureType.PERMANENT
    assert by_run[good_run].status == EnaRunRegistrationStatus.REGISTERED.value
