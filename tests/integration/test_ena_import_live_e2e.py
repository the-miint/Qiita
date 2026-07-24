"""Gated live system tests: the ENA import surface against the REAL ENA network,
in two INDEPENDENT paths.

Two separate test bodies because no tiny ENA study carries BOTH a shared-biosample
run pair AND runs small enough to download in a test: PRJNA48739's two runs share
one biosample but are 22-89 MB apiece, while the smallest reliably tiny public run
(DRR037815, ~1.7 KB) belongs to a study with no shared-biosample pair. So (i) the
batch driver's metadata resolution + registration + de-dup, WITHOUT downloading
run bytes; (ii) the real read-download + DuckLake tail, reached DIRECTLY.

`@pytest.mark.system` keeps these out of CI (`make test-integration` passes `-m
'not system'`); they are a human-run `make test-system` gate. Both clean-skip when
the failure looks network/infra-shaped rather than a genuine regression.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import duckdb
import pytest
import pytest_asyncio
import yaml
from conftest import ducklake_connect
from qiita_common.actions import ActionDefinition, WorkflowAction
from qiita_common.api_paths import LOOPBACK_HOST
from qiita_common.auth_constants import SystemRole
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models.ena import ResolverKind, SourceArchive
from qiita_common.models.ena_import import BatchItemState

from qiita_control_plane.auth.principal import HumanUser
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.ena_import.batch import _process_one_study, create_ena_import_batch
from qiita_control_plane.testing.db_seeds import seed_user_principal

_DOWNLOAD_ENA_STUDY_YAML_PATH = (
    Path(__file__).parent.parent.parent / "workflows" / "download-ena-study" / "1.0.0.yaml"
)

# Markers meaning "network/infra unavailable, not a real bug" -- deliberately
# narrow (no "not found"/"extension", which also appear in genuine regressions
# like EnaAccessionNotFoundError and must still fail loud).
_NETWORK_ABSENT_MARKERS = (
    "connection",
    "timed out",
    "timeout",
    "network",
    "resolve host",
    "could not resolve",
    "unreachable",
    "temporarily",
    "curl",
    "dns",
    "name or service not known",
)


def _looks_like_network_absence(text: str) -> bool:
    lowered = text.lower()
    return any(marker in lowered for marker in _NETWORK_ABSENT_MARKERS)


# Shared helpers, duplicated per-suite rather than imported across test modules.


def _download_ena_study_action_entries():
    data = yaml.safe_load(_DOWNLOAD_ENA_STUDY_YAML_PATH.read_text())
    action = ActionDefinition.model_validate(data)
    return [e for e in action.steps if isinstance(e, WorkflowAction)]


def _entry_by_name(name: str):
    for entry in _download_ena_study_action_entries():
        if entry.name == name:
            return entry
    raise AssertionError(f"no action entry named {name!r} in download-ena-study YAML")


def _write_run_map(path: Path, roster: list[tuple[int, str]]) -> None:
    rows = ", ".join(f"({idx}, '{acc}')" for idx, acc in roster)
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES " + rows + ") AS t(prep_sample_idx, ena_run_accession)) "
            f"TO '{path}' (FORMAT parquet)"
        )


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
    """No-op the actual dispatch of a submitted download-ena-study ticket -- path
    (i) only checks registration and must never trigger a real download of
    PRJNA48739's 22-89 MB runs."""

    async def _noop(_app, _idx, **_kwargs):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest_asyncio.fixture
async def batch_app(postgres_pool):
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

    yield app

    pending = list(app.state.running_dispatches) + list(app.state.running_ena_import_batches)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    app.state.compute_backend_client = saved_compute_backend_client


@pytest_asyncio.fixture
async def admin_principal(postgres_pool):
    pidx = await seed_user_principal(
        postgres_pool,
        prefix="ena-live-admin",
        suffix="t07",
        system_role=SystemRole.WET_LAB_ADMIN,
    )
    principal = HumanUser(
        principal_idx=pidx,
        email=f"ena-live-admin-{pidx}@test.local",
        system_role=SystemRole.WET_LAB_ADMIN,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )
    yield principal
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", pidx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", pidx)


@pytest_asyncio.fixture
async def download_ena_study_action(postgres_pool):
    steps = [
        {
            "kind": "step",
            "name": "ingest_ena_reads",
            "step_type": "singleton",
            "module": "qiita_compute_orchestrator.jobs.ingest_ena_reads",
            "inputs": ["run_map", "reads_staging_root"],
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
            "DELETE FROM qiita.ena_import_batch WHERE idx = ANY($1::bigint[])", batch_idxs
        )


async def _cleanup_study(postgres_pool, study_accession: str) -> None:
    study_idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.study WHERE bioproject_accession = $1", study_accession
    )
    if study_idx is None:
        return
    await postgres_pool.execute(
        "DELETE FROM qiita.ena_import_batch_item WHERE study_idx = $1", study_idx
    )
    ps_rows = await postgres_pool.fetch(
        "SELECT prep_sample_idx FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
    )
    ps_idxs = [r["prep_sample_idx"] for r in ps_rows]
    if ps_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])", ps_idxs
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
    )
    if ps_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_idxs
        )
    bs_rows = await postgres_pool.fetch(
        "SELECT biosample_idx FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
    )
    bs_idxs = [r["biosample_idx"] for r in bs_rows]
    if bs_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])", bs_idxs
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_study_field WHERE study_idx = $1", study_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
    )
    if bs_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bs_idxs
        )
    run_rows = await postgres_pool.fetch(
        "SELECT idx FROM qiita.sequencing_run WHERE instrument_run_id LIKE $1",
        f"{study_accession}:%",
    )
    run_idxs = [r["idx"] for r in run_rows]
    if run_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx IN"
            " (SELECT idx FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[]))",
            run_idxs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[])",
            run_idxs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.sequencing_run WHERE idx = ANY($1::bigint[])", run_idxs
        )
    await postgres_pool.execute("DELETE FROM qiita.study_access WHERE study_idx = $1", study_idx)
    await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


# (i) Metadata / de-dup path -- real MiintEnaResolver, real registration, NO read
# download. PRJNA48739 is a tiny (2 runs, 1 sample) long-finished deposit whose
# two runs share ONE sample accession (SAMN00199006), the shape that exercises
# cross-run de-dup for real (see test_ena_resolver_live.py for accession choice).

_STUDY_ACCESSION = "PRJNA48739"
_SHARED_SAMPLE_ACCESSION = "SAMN00199006"


@pytest.mark.system
async def test_batch_driver_registers_and_dedupes_a_real_small_study(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """Drive the real batch driver against the REAL MiintEnaResolver for
    PRJNA48739. Ticket dispatch is no-op'd, so only metadata resolution +
    registration are exercised, never a fetch of the runs (22-89 MB apiece)."""
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[_STUDY_ACCESSION],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    await _process_one_study(
        batch_app,
        postgres_pool,
        item=items[0],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
    )

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    if item_row["state"] == BatchItemState.FAILED.value:
        reason = item_row["failure_reason"] or ""
        if _looks_like_network_absence(reason):
            pytest.skip(f"ENA appears unreachable from this host: {reason}")
        pytest.fail(f"real ENA study import of {_STUDY_ACCESSION} failed: {reason}")

    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert item_row["study_idx"] is not None

    try:
        study_count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", _STUDY_ACCESSION
        )
        assert study_count == 1

        biosample_rows = await postgres_pool.fetch(
            "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
            _SHARED_SAMPLE_ACCESSION,
        )
        assert len(biosample_rows) == 1
        biosample_idx = biosample_rows[0]["idx"]

        link_count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.biosample_to_study"
            " WHERE biosample_idx = $1 AND study_idx = $2",
            biosample_idx,
            item_row["study_idx"],
        )
        assert link_count == 1
    finally:
        await _cleanup_study(postgres_pool, _STUDY_ACCESSION)


# (ii) Download path -- ingest_ena_reads.execute() called DIRECTLY, bypassing
# study resolution, against real DuckLake. DRR037815: SINGLE-layout ILLUMINA,
# fastq_bytes=1774 (~1.7 KB gzipped), 14 reads (see test_ingest_ena_reads.py for
# the full accession-choice rationale).

_LIVE_RUN_ACCESSION = "DRR037815"
_LIVE_RUN_READ_COUNT = 14
_LIVE_PREP_SAMPLE_IDX = 930001
_LIVE_SEQUENCED_POOL_IDX = 930002
_LIVE_SEQUENCING_RUN_IDX = 930003
_LIVE_WORK_TICKET_IDX = 930004


@pytest.mark.system
async def test_ingest_ena_reads_downloads_a_real_small_run_into_ducklake(
    data_plane, postgres_pool, tmp_path, monkeypatch
):
    """`ingest_ena_reads.execute()` called directly against a real, tiny public
    ENA run, through the UNMOCKED `_stage_run_reads` seam (real miint + network)
    and the real `register-files` tail into a real DuckLake. mint_sequence_range
    is faked, since study resolution is bypassed and no real prep_sample exists."""
    from qiita_compute_orchestrator import sequence_range_retry
    from qiita_compute_orchestrator.jobs import ingest_ena_reads
    from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

    async def _fake_mint(*, http, prep_sample_idx, count, work_ticket_idx):
        base = 1000 * prep_sample_idx
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=base,
            sequence_idx_stop=base + count - 1,
        )

    monkeypatch.setattr(sequence_range_retry, "mint_sequence_range", _fake_mint)

    run_map_path = tmp_path / "run_map.parquet"
    _write_run_map(run_map_path, [(_LIVE_PREP_SAMPLE_IDX, _LIVE_RUN_ACCESSION)])
    inputs = ingest_ena_reads.Inputs(
        run_map=run_map_path,
        reads_staging_root=tmp_path / "reads-staging",
        sequenced_pool_idx=_LIVE_SEQUENCED_POOL_IDX,
        sequencing_run_idx=_LIVE_SEQUENCING_RUN_IDX,
        work_ticket_idx=_LIVE_WORK_TICKET_IDX,
    )

    try:
        outputs = await ingest_ena_reads.execute(inputs, tmp_path / "ws")
    except BackendFailure as exc:
        if exc.kind == FailureKind.EXTERNAL_FETCH_TRANSIENT:
            pytest.skip(f"ENA appears unreachable from this host: {exc.reason}")
        raise

    await _run_register_files(
        postgres_pool,
        data_plane,
        staging_dir=outputs["read_staging_dir"],
        work_ticket_idx=_LIVE_WORK_TICKET_IDX,
    )

    assert _count_read_rows(data_plane, [_LIVE_PREP_SAMPLE_IDX]) == _LIVE_RUN_READ_COUNT
