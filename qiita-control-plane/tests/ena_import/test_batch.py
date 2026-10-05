"""DB-bound tests for the batch multi-study ENA import driver.

Network-free: the resolver seam (`miint_resolver._query_ena_*`) is monkeypatched per
accession and `_run_and_log` is a no-op so no real orchestrator is reached.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import httpx
import pytest
import pytest_asyncio
from qiita_common.auth_constants import MSG_PRINCIPAL_DISABLED_OR_RETIRED, SystemRole
from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES
from qiita_common.models.ena_import import BatchItemState

from qiita_control_plane.auth.principal import HumanUser
from qiita_control_plane.dispatch import build_dispatch_semaphore
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.ena_import.availability import EnaAvailabilityClient
from qiita_control_plane.ena_import.batch import (
    build_ena_import_study_semaphore,
    create_ena_import_batch,
    fetch_batch_status,
    reconcile_inflight_batches,
    schedule_ena_import_batch,
)
from qiita_control_plane.ena_import.registration import EnaRunRegistrationStatus
from qiita_control_plane.repositories.sequence_range import mint_sequence_range
from qiita_control_plane.repositories.study import create_study
from qiita_control_plane.testing.db_seeds import (
    disable_principal,
    retire_principal,
    seed_user_principal,
)
from qiita_control_plane.testing.postgres import POSTGRES_POOL_MAX_SIZE
from qiita_control_plane.testing.unique_names import unique_accession

pytestmark = pytest.mark.db

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


def _fake_study_header(accession: str, *, status: str = "public") -> tuple[list[str], list[tuple]]:
    return (
        ["study_accession", "secondary_study_accession", "study_title", "status"],
        [(accession, None, f"title for {accession}", status)],
    )


def _fake_runs(accession: str, *, status: str = "public") -> tuple[list[str], list[tuple]]:
    row = (
        f"SRR-{accession}",
        f"SRX-{accession}",
        f"SAMN-{accession}",
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
        status,
    )
    return list(_RUN_COLUMNS), [row]


def _fake_attrs(accession: str) -> list[tuple[str, dict[str, str]]]:
    # At least one sample so most tests exercise the harmonized-metadata path; the
    # empty-attributes case is covered separately by monkeypatching _QUERY_ATTRS to [].
    return [(f"SAMN-{accession}", {"collection date": "2020-01-01"})]


@pytest.fixture(autouse=True)
def _monkeypatch_resolver_seam(monkeypatch):
    """Network-free resolver keyed on the accession, so distinct items land on
    distinct studies/runs/samples."""
    monkeypatch.setattr(_QUERY_STUDY, lambda accession: _fake_study_header(accession))
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: _fake_runs(accession))
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: _fake_attrs(accession))


@pytest.fixture(autouse=True)
def _patch_run_and_log(monkeypatch):
    """No-op the workflow dispatch -- these tests only assert the ticket row was
    submitted, not that an orchestrator ran it."""

    async def _noop(_app, _idx, **_kwargs):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest_asyncio.fixture
async def batch_app(postgres_pool):
    """The shared main.app configured for direct (non-HTTP) calls into the batch driver."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    # Save/restore: `app` is a process-wide singleton, so a stub left on
    # compute_backend_client would leak into a later test on the same xdist worker.
    saved_compute_backend_client = getattr(app.state, "compute_backend_client", None)
    app.state.compute_backend_client = object()
    app.state.running_dispatches = set()
    app.state.running_ena_import_batches = set()
    app.state.ena_import_study_semaphore = build_ena_import_study_semaphore()
    app.state.dispatch_semaphore = build_dispatch_semaphore()

    yield app

    pending = list(app.state.running_dispatches) + list(app.state.running_ena_import_batches)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    app.state.compute_backend_client = saved_compute_backend_client


@pytest_asyncio.fixture
async def admin_principal(postgres_pool):
    """A real seeded wet_lab_admin principal (needed for the work_ticket FK and the
    download-ena-study action's audience)."""
    pidx = await seed_user_principal(
        postgres_pool,
        prefix="ena-batch-admin",
        suffix="t06",
        system_role=SystemRole.WET_LAB_ADMIN,
    )
    principal = HumanUser(
        principal_idx=pidx,
        email="ena-batch-admin@test.local",
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
    """Seed the pinned `download-ena-study`/`1.0.0` action row so
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


async def _cleanup_study(postgres_pool, study_accession: str) -> None:
    """Best-effort FK-reverse cleanup for one study this test created,
    looked up by either accession column."""
    study_idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.study WHERE bioproject_accession = $1 OR ena_study_accession = $1",
        study_accession,
    )
    if study_idx is None:
        return
    # ena_import_batch_item.study_idx FKs (RESTRICT) into qiita.study; clear it
    # before the study DELETE below.
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
        # prep_sample_metadata RESTRICTs its prep_sample and study field, so
        # sweep both before prep_sample / prep_sample_study_field / study below.
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample_metadata WHERE prep_sample_idx = ANY($1::bigint[])",
            ps_idxs,
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
    )
    if ps_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_idxs
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample_study_field WHERE study_idx = $1", study_idx
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


async def _cleanup_two_studies_sharing_biosample(
    postgres_pool, *, study_accessions: list[str], shared_sample_accession: str
) -> None:
    """Teardown twin of `_cleanup_study` where two studies share ONE biosample row:
    clear both studies' links/prep first, then drop the shared biosample once, then
    each study -- deleting the biosample early would trip its RESTRICT FK.
    """
    # biosample_metadata FKs (RESTRICT) into biosample_study_field, so clear the shared
    # biosample's metadata before either study's field rows are dropped below.
    biosample_idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    if biosample_idx is not None:
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = $1", biosample_idx
        )

    study_idxs: list[int] = []
    for accession in study_accessions:
        study_idx = await postgres_pool.fetchval(
            "SELECT idx FROM qiita.study WHERE bioproject_accession = $1", accession
        )
        if study_idx is None:
            continue
        study_idxs.append(study_idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.ena_import_batch_item WHERE study_idx = $1", study_idx
        )
        ps_rows = await postgres_pool.fetch(
            "SELECT prep_sample_idx FROM qiita.prep_sample_to_study WHERE study_idx = $1",
            study_idx,
        )
        ps_idxs = [r["prep_sample_idx"] for r in ps_rows]
        if ps_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.sequenced_sample WHERE prep_sample_idx = ANY($1::bigint[])",
                ps_idxs,
            )
            # prep_sample_metadata RESTRICTs its prep_sample and study field, so
            # sweep both before prep_sample / prep_sample_study_field / study below.
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample_metadata WHERE prep_sample_idx = ANY($1::bigint[])",
                ps_idxs,
            )
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
        )
        if ps_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_idxs
            )
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample_study_field WHERE study_idx = $1", study_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_study_field WHERE study_idx = $1", study_idx
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
        )
        run_rows = await postgres_pool.fetch(
            "SELECT idx FROM qiita.sequencing_run WHERE instrument_run_id LIKE $1",
            f"{accession}:%",
        )
        run_idxs = [r["idx"] for r in run_rows]
        if run_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE sequenced_pool_idx IN"
                " (SELECT idx FROM qiita.sequenced_pool"
                "  WHERE sequencing_run_idx = ANY($1::bigint[]))",
                run_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.sequenced_pool WHERE sequencing_run_idx = ANY($1::bigint[])",
                run_idxs,
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.sequencing_run WHERE idx = ANY($1::bigint[])", run_idxs
            )

    if biosample_idx is not None:
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)

    for study_idx in study_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1", study_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)


@pytest_asyncio.fixture
async def dummy_reference_idx(postgres_pool, admin_principal):
    """A bare `qiita.reference` row to satisfy the scope-target constraint for the
    reference-scoped tickets the rollup tests INSERT directly."""
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', 'pending', $2)"
        " RETURNING reference_idx",
        f"ena-batch-rollup-{uuid.uuid4()}",
        admin_principal.principal_idx,
    )
    yield idx
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx)
    await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


@pytest_asyncio.fixture
async def batch_cleanup(postgres_pool):
    """Tracks batch idxs created by a test; deletes them (CASCADE handles items) at teardown."""
    batch_idxs: list[int] = []
    yield batch_idxs
    if batch_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.ena_import_batch WHERE idx = ANY($1::bigint[])", batch_idxs
        )


# ---------------------------------------------------------------------------
# create_ena_import_batch
# ---------------------------------------------------------------------------


async def test_create_ena_import_batch_seeds_pending_items(
    postgres_pool, admin_principal, batch_cleanup
):
    accessions = [unique_accession("PRJNA"), unique_accession("PRJEB")]
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=accessions,
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    assert {item.ena_study_accession for item in items} == set(accessions)

    batch_row = await postgres_pool.fetchrow(
        "SELECT submitted_by_principal_idx FROM qiita.ena_import_batch WHERE idx = $1",
        batch_idx,
    )
    assert batch_row["submitted_by_principal_idx"] == admin_principal.principal_idx

    item_rows = await postgres_pool.fetch(
        "SELECT ena_study_accession, state FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1 ORDER BY idx",
        batch_idx,
    )
    assert len(item_rows) == 2
    assert {r["ena_study_accession"] for r in item_rows} == set(accessions)
    assert {r["state"] for r in item_rows} == {BatchItemState.PENDING.value}


async def test_create_ena_import_batch_rejects_invalid_accession_writes_nothing(
    postgres_pool, admin_principal, batch_cleanup
):
    from qiita_common.ena_accession import InvalidEnaAccessionError

    good = unique_accession("PRJNA")
    bad = "SAMN0000001"  # a SAMPLE accession, not a study accession

    with pytest.raises(InvalidEnaAccessionError):
        await create_ena_import_batch(
            postgres_pool,
            accessions=[good, bad],
            principal=admin_principal,
        )

    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch_item WHERE ena_study_accession = $1", good
    )
    assert count == 0


# ---------------------------------------------------------------------------
# Resolve + register + submit ONE download ticket per pool
# ---------------------------------------------------------------------------


async def test_process_one_study_registers_and_submits_download_ticket(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, download_work_ticket_idxs, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert item_row["failure_reason"] is None
    assert item_row["study_idx"] is not None
    assert len(item_row["download_work_ticket_idxs"]) == 1

    work_ticket_idx = item_row["download_work_ticket_idxs"][0]
    ticket_row = await postgres_pool.fetchrow(
        "SELECT action_id, action_version, scope_target_kind, sequenced_pool_idx,"
        " action_context, state, originator_principal_idx"
        " FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        work_ticket_idx,
    )
    assert ticket_row["action_id"] == DOWNLOAD_ENA_STUDY_ACTION_ID
    assert ticket_row["action_version"] == DOWNLOAD_ENA_STUDY_ACTION_VERSION
    assert ticket_row["scope_target_kind"] == "sequenced_pool"
    assert ticket_row["sequenced_pool_idx"] is not None
    assert ticket_row["originator_principal_idx"] == admin_principal.principal_idx
    context = json.loads(ticket_row["action_context"])
    assert context["ena_study_accession"] == accession

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_empty_sample_attributes_registers_not_failed(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Real DDBJ finding: a sample can have ZERO ENA attributes (PRJDB40364's
    SAMD01818724). An empty resolve result must register normally, never fail the item."""
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: [])

    accession = unique_accession("PRJDB")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    # NOT failed -- an empty attribute set is a legitimate resolve result.
    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert item_row["failure_reason"] is None
    assert item_row["study_idx"] is not None

    sample_accession = f"SAMN-{accession}"
    biosample_row = await postgres_pool.fetchrow(
        "SELECT idx, metadata_checklist_idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        sample_accession,
    )
    assert biosample_row is not None
    assert biosample_row["metadata_checklist_idx"] is not None

    # Nothing harmonized -- there were no attributes; the one global row is the
    # host-taxon-id marker the import composer enforces.
    global_metadata_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx IS NOT NULL",
        biosample_row["idx"],
    )
    assert global_metadata_count == 1

    prep_sample_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.prep_sample_to_study WHERE study_idx = $1",
        item_row["study_idx"],
    )
    assert prep_sample_count == 1

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_rejects_non_audience_principal_no_ticket_created(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """The action's own audience is enforced against the batch's submitting principal,
    not bypassed because the batch route is admin-gated. A plain `user`-role submitter is
    outside the action's audience: the ticket is rejected (403) with no work_ticket row,
    while the (ungated) study/pool registration still succeeds.
    """
    non_audience_pidx = await seed_user_principal(
        postgres_pool, prefix="ena-batch-non-audience", suffix="t06", system_role=SystemRole.USER
    )
    non_audience_principal = HumanUser(
        principal_idx=non_audience_pidx,
        email=f"ena-batch-non-audience-{non_audience_pidx}@test.local",
        system_role=SystemRole.USER,
        scopes=frozenset(),
        profile_complete=True,
        disabled=False,
        retired=False,
    )

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=non_audience_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=non_audience_principal,
    )
    # Must not raise -- a rejected submission is a per-item failure, not a batch failure.
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, download_work_ticket_idxs, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    # register_ena_study has no audience gate -- the study itself registers.
    assert item_row["study_idx"] is not None
    assert item_row["state"] == BatchItemState.FAILED.value
    assert item_row["download_work_ticket_idxs"] == []
    assert "403" in item_row["failure_reason"]
    assert "audience" in item_row["failure_reason"].lower()

    ticket_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    assert ticket_count == 0

    await _cleanup_study(postgres_pool, accession)
    # Drop the batch before its submitting principal -- submitted_by_principal_idx
    # FKs (RESTRICT) into qiita.principal, and batch_cleanup only runs at teardown.
    await postgres_pool.execute("DELETE FROM qiita.ena_import_batch WHERE idx = $1", batch_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = $1", non_audience_pidx
    )
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", non_audience_pidx)


# ---------------------------------------------------------------------------
# Batch-driver-level de-dup: two items of the SAME batch whose runs resolve to a
# SHARED sample_accession still land as one biosample row (the register-level
# concurrency case is covered in test_registration.py).
# ---------------------------------------------------------------------------


def _make_shared_sample_fakes(shared_sample_accession: str):
    """Build a (runs, attrs) fake-resolver pair where every accession's run carries the
    SAME `sample_accession` but distinct study/run/experiment accessions."""

    def _fake_runs_shared(accession: str) -> tuple[list[str], list[tuple]]:
        row = (
            f"SRR-{accession}",
            f"SRX-{accession}",
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
        )
        return list(_RUN_COLUMNS), [row]

    def _fake_attrs_shared(accession: str) -> list[tuple[str, dict[str, str]]]:
        return [(shared_sample_accession, {"collection date": "2020-01-01"})]

    return _fake_runs_shared, _fake_attrs_shared


async def test_batch_dedupes_shared_biosample_across_two_items(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    shared_sample_accession = unique_accession("SAMN")
    fake_runs, fake_attrs = _make_shared_sample_fakes(shared_sample_accession)
    monkeypatch.setattr(_QUERY_RUNS, fake_runs)
    monkeypatch.setattr(_QUERY_ATTRS, fake_attrs)

    accession_a = unique_accession("PRJNA")
    accession_b = unique_accession("PRJEB")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession_a, accession_b],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    await task

    item_rows = await postgres_pool.fetch(
        "SELECT ena_study_accession, state, failure_reason, study_idx"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert len(item_rows) == 2
    for row in item_rows:
        assert row["state"] == BatchItemState.DOWNLOADING.value, row["failure_reason"]
        assert row["study_idx"] is not None

    biosample_rows = await postgres_pool.fetch(
        "SELECT idx FROM qiita.biosample WHERE ena_sample_accession = $1",
        shared_sample_accession,
    )
    assert len(biosample_rows) == 1
    biosample_idx = biosample_rows[0]["idx"]

    link_rows = await postgres_pool.fetch(
        "SELECT study_idx FROM qiita.biosample_to_study WHERE biosample_idx = $1", biosample_idx
    )
    assert len(link_rows) == 2
    assert {r["study_idx"] for r in link_rows} == {r["study_idx"] for r in item_rows}

    await _cleanup_two_studies_sharing_biosample(
        postgres_pool,
        study_accessions=[accession_a, accession_b],
        shared_sample_accession=shared_sample_accession,
    )


# ---------------------------------------------------------------------------
# Per-item isolation -- one accession's failure never affects siblings or the batch
# ---------------------------------------------------------------------------


async def test_run_batch_isolates_per_study_failure(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    ok_accession = unique_accession("PRJNA")
    bad_accession = unique_accession("PRJEB")

    real_query_runs = __import__(
        "qiita_control_plane.ena_import.miint_resolver", fromlist=["_query_ena_runs"]
    )._query_ena_runs

    def _maybe_fail(accession: str):
        if accession == bad_accession:
            raise RuntimeError(f"simulated resolver failure for {accession}")
        return real_query_runs(accession)

    monkeypatch.setattr(_QUERY_RUNS, _maybe_fail)

    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[ok_accession, bad_accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    # Must not raise -- the batch as a whole never fails.
    await task

    rows = await postgres_pool.fetch(
        "SELECT ena_study_accession, state, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    by_accession = {r["ena_study_accession"]: r for r in rows}

    ok_row = by_accession[ok_accession]
    assert ok_row["state"] == BatchItemState.DOWNLOADING.value
    assert ok_row["failure_reason"] is None

    bad_row = by_accession[bad_accession]
    assert bad_row["state"] == BatchItemState.FAILED.value
    assert "simulated resolver failure" in bad_row["failure_reason"]

    await _cleanup_study(postgres_pool, ok_accession)


# ---------------------------------------------------------------------------
# fetch_batch_status -- download-ticket rollup
# ---------------------------------------------------------------------------


async def test_fetch_batch_status_rolls_up_downloading_to_done(
    postgres_pool, admin_principal, download_ena_study_action, dummy_reference_idx, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)
    item = items[0]

    action_id, version = download_ena_study_action
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state)"
        " VALUES ($1, $2, $3, 'reference'::qiita.scope_target_kind, $4, '{}'::jsonb,"
        "         'completed'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_principal.principal_idx,
        dummy_reference_idx,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'downloading', download_work_ticket_idxs = $2"
        " WHERE idx = $1",
        item.idx,
        [ticket_idx],
    )

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status is not None
    assert status.items[0].state == BatchItemState.DONE

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
    )


async def test_fetch_batch_status_rolls_up_in_flight_ticket_to_downloading(
    postgres_pool, admin_principal, download_ena_study_action, dummy_reference_idx, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)
    item = items[0]

    action_id, version = download_ena_study_action
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state)"
        " VALUES ($1, $2, $3, 'reference'::qiita.scope_target_kind, $4, '{}'::jsonb,"
        "         'processing'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_principal.principal_idx,
        dummy_reference_idx,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'downloading', download_work_ticket_idxs = $2"
        " WHERE idx = $1",
        item.idx,
        [ticket_idx],
    )

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status.items[0].state == BatchItemState.DOWNLOADING

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
    )


async def test_fetch_batch_status_rolls_up_failed_ticket_without_failing_batch(
    postgres_pool, admin_principal, download_ena_study_action, dummy_reference_idx, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)
    item = items[0]

    action_id, version = download_ena_study_action
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state,"
        "  failure_type, failure_stage, failure_reason)"
        " VALUES ($1, $2, $3, 'reference'::qiita.scope_target_kind, $4, '{}'::jsonb,"
        "         'failed'::qiita.work_ticket_state,"
        "         'permanent'::qiita.failure_type, 'submission'::qiita.work_ticket_failure_stage,"
        "         'boom')"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_principal.principal_idx,
        dummy_reference_idx,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'downloading', download_work_ticket_idxs = $2"
        " WHERE idx = $1",
        item.idx,
        [ticket_idx],
    )

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status.items[0].state == BatchItemState.FAILED
    assert str(ticket_idx) in status.items[0].failure_reason

    # The rollup is read-only/on-demand -- the underlying item row is not mutated.
    persisted_state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.ena_import_batch_item WHERE idx = $1", item.idx
    )
    assert persisted_state == "downloading"

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
    )


async def test_fetch_batch_status_missing_batch_returns_none(postgres_pool):
    missing_idx = 999_999_999
    status = await fetch_batch_status(postgres_pool, batch_idx=missing_idx)
    assert status is None


# ---------------------------------------------------------------------------
# reconcile_inflight_batches -- restart durability
# ---------------------------------------------------------------------------


async def test_reconcile_inflight_batches_redrives_pending_items(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, _items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 1

    # Let the re-driven background task run to completion.
    tasks = list(batch_app.state.running_ena_import_batches)
    assert len(tasks) == 1
    await tasks[0]

    item_row = await postgres_pool.fetchrow(
        "SELECT state, download_work_ticket_idxs FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value

    await _cleanup_study(postgres_pool, accession)


async def test_reconcile_inflight_batches_no_op_when_nothing_in_flight(batch_app):
    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0


# ---------------------------------------------------------------------------
# reconcile_inflight_batches -- a disabled/retired submitting principal must NOT
# be re-driven on their behalf
# ---------------------------------------------------------------------------


async def test_reconcile_inflight_batches_refuses_disabled_principal(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """A batch whose admin was DISABLED after submission is not re-driven on their
    behalf: the item fails with the reason, no study or ticket is created."""
    accession = unique_accession("PRJNA")
    batch_idx, _items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    await disable_principal(postgres_pool, admin_principal.principal_idx)

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0

    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, study_idx, download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert MSG_PRINCIPAL_DISABLED_OR_RETIRED in item_row["failure_reason"]
    assert str(admin_principal.principal_idx) not in item_row["failure_reason"]
    assert item_row["study_idx"] is None
    assert item_row["download_work_ticket_idxs"] == []

    ticket_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    assert ticket_count == 0

    study_idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.study WHERE bioproject_accession = $1", accession
    )
    assert study_idx is None


async def test_reconcile_inflight_batches_refuses_retired_principal(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """Same guard, retired instead of disabled -- failed identically."""
    accession = unique_accession("PRJEB")
    batch_idx, _items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    await retire_principal(postgres_pool, admin_principal.principal_idx)

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0

    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, study_idx FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert MSG_PRINCIPAL_DISABLED_OR_RETIRED in item_row["failure_reason"]
    assert item_row["study_idx"] is None


# ---------------------------------------------------------------------------
# Review follow-ups: batch-creation dedup, zero-pool terminal state, and
# rollup strictness on a missing / unknown ticket state.
# ---------------------------------------------------------------------------


async def test_create_ena_import_batch_dedupes_repeated_accession(
    postgres_pool, admin_principal, batch_cleanup
):
    """A repeated accession in one request fans out a single item, not two
    concurrent items registering the same study."""
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession, accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    assert [item.ena_study_accession for item in items] == [accession]
    item_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch_item WHERE batch_idx = $1", batch_idx
    )
    assert item_count == 1


async def test_process_one_study_zero_pools_reaches_terminal_failed(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A study whose only run maps to a rejected platform registers zero pools.
    The item must reach a terminal state (failed), not sit in `downloading`
    forever with an empty ticket list."""

    def _fake_runs_unmappable(accession):
        cols, rows = _fake_runs(accession)
        row = list(rows[0])
        row[8] = "UNMAPPABLE_PLATFORM_XYZ"  # index 8 = instrument_platform
        return cols, [tuple(row)]

    monkeypatch.setattr(_QUERY_RUNS, _fake_runs_unmappable)

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, download_work_ticket_idxs, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert list(item_row["download_work_ticket_idxs"]) == []
    assert "no run mapped to a downloadable pool" in item_row["failure_reason"]

    await _cleanup_study(postgres_pool, accession)


async def test_fetch_batch_status_missing_ticket_row_stays_downloading(
    postgres_pool, admin_principal, batch_cleanup
):
    """A ticket idx with no matching work_ticket row yields state None in the
    rollup -- not terminal-success -- so the item stays `downloading`, never
    reads as `done`."""
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    batch_cleanup.append(batch_idx)

    missing_ticket_idx = 2_000_000_000  # no work_ticket row exists for this idx
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'downloading', download_work_ticket_idxs = $2"
        " WHERE idx = $1",
        items[0].idx,
        [missing_ticket_idx],
    )

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status is not None
    assert status.items[0].state == BatchItemState.DOWNLOADING


# ---------------------------------------------------------------------------
# Per-run outcomes surfaced on the status endpoint; all-runs-failed terminal;
# registered-item re-drive durability + idempotent re-submit.
# ---------------------------------------------------------------------------


async def _drive_one_study(batch_app, postgres_pool, admin_principal, accession):
    """Create a one-accession batch and run it to completion; return (batch_idx, items)."""
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
    )
    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
    )
    await task
    return batch_idx, items


async def test_process_one_study_surfaces_per_run_outcomes(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """GET /ena-import-batch/{idx} carries per-run outcomes: each run's status,
    which the driver used to compute and store."""
    accession = unique_accession("PRJNA")
    batch_idx, _items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status is not None
    item = status.items[0]
    assert len(item.ena_runs) == 1
    run = item.ena_runs[0]
    assert run.status == EnaRunRegistrationStatus.REGISTERED.value
    assert run.failure_reason is None

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_all_runs_failed_reaches_terminal_failed(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A study whose platform maps (a pool IS created) but whose every run then
    fails registration must reach terminal `failed`, not `downloading` -- otherwise
    it reports success over an all-failed study, and submits doomed download
    tickets. The per-run reason is surfaced on the endpoint."""

    def _bad_latitude_attrs(accession):
        # ILLUMINA maps (pool created), but an unparseable latitude fails the run
        # in harmonization -- the created_pools-non-empty / all-runs-failed case.
        return [(f"SAMN-{accession}", {"geographic location (latitude)": "not-a-number"})]

    monkeypatch.setattr(_QUERY_ATTRS, _bad_latitude_attrs)

    accession = unique_accession("PRJNA")
    batch_idx, _items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "every run failed to register" in item_row["failure_reason"]
    assert list(item_row["download_work_ticket_idxs"]) == []
    ticket_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    assert ticket_count == 0

    # The failed run's detail is on the endpoint, not only the item-level reason.
    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    failed_run = status.items[0].ena_runs[0]
    assert failed_run.status == EnaRunRegistrationStatus.FAILED.value
    assert failed_run.failure_reason is not None

    await _cleanup_study(postgres_pool, accession)


# ---------------------------------------------------------------------------
# Non-public studies and runs: refuse a suppressed study; exclude a suppressed
# run; fail loud on a status this codebase doesn't recognize.
# ---------------------------------------------------------------------------


async def test_process_one_study_suppressed_study_fails_before_any_write(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A suppressed study must fail the item before `get_or_create_study_by_ena_accessions`
    even runs -- nothing (no study, no run, no ticket) is written."""
    monkeypatch.setattr(
        _QUERY_STUDY, lambda accession: _fake_study_header(accession, status="suppressed")
    )

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, failure_reason FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert item_row["study_idx"] is None
    assert f"study {accession} is suppressed" in item_row["failure_reason"]

    study_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", accession
    )
    assert study_count == 0


async def test_process_one_study_all_runs_suppressed_fails_before_any_write(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A study whose every run is suppressed must fail before the study is even
    resolved/created -- there is nothing public left to import."""
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: _fake_runs(accession, status="suppressed"))

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, failure_reason FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert item_row["study_idx"] is None
    assert item_row["failure_reason"] == f"no public runs: SRR-{accession} is suppressed"

    study_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.study WHERE bioproject_accession = $1", accession
    )
    assert study_count == 0


async def test_process_one_study_one_suppressed_run_excluded_other_registered(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A study with one public and one suppressed run registers the public one
    and reports the suppressed one `excluded`, with no sequenced_sample for it."""
    accession = unique_accession("PRJNA")

    def _mixed_runs(a):
        _, (ok_row,) = _fake_runs(a, status="public")
        suppressed_row = list(ok_row)
        run_i = list(_RUN_COLUMNS).index("run_accession")
        exp_i = list(_RUN_COLUMNS).index("experiment_accession")
        sample_i = list(_RUN_COLUMNS).index("sample_accession")
        status_i = list(_RUN_COLUMNS).index("status")
        suppressed_row[run_i] = f"SRR-{a}-suppressed"
        suppressed_row[exp_i] = f"SRX-{a}-suppressed"
        suppressed_row[sample_i] = f"SAMN-{a}-suppressed"
        suppressed_row[status_i] = "suppressed"
        return list(_RUN_COLUMNS), [ok_row, tuple(suppressed_row)]

    monkeypatch.setattr(_QUERY_RUNS, _mixed_runs)

    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    status_result = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    item = status_result.items[0]
    assert item.state == BatchItemState.DOWNLOADING
    by_accession = {r.run_accession: r for r in item.ena_runs}
    ok_outcome = by_accession[f"SRR-{accession}"]
    assert ok_outcome.status == EnaRunRegistrationStatus.REGISTERED.value

    excluded_outcome = by_accession[f"SRR-{accession}-suppressed"]
    assert excluded_outcome.status == EnaRunRegistrationStatus.EXCLUDED.value
    assert excluded_outcome.failure_reason is not None
    assert "suppressed" in excluded_outcome.failure_reason

    orphan_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        f"SRR-{accession}-suppressed",
    )
    assert orphan_count == 0

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_unknown_status_item_fails_naming_value(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A status ENA reports that this codebase doesn't model must fail the item
    loudly (via the model's own ValidationError, caught by _process_one_study's
    outer handler), naming the offending value -- never silently treated as
    public."""
    monkeypatch.setattr(
        _QUERY_STUDY, lambda accession: _fake_study_header(accession, status="cancelled")
    )

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "cancelled" in item_row["failure_reason"]


async def test_process_one_study_mixed_excluded_and_failed_every_run_failed_message(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A study with one suppressed (excluded) run and one run that maps a
    platform (a pool IS created) but then fails harmonization has no
    successful run either way -- terminal `failed`, and the reasons string
    names both."""
    accession = unique_accession("PRJNA")

    def _mixed_runs(a):
        _, (template,) = _fake_runs(a, status="public")
        run_i = list(_RUN_COLUMNS).index("run_accession")
        exp_i = list(_RUN_COLUMNS).index("experiment_accession")
        sample_i = list(_RUN_COLUMNS).index("sample_accession")
        status_i = list(_RUN_COLUMNS).index("status")

        suppressed_row = list(template)
        suppressed_row[run_i] = f"SRR-{a}-suppressed"
        suppressed_row[exp_i] = f"SRX-{a}-suppressed"
        suppressed_row[sample_i] = f"SAMN-{a}-suppressed"
        suppressed_row[status_i] = "suppressed"

        bad_harmonization_row = list(template)
        bad_harmonization_row[run_i] = f"SRR-{a}-badharm"
        bad_harmonization_row[exp_i] = f"SRX-{a}-badharm"
        bad_harmonization_row[sample_i] = f"SAMN-{a}-badharm"

        return list(_RUN_COLUMNS), [tuple(suppressed_row), tuple(bad_harmonization_row)]

    def _bad_latitude_attrs(a):
        return [(f"SAMN-{a}-badharm", {"geographic location (latitude)": "not-a-number"})]

    monkeypatch.setattr(_QUERY_RUNS, _mixed_runs)
    monkeypatch.setattr(_QUERY_ATTRS, _bad_latitude_attrs)

    batch_idx, _items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "every run failed to register" in item_row["failure_reason"]
    assert f"SRR-{accession}-suppressed" in item_row["failure_reason"]
    assert f"SRR-{accession}-badharm" in item_row["failure_reason"]
    assert list(item_row["download_work_ticket_idxs"]) == []

    await _cleanup_study(postgres_pool, accession)


async def test_reconcile_redrives_registered_item_stranded_before_submit(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """A crash in the registered->downloading window leaves the item at `registered`
    with no tickets. reconcile must re-drive it -- previously it stranded because
    the filter matched pending/resolving only."""
    accession = unique_accession("PRJNA")
    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    # Simulate the crash: delete the submitted ticket, clear the array, and rewind
    # the item to `registered` (its study + pool already exist, and are idempotent).
    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'registered', download_work_ticket_idxs = '{}'"
        " WHERE idx = $1",
        items[0].idx,
    )

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 1
    await asyncio.gather(*list(batch_app.state.running_ena_import_batches))

    item_row = await postgres_pool.fetchrow(
        "SELECT state, download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert len(item_row["download_work_ticket_idxs"]) == 1

    await _cleanup_study(postgres_pool, accession)


async def test_reconcile_registered_item_reuses_already_submitted_ticket(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """Re-driving a `registered` item that already submitted its ticket must REUSE
    it, not re-submit -- a sequenced_pool re-submit would 409 and wrongly fail the
    whole item. Exactly one ticket, no duplicate."""
    accession = unique_accession("PRJNA")
    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    ticket_idxs_before = await postgres_pool.fetchval(
        "SELECT download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert len(ticket_idxs_before) == 1

    # Rewind to `registered` but KEEP the ticket + array (crash after the submit,
    # before the downloading flip).
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item SET state = 'registered' WHERE idx = $1",
        items[0].idx,
    )

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 1
    await asyncio.gather(*list(batch_app.state.running_ena_import_batches))

    item_row = await postgres_pool.fetchrow(
        "SELECT state, download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert list(item_row["download_work_ticket_idxs"]) == list(ticket_idxs_before)

    ticket_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        DOWNLOAD_ENA_STUDY_ACTION_ID,
        DOWNLOAD_ENA_STUDY_ACTION_VERSION,
    )
    assert ticket_count == 1

    await _cleanup_study(postgres_pool, accession)


async def test_interrupted_item_keeps_the_record_of_the_study_it_created(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """An item cancelled after committing its study (e.g. by the shutdown drain)
    must still be recorded as that study's creator, or its re-drive -- and every
    later import of the accession -- is refused as a native study."""
    from qiita_control_plane.ena_import import batch as batch_module

    real_register = batch_module.register_ena_study

    async def _cancelled(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(batch_module, "register_ena_study", _cancelled)

    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)
    with pytest.raises(asyncio.CancelledError):
        await schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)

    row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, study_created FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert row["state"] == BatchItemState.RESOLVING.value
    assert row["study_idx"] is not None
    assert row["study_created"] is True

    monkeypatch.setattr(batch_module, "register_ena_study", real_register)
    assert await reconcile_inflight_batches(batch_app) == 1
    await asyncio.gather(*list(batch_app.state.running_ena_import_batches))

    state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.ena_import_batch_item WHERE idx = $1", items[0].idx
    )
    assert state == BatchItemState.DOWNLOADING.value

    await _cleanup_study(postgres_pool, accession)


async def test_item_failed_after_creating_its_study_does_not_block_reimport(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    from qiita_control_plane.ena_import import batch as batch_module

    real_register = batch_module.register_ena_study

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(batch_module, "register_ena_study", _boom)
    accession = unique_accession("PRJNA")
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.ena_import_batch_item WHERE idx = $1", first_items[0].idx
        )
        == BatchItemState.FAILED.value
    )

    monkeypatch.setattr(batch_module, "register_ena_study", real_register)
    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)
    assert (
        await postgres_pool.fetchval(
            "SELECT state FROM qiita.ena_import_batch_item WHERE idx = $1", second_items[0].idx
        )
        == BatchItemState.DOWNLOADING.value
    )

    await _cleanup_study(postgres_pool, accession)


async def test_import_refuses_a_study_no_import_created(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """A study Qiita created natively and later deposited carries a
    bioproject_accession too, so the accession lookup matches it. Importing must
    refuse rather than merge ENA samples into curated data -- and refuse before
    writing anything."""
    accession = unique_accession("PRJNA")
    async with postgres_pool.acquire() as conn, conn.transaction():
        native = await create_study(
            conn,
            owner_idx=admin_principal.principal_idx,
            created_by_idx=admin_principal.principal_idx,
            title=f"natively created {accession}",
            bioproject_accession=accession,
        )
    native_idx = native["idx"]

    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, study_idx FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert row["state"] == BatchItemState.FAILED.value
    assert "not created by an ENA import" in row["failure_reason"]
    # Refused before any write: no sample linked, and no download ticket.
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.prep_sample_to_study WHERE study_idx = $1", native_idx
        )
        == 0
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
            DOWNLOAD_ENA_STUDY_ACTION_ID,
            DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        )
        == 0
    )

    await _cleanup_study(postgres_pool, accession)


async def test_import_refuses_a_study_no_import_created_matched_by_secondary_accession(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Same guard as `test_import_refuses_a_study_no_import_created`, but the
    native study is matched via ena_study_accession (bioproject_accession
    NULL) rather than bioproject_accession."""
    ena_accession = unique_accession("ERP")
    async with postgres_pool.acquire() as conn, conn.transaction():
        native = await create_study(
            conn,
            owner_idx=admin_principal.principal_idx,
            created_by_idx=admin_principal.principal_idx,
            title=f"natively created {ena_accession}",
            ena_study_accession=ena_accession,
        )
    native_idx = native["idx"]

    fresh_bioproject = unique_accession("PRJEB")
    monkeypatch.setattr(
        _QUERY_STUDY,
        lambda accession: (
            ["study_accession", "secondary_study_accession", "study_title", "status"],
            [(fresh_bioproject, accession, f"title for {accession}", "public")],
        ),
    )

    batch_idx, items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, ena_accession
    )
    batch_cleanup.append(batch_idx)

    row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, study_idx FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert row["state"] == BatchItemState.FAILED.value
    assert "not created by an ENA import" in row["failure_reason"]
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.prep_sample_to_study WHERE study_idx = $1", native_idx
        )
        == 0
    )
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
            DOWNLOAD_ENA_STUDY_ACTION_ID,
            DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        )
        == 0
    )

    await _cleanup_study(postgres_pool, ena_accession)


async def test_import_allows_a_study_an_earlier_batch_created(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """The guard must not block the ongoing-bioproject case: an accession a
    previous batch imported is re-importable, which is how a study that gains
    runs over time picks them up."""
    accession = unique_accession("PRJNA")
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    rows = await postgres_pool.fetch(
        "SELECT idx, state, study_idx, study_created FROM qiita.ena_import_batch_item"
        " WHERE idx = ANY($1::bigint[]) ORDER BY idx",
        [first_items[0].idx, second_items[0].idx],
    )
    first, second = rows
    assert first["state"] == BatchItemState.DOWNLOADING.value
    assert second["state"] == BatchItemState.DOWNLOADING.value
    assert first["study_idx"] == second["study_idx"]
    # Only the batch that actually created the study records it.
    assert first["study_created"] is True
    assert second["study_created"] is False

    await _cleanup_study(postgres_pool, accession)


# ---------------------------------------------------------------------------
# Download tickets across re-imports, multi-platform studies, and cancellation.
# ---------------------------------------------------------------------------


def _fake_run_rows(accession: str, runs: list[tuple[str, str]]) -> tuple[list[str], list[tuple]]:
    """One row per `(run_suffix, instrument_platform)`, each with its own sample."""
    _, (template,) = _fake_runs(accession)
    rows = []
    for suffix, platform in runs:
        row = list(template)
        row[0] = f"SRR-{accession}-{suffix}"
        row[1] = f"SRX-{accession}-{suffix}"
        row[2] = f"SAMN-{accession}-{suffix}"
        row[8] = platform
        rows.append(tuple(row))
    return list(_RUN_COLUMNS), rows


async def _item_ticket_idxs(postgres_pool, item_idx: int) -> list[int]:
    return list(
        await postgres_pool.fetchval(
            "SELECT download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
            item_idx,
        )
    )


async def _ticket_pool_run_accessions(postgres_pool, ticket_idx: int) -> set[str]:
    rows = await postgres_pool.fetch(
        "SELECT ss.ena_run_accession FROM qiita.work_ticket wt"
        " JOIN qiita.sequenced_sample ss ON ss.sequenced_pool_idx = wt.sequenced_pool_idx"
        " WHERE wt.work_ticket_idx = $1",
        ticket_idx,
    )
    return {r["ena_run_accession"] for r in rows}


async def test_reimport_puts_new_runs_in_a_new_pool_once_the_old_download_completed(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A completed download will not see runs added to its pool (its roster was
    read at dispatch, and a resubmit would re-register the old reads), so a
    re-import's new runs get their own pool and ticket, and the item reports
    `done` only once that ticket completes too."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'completed' WHERE work_ticket_idx = $1", first_ticket
    )

    monkeypatch.setattr(
        _QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA"), ("2", "ILLUMINA")])
    )
    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    ticket_idxs = await _item_ticket_idxs(postgres_pool, second_items[0].idx)
    assert len(ticket_idxs) == 2
    assert ticket_idxs[0] == first_ticket
    assert await _ticket_pool_run_accessions(postgres_pool, first_ticket) == {f"SRR-{accession}-1"}
    assert await _ticket_pool_run_accessions(postgres_pool, ticket_idxs[1]) == {
        f"SRR-{accession}-2"
    }

    status = await fetch_batch_status(postgres_pool, batch_idx=second_idx)
    assert status.items[0].state == BatchItemState.DOWNLOADING

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_with_no_new_runs_reuses_the_completed_ticket(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    accession = unique_accession("PRJNA")
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'completed' WHERE work_ticket_idx = $1", first_ticket
    )

    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    assert await _item_ticket_idxs(postgres_pool, second_items[0].idx) == [first_ticket]
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.sequenced_pool sp"
            " JOIN qiita.sequencing_run sr ON sr.idx = sp.sequencing_run_idx"
            " WHERE sr.instrument_run_id LIKE $1",
            f"{accession}:%",
        )
        == 1
    )
    status = await fetch_batch_status(postgres_pool, batch_idx=second_idx)
    assert status.items[0].state == BatchItemState.DONE

    await _cleanup_study(postgres_pool, accession)


@pytest.mark.parametrize("ended_state", ["failed", "cancelled"])
async def test_reimport_resubmits_a_download_that_did_not_complete(
    batch_app,
    postgres_pool,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    ended_state,
):
    accession = unique_accession("PRJNA")
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)
    if ended_state == "failed":
        await postgres_pool.execute(
            "UPDATE qiita.work_ticket SET state = 'failed', failure_type = 'permanent',"
            " failure_stage = 'submission', failure_reason = 'boom'"
            " WHERE work_ticket_idx = $1",
            first_ticket,
        )
    else:
        await postgres_pool.execute(
            "UPDATE qiita.work_ticket SET state = 'cancelled' WHERE work_ticket_idx = $1",
            first_ticket,
        )

    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    (second_ticket,) = await _item_ticket_idxs(postgres_pool, second_items[0].idx)
    assert second_ticket != first_ticket
    assert await postgres_pool.fetchval(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        second_ticket,
    ) == await postgres_pool.fetchval(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1",
        first_ticket,
    )

    await _cleanup_study(postgres_pool, accession)


async def test_submit_conflict_reuses_the_ticket_a_concurrent_batch_submitted(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Two batches can both read a pool as uncovered; the second's submit then
    409s and must reuse the first's in-flight ticket, not fail the item. The
    conflict recovery reads that one pool's ticket directly rather than
    re-running the run-wide fetch."""
    from qiita_control_plane.ena_import import batch as batch_module

    accession = unique_accession("PRJNA")
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)
    expected_pool_idx = await postgres_pool.fetchval(
        "SELECT sequenced_pool_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", first_ticket
    )

    real_fetch = batch_module.fetch_download_pool_states
    reads = 0

    async def stale_first_read(pool_or_conn, sequencing_run_idx):
        nonlocal reads
        reads += 1
        rows = await real_fetch(pool_or_conn, sequencing_run_idx)
        if reads == 1:
            return [{**r, "work_ticket_idx": None, "work_ticket_state": None} for r in rows]
        return rows

    monkeypatch.setattr(batch_module, "fetch_download_pool_states", stale_first_read)

    real_pool_ticket = batch_module.fetch_pool_download_ticket
    pool_ticket_calls: list[int] = []

    async def counting_pool_ticket(pool_or_conn, *, sequenced_pool_idx):
        pool_ticket_calls.append(sequenced_pool_idx)
        return await real_pool_ticket(pool_or_conn, sequenced_pool_idx=sequenced_pool_idx)

    monkeypatch.setattr(batch_module, "fetch_pool_download_ticket", counting_pool_ticket)

    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    # Conflict recovery reads only the conflicted pool, never the run-wide states.
    assert reads == 1
    assert pool_ticket_calls == [expected_pool_idx]
    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item WHERE idx = $1",
        second_items[0].idx,
    )
    assert item_row["failure_reason"] is None
    assert item_row["state"] == BatchItemState.DOWNLOADING.value
    assert list(item_row["download_work_ticket_idxs"]) == [first_ticket]
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
            DOWNLOAD_ENA_STUDY_ACTION_ID,
            DOWNLOAD_ENA_STUDY_ACTION_VERSION,
        )
        == 1
    )

    await _cleanup_study(postgres_pool, accession)


async def test_platform_whose_runs_all_failed_gets_no_ticket(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A failed platform must neither submit a ticket that can only fail on an empty
    pool nor drag the item to `failed` when the other platform downloads."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(
        _QUERY_RUNS, lambda a: _fake_run_rows(a, [("ok", "ILLUMINA"), ("bad", "OXFORD_NANOPORE")])
    )
    monkeypatch.setattr(
        _QUERY_ATTRS,
        lambda a: [
            (f"SAMN-{a}-ok", {"collection date": "2020-01-01"}),
            (f"SAMN-{a}-bad", {"geographic location (latitude)": "not-a-number"}),
        ],
    )
    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)

    row = await postgres_pool.fetchrow(
        "SELECT state, download_work_ticket_idxs FROM qiita.ena_import_batch_item WHERE idx = $1",
        items[0].idx,
    )
    assert row["state"] == BatchItemState.DOWNLOADING.value
    (ticket_idx,) = row["download_work_ticket_idxs"]
    assert await _ticket_pool_run_accessions(postgres_pool, ticket_idx) == {f"SRR-{accession}-ok"}

    await _cleanup_study(postgres_pool, accession)


async def test_fetch_batch_status_rolls_up_cancelled_ticket_to_failed(
    postgres_pool, admin_principal, download_ena_study_action, dummy_reference_idx, batch_cleanup
):
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool, accessions=[accession], principal=admin_principal
    )
    batch_cleanup.append(batch_idx)

    action_id, version = download_ena_study_action
    ticket_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, action_context, state)"
        " VALUES ($1, $2, $3, 'reference'::qiita.scope_target_kind, $4, '{}'::jsonb,"
        "         'cancelled'::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        admin_principal.principal_idx,
        dummy_reference_idx,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item"
        " SET state = 'downloading', download_work_ticket_idxs = $2"
        " WHERE idx = $1",
        items[0].idx,
        [ticket_idx],
    )

    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status.items[0].state == BatchItemState.FAILED
    assert f"{ticket_idx} (cancelled)" in status.items[0].failure_reason

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket_idx
    )


async def test_redrive_drops_a_ticket_it_replaced(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """A ticket that failed before a restart is replaced on re-drive, and must not
    stay on the item to roll it up as `failed` once its replacement completes."""
    accession = unique_accession("PRJNA")
    batch_idx, items = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(batch_idx)
    (failed_ticket,) = await _item_ticket_idxs(postgres_pool, items[0].idx)
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'cancelled' WHERE work_ticket_idx = $1",
        failed_ticket,
    )
    await postgres_pool.execute(
        "UPDATE qiita.ena_import_batch_item SET state = 'registered' WHERE idx = $1",
        items[0].idx,
    )

    assert await reconcile_inflight_batches(batch_app) == 1
    await asyncio.gather(*list(batch_app.state.running_ena_import_batches))

    (replacement,) = await _item_ticket_idxs(postgres_pool, items[0].idx)
    assert replacement != failed_ticket
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'completed' WHERE work_ticket_idx = $1", replacement
    )
    status = await fetch_batch_status(postgres_pool, batch_idx=batch_idx)
    assert status.items[0].state == BatchItemState.DONE

    await _cleanup_study(postgres_pool, accession)


# ---------------------------------------------------------------------------
# The concurrency bound is process-wide: three batches scheduled together must
# never together hold more than _STUDY_CONCURRENCY connections.
# ---------------------------------------------------------------------------


async def test_concurrency_bound_is_shared_across_batches(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Three batches scheduled at once are throttled by ONE process-wide limit,
    not one limit per batch. Gated at ticket submission -- the last thing
    `_process_one_study` does -- so the semaphore permit must still be held
    that far into the item's flow, not just across `register_ena_study`."""
    from qiita_control_plane.ena_import import batch as batch_module
    from qiita_control_plane.routes import work_ticket as work_ticket_module

    concurrency = batch_module._STUDY_CONCURRENCY
    # A misconfigured `_STUDY_CONCURRENCY` must fail here with a clear message,
    # not as the spare-connection acquire timing out below.
    assert concurrency <= POSTGRES_POOL_MAX_SIZE - 1, (
        f"_STUDY_CONCURRENCY ({concurrency}) leaves the test pool"
        f" (max_size={POSTGRES_POOL_MAX_SIZE}) no spare connection"
    )

    real_submit = work_ticket_module.submit_work_ticket_core
    current_holders = 0
    max_holders = 0
    all_slots_held = asyncio.Event()
    release = asyncio.Event()

    async def _gated_submit_work_ticket_core(*, app, principal, body):
        nonlocal current_holders, max_holders
        async with postgres_pool.acquire():
            current_holders += 1
            max_holders = max(max_holders, current_holders)
            if current_holders == concurrency:
                all_slots_held.set()
            await release.wait()
            current_holders -= 1
        return await real_submit(app=app, principal=principal, body=body)

    monkeypatch.setattr(
        work_ticket_module, "submit_work_ticket_core", _gated_submit_work_ticket_core
    )

    # Fewer items per batch than the concurrency limit, so saturating it draws
    # from more than one batch -- proving the bound is shared, not per-batch --
    # while the total leaves items pending behind the gate.
    items_per_batch = concurrency - 1
    num_batches = 3
    total_items = items_per_batch * num_batches

    batches: list[tuple[int, list, list[str]]] = []
    for _ in range(num_batches):
        accessions = [unique_accession("PRJNA") for _ in range(items_per_batch)]
        batch_idx, items = await create_ena_import_batch(
            postgres_pool, accessions=accessions, principal=admin_principal
        )
        batch_cleanup.append(batch_idx)
        batches.append((batch_idx, items, accessions))

    tasks = [
        schedule_ena_import_batch(batch_app, items=items, principal=admin_principal)
        for _, items, _ in batches
    ]

    batch_idxs = [batch_idx for batch_idx, _, _ in batches]
    try:
        # Hang guard only -- the assertions below are what proves the bound.
        await asyncio.wait_for(all_slots_held.wait(), timeout=10)

        assert max_holders == concurrency

        # A saturated pool must still have a spare connection for an unrelated
        # caller -- every query below uses an explicit short timeout so a bound
        # that leaks past the pool's max_size fails fast here instead of hanging
        # the test on an un-timed-out acquire.
        async with postgres_pool.acquire(timeout=2) as conn:
            pending_count = await conn.fetchval(
                "SELECT count(*) FROM qiita.ena_import_batch_item"
                " WHERE batch_idx = ANY($1::bigint[]) AND state = $2",
                batch_idxs,
                BatchItemState.PENDING.value,
            )
        assert pending_count == total_items - concurrency

        async with postgres_pool.acquire(timeout=2) as conn:
            assert await conn.fetchval("SELECT 1") == 1

        release.set()
        await asyncio.gather(*tasks)

        assert max_holders == concurrency

        item_states = await postgres_pool.fetch(
            "SELECT state FROM qiita.ena_import_batch_item WHERE batch_idx = ANY($1::bigint[])",
            batch_idxs,
        )
        assert len(item_states) == total_items
        assert {r["state"] for r in item_states} == {BatchItemState.DOWNLOADING.value}
    finally:
        # Always unblock the gate and let the batch tasks run to completion,
        # even on assertion failure -- otherwise they keep running past the
        # test, and the cleanup below (needed so admin_principal's teardown FK
        # delete doesn't fail against rows a still-running task just wrote)
        # never happens.
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        for _, _, accessions in batches:
            for accession in accessions:
                await _cleanup_study(postgres_pool, accession)


# ---------------------------------------------------------------------------
# ENA availability re-check on re-import: a run Qiita already holds that the
# Portal's fresh response no longer names is checked against the Browser API
# and flagged/cleared; the study itself being absent is a variant of the same
# detection.
# ---------------------------------------------------------------------------


def _empty_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    """Zero rows -- the shape `MiintEnaResolver.resolve_study_header` turns
    into `EnaAccessionNotFoundError`."""
    return ["study_accession", "secondary_study_accession", "study_title", "status"], []


class _FakeAvailabilityClient:
    """Network-free stand-in for `EnaAvailabilityClient`, injected by
    monkeypatching the name `ena_import.batch` imports it under. `results`
    maps run_accession -> the status `check_runs` should report; a lookup for a
    run not in `results` is a test bug (KeyError), not a silent None.

    `raise_after_calls`, if given, lets the first N lookups succeed normally and
    raises on the next one -- for proving a multi-candidate re-import
    writes no flags at all when a later lookup fails, not just the one that
    failed."""

    def __init__(
        self,
        results: dict[str, str | None] | None = None,
        *,
        raises: Exception | None = None,
        raise_after_calls: int | None = None,
    ):
        self._results = results or {}
        self._raises = raises
        self._raise_after_calls = raise_after_calls
        self._calls = 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return None

    async def check_runs(self, run_accessions: list[str]) -> dict[str, str | None]:
        statuses = {}
        for accession in run_accessions:
            self._calls += 1
            if self._raise_after_calls is not None and self._calls > self._raise_after_calls:
                raise RuntimeError(f"boom after {self._raise_after_calls} lookup(s)")
            if self._raises is not None:
                raise self._raises
            statuses[accession] = self._results[accession]
        return statuses


def _patch_availability_client(monkeypatch, **kwargs):
    monkeypatch.setattr(
        "qiita_control_plane.ena_import.batch.EnaAvailabilityClient",
        lambda: _FakeAvailabilityClient(**kwargs),
    )


async def _ena_status(postgres_pool, run_accession: str) -> str | None:
    return await postgres_pool.fetchval(
        "SELECT ena_status FROM qiita.sequenced_sample WHERE ena_run_accession = $1",
        run_accession,
    )


async def test_reimport_flags_a_held_run_the_portal_stopped_returning(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    assert await _ena_status(postgres_pool, held_run) is None

    # The Portal now returns only a new run -- run "1" is a candidate.
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) == "suppressed"
    status = await fetch_batch_status(postgres_pool, batch_idx=second_idx)
    flagged = [o for o in status.items[0].ena_runs if o.run_accession == held_run]
    assert len(flagged) == 1
    assert flagged[0].status == EnaRunRegistrationStatus.FLAGGED_UNAVAILABLE.value
    assert flagged[0].failure_reason == "suppressed"

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_clears_flag_when_public_again(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(
        _QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA"), ("2", "ILLUMINA")])
    )
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    await postgres_pool.execute(
        "UPDATE qiita.sequenced_sample SET ena_status = 'suppressed',"
        " ena_availability_checked_at = now()"
        " WHERE ena_run_accession = $1",
        held_run,
    )

    # The Portal reports both runs again -- run "1" reappeared, so its flag clears
    # with no Browser API lookup.
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) is None

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_absent_study_flags_held_runs_and_fails_naming_why(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"

    monkeypatch.setattr(_QUERY_STUDY, lambda a: _empty_study_header(a))
    _patch_availability_client(monkeypatch, results={held_run: "withdrawn"})
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) == "withdrawn"
    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        second_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "held runs re-checked" in item_row["failure_reason"]
    assert held_run in item_row["failure_reason"]

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_availability_check_failure_writes_no_flags(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Any Browser API failure -- a non-200 response or an unparseable body --
    fails the item and leaves every held run's flag exactly as found."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, raises=RuntimeError("boom"))
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) is None
    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        second_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "boom" in item_row["failure_reason"]

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_availability_failure_registers_no_new_run(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """A held run's Browser API lookup failing must fail the item before
    `register_ena_study` writes anything -- a new public run the same
    re-import would otherwise register must not land either, or the item's
    audit trail would undercount what was durably written."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    new_run = f"SRR-{accession}-2"

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, raises=RuntimeError("boom"))
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) is None
    assert (
        await postgres_pool.fetchval(
            "SELECT idx FROM qiita.sequenced_sample WHERE ena_run_accession = $1", new_run
        )
        is None
    )
    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason, ena_run_outcomes FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1",
        second_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "boom" in item_row["failure_reason"]
    assert json.loads(item_row["ena_run_outcomes"]) == []

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_held_run_http_500_fails_with_no_flags(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """End-to-end through the real `EnaAvailabilityClient` (no `_FakeAvailabilityClient`
    stand-in): a held run's Browser API summary returning HTTP 500 fails the
    re-import with a clear reason and writes no flag, exactly like any other
    HTTP error -- there is no `not_retrievable` flag to write."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, content=b"{}")

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    monkeypatch.setattr(
        "qiita_control_plane.ena_import.batch.EnaAvailabilityClient",
        lambda: EnaAvailabilityClient(
            http_client=httpx.AsyncClient(
                base_url="https://www.ebi.ac.uk/ena/browser/api",
                transport=httpx.MockTransport(handler),
            )
        ),
    )
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) is None
    item_row = await postgres_pool.fetchrow(
        "SELECT state, failure_reason FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        second_idx,
    )
    assert item_row["state"] == BatchItemState.FAILED.value
    assert "500" in item_row["failure_reason"]

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_availability_check_partial_failure_writes_no_flags(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Two held runs are candidates; the Browser API check succeeds for the
    first and raises on the second. Neither flag is written -- every lookup
    must succeed before any write lands, not just the ones checked before the
    failure (see `batch._reconcile_held_run_availability`)."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(
        _QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA"), ("2", "ILLUMINA")])
    )
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run_1 = f"SRR-{accession}-1"
    held_run_2 = f"SRR-{accession}-2"

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("3", "ILLUMINA")]))
    _patch_availability_client(
        monkeypatch,
        results={held_run_1: "suppressed", held_run_2: "suppressed"},
        raise_after_calls=1,
    )
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run_1) is None
    assert await _ena_status(postgres_pool, held_run_2) is None
    item_row = await postgres_pool.fetchrow(
        "SELECT state FROM qiita.ena_import_batch_item WHERE batch_idx = $1", second_idx
    )
    assert item_row["state"] == BatchItemState.FAILED.value

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_flagged_run_stays_in_its_pool(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Flagging is a read-time exclusion, not a deletion or retirement -- the
    row and its pool membership are untouched."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    before = await postgres_pool.fetchrow(
        "SELECT idx, sequenced_pool_idx, prep_sample_idx FROM qiita.sequenced_sample"
        " WHERE ena_run_accession = $1",
        held_run,
    )

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    after = await postgres_pool.fetchrow(
        "SELECT idx, sequenced_pool_idx, prep_sample_idx, ena_status FROM qiita.sequenced_sample"
        " WHERE ena_run_accession = $1",
        held_run,
    )
    assert after["idx"] == before["idx"]
    assert after["sequenced_pool_idx"] == before["sequenced_pool_idx"]
    assert after["prep_sample_idx"] == before["prep_sample_idx"]
    assert after["ena_status"] == "suppressed"
    ps_retired = await postgres_pool.fetchval(
        "SELECT retired FROM qiita.prep_sample WHERE idx = $1", after["prep_sample_idx"]
    )
    assert ps_retired is False

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_logs_non_terminal_ticket_for_flagged_run(
    batch_app,
    postgres_pool,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    monkeypatch,
    caplog,
):
    """A flagged run is not auto-cancelled; any non-terminal work_ticket
    touching its pool is logged for an operator to act on."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)
    ticket_state = await postgres_pool.fetchval(
        "SELECT state FROM qiita.work_ticket WHERE work_ticket_idx = $1", first_ticket
    )
    assert ticket_state in NON_TERMINAL_WORK_TICKET_STATES  # precondition: still in flight

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    with caplog.at_level("WARNING"):
        second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert any(
        held_run in record.message and str(first_ticket) in record.message
        for record in caplog.records
    )

    await _cleanup_study(postgres_pool, accession)


async def test_flagged_only_pool_gets_no_download_ticket_in_the_batch_flow(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """`fetch_download_pool_states` (`has_sequenced_sample`) is read per
    sequencing_run, so a re-import that opens a NEW pool for a new run also
    re-reads every OLDER pool sharing that run. A pool whose only sample was
    just flagged must not surface a download ticket in the batch flow -- only
    the new pool's ticket does."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    (first_ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)

    # Run "1" is now a candidate (missing from the Portal); run "2" is new and
    # needs its own pool, since the first pool's ticket (still pending) covers it.
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    second_idx, second_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) == "suppressed"
    ticket_idxs = await _item_ticket_idxs(postgres_pool, second_items[0].idx)
    assert first_ticket not in ticket_idxs
    assert len(ticket_idxs) == 1
    assert await _ticket_pool_run_accessions(postgres_pool, ticket_idxs[0]) == {
        f"SRR-{accession}-2"
    }

    await _cleanup_study(postgres_pool, accession)


async def _item_row(postgres_pool, batch_idx: int):
    return await postgres_pool.fetchrow(
        "SELECT state, failure_reason, ena_run_outcomes FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1",
        batch_idx,
    )


def _outcome_statuses(item_row) -> dict[str, str]:
    return {o["run_accession"]: o["status"] for o in json.loads(item_row["ena_run_outcomes"])}


@pytest.mark.parametrize(
    "portal",
    ["no_runs", "study_not_public", "no_public_runs"],
)
async def test_reimport_early_exit_still_checks_held_runs(
    portal,
    batch_app,
    postgres_pool,
    admin_principal,
    download_ena_study_action,
    batch_cleanup,
    monkeypatch,
):
    """Every exit that fails a re-import before registration still re-checks
    the held runs: the Portal drops non-public runs, so "every run suppressed"
    arrives as one of these."""
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"

    if portal == "no_runs":
        monkeypatch.setattr(_QUERY_RUNS, lambda a: (list(_RUN_COLUMNS), []))
    elif portal == "study_not_public":
        monkeypatch.setattr(_QUERY_STUDY, lambda a: _fake_study_header(a, status="suppressed"))
        monkeypatch.setattr(_QUERY_RUNS, lambda a: (list(_RUN_COLUMNS), []))
    else:
        monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_runs(a, status="suppressed"))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) == "suppressed"
    row = await _item_row(postgres_pool, second_idx)
    assert row["state"] == BatchItemState.FAILED.value
    assert held_run in row["failure_reason"]
    assert _outcome_statuses(row) == {held_run: "flagged_unavailable"}

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_keeps_flagged_outcomes_when_registration_then_fails(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("2", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("register blew up")

    with monkeypatch.context() as m:
        m.setattr("qiita_control_plane.ena_import.batch.register_ena_study", _boom)
        second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    row = await _item_row(postgres_pool, second_idx)
    assert row["state"] == BatchItemState.FAILED.value
    assert "register blew up" in row["failure_reason"]
    assert _outcome_statuses(row) == {held_run: "flagged_unavailable"}

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_reports_a_held_run_its_completed_download_never_fetched(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Run 1 was flagged before its pool's download read the roster, the
    download completed without it, and ENA then re-released it. Nothing will
    download it, so the re-import says so instead of `skipped_already_present`."""
    accession = unique_accession("PRJNA")
    both = lambda a: _fake_run_rows(a, [("1", "ILLUMINA"), ("3", "ILLUMINA")])  # noqa: E731
    monkeypatch.setattr(_QUERY_RUNS, both)
    first_idx, first_items = await _drive_one_study(
        batch_app, postgres_pool, admin_principal, accession
    )
    batch_cleanup.append(first_idx)
    run1, run3 = f"SRR-{accession}-1", f"SRR-{accession}-3"
    (ticket,) = await _item_ticket_idxs(postgres_pool, first_items[0].idx)

    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("3", "ILLUMINA")]))
    _patch_availability_client(monkeypatch, results={run1: "suppressed"})
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    # The download reads a roster without run 1, stores run 3, and completes.
    run3_prep_sample = await postgres_pool.fetchval(
        "SELECT prep_sample_idx FROM qiita.sequenced_sample WHERE ena_run_accession = $1", run3
    )
    await mint_sequence_range(
        postgres_pool,
        prep_sample_idx=run3_prep_sample,
        count=10,
        principal_idx=admin_principal.principal_idx,
        work_ticket_idx=None,
    )
    await postgres_pool.execute(
        "UPDATE qiita.work_ticket SET state = 'completed' WHERE work_ticket_idx = $1", ticket
    )

    monkeypatch.setattr(_QUERY_RUNS, both)
    third_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(third_idx)

    assert await _ena_status(postgres_pool, run1) is None
    row = await _item_row(postgres_pool, third_idx)
    assert _outcome_statuses(row) == {
        run1: "held_not_downloaded",
        run3: "skipped_already_present",
    }

    await _cleanup_study(postgres_pool, accession)


async def test_reimport_does_not_clear_a_flag_for_a_run_returned_as_non_public(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    accession = unique_accession("PRJNA")
    monkeypatch.setattr(_QUERY_RUNS, lambda a: _fake_run_rows(a, [("1", "ILLUMINA")]))
    first_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(first_idx)
    held_run = f"SRR-{accession}-1"
    await postgres_pool.execute(
        "UPDATE qiita.sequenced_sample SET ena_status = 'suppressed',"
        " ena_availability_checked_at = now() WHERE ena_run_accession = $1",
        held_run,
    )

    def _runs(a):
        cols, rows = _fake_run_rows(a, [("1", "ILLUMINA"), ("2", "ILLUMINA")])
        status_i = cols.index("status")
        first = list(rows[0])
        first[status_i] = "suppressed"
        return cols, [tuple(first), rows[1]]

    monkeypatch.setattr(_QUERY_RUNS, _runs)
    _patch_availability_client(monkeypatch, results={held_run: "suppressed"})
    second_idx, _ = await _drive_one_study(batch_app, postgres_pool, admin_principal, accession)
    batch_cleanup.append(second_idx)

    assert await _ena_status(postgres_pool, held_run) == "suppressed"

    await _cleanup_study(postgres_pool, accession)
