"""DB-bound tests for the batch multi-study ENA import driver.

Network-free: the DuckDB+miint resolver seam (`miint_resolver._query_ena_*`)
is monkeypatched per accession, exactly like `test_miint_resolver.py` —
mirrors `qiita_control_plane.runner._stream_masked_reads_to_fastq`'s
established seam for connect_with_miint()-touching code. `_run_and_log` is
patched to a no-op so a submitted download-ena-study ticket doesn't try to
reach a real orchestrator, mirroring `tests/routes/test_work_ticket.py`'s
`_patch_run_and_log` fixture.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from types import SimpleNamespace

import pytest
import pytest_asyncio
from qiita_common.auth_constants import SystemRole
from qiita_common.models.ena import ResolverKind, SourceArchive
from qiita_common.models.ena_import import BatchItemState

from qiita_control_plane.auth.principal import HumanUser
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.ena_import.batch import (
    _process_one_study,
    create_ena_import_batch,
    fetch_batch_status,
    reconcile_inflight_batches,
    schedule_ena_import_batch,
)
from qiita_control_plane.testing.db_seeds import (
    disable_principal,
    retire_principal,
    seed_user_principal,
)
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
)


def _fake_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    return (
        ["study_accession", "secondary_study_accession", "study_title"],
        [(accession, None, f"title for {accession}")],
    )


def _fake_runs(accession: str) -> tuple[list[str], list[tuple]]:
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
        "",
        "",
        "",
        "",
        "",
        "",
    )
    return list(_RUN_COLUMNS), [row]


def _fake_attrs(accession: str) -> tuple[list[str], list[tuple]]:
    # At least one row, so most tests exercise the harmonized-metadata path
    # rather than the empty-attributes carve-out --
    # test_process_one_study_empty_sample_attributes_registers_not_failed
    # below monkeypatches _QUERY_ATTRS to zero rows specifically to cover
    # that case (a legitimate "no attributes" result, not a resolve error).
    return (
        ["sample_accession", "tag", "value"],
        [(f"SAMN-{accession}", "collection date", "2020-01-01")],
    )


@pytest.fixture(autouse=True)
def _monkeypatch_resolver_seam(monkeypatch):
    """Network-free resolver: every accession resolves to one run + no
    sample attributes, keyed on the accession itself so distinct batch
    items land on distinct studies/runs/samples."""
    monkeypatch.setattr(_QUERY_STUDY, lambda accession: _fake_study_header(accession))
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: _fake_runs(accession))
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: _fake_attrs(accession))


@pytest.fixture(autouse=True)
def _patch_run_and_log(monkeypatch):
    """No-op the actual workflow dispatch -- these tests only care that a
    download-ena-study work_ticket row was correctly submitted, not that a
    real orchestrator ran it. Mirrors test_work_ticket.py."""

    async def _noop(_app, _idx, **_kwargs):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest_asyncio.fixture
async def batch_app(postgres_pool):
    """The shared main.app, configured for direct (non-HTTP) calls into the
    batch driver -- mirrors tests/routes/test_work_ticket.py's `wt_client`
    app-state setup, minus the HTTP client (these tests call
    `ena_import.batch` functions directly, not through a route)."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    # Save/restore: `app` is the process-wide FastAPI singleton, so a bare
    # stub left on `app.state.compute_backend_client` after this fixture
    # tears down would leak into a LATER test on the same xdist worker whose
    # own fixture does not set it (e.g. test_reference_delete.py's `client`,
    # which assumes the attribute is unset/None) -- that later test's route
    # then calls a method the generic stub doesn't have. Mirrors
    # tests/routes/test_ena_import.py's `eib_client` fixture.
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
    """A real seeded wet_lab_admin principal wrapped as a `HumanUser` --
    `submit_work_ticket_core`'s INSERT needs a real `qiita.principal` row
    (FK), and the download-ena-study action's audience admits
    wet_lab_admin/system_admin humans."""
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
    """Seed the real, pinned `download-ena-study`/`1.0.0` action row so
    `submit_work_ticket_core` can resolve it -- mirrors
    tests/test_runner.py's `_make_sequenced_pool_action` shape but pinned to
    the exact action_id/version `ena_import.submit` hardcodes."""
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


async def _cleanup_study(postgres_pool, study_accession: str) -> None:
    """Best-effort FK-reverse cleanup for one study this test created,
    keyed on the accession prefix -- mirrors test_registration.py's
    `_cleanup`, scoped to a single study rather than a tracked list."""
    study_idx = await postgres_pool.fetchval(
        "SELECT idx FROM qiita.study WHERE bioproject_accession = $1", study_accession
    )
    if study_idx is None:
        return
    # ena_import_batch_item.study_idx FKs (RESTRICT) into qiita.study; clear
    # the item's reference before the study DELETE below. The parent
    # ena_import_batch row is still removed by the `batch_cleanup` fixture
    # (idempotent no-op on this now-childless row).
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


async def _cleanup_two_studies_sharing_biosample(
    postgres_pool, *, study_accessions: list[str], shared_sample_accession: str
) -> None:
    """Teardown twin of `_cleanup_study` for a test whose two studies share
    ONE biosample row (batch-level de-dup). Unlike `_cleanup_study`
    (which deletes a study's own biosample rows outright), the shared
    biosample here still carries a live `biosample_to_study` link to the
    OTHER study while either single study is being torn down -- deleting it
    early would trip `biosample_to_study`'s `ON DELETE RESTRICT` FK. Clear
    both studies' links/prep data first, THEN drop the now-unlinked shared
    biosample exactly once, THEN each study row itself.
    """
    # Resolve + clear the shared biosample's metadata FIRST, before either
    # study's biosample_study_field rows are dropped below --
    # biosample_metadata FKs (RESTRICT) into biosample_study_field, so
    # metadata must go before the field rows it references, for BOTH
    # studies, not just the one being processed at the time.
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
        await postgres_pool.execute(
            "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = $1", study_idx
        )
        if ps_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", ps_idxs
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
    """A bare, unrelated `qiita.reference` row -- just something to satisfy
    `work_ticket_scope_target_consistent` for a `reference`-scoped ticket the
    rollup tests INSERT directly (the scope target itself is irrelevant to
    what's under test: `fetch_batch_status`'s ticket-state rollup)."""
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
    """Tracks batch idxs created by a test; deletes them (CASCADE handles
    ena_import_batch_item) at teardown."""
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    assert {item.ena_study_accession for item in items} == set(accessions)

    batch_row = await postgres_pool.fetchrow(
        "SELECT submitted_by_principal_idx, resolver_backend, source_archive, download_method"
        " FROM qiita.ena_import_batch WHERE idx = $1",
        batch_idx,
    )
    assert batch_row["submitted_by_principal_idx"] == admin_principal.principal_idx
    assert batch_row["resolver_backend"] == "miint"
    assert batch_row["source_archive"] == "ena"
    assert batch_row["download_method"] == "http"

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
    from qiita_control_plane.ena_import.accession import InvalidEnaAccessionError

    good = unique_accession("PRJNA")
    bad = "SAMN0000001"  # a SAMPLE accession, not a study accession

    with pytest.raises(InvalidEnaAccessionError):
        await create_ena_import_batch(
            postgres_pool,
            accessions=[good, bad],
            principal=admin_principal,
            resolver_backend="miint",
            source_archive=SourceArchive.ENA,
            download_method="http",
        )

    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch_item WHERE ena_study_accession = $1", good
    )
    assert count == 0


async def test_create_ena_import_batch_rejects_unknown_backend(
    postgres_pool, admin_principal, batch_cleanup
):
    accession = unique_accession("PRJNA")
    with pytest.raises(ValueError, match="unknown ENA resolver backend"):
        await create_ena_import_batch(
            postgres_pool,
            accessions=[accession],
            principal=admin_principal,
            resolver_backend="not-a-backend",
            source_archive=SourceArchive.ENA,
            download_method="http",
        )
    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch_item WHERE ena_study_accession = $1", accession
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
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
    assert context["download_method"] == "http"

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_empty_sample_attributes_registers_not_failed(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup, monkeypatch
):
    """Real DDBJ finding: a sample can have ZERO ENA attributes (study
    PRJDB40364's SAMD01818724 has no `<SAMPLE_ATTRIBUTE>` elements at all).
    `resolve_sample_attributes` returning `[]` for that case (rather than
    raising `EnaAccessionNotFoundError`) must not fail the whole study --
    it registers normally end to end through the batch driver, and the
    item is never marked `failed`."""
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: (["sample_accession", "tag", "value"], []))

    accession = unique_accession("PRJDB")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
    )
    await task

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, failure_reason"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    # NOT failed -- an empty attribute set is a legitimate resolve result,
    # not a resolve error.
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

    # No harmonized global metadata -- there were no attributes to harmonize.
    global_metadata_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND global_field_idx IS NOT NULL",
        biosample_row["idx"],
    )
    assert global_metadata_count == 0

    # Reported, not fatal: ERC000011's two mandatory fields are listed as
    # missing.
    missing_rows = await postgres_pool.fetch(
        "SELECT gf.display_name"
        " FROM qiita.metadata_checklist_field mcf"
        " JOIN qiita.biosample_global_field gf ON gf.idx = mcf.biosample_global_field_idx"
        " WHERE mcf.metadata_checklist_idx = $1"
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM qiita.biosample_metadata bm"
        "      WHERE bm.biosample_idx = $2 AND bm.global_field_idx = gf.idx"
        "   )"
        " ORDER BY gf.display_name",
        biosample_row["metadata_checklist_idx"],
        biosample_row["idx"],
    )
    assert [r["display_name"] for r in missing_rows] == [
        "collection date",
        "geographic location (country and/or sea)",
    ]

    prep_sample_count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.prep_sample_to_study WHERE study_idx = $1",
        item_row["study_idx"],
    )
    assert prep_sample_count == 1

    await _cleanup_study(postgres_pool, accession)


async def test_process_one_study_threads_batch_download_method_not_hardcoded_default(
    batch_app, postgres_pool, admin_principal, batch_cleanup, monkeypatch
):
    """The download-ena-study ticket's `action_context.download_method`
    must come from the CALLER'S `download_method` argument, never from a
    hardcoded `submit.DEFAULT_DOWNLOAD_METHOD` fallback inside the driver.

    The route persists (and the real DB CHECK only allows) `'http'` today,
    which is also `DEFAULT_DOWNLOAD_METHOD` -- so an end-to-end test alone
    cannot distinguish "correctly threaded" from "coincidentally matches the
    hardcoded default". This test closes that gap directly: it drives
    `_process_one_study` with a deliberately non-default value and a
    monkeypatched `submit_work_ticket_core` (bypassing the real action's
    context_schema, which legitimately only allows `'http'`), and asserts
    that exact value reaches the submitted ticket body verbatim.
    """
    accession = unique_accession("PRJNA")
    batch_idx, items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    captured_bodies = []

    async def _fake_submit_work_ticket_core(*, app, principal, body):
        captured_bodies.append(body)
        return SimpleNamespace(work_ticket_idx=999_999_999)

    monkeypatch.setattr(
        "qiita_control_plane.routes.work_ticket.submit_work_ticket_core",
        _fake_submit_work_ticket_core,
    )

    distinctive_download_method = "not-the-hardcoded-default"
    await _process_one_study(
        batch_app,
        postgres_pool,
        item=items[0],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method=distinctive_download_method,
    )

    assert len(captured_bodies) == 1
    assert captured_bodies[0].action_context["download_method"] == distinctive_download_method

    await _cleanup_study(postgres_pool, accession)


# ---------------------------------------------------------------------------
# Audience enforcement -- submit_work_ticket_core must reject a principal
# not in the download-ena-study action's own audience, even though the
# batch route itself is admin-gated
# ---------------------------------------------------------------------------


async def test_process_one_study_rejects_non_audience_principal_no_ticket_created(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """`_process_one_study` propagates the BATCH's own submitting principal
    into `submit_work_ticket_core`, which enforces the download-ena-study
    action's own audience against it -- never bypassed just because
    `POST /ena-import-batch` itself is admin-gated (see both functions'
    docstrings). Here the submitting principal is a plain `user`-role
    human, outside the action's `wet_lab_admin`/`system_admin`-only
    audience (`download_ena_study_action` fixture): the ticket submission
    must be rejected (403, audience) and NO `qiita.work_ticket` row
    created for it -- the study/pool registration itself has no audience
    gate and still succeeds, isolated per the batch's per-item failure model.
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=non_audience_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
    )
    # Must not raise -- rejected submissions are recorded as a per-item
    # failure, never propagated to fail the whole batch task.
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
    # Drop the batch (and its item, via CASCADE) explicitly before deleting
    # its submitting principal -- ena_import_batch.submitted_by_principal_idx
    # FKs (ON DELETE RESTRICT) into qiita.principal, and the `batch_cleanup`
    # fixture's own DELETE only runs at teardown, after this test function
    # returns. Making that fixture's later DELETE a no-op is fine (0 rows).
    await postgres_pool.execute("DELETE FROM qiita.ena_import_batch WHERE idx = $1", batch_idx)
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = $1", non_audience_pidx
    )
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", non_audience_pidx)


# ---------------------------------------------------------------------------
# Batch-driver-level de-dup: two items of the SAME batch whose runs
# resolve to a SHARED sample_accession still land as one biosample row
# (the register-level concurrency case -- two INDEPENDENT
# register_ena_study calls racing via asyncio.gather -- is already covered
# by test_registration.py's
# test_concurrent_registration_of_shared_biosample_dedupes_to_one_row; this
# closes the same invariant one level up, through the actual batch driver).
# ---------------------------------------------------------------------------


def _make_shared_sample_fakes(shared_sample_accession: str):
    """Build a (runs, attrs) fake-resolver pair where every accession's run
    carries the SAME `sample_accession`, keeping `study_accession` /
    `run_accession` / `experiment_accession` distinct per input accession
    (mirrors `_fake_runs` / `_fake_attrs` above, just pinning the sample
    accession to one shared value instead of deriving it from the input).
    """

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
            "",
            "",
            "",
            "",
            "",
            "",
        )
        return list(_RUN_COLUMNS), [row]

    def _fake_attrs_shared(accession: str) -> tuple[list[str], list[tuple]]:
        return (
            ["sample_accession", "tag", "value"],
            [(shared_sample_accession, "collection date", "2020-01-01")],
        )

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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
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
# Per-item isolation -- one accession's failure never affects its siblings or the batch
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    task = schedule_ena_import_batch(
        batch_app,
        items=items,
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        resolver_kind=ResolverKind.MIINT,
        download_method="http",
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
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

    # The batch row itself carries no state of its own to "fail" -- assert
    # the underlying item row was NOT mutated (rollup is read-only/on-demand).
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
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
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

    # The re-driven ticket's download_method must be the batch's OWN
    # persisted value (SELECTed by reconcile_inflight_batches), not a
    # hardcoded default -- see test_process_one_study_threads_batch_download_
    # method_not_hardcoded_default for the value-distinguishing unit test.
    work_ticket_idx = item_row["download_work_ticket_idxs"][0]
    context = json.loads(
        await postgres_pool.fetchval(
            "SELECT action_context FROM qiita.work_ticket WHERE work_ticket_idx = $1",
            work_ticket_idx,
        )
    )
    assert context["download_method"] == "http"

    await _cleanup_study(postgres_pool, accession)


async def test_reconcile_inflight_batches_no_op_when_nothing_in_flight(batch_app):
    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0


# ---------------------------------------------------------------------------
# reconcile_inflight_batches -- disabled/retired submitting principal must
# NOT be re-driven on their behalf (mirrors auth.principal._build_human_user's
# guard, reused here for the no-live-HTTP-request reconcile/batch path)
# ---------------------------------------------------------------------------


async def test_reconcile_inflight_batches_refuses_disabled_principal(
    batch_app, postgres_pool, admin_principal, download_ena_study_action, batch_cleanup
):
    """A batch submitted by an admin who was DISABLED after submission must
    not be re-driven on their behalf across a CP restart: `_load_principal`
    raises, `reconcile_inflight_batches` catches it and skips the batch
    entirely -- the item is left `pending` (untouched), no study or ticket
    is created, and nothing is scheduled for it."""
    accession = unique_accession("PRJNA")
    batch_idx, _items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    await disable_principal(postgres_pool, admin_principal.principal_idx)

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx, download_work_ticket_idxs"
        " FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.PENDING.value
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
    """Same guard, retired instead of disabled -- `qiita.principal.retired`
    is refused identically (mirrors `auth.principal._build_human_user`,
    which treats `disabled` and `retired` the same way)."""
    accession = unique_accession("PRJEB")
    batch_idx, _items = await create_ena_import_batch(
        postgres_pool,
        accessions=[accession],
        principal=admin_principal,
        resolver_backend="miint",
        source_archive=SourceArchive.ENA,
        download_method="http",
    )
    batch_cleanup.append(batch_idx)

    await retire_principal(postgres_pool, admin_principal.principal_idx)

    scheduled = await reconcile_inflight_batches(batch_app)
    assert scheduled == 0
    assert len(batch_app.state.running_ena_import_batches) == 0

    item_row = await postgres_pool.fetchrow(
        "SELECT state, study_idx FROM qiita.ena_import_batch_item WHERE batch_idx = $1",
        batch_idx,
    )
    assert item_row["state"] == BatchItemState.PENDING.value
    assert item_row["study_idx"] is None
