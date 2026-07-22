"""DB-bound integration tests for /api/v1/ena-import-batch.

Network-free: the DuckDB+miint resolver seam (`miint_resolver._query_ena_*`)
is monkeypatched per accession, mirroring `test_miint_resolver.py` /
`tests/ena_import/test_batch.py`. `_run_and_log` is patched to a no-op so a
submitted download-ena-study ticket doesn't try to reach a real
orchestrator, mirroring `tests/routes/test_work_ticket.py`.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ENA_IMPORT_BATCH_BY_IDX, URL_ENA_IMPORT_BATCH_PREFIX
from qiita_common.auth_constants import Scope, SystemRole

from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
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
    return (
        ["sample_accession", "tag", "value"],
        [(f"SAMN-{accession}", "collection date", "2020-01-01")],
    )


@pytest.fixture(autouse=True)
def _monkeypatch_resolver_seam(monkeypatch):
    monkeypatch.setattr(_QUERY_STUDY, lambda accession: _fake_study_header(accession))
    monkeypatch.setattr(_QUERY_RUNS, lambda accession: _fake_runs(accession))
    monkeypatch.setattr(_QUERY_ATTRS, lambda accession: _fake_attrs(accession))


@pytest.fixture(autouse=True)
def _patch_run_and_log(monkeypatch):
    async def _noop(_app, _idx, **_kwargs):
        return None

    monkeypatch.setattr("qiita_control_plane.dispatch._run_and_log", _noop)


@pytest.fixture
async def stub_compute_backend_client():
    return object()


@pytest.fixture
async def eib_client(postgres_pool, stub_compute_backend_client):
    """App configured for ena-import-batch route tests -- mirrors
    tests/routes/test_work_ticket.py's `wt_client` fixture, plus the
    batch driver's own tracked task set."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    # Save/restore (mirrors test_work_ticket.py's own save/restore idiom for
    # this exact attribute): `app` is the process-wide FastAPI singleton, so a
    # bare stub left on `app.state.compute_backend_client` after this fixture
    # tears down would leak into a LATER test on the same xdist worker whose
    # own fixture does not set it (e.g. test_reference_delete.py's `client`,
    # which assumes the attribute is unset/None) -- that later test's route
    # then calls a method the generic stub doesn't have.
    saved_compute_backend_client = getattr(app.state, "compute_backend_client", None)
    app.state.compute_backend_client = stub_compute_backend_client
    app.state.running_dispatches = set()
    app.state.running_ena_import_batches = set()

    created_principals: list[int] = []
    created_batches: list[int] = []
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        ac._created_principals = created_principals  # type: ignore[attr-defined]
        ac._created_batches = created_batches  # type: ignore[attr-defined]
        yield ac

    if created_batches:
        await postgres_pool.execute(
            "DELETE FROM qiita.ena_import_batch WHERE idx = ANY($1::bigint[])", created_batches
        )
    if created_principals:
        async with postgres_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "ALTER TABLE qiita.auth_event DISABLE TRIGGER auth_event_no_delete"
                )
                try:
                    for table in ("api_token", "user_identity", "user", "service_account"):
                        await conn.execute(
                            f"DELETE FROM qiita.{table} WHERE principal_idx = ANY($1::bigint[])",
                            created_principals,
                        )
                    await conn.execute(
                        "DELETE FROM qiita.auth_event"
                        " WHERE principal_idx = ANY($1::bigint[])"
                        "    OR actor_principal_idx = ANY($1::bigint[])",
                        created_principals,
                    )
                    await conn.execute(
                        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
                        created_principals,
                    )
                finally:
                    await conn.execute(
                        "ALTER TABLE qiita.auth_event ENABLE TRIGGER auth_event_no_delete"
                    )

    pending = list(app.state.running_dispatches) + list(app.state.running_ena_import_batches)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)
    app.state.compute_backend_client = saved_compute_backend_client


@pytest.fixture
async def admin_token(postgres_pool, eib_client):
    from qiita_control_plane.auth.token import mint_api_token

    email = f"eib-admin-{uuid.uuid4()}@example.com"
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        SystemRole.WET_LAB_ADMIN,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        pidx,
        email,
    )
    eib_client._created_principals.append(pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="eib-admin",
        scopes=[Scope.SELF_PROFILE],
    )
    return plaintext, pidx


@pytest.fixture
async def regular_token(postgres_pool, eib_client):
    from qiita_control_plane.auth.token import mint_api_token

    email = f"eib-user-{uuid.uuid4()}@example.com"
    pidx = await postgres_pool.fetchval(
        "INSERT INTO qiita.principal (display_name, system_role, created_by_idx)"
        " VALUES ($1, $2, 1) RETURNING idx",
        email,
        SystemRole.USER,
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.user (principal_idx, email, affiliation, address, phone)"
        " VALUES ($1, $2, 'X', 'Y', 'Z')",
        pidx,
        email,
    )
    eib_client._created_principals.append(pidx)
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=pidx,
        label="eib-user",
        scopes=[Scope.SELF_PROFILE],
    )
    return plaintext, pidx


@pytest.fixture
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
    bs_rows = await postgres_pool.fetch(
        "SELECT biosample_idx FROM qiita.biosample_to_study WHERE study_idx = $1", study_idx
    )
    bs_idxs = [r["biosample_idx"] for r in bs_rows]
    if bs_idxs:
        await postgres_pool.execute(
            "DELETE FROM qiita.biosample_metadata WHERE biosample_idx = ANY($1::bigint[])",
            bs_idxs,
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


async def _await_batch_tasks(eib_client) -> None:
    """Await every ena_import_batch background task the app currently
    tracks, so a test can assert on post-processing state without racing
    the background task."""
    from qiita_control_plane.main import app

    tasks = list(app.state.running_ena_import_batches)
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# POST /api/v1/ena-import-batch
# ---------------------------------------------------------------------------


async def test_submit_returns_202_with_pending_items(
    eib_client, postgres_pool, admin_token, download_ena_study_action
):
    """POST N accessions -> 202 with N pending items; N studies get
    registered by the (monkeypatched, network-free) background task."""
    token, _ = admin_token
    accessions = [unique_accession("PRJNA"), unique_accession("PRJEB")]

    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": accessions},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    eib_client._created_batches.append(body["ena_import_batch_idx"])
    assert {item["ena_study_accession"] for item in body["items"]} == set(accessions)
    assert {item["state"] for item in body["items"]} == {"pending"}

    await _await_batch_tasks(eib_client)

    rows = await postgres_pool.fetch(
        "SELECT ena_study_accession, state, study_idx FROM qiita.ena_import_batch_item"
        " WHERE batch_idx = $1",
        body["ena_import_batch_idx"],
    )
    assert len(rows) == 2
    assert {r["state"] for r in rows} == {"downloading"}
    assert all(r["study_idx"] is not None for r in rows)

    for accession in accessions:
        await _cleanup_study(postgres_pool, accession)


async def test_submit_requires_admin_role(eib_client, regular_token):
    token, _ = regular_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [unique_accession("PRJNA")]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text


async def test_submit_rejects_empty_accessions(eib_client, admin_token):
    token, _ = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": []},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text


async def test_submit_rejects_malformed_accession(eib_client, admin_token, postgres_pool):
    token, _ = admin_token
    good = unique_accession("PRJNA")
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [good, "SAMN0000001"]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text

    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch_item WHERE ena_study_accession = $1", good
    )
    assert count == 0


async def test_submit_rejects_unsupported_download_method(eib_client, admin_token):
    token, _ = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [unique_accession("PRJNA")], "download_method": "aspera"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    assert "download_method" in resp.text


async def test_submit_rejects_unknown_source(eib_client, admin_token):
    token, _ = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [unique_accession("PRJNA")], "source": "not-a-real-archive"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text


async def test_submit_503_when_compute_backend_unconfigured(eib_client, admin_token):
    from qiita_control_plane.main import app

    saved = app.state.compute_backend_client
    app.state.compute_backend_client = None
    try:
        token, _ = admin_token
        resp = await eib_client.post(
            URL_ENA_IMPORT_BATCH_PREFIX,
            json={"accessions": [unique_accession("PRJNA")]},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert resp.status_code == 503
    finally:
        app.state.compute_backend_client = saved


# ---------------------------------------------------------------------------
# Per-item isolation -- one accession's failure never fails the batch as a whole
# ---------------------------------------------------------------------------


async def test_submit_isolates_per_study_failure_and_reports_per_item(
    eib_client, postgres_pool, admin_token, download_ena_study_action, monkeypatch
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

    token, _ = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [ok_accession, bad_accession]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 202, resp.text
    batch_idx = resp.json()["ena_import_batch_idx"]
    eib_client._created_batches.append(batch_idx)

    await _await_batch_tasks(eib_client)

    get_resp = await eib_client.get(
        URL_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=batch_idx),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert get_resp.status_code == 200, get_resp.text
    by_accession = {item["ena_study_accession"]: item for item in get_resp.json()["items"]}

    assert by_accession[ok_accession]["state"] == "downloading"
    assert by_accession[bad_accession]["state"] == "failed"
    assert "simulated resolver failure" in by_accession[bad_accession]["failure_reason"]

    await _cleanup_study(postgres_pool, ok_accession)


# ---------------------------------------------------------------------------
# GET /api/v1/ena-import-batch/{idx}
# ---------------------------------------------------------------------------


async def test_get_requires_admin_role(eib_client, regular_token):
    token, _ = regular_token
    resp = await eib_client.get(
        URL_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=1),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text


async def test_get_unknown_batch_404(eib_client, admin_token):
    token, _ = admin_token
    resp = await eib_client.get(
        URL_ENA_IMPORT_BATCH_BY_IDX.format(ena_import_batch_idx=999_999_999),
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 404, resp.text
