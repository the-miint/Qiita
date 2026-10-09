"""DB-bound integration tests for /api/v1/ena-import-batch.

Network-free: the DuckDB+miint resolver seam is monkeypatched per accession and
`_run_and_log` is patched to a no-op so a submitted ticket never reaches a real
orchestrator.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_ENA_IMPORT_BATCH_BY_IDX, URL_ENA_IMPORT_BATCH_PREFIX
from qiita_common.auth_constants import Scope, SystemRole

from qiita_control_plane.dispatch import build_dispatch_semaphore
from qiita_control_plane.ena_import import (
    DOWNLOAD_ENA_STUDY_ACTION_ID,
    DOWNLOAD_ENA_STUDY_ACTION_VERSION,
)
from qiita_control_plane.ena_import.batch import build_ena_import_study_semaphore
from qiita_control_plane.testing.db_teardown import (
    resolve_ena_study_idxs,
    teardown_ena_study_graph,
)
from qiita_control_plane.testing.unique_names import unique_ena_accession

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


def _fake_study_header(accession: str) -> tuple[list[str], list[tuple]]:
    return (
        ["study_accession", "secondary_study_accession", "study_title", "status"],
        [(accession, None, f"title for {accession}", "public")],
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
        [],
        [],
        [],
        [],
        None,
        None,
        "public",
    )
    return list(_RUN_COLUMNS), [row]


def _fake_attrs(accession: str) -> list[tuple[str, dict[str, list[str]]]]:
    return [(f"SAMN-{accession}", {"collection date": ["2020-01-01"]})]


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
    """App configured for ena-import-batch route tests, plus the batch driver's
    own tracked task set."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.oidc_verifier = None
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    # Save/restore: `app` is the process-wide FastAPI singleton, so a stub left
    # on `app.state.compute_backend_client` would leak into a later test on the
    # same xdist worker whose fixture assumes it unset.
    saved_compute_backend_client = getattr(app.state, "compute_backend_client", None)
    app.state.compute_backend_client = stub_compute_backend_client
    app.state.running_dispatches = set()
    app.state.running_ena_import_batches = set()
    app.state.ena_import_study_semaphore = build_ena_import_study_semaphore()
    app.state.dispatch_semaphore = build_dispatch_semaphore()

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
    """Tear down the study registered under this accession, its entity graph,
    and the runs it registered."""
    study_idxs = await resolve_ena_study_idxs(postgres_pool, [study_accession])
    await teardown_ena_study_graph(
        postgres_pool, study_idxs=study_idxs, run_accessions=[study_accession]
    )


async def _await_batch_tasks(eib_client) -> None:
    """Await every tracked ena_import_batch background task so a test can assert
    on post-processing state without racing it."""
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
    """POST N accessions -> 202 with N pending items; N studies get registered
    by the background task."""
    token, _ = admin_token
    accessions = [unique_ena_accession("PRJNA"), unique_ena_accession("PRJEB")]

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
        json={"accessions": [unique_ena_accession("PRJNA")]},
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
    good = unique_ena_accession("PRJNA")
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


async def test_submit_rejects_bare_prefix_without_creating_batch(
    eib_client, admin_token, postgres_pool
):
    token, pidx = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [unique_ena_accession("PRJNA"), "PRJEB"]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 422, resp.text
    assert "followed by digits" in resp.json()["detail"]

    count = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.ena_import_batch WHERE submitted_by_principal_idx = $1",
        pidx,
    )
    assert count == 0


@pytest.mark.parametrize("unknown_field", ["backend", "source"])
async def test_submit_rejects_unknown_request_fields(eib_client, admin_token, unknown_field):
    """`BatchImportRequest` pins extra="forbid" like every other *Request model
    here, so an undeclared field 422s rather than being silently dropped.
    `backend` and `source` are the two this PR removed."""
    token, _ = admin_token
    resp = await eib_client.post(
        URL_ENA_IMPORT_BATCH_PREFIX,
        json={"accessions": [unique_ena_accession("PRJNA")], unknown_field: "whatever"},
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
            json={"accessions": [unique_ena_accession("PRJNA")]},
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
    ok_accession = unique_ena_accession("PRJNA")
    bad_accession = unique_ena_accession("PRJEB")

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
