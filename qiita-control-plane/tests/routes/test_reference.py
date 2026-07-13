"""Integration tests for reference routes — exercises POST/GET against real Postgres."""

import base64
import json
import struct
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_REFERENCE_BY_IDX,
    URL_REFERENCE_DOGET,
    URL_REFERENCE_INDEX,
    URL_REFERENCE_PREFIX,
    URL_REFERENCE_SHARD_INDEX_STATUS,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

pytestmark = pytest.mark.db


@pytest.fixture
async def client(postgres_pool, human_admin_session):
    """AsyncClient wired to the control plane app with the integration test pool
    and a session-scoped admin PAT preset on the Authorization header.

    ASGITransport does not trigger FastAPI lifespan, so the pool from conftest
    is injected directly into app.state.
    """
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool

    created_refs: list[int] = []

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {human_admin_session['token']}"},
    ) as ac:
        ac._created_refs = created_refs
        yield ac

    # Cleanup only rows we created, in FK dependency order (RESTRICT FKs).
    if created_refs:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_membership WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )
        await postgres_pool.execute(
            "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
            created_refs,
        )


async def _create_ref(client, name, version="1.0", kind="sequence_reference"):
    """Helper: create a reference and track its idx for cleanup."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={"name": name, "version": version, "kind": kind},
    )
    if resp.status_code == 201:
        client._created_refs.append(resp.json()["reference_idx"])
    return resp


async def test_create_reference_returns_201(client, human_admin_session):
    """POST /api/v1/reference with valid payload returns 201."""
    resp = await _create_ref(client, "test-ref-create")
    assert resp.status_code == 201
    body = resp.json()
    assert "reference_idx" in body
    assert body["reference_idx"] > 0
    assert body["status"] == "pending"
    assert body["name"] == "test-ref-create"
    # created_by_idx is the canonical owner reference.
    assert body["created_by_idx"] == human_admin_session["principal_idx"]
    assert "created_by" not in body


async def test_create_reference_defaults_is_host_false(client):
    """A reference created without is_host is a regular (non-host) reference."""
    resp = await _create_ref(client, "test-ref-nonhost")
    assert resp.status_code == 201
    assert resp.json()["is_host"] is False


async def test_create_host_reference_round_trips(client):
    """is_host=true persists and surfaces on both create and GET."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={
            "name": "test-host-ref",
            "version": "1.0",
            "kind": "sequence_reference",
            "is_host": True,
        },
    )
    assert resp.status_code == 201
    idx = resp.json()["reference_idx"]
    client._created_refs.append(idx)
    assert resp.json()["is_host"] is True

    get_resp = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=idx))
    assert get_resp.json()["is_host"] is True


async def test_create_reference_rejects_invalid_kind(client):
    """POST /api/v1/reference with invalid kind returns 422."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={"name": "bad", "version": "1.0", "kind": "bogus"},
    )
    assert resp.status_code == 422


async def test_create_reference_rejects_empty_name(client):
    """POST /api/v1/reference with empty name returns 422."""
    resp = await client.post(
        URL_REFERENCE_PREFIX,
        json={"name": "", "version": "1.0", "kind": "sequence_reference"},
    )
    assert resp.status_code == 422


async def test_get_reference_by_idx(client):
    """GET /api/v1/reference/{idx} returns the created reference."""
    create_resp = await _create_ref(client, "test-ref-get")
    idx = create_resp.json()["reference_idx"]

    get_resp = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=idx))
    assert get_resp.status_code == 200
    body = get_resp.json()
    assert body["reference_idx"] == idx
    assert body["name"] == "test-ref-get"


async def test_get_reference_not_found(client, postgres_pool):
    """GET for a reference_idx beyond any existing row returns 404."""
    max_idx = await postgres_pool.fetchval(
        "SELECT COALESCE(MAX(reference_idx), 0) FROM qiita.reference"
    )
    resp = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=max_idx + 1))
    assert resp.status_code == 404


async def test_get_reference_rejects_zero(client):
    """GET /api/v1/reference/0 returns 422 (gt=0 constraint)."""
    resp = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=0))
    assert resp.status_code == 422


async def test_get_reference_rejects_negative(client):
    """GET /api/v1/reference/-1 returns 422 (gt=0 constraint)."""
    resp = await client.get(URL_REFERENCE_BY_IDX.format(reference_idx=-1))
    assert resp.status_code == 422


async def test_create_duplicate_reference_returns_409(client):
    """POST with duplicate (name, version) returns 409."""
    resp1 = await _create_ref(client, "test-ref-dup")
    assert resp1.status_code == 201

    resp2 = await _create_ref(client, "test-ref-dup")
    assert resp2.status_code == 409


# ---------------------------------------------------------------------------
# GET /reference (list) + GET /reference/{idx}/index
# ---------------------------------------------------------------------------


async def test_list_references_returns_created(client):
    """GET /reference returns a list including references we just created."""
    r1 = await _create_ref(client, "test-list-a")
    r2 = await _create_ref(client, "test-list-b")
    idxs = {r1.json()["reference_idx"], r2.json()["reference_idx"]}

    resp = await client.get(URL_REFERENCE_PREFIX)
    assert resp.status_code == 200
    body = resp.json()
    assert isinstance(body, list)
    listed = {r["reference_idx"] for r in body}
    assert idxs <= listed


async def test_list_references_respects_limit(client):
    """The anonymous listing is bounded — ?limit=1 returns at most one row, and
    an out-of-range limit is rejected by the query-param validator (422)."""
    await _create_ref(client, "test-limit-a")
    await _create_ref(client, "test-limit-b")

    resp = await client.get(f"{URL_REFERENCE_PREFIX}?limit=1")
    assert resp.status_code == 200
    assert len(resp.json()) <= 1

    too_big = await client.get(f"{URL_REFERENCE_PREFIX}?limit=1000000")
    assert too_big.status_code == 422


async def test_list_references_filters_is_host(client):
    """GET /reference?is_host=true returns only host references."""
    host = await client.post(
        URL_REFERENCE_PREFIX,
        json={
            "name": "test-list-host",
            "version": "1.0",
            "kind": "sequence_reference",
            "is_host": True,
        },
    )
    host_idx = host.json()["reference_idx"]
    client._created_refs.append(host_idx)
    nonhost = await _create_ref(client, "test-list-nonhost")
    nonhost_idx = nonhost.json()["reference_idx"]

    resp = await client.get(URL_REFERENCE_PREFIX, params={"is_host": "true"})
    assert resp.status_code == 200
    listed = {r["reference_idx"]: r for r in resp.json()}
    assert host_idx in listed
    assert listed[host_idx]["is_host"] is True
    assert nonhost_idx not in listed


async def test_list_references_filters_status(client):
    """GET /reference?status=pending returns only references in that status."""
    r = await _create_ref(client, "test-list-status")
    idx = r.json()["reference_idx"]

    resp = await client.get(URL_REFERENCE_PREFIX, params={"status": "pending"})
    assert resp.status_code == 200
    listed = {row["reference_idx"] for row in resp.json()}
    assert idx in listed

    resp_active = await client.get(URL_REFERENCE_PREFIX, params={"status": "active"})
    assert idx not in {row["reference_idx"] for row in resp_active.json()}


async def test_list_references_rejects_bad_status(client):
    """An out-of-enum status filter is a 422."""
    resp = await client.get(URL_REFERENCE_PREFIX, params={"status": "bogus"})
    assert resp.status_code == 422


async def test_get_reference_index_empty_when_none(client):
    """A reference with no built index returns an empty list (200, not 404)."""
    r = await _create_ref(client, "test-index-empty")
    idx = r.json()["reference_idx"]
    resp = await client.get(URL_REFERENCE_INDEX.format(reference_idx=idx))
    assert resp.status_code == 200
    assert resp.json() == []


async def test_get_reference_index_returns_rows(client, postgres_pool):
    """After an index row exists, GET returns it with path + params."""
    r = await _create_ref(client, "test-index-rows")
    idx = r.json()["reference_idx"]
    await postgres_pool.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
        " VALUES ($1, 'rype', '/srv/qiita/references/x/rype/index.ryxdi', $2::jsonb)",
        idx,
        '{"k": 64, "w": 25}',
    )
    resp = await client.get(URL_REFERENCE_INDEX.format(reference_idx=idx))
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["index_type"] == "rype"
    assert body[0]["fs_path"].endswith("index.ryxdi")
    assert body[0]["params"]["k"] == 64
    assert body[0]["reference_idx"] == idx


async def test_get_reference_index_returns_shard_id(client, postgres_pool):
    """A sharded analysis reference surfaces its ONE whole-reference rype ROUTER
    (`shard_id: null` — RYpe holds every shard as a bucket in a single index, there
    is NO per-shard rype index) plus one flat row PER SHARD for each ALIGNER index
    (minimap2 / bowtie2), each carrying its `shard_id`. Grouping into "one logical
    index with N shards" is a later concern."""
    r = await _create_ref(client, "test-index-shards")
    idx = r.json()["reference_idx"]
    await postgres_pool.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params, shard_id)"
        " VALUES"
        "   ($1, 'rype_router', '/srv/x/rype-router.ryxdi', '{}'::jsonb, NULL),"
        "   ($1, 'minimap2', '/srv/x/minimap2-shards/0.mmi', '{}'::jsonb, 0),"
        "   ($1, 'minimap2', '/srv/x/minimap2-shards/1.mmi', '{}'::jsonb, 1)",
        idx,
    )
    resp = await client.get(URL_REFERENCE_INDEX.format(reference_idx=idx))
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 3
    shard_by_path = {row["fs_path"]: row["shard_id"] for row in body}
    assert shard_by_path == {
        "/srv/x/rype-router.ryxdi": None,
        "/srv/x/minimap2-shards/0.mmi": 0,
        "/srv/x/minimap2-shards/1.mmi": 1,
    }


async def test_get_reference_index_404_when_reference_absent(client, postgres_pool):
    """GET index for a non-existent reference is 404 (distinct from empty list)."""
    max_idx = await postgres_pool.fetchval(
        "SELECT COALESCE(MAX(reference_idx), 0) FROM qiita.reference"
    )
    resp = await client.get(URL_REFERENCE_INDEX.format(reference_idx=max_idx + 1))
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /reference/{idx}/shard-index-status — sharded-build progress (B5)
# ---------------------------------------------------------------------------

_BUILD_SHARD_INDEX_ACTION_ID = "build-shard-index"
_BUILD_SHARD_INDEX_VERSION = "1.0.0"


async def _seed_build_shard_action(pool):
    """Idempotently seed the build-shard-index action FK target (shared id,
    never torn down — mirrors test_shard_fanout._scaffold)."""
    await pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', NULL, 'failed')"
        " ON CONFLICT (action_id, version) DO NOTHING",
        _BUILD_SHARD_INDEX_ACTION_ID,
        _BUILD_SHARD_INDEX_VERSION,
        '{"service": false, "human_roles": ["system_admin"]}',
    )


async def test_shard_index_status_404_when_reference_absent(client, postgres_pool):
    """Status of a non-existent reference is 404, like the other reference GETs."""
    max_idx = await postgres_pool.fetchval(
        "SELECT COALESCE(MAX(reference_idx), 0) FROM qiita.reference"
    )
    resp = await client.get(URL_REFERENCE_SHARD_INDEX_STATUS.format(reference_idx=max_idx + 1))
    assert resp.status_code == 404


async def test_shard_index_status_unsharded_reads_empty(client):
    """A reference that was never sharded reads all-zero / empty — a valid
    "nothing sharded here" answer, not an error."""
    r = await _create_ref(client, "test-shard-status-unsharded")
    idx = r.json()["reference_idx"]
    resp = await client.get(URL_REFERENCE_SHARD_INDEX_STATUS.format(reference_idx=idx))
    assert resp.status_code == 200
    assert resp.json() == {
        "reference_idx": idx,
        "expected_shards": 0,
        "registered_shards": {},
        "failed_shard_tickets": 0,
    }


async def test_shard_index_status_reports_progress(client, postgres_pool):
    """A sharded reference reports N (from membership), per-expected-type
    registered counts (a wholly-unbuilt expected type shows 0, not absent), and
    the count of failed build-shard-index tickets — the wedge diagnostic that
    makes a reference stuck in `indexing` visible."""
    r = await _create_ref(client, "test-shard-status-progress")
    idx = r.json()["reference_idx"]

    # N = 3 shards (membership rows shard_id 0..2, one distinct feature each).
    for shard_id in range(3):
        feat = await postgres_pool.fetchval(
            "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid())"
            " RETURNING feature_idx"
        )
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
            " VALUES ($1, $2, $3)",
            idx,
            feat,
            shard_id,
        )

    # Per-shard indexes are minimap2 + bowtie2 (no per-shard rype). minimap2
    # registered for 2 of 3 shards; bowtie2 for none.
    for shard_id in range(2):
        await postgres_pool.execute(
            "INSERT INTO qiita.reference_index"
            " (reference_idx, index_type, fs_path, params, shard_id)"
            " VALUES ($1, 'minimap2', $2, '{}'::jsonb, $3)",
            idx,
            f"/derived/{idx}/minimap2-shards/{shard_id}.mmi",
            shard_id,
        )

    # build-shard-index tickets: both build_* flags on (so bowtie2 is an
    # expected type despite zero registered rows); shard 0 completed, 1 & 2 failed.
    await _seed_build_shard_action(postgres_pool)
    principal_idx = await postgres_pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    for shard_id, state in ((0, "completed"), (1, "failed"), (2, "failed")):
        failed = state == "failed"
        await postgres_pool.execute(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  reference_idx, shard_id, state, action_context,"
            "  failure_type, failure_stage, failure_reason)"
            " VALUES ($1, $2, $3, 'reference', $4, $5, $6, $7::jsonb,"
            "         $8::qiita.failure_type, $9::qiita.work_ticket_failure_stage, $10)",
            _BUILD_SHARD_INDEX_ACTION_ID,
            _BUILD_SHARD_INDEX_VERSION,
            principal_idx,
            idx,
            shard_id,
            state,
            '{"build_minimap2": true, "build_bowtie2": true}',
            # Failed tickets must carry failure metadata (work_ticket_failure_consistent);
            # `finalize` stage needs no failure_step_name (…_step_name_consistent).
            "permanent" if failed else None,
            "finalize" if failed else None,
            "shard build failed" if failed else None,
        )

    resp = await client.get(URL_REFERENCE_SHARD_INDEX_STATUS.format(reference_idx=idx))
    body = resp.json()

    # Tear down the work tickets we inserted before asserting, so the client
    # fixture's reference cleanup (which doesn't touch work_ticket) can't
    # RESTRICT-fail on a leaked FK even if an assertion below trips.
    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx)

    assert resp.status_code == 200
    assert body["expected_shards"] == 3
    assert body["registered_shards"] == {"minimap2": 2, "bowtie2": 0}
    assert body["failed_shard_tickets"] == 2


async def test_shard_index_status_tolerates_non_object_context(client, postgres_pool):
    """A malformed / JSON-`null` ticket `action_context` must not 500 this
    diagnostic endpoint — it degrades to observed-only (no expected-type seeding)
    rather than raising when the copied context isn't a JSON object."""
    r = await _create_ref(client, "test-shard-status-badctx")
    idx = r.json()["reference_idx"]

    feat = await postgres_pool.fetchval(
        "INSERT INTO qiita.feature (sequence_hash) VALUES (gen_random_uuid()) RETURNING feature_idx"
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.reference_membership (reference_idx, feature_idx, shard_id)"
        " VALUES ($1, $2, 0)",
        idx,
        feat,
    )
    await _seed_build_shard_action(postgres_pool)
    principal_idx = await postgres_pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    await postgres_pool.execute(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx, scope_target_kind,"
        "  reference_idx, shard_id, state, action_context)"
        " VALUES ($1, $2, $3, 'reference', $4, 0, 'completed', 'null'::jsonb)",
        _BUILD_SHARD_INDEX_ACTION_ID,
        _BUILD_SHARD_INDEX_VERSION,
        principal_idx,
        idx,
    )

    resp = await client.get(URL_REFERENCE_SHARD_INDEX_STATUS.format(reference_idx=idx))
    body = resp.json()

    await postgres_pool.execute("DELETE FROM qiita.work_ticket WHERE reference_idx = $1", idx)

    assert resp.status_code == 200
    assert body["expected_shards"] == 1
    # No expected-type seeding (context isn't an object) and no registered rows.
    assert body["registered_shards"] == {}
    assert body["failed_shard_tickets"] == 0


# ---------------------------------------------------------------------------
# POST /reference/{idx}/ticket/doget — feature_idx-scoped ticket (B6)
# ---------------------------------------------------------------------------
# The doget route is scope-gated on tickets:doget, which SYSTEM_ADMIN does NOT
# hold (only the service-account ceiling does), so these tests drive it with the
# compute SA client rather than the module's admin `client`. The app's HMAC
# secret is pinned to a known value so the test decodes the signed ticket
# payload (not the MAC) and asserts the exact filter shape.

# Any 32-byte value works — the test parses the payload, never verifies the MAC.
_DOGET_HMAC_SECRET = b"\x00" * 32


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><32B HMAC><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


@pytest.fixture
async def doget_ctx(postgres_pool, compute_worker_service_account):
    """SA client (holds tickets:doget) + a reference-seeding helper, with the
    app HMAC secret pinned so the test can decode the signed ticket payload.

    `seed_reference(status)` inserts a reference directly at an arbitrary
    status (the public create route only mints `pending`) and tracks it for
    FK-reverse cleanup at teardown.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=_DOGET_HMAC_SECRET,
        data_plane_url="unused",
    )

    created: list[int] = []

    async def _seed_reference(status: str) -> int:
        idx = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference (name, version, kind, status, created_by_idx)"
            " VALUES ($1, '1.0', 'sequence_reference', $2, $3) RETURNING reference_idx",
            f"b6-{uuid.uuid4()}",
            status,
            SYSTEM_PRINCIPAL_IDX,
        )
        created.append(idx)
        return idx

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
    ) as sa:
        yield {"sa": sa, "seed_reference": _seed_reference}

    if created:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference WHERE reference_idx = ANY($1::bigint[])",
            created,
        )


async def test_doget_feature_idx_subset_signs_scoped_filter(doget_ctx):
    """feature_idx present ⇒ the ticket scopes to reference_idx AND feature_idx."""
    ref = await doget_ctx["seed_reference"]("active")
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={"table": "reference_sequence_chunks", "feature_idx": [11, 22, 33]},
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["table"] == "reference_sequence_chunks"
    assert payload["filter"] == {"reference_idx": [ref], "feature_idx": [11, 22, 33]}


async def test_doget_feature_idx_omitted_signs_whole_reference(doget_ctx):
    """feature_idx omitted ⇒ the historical whole-reference filter, unchanged."""
    ref = await doget_ctx["seed_reference"]("active")
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={"table": "reference_sequence_chunks"},
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["filter"] == {"reference_idx": [ref]}


async def test_doget_indexing_reference_yields_ticket(doget_ctx):
    """A shard build streams mid-ingest: status 'indexing' now signs (was 409)."""
    ref = await doget_ctx["seed_reference"]("indexing")
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={"table": "reference_sequence_chunks", "feature_idx": [7]},
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["filter"] == {"reference_idx": [ref], "feature_idx": [7]}


@pytest.mark.parametrize("status", ["pending", "loading"])
async def test_doget_pre_ducklake_status_409(doget_ctx, status):
    """pending/loading are pre-DuckLake (no chunk data to stream yet) → 409."""
    ref = await doget_ctx["seed_reference"](status)
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={"table": "reference_sequence_chunks"},
    )
    assert resp.status_code == 409, resp.text


async def test_doget_missing_reference_404(doget_ctx, postgres_pool):
    """A reference_idx with no row is 404, distinct from the 409 status gate."""
    max_idx = await postgres_pool.fetchval(
        "SELECT COALESCE(MAX(reference_idx), 0) FROM qiita.reference"
    )
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=max_idx + 1),
        json={"table": "reference_sequence_chunks"},
    )
    assert resp.status_code == 404, resp.text


async def test_doget_feature_idx_over_bound_422(doget_ctx):
    """The _MAX_DOGET_FEATURE_IDX bound rejects an over-long subset at the
    request layer (422), before any reference lookup."""
    from qiita_common.models.step import _MAX_DOGET_FEATURE_IDX

    ref = await doget_ctx["seed_reference"]("active")
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={
            "table": "reference_sequence_chunks",
            "feature_idx": list(range(1, _MAX_DOGET_FEATURE_IDX + 2)),
        },
    )
    assert resp.status_code == 422, resp.text


async def test_doget_feature_idx_empty_list_422(doget_ctx):
    """An explicit empty feature_idx list is a 422 (min_length=1), never a silent
    widen to a whole-reference ticket — whole-reference is expressed by omitting
    the field. Guards against a shard builder with an empty roster accidentally
    streaming the entire reference."""
    ref = await doget_ctx["seed_reference"]("active")
    resp = await doget_ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=ref),
        json={"table": "reference_sequence_chunks", "feature_idx": []},
    )
    assert resp.status_code == 422, resp.text
