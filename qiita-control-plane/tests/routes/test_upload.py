"""Route tests for /upload — generic Arrow-data staging slot domain.

The upload domain is content-agnostic by design: no reference_idx, no role
enum, no FASTA-specific fields. Tests here lock that in — every assertion
is about the staging-slot state machine and the signed DoPut ticket, never
about what's being uploaded.
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_UPLOAD_BY_IDX,
    URL_UPLOAD_DONE,
    URL_UPLOAD_PREFIX,
)
from qiita_common.auth_constants import Scope

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(
    postgres_pool,
    human_admin_session,
    regular_user_session,
):
    """Yield {pool, admin, user, admin_session, created} for upload route tests.

    `created` accumulates `upload_idx` values for FK-reverse cleanup at
    teardown — the upload row has only `created_by_idx → principal` as an
    outgoing FK, so a simple DELETE WHERE upload_idx = ANY(...) is enough.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    created: dict[str, list[int]] = {"upload": []}

    async with (
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {human_admin_session['token']}"},
        ) as admin,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as user,
    ):
        yield {
            "pool": postgres_pool,
            "admin": admin,
            "user": user,
            "admin_session": human_admin_session,
            "user_session": regular_user_session,
            "created": created,
        }

    if created["upload"]:
        await postgres_pool.execute(
            "DELETE FROM qiita.upload WHERE upload_idx = ANY($1::bigint[])",
            created["upload"],
        )


@pytest_asyncio.fixture
async def no_doput_client(make_pat_client):
    """Regular-user PAT that explicitly OMITS Scope.TICKET_DOPUT so the
    require_scope guard's 403 surfaces."""
    return await make_pat_client(label="no-doput", scopes=[Scope.SELF_PROFILE])


async def _create_upload(client, *, description: str | None = "test upload"):
    body: dict = {}
    if description is not None:
        body["description"] = description
    return await client.post(URL_UPLOAD_PREFIX, json=body)


def _track(ctx, resp) -> int:
    idx = resp.json()["upload_idx"]
    ctx["created"]["upload"].append(idx)
    return idx


# ---------------------------------------------------------------------------
# POST /upload
# ---------------------------------------------------------------------------


async def test_create_upload_slot_ok(ctx):
    """Admin POST /upload returns 201 with upload_idx + doput_ticket; row at 'pending'."""
    resp = await _create_upload(ctx["admin"], description="cycle1 happy path")
    assert resp.status_code == 201, resp.text

    body = resp.json()
    assert body["upload_idx"] > 0
    assert isinstance(body["doput_ticket"], str)
    assert len(body["doput_ticket"]) > 0

    _track(ctx, resp)
    row = await ctx["pool"].fetchrow(
        "SELECT status, description, created_by_idx, sha256, row_count, completed_at"
        " FROM qiita.upload WHERE upload_idx = $1",
        body["upload_idx"],
    )
    assert row["status"] == "pending"
    assert row["description"] == "cycle1 happy path"
    assert row["created_by_idx"] == ctx["admin_session"]["principal_idx"]
    # done-fields must be NULL on a freshly-minted slot.
    assert row["sha256"] is None
    assert row["row_count"] is None
    assert row["completed_at"] is None


async def test_create_upload_slot_without_description(ctx):
    """description is optional — POST with no body still mints a slot."""
    resp = await _create_upload(ctx["admin"], description=None)
    assert resp.status_code == 201, resp.text
    _track(ctx, resp)
    row = await ctx["pool"].fetchrow(
        "SELECT description FROM qiita.upload WHERE upload_idx = $1",
        resp.json()["upload_idx"],
    )
    assert row["description"] is None


async def test_create_upload_slot_doput_ticket_decodes_to_upload_idx(ctx):
    """The doput_ticket bytes embed the same upload_idx the response carries.

    Locks the wire contract the data-plane Rust verifier keys off — payload
    JSON is `{"action": "doput", "upload_idx": N}`.
    """
    import base64
    import json
    import struct

    resp = await _create_upload(ctx["admin"])
    _track(ctx, resp)
    ticket_bytes = base64.b64decode(resp.json()["doput_ticket"])
    payload_len = struct.unpack(">I", ticket_bytes[1:5])[0]
    payload = json.loads(ticket_bytes[5 : 5 + payload_len])
    assert payload == {"action": "doput", "upload_idx": resp.json()["upload_idx"]}


async def test_create_upload_slot_requires_scope(no_doput_client):
    """A PAT without ticket:doput is rejected with 403."""
    resp = await no_doput_client.post(URL_UPLOAD_PREFIX, json={})
    assert resp.status_code == 403
    assert "ticket:doput" in resp.json()["detail"]


async def test_create_upload_slot_anonymous_rejected(ctx):
    """Anonymous caller is rejected with 401."""
    transport = ASGITransport(app=ctx["admin"]._transport.app)
    async with AsyncClient(transport=transport, base_url="http://test") as anon:
        resp = await anon.post(URL_UPLOAD_PREFIX, json={})
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /upload/{idx}/done
# ---------------------------------------------------------------------------


async def test_upload_done_transitions_to_ready(ctx):
    """Admin POSTs /done with the client's claim; row transitions pending → ready."""
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)

    done_resp = await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "a" * 64, "row_count": 3, "bytes_received": 1024},
    )
    assert done_resp.status_code == 200, done_resp.text

    body = done_resp.json()
    assert body["status"] == "ready"
    assert body["sha256"] == "a" * 64
    assert body["row_count"] == 3
    assert body["bytes_received"] == 1024
    assert body["completed_at"] is not None


async def test_upload_done_idempotent_on_ready_same_claim(ctx):
    """Calling /done twice with the same claim is a no-op (200 with current state).

    Idempotency lets a flaky client retry the completion call without
    needing to re-upload. Different claims on the second call MUST 409;
    that's a different test below.
    """
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)
    claim = {"sha256": "b" * 64, "row_count": 5, "bytes_received": 2048}

    first = await ctx["admin"].post(URL_UPLOAD_DONE.format(upload_idx=idx), json=claim)
    assert first.status_code == 200
    second = await ctx["admin"].post(URL_UPLOAD_DONE.format(upload_idx=idx), json=claim)
    assert second.status_code == 200
    assert first.json()["completed_at"] == second.json()["completed_at"]


async def test_upload_done_rejects_conflicting_retry(ctx):
    """Calling /done on a `ready` row with a DIFFERENT claim → 409.

    A second call with a different sha256 / row_count / bytes_received is a
    contract violation (the underlying file is immutable post-DoPut); we
    surface it loudly rather than silently overwriting the recorded claim.
    """
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)
    first_claim = {"sha256": "c" * 64, "row_count": 1, "bytes_received": 100}
    await ctx["admin"].post(URL_UPLOAD_DONE.format(upload_idx=idx), json=first_claim)

    conflict = await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "d" * 64, "row_count": 1, "bytes_received": 100},
    )
    assert conflict.status_code == 409


async def test_upload_done_on_consumed_row_returns_409(ctx):
    """Calling /done on a `consumed` row (the workflow runner transitions
    ready→consumed) falls through to the catch-all 409 with the current
    status in the detail.

    Direct-DB-insert because no public route lands a row in `consumed`.
    """
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)
    await ctx["pool"].execute(
        "UPDATE qiita.upload SET status = 'consumed', completed_at = now() WHERE upload_idx = $1",
        idx,
    )
    resp = await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "0" * 64, "row_count": 0, "bytes_received": 0},
    )
    assert resp.status_code == 409
    assert "consumed" in resp.json()["detail"]


async def test_upload_done_unknown_idx(ctx):
    """Calling /done on a never-minted upload_idx → 404."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(upload_idx), 0) FROM qiita.upload")
    resp = await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=max_idx + 100_000),
        json={"sha256": "0" * 64, "row_count": 0, "bytes_received": 0},
    )
    assert resp.status_code == 404


async def test_upload_done_requires_scope(no_doput_client, ctx):
    """The /done endpoint is also scope-gated — without ticket:doput, 403."""
    # Need a real upload row to point at; mint it via the admin client.
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)

    resp = await no_doput_client.post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "e" * 64, "row_count": 1, "bytes_received": 1},
    )
    assert resp.status_code == 403


async def test_upload_done_rejects_malformed_sha256(ctx):
    """sha256 must be a 64-char hex string — 422 otherwise.

    The recorded value is descriptive but unparseable hex would clearly
    indicate a misbehaving client; fail loud at the boundary.
    """
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)

    resp = await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "not-hex", "row_count": 1, "bytes_received": 1},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /upload/{idx}
# ---------------------------------------------------------------------------


async def test_get_upload_status_pending(ctx):
    """GET on a freshly-minted upload returns the pending row."""
    create_resp = await _create_upload(ctx["admin"], description="just minted")
    idx = _track(ctx, create_resp)

    resp = await ctx["admin"].get(URL_UPLOAD_BY_IDX.format(upload_idx=idx))
    assert resp.status_code == 200
    body = resp.json()
    assert body["upload_idx"] == idx
    assert body["status"] == "pending"
    assert body["description"] == "just minted"
    assert body["sha256"] is None
    assert body["completed_at"] is None


async def test_get_upload_status_after_done(ctx):
    """GET after /done returns the ready row including the recorded claim."""
    create_resp = await _create_upload(ctx["admin"])
    idx = _track(ctx, create_resp)
    await ctx["admin"].post(
        URL_UPLOAD_DONE.format(upload_idx=idx),
        json={"sha256": "f" * 64, "row_count": 7, "bytes_received": 4096},
    )

    resp = await ctx["admin"].get(URL_UPLOAD_BY_IDX.format(upload_idx=idx))
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["sha256"] == "f" * 64
    assert body["row_count"] == 7
    assert body["bytes_received"] == 4096


async def test_get_upload_not_found(ctx):
    """GET for a never-minted upload_idx → 404."""
    max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(upload_idx), 0) FROM qiita.upload")
    resp = await ctx["admin"].get(URL_UPLOAD_BY_IDX.format(upload_idx=max_idx + 100_000))
    assert resp.status_code == 404


async def test_get_upload_rejects_zero(ctx):
    """upload_idx=0 trips the gt=0 Pydantic constraint."""
    resp = await ctx["admin"].get(URL_UPLOAD_BY_IDX.format(upload_idx=0))
    assert resp.status_code == 422
