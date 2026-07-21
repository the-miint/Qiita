"""Route tests for POST /read/ticket/doget.

Signs an Ed25519 Flight DoGet ticket scoped to ONE block's `(prep_sample_idx,
sequence_idx sub-range)` members, so a block-scoped compute job streams its
reads instead of reading a control-plane-materialized Parquet.
Service-account-only (Scope.TICKET_DOGET), same as the alignment doget route.

The raw-vs-masked rule itself is unit-tested in tests/test_block_read.py; these
tests cover the auth matrix, the DB reads the route does (block members, the
work_ticket columns), and the shape of what actually gets SIGNED — the part a
pure-function test cannot reach.
"""

import base64
import json
import secrets
import struct
import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import URL_READ_DOGET
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.block import add_block_members, create_block
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# Ed25519 signing seed the test app signs tickets with; the test decodes the
# payload (not the signature) so any 32-byte value works.
_TEST_SEED = b"\x00" * 32


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><64B Ed25519 signature><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


async def _seed_block_ticket(
    pool,
    *,
    members,
    action_context=None,
    alignment_idx=None,
    mask_idx=None,
):
    """Insert a block + its members + a BLOCK-scoped work_ticket.

    Returns `(work_ticket_idx, block_idx, action_id, version)` for teardown.
    `members` is a list of `(prep_sample_idx, min_sequence_idx, max_sequence_idx)`
    whose prep_sample_idx values must be real rows — block_member has an FK.
    """
    async with pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
        if members:
            await add_block_members(conn, block_idx=block_idx, members=members)
    action_id = "read-doget-test-action"
    version = f"v-{uuid.uuid4()}"
    await pool.execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'block', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["prep_sample:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    wt_idx = await pool.fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, block_idx, state, action_context, alignment_idx, mask_idx)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'block', $3, 'processing'::qiita.work_ticket_state, $4::jsonb, $5, $6)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        block_idx,
        json.dumps(action_context or {}),
        alignment_idx,
        mask_idx,
    )
    return wt_idx, block_idx, action_id, version


async def _cleanup_block_ticket(pool, wt_idx, block_idx, action_id, version):
    await pool.execute("DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx)
    await pool.execute("DELETE FROM qiita.block_member WHERE block_idx = $1", block_idx)
    await pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", block_idx)
    await pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )


@pytest_asyncio.fixture
async def ctx(postgres_pool, regular_user_session, compute_worker_service_account):
    """Route-test context: anon + regular-user + compute-SA AsyncClients."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=_TEST_SEED,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    async with (
        AsyncClient(transport=transport, base_url="http://test") as anon,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {regular_user_session['token']}"},
        ) as user,
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {compute_worker_service_account['token']}"},
        ) as sa,
    ):
        yield {"pool": postgres_pool, "anon": anon, "user": user, "sa": sa}


@pytest_asyncio.fixture
async def sa_no_scope_client(postgres_pool, compute_worker_service_account):
    """An SA token carrying a scope that is NOT ticket:doget, to exercise the
    require_scope 403 path."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-read-doget-{secrets.token_hex(4)}",
        scopes=[Scope.FEATURE_MINT],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


@pytest_asyncio.fixture
async def env(postgres_pool):
    """Real FK targets for a block work ticket.

    `block_member.prep_sample_idx`, `work_ticket.mask_idx`, and
    `work_ticket.alignment_idx` are all foreign keys, so none of these can be
    invented — the ticket the route reads has to be a real one.

    Yields `{"prep_sample_idx": [sorted pair], "mask_idx": ..., "alignment_idx": ...}`.
    The sample pair is sorted because `fetch_block_members` orders by
    prep_sample_idx and the tests assert the signed member list verbatim.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="read-doget-test", suffix=suffix
    )
    bs1, ps1 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bs2, ps2 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"workflow": "host_filter", "version": "1.0.0", "s": suffix},
            principal_idx=principal_idx,
        )
        alignment = await mint_alignment_definition(
            conn,
            params={
                "reference_idx": 1,
                "aligner": "minimap2",
                "mask_idx": mask["mask_idx"],
                "shard_ids": [0],
                "s": suffix,
            },
            principal_idx=principal_idx,
        )

    yield {
        "prep_sample_idx": sorted([ps1, ps2]),
        "mask_idx": mask["mask_idx"],
        "alignment_idx": alignment["alignment_idx"],
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1",
        alignment["alignment_idx"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps1, ps2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs1, bs2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask["mask_idx"]
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


# ---------------------------------------------------------------------------
# Auth matrix — raw block reads are human-containing, so this is load-bearing.
# ---------------------------------------------------------------------------


async def test_read_doget_anonymous_401(ctx):
    resp = await ctx["anon"].post(URL_READ_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 401, resp.text


async def test_read_doget_human_user_403(ctx, postgres_pool, regular_user_session):
    """Humans can't mint even carrying the scope — require_service rejects the
    HumanUser before require_scope runs."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-read-doget-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.TICKET_DOGET],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as human:
        resp = await human.post(URL_READ_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 403, resp.text
    assert "service accounts" in resp.json()["detail"]


async def test_read_doget_sa_without_scope_403(ctx, sa_no_scope_client):
    resp = await sa_no_scope_client.post(URL_READ_DOGET, json={"work_ticket_idx": 1})
    assert resp.status_code == 403, resp.text
    assert "ticket:doget" in resp.json()["detail"]


async def test_read_doget_unknown_ticket_404(ctx):
    resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": 2_000_000_000})
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# Signed ticket contents
# ---------------------------------------------------------------------------


async def test_read_doget_signs_raw_block_selector(ctx, env):
    """A read-mask block (no alignment intent) signs the RAW selector, scoped by
    members alone — the reads a mask is about to be computed over."""
    ps1, ps2 = env["prep_sample_idx"]
    wt_idx, *rest = await _seed_block_ticket(
        ctx["pool"],
        members=[(ps1, 100, 199), (ps2, 500, 549)],
        action_context={"instrument_model": "NovaSeq"},
        mask_idx=None,
    )
    try:
        resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 201, resp.text
        payload = _decode_ticket_payload(resp.json()["ticket"])
        assert payload["table"] == "read_block"
        assert payload["filter"] == {}
        assert payload["members"] == [
            {"prep_sample_idx": ps1, "sequence_idx_start": 100, "sequence_idx_stop": 199},
            {"prep_sample_idx": ps2, "sequence_idx_start": 500, "sequence_idx_stop": 549},
        ]
    finally:
        await _cleanup_block_ticket(ctx["pool"], wt_idx, *rest)


async def test_read_doget_signs_masked_block_selector(ctx, env):
    """An align block signs the MASK-scoped selector, so the job aligns exactly
    the reads that survived the host-depletion mask."""
    wt_idx, *rest = await _seed_block_ticket(
        ctx["pool"],
        members=[(env["prep_sample_idx"][0], 100, 199)],
        action_context={"alignment_idx": env["alignment_idx"]},
        alignment_idx=env["alignment_idx"],
        mask_idx=env["mask_idx"],
    )
    try:
        resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 201, resp.text
        payload = _decode_ticket_payload(resp.json()["ticket"])
        assert payload["table"] == "read_masked_block"
        assert payload["filter"] == {"mask_idx": [env["mask_idx"]]}
        assert payload["members"] == [
            {
                "prep_sample_idx": env["prep_sample_idx"][0],
                "sequence_idx_start": 100,
                "sequence_idx_stop": 199,
            }
        ]
    finally:
        await _cleanup_block_ticket(ctx["pool"], wt_idx, *rest)


async def test_read_doget_alignment_deleted_mid_flight_422(ctx, env):
    """work_ticket.alignment_idx is ON DELETE SET NULL. With action_context still
    naming the alignment, falling through to the raw selector would stream
    un-QC'd, non-host-depleted reads into an aligner — 422 instead."""
    wt_idx, *rest = await _seed_block_ticket(
        ctx["pool"],
        members=[(env["prep_sample_idx"][0], 100, 199)],
        action_context={"alignment_idx": env["alignment_idx"]},
        alignment_idx=None,
        mask_idx=env["mask_idx"],
    )
    try:
        resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
        assert "deleted mid-flight" in resp.json()["detail"]
    finally:
        await _cleanup_block_ticket(ctx["pool"], wt_idx, *rest)


async def test_read_doget_block_with_no_members_422(ctx):
    """An empty block is a planning bug, never a licence to read unscoped. The
    route refuses before signing (sign_ticket and the data plane refuse again)."""
    wt_idx, *rest = await _seed_block_ticket(
        ctx["pool"], members=[], action_context={}, mask_idx=None
    )
    try:
        resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
        assert "no members" in resp.json()["detail"]
    finally:
        await _cleanup_block_ticket(ctx["pool"], wt_idx, *rest)


async def test_read_doget_non_block_ticket_422(ctx):
    """The per-sample read path does not use this route; only a block has members."""
    ref_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', false,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        f"read-doget-{uuid.uuid4()}",
    )
    action_id = "read-doget-nonblock-action"
    version = f"v-{uuid.uuid4()}"
    await ctx["pool"].execute(
        "INSERT INTO qiita.action ("
        "  action_id, version, target_kind, scopes, audience, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling"
        ") VALUES ($1, $2, 'reference', $3::text[], $4::jsonb, $5::jsonb,"
        "          1, 1, '1 minute')",
        action_id,
        version,
        ["reference:write"],
        json.dumps({"service": False, "human_roles": ["system_admin"]}),
        json.dumps([]),
    )
    wt_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.work_ticket"
        " (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, reference_idx, state, action_context)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal),"
        "         'reference', $3, 'processing'::qiita.work_ticket_state, '{}'::jsonb)"
        " RETURNING work_ticket_idx",
        action_id,
        version,
        ref_idx,
    )
    try:
        resp = await ctx["sa"].post(URL_READ_DOGET, json={"work_ticket_idx": wt_idx})
        assert resp.status_code == 422, resp.text
        assert "block-scoped" in resp.json()["detail"]
    finally:
        await ctx["pool"].execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", wt_idx
        )
        await ctx["pool"].execute(
            "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
        )
        await ctx["pool"].execute("DELETE FROM qiita.reference WHERE reference_idx = $1", ref_idx)
