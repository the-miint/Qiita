"""Route tests for the admin per-pool masked-read export:

  * GET  /admin/sequenced-pool/{sequenced_pool_idx}/masked-read-export?mask_idx=
    — the roster manifest (one row per non-retired sample, with filename parts).
  * POST /admin/masked-read-export/ticket
    — a per-sample DoGet ticket scoped to (prep_sample_idx, mask_idx) on the
      data plane's read_masked view.

Both are dual-gated: system_admin role PLUS admin:masked_read_export scope. The
ticket is the human counterpart to the service-account POST
/read-masked/ticket/doget (which is untouched). The DoGet round-trip against a
live data plane is not exercised here — these pin auth, the roster shape
(retired excluded, null accession surfaced), and the signed ticket's mandatory
(prep_sample_idx, mask_idx) filter + max TTL.
"""

import base64
import json
import secrets
import struct
import time

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_ADMIN_MASKED_READ_EXPORT_TICKET,
    URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.main import app
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample,
    seed_sequenced_prep_sample,
    seed_sequenced_sample_subtype,
)

pytestmark = pytest.mark.db

# Any 32-byte value: the tests decode the ticket payload + expiry, not the MAC.
_HMAC_SECRET = b"\x00" * 32
_TICKET_BODY = {"prep_sample_idx": 11, "mask_idx": 4}


@pytest.fixture
def ctx(role_keyed_clients):
    """Shared role-keyed clients, plus a known hmac secret on app.state so the
    ticket route's get_hmac_secret resolves (role_keyed_clients sets only the
    pool)."""
    from qiita_control_plane.config import Settings

    app.state.settings = Settings(
        database_url="unused", hmac_secret_key=_HMAC_SECRET, data_plane_url="unused"
    )
    return role_keyed_clients


def _manifest_url(sequenced_pool_idx: int) -> str:
    return URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT.format(sequenced_pool_idx=sequenced_pool_idx)


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload from a base64 signed Flight ticket.
    Wire: <1B version><4B payload_len><payload><32B HMAC><8B expiry>."""
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


def _decode_ticket_expiry(ticket_b64: str) -> int:
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    expiry_start = 1 + 4 + payload_len + 32
    return struct.unpack(">Q", raw[expiry_start : expiry_start + 8])[0]


@pytest_asyncio.fixture
async def seeded(ctx):
    """Seed one sequenced_pool with three samples:

      - A: has a biosample_accession; in the pool.
      - B: no accession (None) — surfaced, not dropped; same pool.
      - C: retired prep_sample; same pool — must be excluded from the manifest.

    Plus a mask_definition. FK-reverse cleanup at teardown.
    """
    pool = ctx["pool"]
    owner = ctx["admin_session"]["principal_idx"]
    token = secrets.token_hex(4)

    # Sample A — accession set, in pool P (which seed_sequenced_sample_subtype mints).
    bs_a = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    acc_a = f"SAMN{token}A"
    await pool.execute(
        "UPDATE qiita.biosample SET biosample_accession = $1 WHERE idx = $2", acc_a, bs_a
    )
    ps_a = await seed_sequenced_prep_sample(pool, biosample_idx=bs_a, owner_idx=owner)
    run_idx, pool_idx, ss_a = await seed_sequenced_sample_subtype(
        pool, prep_sample_idx=ps_a, owner_idx=owner, sequenced_pool_item_id=f"item-a-{token}"
    )

    # Sample B — no accession; same pool.
    bs_b = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    ps_b = await seed_sequenced_prep_sample(pool, biosample_idx=bs_b, owner_idx=owner)
    ss_b = await pool.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ps_b,
        pool_idx,
        f"item-b-{token}",
        owner,
    )

    # Sample C — retired prep_sample; same pool. Excluded from the roster.
    bs_c = await seed_biosample(pool, owner_idx=owner, created_by_idx=owner)
    ps_c = await seed_sequenced_prep_sample(pool, biosample_idx=bs_c, owner_idx=owner)
    # retired_at + retired_by_idx must be set together (prep_sample_retirement_consistent).
    await pool.execute(
        "UPDATE qiita.prep_sample"
        " SET retired = true, retired_at = now(), retired_by_idx = $2 WHERE idx = $1",
        ps_c,
        owner,
    )
    ss_c = await pool.fetchval(
        "INSERT INTO qiita.sequenced_sample"
        "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ps_c,
        pool_idx,
        f"item-c-{token}",
        owner,
    )

    async with pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"k": token},
            principal_idx=owner,
        )
    mask_idx = mask["mask_idx"]

    yield {
        "pool": pool,
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "mask_idx": mask_idx,
        "ps_a": ps_a,
        "acc_a": acc_a,
        "ps_b": ps_b,
        "ps_c": ps_c,
    }

    # mask_sample rows (a test may insert them to exercise the completion gate)
    # FK into BOTH prep_sample and mask_definition, so drop them first.
    await pool.execute("DELETE FROM qiita.mask_sample WHERE mask_idx = $1", mask_idx)
    await pool.execute(
        "DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])", [ss_a, ss_b, ss_c]
    )
    await pool.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await pool.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [ps_a, ps_b, ps_c]
    )
    await pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bs_a, bs_b, bs_c]
    )
    await pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask_idx)


# ---------------------------------------------------------------------------
# Manifest — auth matrix
# ---------------------------------------------------------------------------


async def test_manifest_anonymous_401(ctx, seeded):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.get(
            _manifest_url(seeded["pool_idx"]), params={"mask_idx": seeded["mask_idx"]}
        )
    assert resp.status_code == 401, resp.text


async def test_manifest_regular_user_403(ctx, seeded):
    resp = await ctx["user"].get(
        _manifest_url(seeded["pool_idx"]), params={"mask_idx": seeded["mask_idx"]}
    )
    assert resp.status_code == 403, resp.text


async def test_manifest_wet_lab_admin_403(ctx, seeded):
    """wet_lab_admin has neither the system_admin role nor the scope."""
    resp = await ctx["wet"].get(
        _manifest_url(seeded["pool_idx"]), params={"mask_idx": seeded["mask_idx"]}
    )
    assert resp.status_code == 403, resp.text


async def test_manifest_admin_missing_scope_403(ctx, seeded):
    """A system_admin token lacking admin:masked_read_export is rejected by the
    scope gate even though the role gate passes."""
    token, _ = await mint_api_token(
        ctx["pool"],
        principal_idx=ctx["admin_session"]["principal_idx"],
        label="admin-without-export-scope",
        scopes=[Scope.SELF_PROFILE],
    )
    resp = await ctx["admin"].get(
        _manifest_url(seeded["pool_idx"]),
        params={"mask_idx": seeded["mask_idx"]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Manifest — behavior
# ---------------------------------------------------------------------------


async def test_manifest_happy_path(ctx, seeded):
    resp = await ctx["admin"].get(
        _manifest_url(seeded["pool_idx"]), params={"mask_idx": seeded["mask_idx"]}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequenced_pool_idx"] == seeded["pool_idx"]
    assert body["sequencing_run_idx"] == seeded["run_idx"]
    assert body["mask_idx"] == seeded["mask_idx"]

    by_prep = {s["prep_sample_idx"]: s for s in body["samples"]}
    # Retired sample C excluded; A and B present.
    assert set(by_prep) == {seeded["ps_a"], seeded["ps_b"]}
    assert by_prep[seeded["ps_a"]]["biosample_accession"] == seeded["acc_a"]
    # B has no accession yet — surfaced as null, not dropped (CLI fails loudly).
    assert by_prep[seeded["ps_b"]]["biosample_accession"] is None


async def test_manifest_surfaces_mask_sample_state(ctx, seeded):
    """The manifest surfaces each sample's per-(mask, prep_sample) completion so
    the CLI can report skips: a block-masked sample carries its mask_sample state
    ('pending'/'completed'); a sample with no mask_sample row (the per-sample
    read-mask path, or unmasked) carries null."""
    pool, mask_idx = seeded["pool"], seeded["mask_idx"]
    # A is block-masked and still pending; B has no mask_sample row (per-sample /
    # unmasked path).
    await pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')",
        mask_idx,
        seeded["ps_a"],
    )
    resp = await ctx["admin"].get(_manifest_url(seeded["pool_idx"]), params={"mask_idx": mask_idx})
    assert resp.status_code == 200, resp.text
    by_prep = {s["prep_sample_idx"]: s for s in resp.json()["samples"]}
    assert by_prep[seeded["ps_a"]]["mask_state"] == "pending"
    assert by_prep[seeded["ps_b"]]["mask_state"] is None


async def test_manifest_unknown_pool_404(ctx, seeded):
    resp = await ctx["admin"].get(
        _manifest_url(999_999_999), params={"mask_idx": seeded["mask_idx"]}
    )
    assert resp.status_code == 404, resp.text


async def test_manifest_unknown_mask_404(ctx, seeded):
    resp = await ctx["admin"].get(
        _manifest_url(seeded["pool_idx"]), params={"mask_idx": 999_999_999}
    )
    assert resp.status_code == 404, resp.text


async def test_manifest_requires_mask_idx_422(ctx, seeded):
    """mask_idx is a mandatory query param — omitting it is a 422, never an
    unscoped dump."""
    resp = await ctx["admin"].get(_manifest_url(seeded["pool_idx"]))
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# Ticket — auth matrix
# ---------------------------------------------------------------------------


async def test_ticket_anonymous_401(ctx):
    app.state.pool = ctx["pool"]
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as anon:
        resp = await anon.post(URL_ADMIN_MASKED_READ_EXPORT_TICKET, json=_TICKET_BODY)
    assert resp.status_code == 401, resp.text


async def test_ticket_regular_user_403(ctx):
    resp = await ctx["user"].post(URL_ADMIN_MASKED_READ_EXPORT_TICKET, json=_TICKET_BODY)
    assert resp.status_code == 403, resp.text


async def test_ticket_admin_missing_scope_403(ctx):
    token, _ = await mint_api_token(
        ctx["pool"],
        principal_idx=ctx["admin_session"]["principal_idx"],
        label="admin-without-ticket-scope",
        scopes=[Scope.SELF_PROFILE],
    )
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET,
        json=_TICKET_BODY,
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Ticket — signed contents
# ---------------------------------------------------------------------------


async def test_ticket_signs_mandatory_filter(ctx):
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET, json={"prep_sample_idx": 11, "mask_idx": 4}
    )
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    assert payload["table"] == "read_masked"
    assert payload["filter"] == {"prep_sample_idx": [11], "mask_idx": [4]}


async def test_ticket_ttl_is_one_hour(ctx):
    """Export tickets mint at the 3600 s max (the data plane's ceiling); expiry
    is checked only at DoGet initiation, so this bounds mint->stream-start, not
    the download."""
    before = int(time.time())
    resp = await ctx["admin"].post(URL_ADMIN_MASKED_READ_EXPORT_TICKET, json=_TICKET_BODY)
    assert resp.status_code == 201, resp.text
    expiry = _decode_ticket_expiry(resp.json()["ticket"])
    assert before + 3600 - 5 <= expiry <= before + 3600 + 5


@pytest.mark.parametrize(
    "bad_body",
    [
        {"prep_sample_idx": 0, "mask_idx": 4},
        {"prep_sample_idx": 11, "mask_idx": 0},
        {"prep_sample_idx": -1, "mask_idx": 4},
        {"prep_sample_idx": 11},
        {"mask_idx": 4},
        {"prep_sample_idx": 11, "mask_idx": 4, "smuggled": "x"},
    ],
)
async def test_ticket_rejects_bad_body_422(ctx, bad_body):
    resp = await ctx["admin"].post(URL_ADMIN_MASKED_READ_EXPORT_TICKET, json=bad_body)
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# Ticket — mask_sample completion gate (block-masked samples)
# ---------------------------------------------------------------------------


async def test_ticket_refuses_pending_mask_sample_409(ctx, seeded):
    """A block-masked sample whose mask_sample gate is still PENDING (a covering
    block unfinished) must NOT get an export ticket — its read_mask is partial, so
    a pull would silently truncate. Fail loud (409), never mint the ticket."""
    pool, mask_idx, ps = seeded["pool"], seeded["mask_idx"], seeded["ps_a"]
    await pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'pending')",
        mask_idx,
        ps,
    )
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET, json={"prep_sample_idx": ps, "mask_idx": mask_idx}
    )
    assert resp.status_code == 409, resp.text


async def test_ticket_allows_completed_mask_sample_201(ctx, seeded):
    """A block-masked sample whose gate is COMPLETED (all covering blocks done) is
    fully masked — the ticket is minted."""
    pool, mask_idx, ps = seeded["pool"], seeded["mask_idx"], seeded["ps_a"]
    await pool.execute(
        "INSERT INTO qiita.mask_sample (mask_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, 'completed')",
        mask_idx,
        ps,
    )
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET, json={"prep_sample_idx": ps, "mask_idx": mask_idx}
    )
    assert resp.status_code == 201, resp.text


async def test_ticket_allows_no_mask_sample_row_201(ctx, seeded):
    """A sample with NO mask_sample row (the per-sample read-mask path, or
    unmasked) is exportable: the per-sample ticket's read_mask is all-or-nothing,
    so absence of the block gate preserves the old guarantee. 201, not 409."""
    mask_idx, ps = seeded["mask_idx"], seeded["ps_b"]
    resp = await ctx["admin"].post(
        URL_ADMIN_MASKED_READ_EXPORT_TICKET, json={"prep_sample_idx": ps, "mask_idx": mask_idx}
    )
    assert resp.status_code == 201, resp.text
