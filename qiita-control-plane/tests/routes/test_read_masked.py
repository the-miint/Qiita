"""Route tests for /mask-definition and /read-masked/ticket/doget.

Covers the mask-definition mint endpoint (service-account + read_masked:doget
guarded; idempotency through HTTP), the masked-read DoGet ticket endpoint
(auth matrix, the signed ticket's mandatory (prep_sample_idx, mask_idx) filter,
and the read_masked table allowlist membership), plus the mandatory-filter
rejection at the request layer.

The DoGet round-trip against a live data plane is NOT exercised here — the
read_masked DuckLake view does not exist until PR 2, so these tests assert the
signing/validation/rejection logic only.
"""

import base64
import json
import secrets
import struct

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_MASK_DEFINITION_PREFIX,
    URL_READ_MASKED_DOGET,
    URL_REFERENCE_DOGET,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


# HMAC secret the test app signs tickets with; the test decodes the payload
# (not the MAC) so any 32-byte value works.
_HMAC_SECRET = b"\x00" * 32


def _decode_ticket_payload(ticket_b64: str) -> dict:
    """Parse the JSON payload out of a base64 signed Flight ticket.

    Wire format: <1B version><4B payload_len><payload><32B HMAC><8B expiry>.
    """
    raw = base64.b64decode(ticket_b64)
    payload_len = struct.unpack(">I", raw[1:5])[0]
    return json.loads(raw[5 : 5 + payload_len])


@pytest_asyncio.fixture
async def ctx(postgres_pool, regular_user_session, compute_worker_service_account):
    """Route-test context: the three AsyncClients (anon, regular user, compute
    SA) plus a seeded principal for FK-reverse mask cleanup."""
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=_HMAC_SECRET,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="rm-route", suffix=suffix)

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
        yield {
            "pool": postgres_pool,
            "anon": anon,
            "user": user,
            "sa": sa,
            "sa_session": compute_worker_service_account,
            "principal_idx": principal_idx,
        }

    # Masks minted by the compute SA principal are cleaned up here.
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE created_by_idx = $1",
        compute_worker_service_account["principal_idx"],
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


@pytest_asyncio.fixture
async def sa_no_scope_client(postgres_pool, compute_worker_service_account):
    """An SA token carrying a worker scope that is NOT read_masked:doget, so the
    require_scope 403 path is exercised."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-rm-{secrets.token_hex(4)}",
        scopes=[Scope.FEATURE_MINT],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


_MASK_BODY = {"filter_workflow": "host_filter", "filter_version": "1.0.0", "params": {"k": "v"}}
_DOGET_BODY = {"prep_sample_idx": 7, "mask_idx": 3}


# ---------------------------------------------------------------------------
# POST /mask-definition — auth matrix
# ---------------------------------------------------------------------------


async def test_mint_anonymous_401(ctx):
    resp = await ctx["anon"].post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    assert resp.status_code == 401, resp.text


async def test_mint_human_user_403(ctx, postgres_pool, regular_user_session):
    """Humans can't mint even carrying the scope — require_service rejects the
    HumanUser before require_scope runs."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-rm-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.READ_MASKED_DOGET],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as human:
        resp = await human.post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    assert resp.status_code == 403, resp.text
    assert "service accounts" in resp.json()["detail"]


async def test_mint_sa_without_scope_403(ctx, sa_no_scope_client):
    resp = await sa_no_scope_client.post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    assert resp.status_code == 403, resp.text
    assert "read_masked:doget" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /mask-definition — happy path + idempotency
# ---------------------------------------------------------------------------


async def test_mint_sa_happy_path(ctx):
    resp = await ctx["sa"].post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["mask_idx"] > 0
    assert body["filter_workflow"] == "host_filter"
    assert body["params"] == {"k": "v"}


async def test_mint_idempotent_same_params_same_idx(ctx):
    first = await ctx["sa"].post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    second = await ctx["sa"].post(
        URL_MASK_DEFINITION_PREFIX,
        json={"filter_workflow": "host_filter", "filter_version": "1.0.0", "params": {"k": "v"}},
    )
    assert first.status_code == 201 and second.status_code == 201
    assert first.json()["mask_idx"] == second.json()["mask_idx"]


async def test_mint_different_params_different_idx(ctx):
    a = await ctx["sa"].post(URL_MASK_DEFINITION_PREFIX, json=_MASK_BODY)
    b = await ctx["sa"].post(
        URL_MASK_DEFINITION_PREFIX,
        json={
            "filter_workflow": "host_filter",
            "filter_version": "1.0.0",
            "params": {"k": "OTHER"},
        },
    )
    assert a.json()["mask_idx"] != b.json()["mask_idx"]


async def test_mint_rejects_extra_fields_422(ctx):
    resp = await ctx["sa"].post(
        URL_MASK_DEFINITION_PREFIX,
        json={**_MASK_BODY, "smuggled": "x"},
    )
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# POST /read-masked/ticket/doget — auth matrix
# ---------------------------------------------------------------------------


async def test_doget_anonymous_401(ctx):
    resp = await ctx["anon"].post(URL_READ_MASKED_DOGET, json=_DOGET_BODY)
    assert resp.status_code == 401, resp.text


async def test_doget_human_user_403(ctx, postgres_pool, regular_user_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-doget-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.READ_MASKED_DOGET],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as human:
        resp = await human.post(URL_READ_MASKED_DOGET, json=_DOGET_BODY)
    assert resp.status_code == 403, resp.text
    assert "service accounts" in resp.json()["detail"]


async def test_doget_sa_without_scope_403(ctx, sa_no_scope_client):
    resp = await sa_no_scope_client.post(URL_READ_MASKED_DOGET, json=_DOGET_BODY)
    assert resp.status_code == 403, resp.text
    assert "read_masked:doget" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /read-masked/ticket/doget — signed ticket contents (mandatory filter)
# ---------------------------------------------------------------------------


async def test_doget_signs_mandatory_filter(ctx):
    resp = await ctx["sa"].post(URL_READ_MASKED_DOGET, json={"prep_sample_idx": 11, "mask_idx": 4})
    assert resp.status_code == 201, resp.text
    payload = _decode_ticket_payload(resp.json()["ticket"])
    # read_masked table + BOTH identifiers present and non-empty.
    assert payload["table"] == "read_masked"
    assert payload["filter"] == {"prep_sample_idx": [11], "mask_idx": [4]}


async def test_doget_table_is_in_cp_allowlist(ctx):
    """The table the route signs must be in the CP-side DoGet allowlist that
    mirrors the data plane's ALLOWED_TABLES."""
    from qiita_control_plane.routes.reference import _DOGET_ALLOWED_TABLES

    assert "read_masked" in _DOGET_ALLOWED_TABLES


async def test_doget_read_masked_not_signable_via_reference_route(ctx):
    """The reference DoGet route must NOT accept read_masked — that surface is
    served only by this route, with its mandatory (prep_sample_idx, mask_idx)
    filter, never with a reference_idx filter."""
    from qiita_control_plane.routes.reference import _REFERENCE_DOGET_TABLES

    assert "read_masked" not in _REFERENCE_DOGET_TABLES


async def test_doget_read_masked_rejected_by_reference_route_http(ctx):
    """HTTP-level pin of the contract above: POSTing table='read_masked' to the
    reference DoGet route is rejected (422) before any reference lookup. The
    constant test passes even if the route stopped consulting the allowlist;
    this exercises the actual behavior. ctx['sa'] holds tickets:doget, and the
    table check precedes the reference-active DB check, so reference_idx need
    not exist."""
    resp = await ctx["sa"].post(
        URL_REFERENCE_DOGET.format(reference_idx=1),
        json={"table": "read_masked"},
    )
    assert resp.status_code == 422, resp.text


@pytest.mark.parametrize(
    "bad_body",
    [
        {"prep_sample_idx": 0, "mask_idx": 4},  # gt=0 violated
        {"prep_sample_idx": 11, "mask_idx": 0},
        {"prep_sample_idx": -1, "mask_idx": 4},
        {"prep_sample_idx": 11},  # missing mask_idx
        {"mask_idx": 4},  # missing prep_sample_idx
        {"prep_sample_idx": 11, "mask_idx": 4, "smuggled": "x"},  # extra field
    ],
)
async def test_doget_rejects_empty_or_partial_filter_422(ctx, bad_body):
    """The mandatory-filter invariant: a request that would produce an empty or
    partial filter is rejected at the request layer (422), so an unfiltered
    read_masked ticket is never signed."""
    resp = await ctx["sa"].post(URL_READ_MASKED_DOGET, json=bad_body)
    assert resp.status_code == 422, resp.text
