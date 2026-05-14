"""Route tests for /sequence-range.

Covers the POST mint endpoint (service-account + scope guarded), the
GET read endpoint (any prep_sample:read holder), the auth matrix on
both, the FK / unique / cap / cascade error paths, and concurrent-mint
behaviour through the HTTP surface.
"""

import asyncio
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.prep_sample import insert_prep_sample

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Seeding helpers
# ---------------------------------------------------------------------------


async def _seed_user_principal(pool, *, suffix: str) -> int:
    principal_idx = await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, created_by_idx) VALUES ($1, $2) RETURNING idx",
        f"sr-route-{suffix}",
        SYSTEM_PRINCIPAL_IDX,
    )
    await pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        principal_idx,
        f"sr-route-{suffix}@test.local",
    )
    return principal_idx


async def _seed_prep_sample(pool, *, owner_idx: int) -> tuple[int, int]:
    protocol_idx = await pool.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1",
        "short_read_metagenomics",
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            bs_idx = await insert_biosample(
                conn,
                owner_idx=owner_idx,
                created_by_idx=owner_idx,
            )
            ps_idx = await insert_prep_sample(
                conn,
                biosample_idx=bs_idx,
                owner_idx=owner_idx,
                prep_protocol_idx=protocol_idx,
                processing_kind="sequenced",
                created_by_idx=owner_idx,
            )
    return bs_idx, ps_idx


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def ctx(
    postgres_pool,
    regular_user_session,
    compute_worker_service_account,
):
    """Yield a route-test context with one prep_sample plus the
    AsyncClient triple needed by every test (anonymous, regular user,
    compute SA), and a `created` dict for FK-reverse teardown.

    The compute_worker_service_account fixture is extended in
    qiita_control_plane.testing.sessions to include Scope.SEQUENCE_RANGE_MINT
    so its token is the "happy SA" client. Tests that need an SA token
    WITHOUT the mint scope mint their own via `sa_no_mint_client`.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    # The route reads Settings.max_sequence_mint_count from app.state;
    # only fields required to construct Settings are passed, every
    # other field falls through to its dataclass default.
    app.state.settings = Settings(
        database_url="unused",
        hmac_secret_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    suffix = secrets.token_hex(4)
    principal_idx = await _seed_user_principal(postgres_pool, suffix=suffix)
    bs_idx, ps_idx = await _seed_prep_sample(postgres_pool, owner_idx=principal_idx)
    created: dict[str, list[int]] = {
        "biosample": [bs_idx],
        "prep_sample": [ps_idx],
        "principal": [principal_idx],
    }

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
            "user_session": regular_user_session,
            "sa_session": compute_worker_service_account,
            "principal_idx": principal_idx,
            "prep_sample_idx": ps_idx,
            "biosample_idx": bs_idx,
            "created": created,
        }

    # FK-reverse cleanup — sequence_range cascades with prep_sample.
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
        created["prep_sample"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])",
        created["biosample"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
        created["principal"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = ANY($1::bigint[])",
        created["principal"],
    )


@pytest_asyncio.fixture
async def sa_no_mint_client(postgres_pool, compute_worker_service_account):
    """A bearer-auth client whose SA token carries every worker scope
    EXCEPT sequence_range:mint, so the require_scope guard's 403 path is
    exercised."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=compute_worker_service_account["principal_idx"],
        label=f"sa-no-mint-{secrets.token_hex(4)}",
        # Any scope on SERVICE_ACCOUNT_SCOPE_CEILING that is NOT
        # SEQUENCE_RANGE_MINT works here — FEATURE_MINT is the picked
        # representative. The intent of this fixture is "SA token
        # missing the specific scope," so a future retirement of
        # FEATURE_MINT just means swapping in another worker scope.
        scopes=[Scope.FEATURE_MINT],
    )
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as client:
        yield client


# ---------------------------------------------------------------------------
# POST /sequence-range — auth matrix
# ---------------------------------------------------------------------------


async def test_post_anonymous_401(ctx):
    resp = await ctx["anon"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    assert resp.status_code == 401, resp.text


async def test_post_human_user_403_even_with_scope(ctx, postgres_pool, regular_user_session):
    """A human can't mint even if their token somehow carries the scope —
    require_service rejects HumanUser before require_scope runs. The
    detail-string assertion locks in the ordering: if require_scope
    ever ran first, the user (who carries the scope here) would pass
    that guard and the 403 detail would change — that drift surfaces
    here as a test failure rather than as a silently misleading 403."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    plaintext, _ = await mint_api_token(
        postgres_pool,
        principal_idx=regular_user_session["principal_idx"],
        label=f"human-with-mint-{secrets.token_hex(4)}",
        scopes=[Scope.SELF_PROFILE, Scope.SEQUENCE_RANGE_MINT],
    )
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {plaintext}"},
    ) as user_with_mint:
        resp = await user_with_mint.post(
            "/api/v1/sequence-range",
            json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
        )
    assert resp.status_code == 403, resp.text
    # Detail comes from require_service, NOT require_scope — proves the
    # kind guard fires first.
    assert "service accounts" in resp.json()["detail"]


async def test_post_sa_without_scope_403(ctx, sa_no_mint_client):
    resp = await sa_no_mint_client.post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    assert resp.status_code == 403, resp.text
    assert "sequence_range:mint" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /sequence-range — happy path
# ---------------------------------------------------------------------------


async def test_post_sa_happy_path_returns_range(ctx):
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["prep_sample_idx"] == ctx["prep_sample_idx"]
    assert body["sequence_idx_stop"] - body["sequence_idx_start"] + 1 == 10
    assert body["sequence_idx_start"] >= 1
    # created_at returned as ISO-8601 string
    assert "created_at" in body and isinstance(body["created_at"], str)

    # Verify the row landed in the DB.
    row = await ctx["pool"].fetchrow(
        "SELECT sequence_idx_start, sequence_idx_stop, created_by_idx"
        "  FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ctx["prep_sample_idx"],
    )
    assert row["sequence_idx_start"] == body["sequence_idx_start"]
    assert row["sequence_idx_stop"] == body["sequence_idx_stop"]
    assert row["created_by_idx"] == ctx["sa_session"]["principal_idx"]


# ---------------------------------------------------------------------------
# POST /sequence-range — failure paths
# ---------------------------------------------------------------------------


async def test_post_duplicate_prep_sample_idx_409(ctx):
    await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    assert resp.status_code == 409, resp.text


@pytest.mark.parametrize("bad_count", [0, -1])
async def test_post_nonpositive_count_422(ctx, bad_count):
    """Pydantic Field(ge=1) catches non-positive counts before the route
    handler runs, surfacing as 422."""
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": bad_count},
    )
    assert resp.status_code == 422, resp.text


async def test_post_count_above_cap_400(ctx, monkeypatch):
    """count > max_sequence_mint_count is rejected at the route with
    400 (not 422 — Pydantic doesn't know the dynamic cap)."""
    # Settings is a frozen dataclass, so swap the whole object on
    # app.state for the duration of this test rather than mutating it
    # in place.
    from dataclasses import replace

    from qiita_control_plane.main import app

    monkeypatch.setattr(
        app.state, "settings", replace(app.state.settings, max_sequence_mint_count=5)
    )
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 6},
    )
    assert resp.status_code == 400, resp.text
    assert "count" in resp.json()["detail"].lower()


async def test_post_unknown_prep_sample_idx_404(ctx):
    bogus_idx = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample")
        + 1_000_000
    )
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": bogus_idx, "count": 10},
    )
    assert resp.status_code == 404, resp.text


async def test_post_rejects_extra_fields_422(ctx):
    """SequenceRangeMintRequest must reject unknown fields
    (model_config extra='forbid')."""
    resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "smuggled_field": "naughty",
        },
    )
    assert resp.status_code == 422, resp.text


# ---------------------------------------------------------------------------
# POST /sequence-range — concurrency
# ---------------------------------------------------------------------------


async def test_post_concurrent_mints_disjoint(ctx):
    """Two POSTs against two different prep_samples, driven by
    asyncio.gather over the ASGI transport, return disjoint ranges.

    Caveat: asyncio.gather over an in-process ASGI transport is not
    truly concurrent at the OS-thread level — the requests interleave
    at asyncio await boundaries within one event loop. The Postgres
    sequence guarantees disjoint ranges unconditionally, so this test
    asserts the end-to-end response contract holds under interleaved
    calls through the full HTTP stack rather than proving the advisory
    lock under real parallelism (that requires the OS-thread-driven
    test reserved for the perf-suite to-do)."""
    _bs2, ps2 = await _seed_prep_sample(ctx["pool"], owner_idx=ctx["principal_idx"])
    ctx["created"]["biosample"].append(_bs2)
    ctx["created"]["prep_sample"].append(ps2)

    r_a, r_b = await asyncio.gather(
        ctx["sa"].post(
            "/api/v1/sequence-range",
            json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 100},
        ),
        ctx["sa"].post(
            "/api/v1/sequence-range",
            json={"prep_sample_idx": ps2, "count": 100},
        ),
    )
    assert r_a.status_code == 201, r_a.text
    assert r_b.status_code == 201, r_b.text
    a = r_a.json()
    b = r_b.json()
    if a["sequence_idx_start"] < b["sequence_idx_start"]:
        lo, hi = a, b
    else:
        lo, hi = b, a
    assert lo["sequence_idx_stop"] < hi["sequence_idx_start"], (lo, hi)


# ---------------------------------------------------------------------------
# GET /sequence-range/{prep_sample_idx}
# ---------------------------------------------------------------------------


async def test_get_anonymous_401(ctx):
    resp = await ctx["anon"].get(f"/api/v1/sequence-range/{ctx['prep_sample_idx']}")
    assert resp.status_code == 401, resp.text


async def test_get_user_with_prep_sample_read_returns_row(ctx):
    """A regular user (USER role) has prep_sample:read by default and
    should be able to read the range for any prep_sample."""
    # Mint a range first via the SA.
    post_resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 5},
    )
    assert post_resp.status_code == 201, post_resp.text
    minted = post_resp.json()

    resp = await ctx["user"].get(f"/api/v1/sequence-range/{ctx['prep_sample_idx']}")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequence_idx_start"] == minted["sequence_idx_start"]
    assert body["sequence_idx_stop"] == minted["sequence_idx_stop"]
    assert body["prep_sample_idx"] == ctx["prep_sample_idx"]


async def test_get_404_when_unminted(ctx):
    resp = await ctx["user"].get(f"/api/v1/sequence-range/{ctx['prep_sample_idx']}")
    assert resp.status_code == 404, resp.text


async def test_get_404_for_unknown_prep_sample(ctx):
    bogus_idx = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 999
    )
    resp = await ctx["user"].get(f"/api/v1/sequence-range/{bogus_idx}")
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# Cascade behaviour through the HTTP surface
# ---------------------------------------------------------------------------


async def test_cascade_then_remint_yields_advanced_start(ctx):
    """Delete the parent prep_sample; the GET 404s; a fresh mint against
    a new prep_sample lands above the deleted range's stop (no recycle)."""
    first_resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ctx["prep_sample_idx"], "count": 10},
    )
    first = first_resp.json()

    await ctx["pool"].execute(
        "DELETE FROM qiita.prep_sample WHERE idx = $1",
        ctx["prep_sample_idx"],
    )
    ctx["created"]["prep_sample"].remove(ctx["prep_sample_idx"])

    # GET against the cascaded prep_sample → 404.
    resp = await ctx["user"].get(f"/api/v1/sequence-range/{ctx['prep_sample_idx']}")
    assert resp.status_code == 404, resp.text

    # Mint against a fresh prep_sample.
    _bs2, ps2 = await _seed_prep_sample(ctx["pool"], owner_idx=ctx["principal_idx"])
    ctx["created"]["biosample"].append(_bs2)
    ctx["created"]["prep_sample"].append(ps2)
    second_resp = await ctx["sa"].post(
        "/api/v1/sequence-range",
        json={"prep_sample_idx": ps2, "count": 5},
    )
    assert second_resp.status_code == 201, second_resp.text
    second = second_resp.json()
    assert second["sequence_idx_start"] > first["sequence_idx_stop"]
