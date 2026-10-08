"""Route tests for /sequence-range.

Covers the POST mint endpoint (service-account + scope guarded), the
GET read endpoint (prep_sample:read plus per-study access, or
sequence_range:mint), the auth matrix on both, the FK / unique / cap /
cascade error paths, and concurrent-mint behaviour through the HTTP surface.
"""

import asyncio
import secrets

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_SEQUENCE_RANGE_BY_PREP_SAMPLE,
    URL_SEQUENCE_RANGE_PREFIX,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.auth.token import mint_api_token
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_to_study_link,
    seed_user_principal,
)

# The ticket the mint records as the range's minter. No FK, so any positive idx is
# accepted at the DB layer; the value only ever gets compared for equality.
_WORK_TICKET_IDX = 7

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


async def _seed_study_with_sample(pool, *, owner_idx, biosample_idx, prep_sample_idx) -> int:
    """A study owned by `owner_idx` with the biosample and prep_sample linked
    into it; returns the study_idx. Grants nobody else access."""
    study_idx = await pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"sr-route-{secrets.token_hex(4)}",
    )
    await seed_biosample_to_study_link(
        pool, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner_idx
    )
    await seed_prep_sample_to_study_link(
        pool, prep_sample_idx=prep_sample_idx, study_idx=study_idx, created_by_idx=owner_idx
    )
    return study_idx


@pytest_asyncio.fixture
async def ctx(
    postgres_pool,
    regular_user_session,
    compute_worker_service_account,
):
    """Yield a route-test context with one prep_sample plus the
    AsyncClient triple needed by every test (anonymous, regular user,
    compute SA), and a `created` dict for FK-reverse teardown.

    The prep_sample is linked to a study owned by a third principal, on which
    the regular user holds `viewer` — the GET's per-study read gate passes for
    `ctx["user"]` on `ctx["prep_sample_idx"]` and for nothing else it did not
    set up.

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
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    transport = ASGITransport(app=app)

    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="sr-route", suffix=suffix)
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    study_idx = await _seed_study_with_sample(
        postgres_pool, owner_idx=principal_idx, biosample_idx=bs_idx, prep_sample_idx=ps_idx
    )
    await postgres_pool.execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, 'viewer', $3)",
        study_idx,
        regular_user_session["principal_idx"],
        principal_idx,
    )
    created: dict[str, list[int]] = {
        "biosample": [bs_idx],
        "prep_sample": [ps_idx],
        "principal": [principal_idx],
        "study": [study_idx],
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
        "DELETE FROM qiita.prep_sample_to_study WHERE study_idx = ANY($1::bigint[])",
        created["study"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample_to_study WHERE study_idx = ANY($1::bigint[])",
        created["study"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])",
        created["study"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", created["study"]
    )
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
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
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
            URL_SEQUENCE_RANGE_PREFIX,
            json={
                "prep_sample_idx": ctx["prep_sample_idx"],
                "count": 10,
                "work_ticket_idx": _WORK_TICKET_IDX,
            },
        )
    assert resp.status_code == 403, resp.text
    # Detail comes from require_service, NOT require_scope — proves the
    # kind guard fires first.
    assert "service accounts" in resp.json()["detail"]


async def test_post_sa_without_scope_403(ctx, sa_no_mint_client):
    resp = await sa_no_mint_client.post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert resp.status_code == 403, resp.text
    assert "sequence_range:mint" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# POST /sequence-range — happy path
# ---------------------------------------------------------------------------


async def test_post_sa_happy_path_returns_range(ctx):
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["prep_sample_idx"] == ctx["prep_sample_idx"]
    assert body["sequence_idx_stop"] - body["sequence_idx_start"] + 1 == 10
    assert body["sequence_idx_start"] >= 1
    # created_at returned as ISO-8601 string
    assert "created_at" in body and isinstance(body["created_at"], str)
    # The minting ticket round-trips. This is the fact the reuse guard rests on: a
    # reads job reuses an orphaned range ONLY when this matches its own ticket.
    assert body["minted_by_work_ticket_idx"] == _WORK_TICKET_IDX

    # Verify the row landed in the DB.
    row = await ctx["pool"].fetchrow(
        "SELECT sequence_idx_start, sequence_idx_stop, created_by_idx,"
        "       minted_by_work_ticket_idx"
        "  FROM qiita.sequence_range WHERE prep_sample_idx = $1",
        ctx["prep_sample_idx"],
    )
    assert row["sequence_idx_start"] == body["sequence_idx_start"]
    assert row["sequence_idx_stop"] == body["sequence_idx_stop"]
    assert row["created_by_idx"] == ctx["sa_session"]["principal_idx"]
    assert row["minted_by_work_ticket_idx"] == _WORK_TICKET_IDX


# ---------------------------------------------------------------------------
# POST /sequence-range — failure paths
# ---------------------------------------------------------------------------


async def test_post_duplicate_prep_sample_idx_409(ctx):
    await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert resp.status_code == 409, resp.text


@pytest.mark.parametrize("bad_count", [0, -1])
async def test_post_nonpositive_count_422(ctx, bad_count):
    """Pydantic Field(ge=1) catches non-positive counts before the route
    handler runs, surfacing as 422."""
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": bad_count,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
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
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 6,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert resp.status_code == 400, resp.text
    assert "count" in resp.json()["detail"].lower()


async def test_post_unknown_prep_sample_idx_404(ctx):
    bogus_idx = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample")
        + 1_000_000
    )
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={"prep_sample_idx": bogus_idx, "count": 10, "work_ticket_idx": _WORK_TICKET_IDX},
    )
    assert resp.status_code == 404, resp.text


async def test_post_rejects_extra_fields_422(ctx):
    """SequenceRangeMintRequest must reject unknown fields
    (model_config extra='forbid')."""
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
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
    _bs2, ps2 = await seed_biosample_with_sequenced_prep_sample(
        ctx["pool"], owner_idx=ctx["principal_idx"]
    )
    ctx["created"]["biosample"].append(_bs2)
    ctx["created"]["prep_sample"].append(ps2)

    r_a, r_b = await asyncio.gather(
        ctx["sa"].post(
            URL_SEQUENCE_RANGE_PREFIX,
            json={
                "prep_sample_idx": ctx["prep_sample_idx"],
                "count": 100,
                "work_ticket_idx": _WORK_TICKET_IDX,
            },
        ),
        ctx["sa"].post(
            URL_SEQUENCE_RANGE_PREFIX,
            json={"prep_sample_idx": ps2, "count": 100, "work_ticket_idx": _WORK_TICKET_IDX},
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
    resp = await ctx["anon"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 401, resp.text


async def test_get_user_with_study_access_returns_row(ctx):
    """A regular user (USER role) holding viewer on the prep_sample's study
    reads its range."""
    # Mint a range first via the SA.
    post_resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 5,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert post_resp.status_code == 201, post_resp.text
    minted = post_resp.json()

    resp = await ctx["user"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequence_idx_start"] == minted["sequence_idx_start"]
    assert body["sequence_idx_stop"] == minted["sequence_idx_stop"]
    assert body["prep_sample_idx"] == ctx["prep_sample_idx"]


async def test_get_404_when_unminted(ctx):
    resp = await ctx["user"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 404, resp.text


async def test_get_unknown_prep_sample_404_for_sa_403_for_user(ctx):
    """An unknown prep_sample is a 404 on the mint arm. A user has no study to
    be authorized against, so the access gate answers first with a 403."""
    bogus_idx = (
        await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 999
    )
    url = URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=bogus_idx)
    assert (await ctx["sa"].get(url)).status_code == 404
    assert (await ctx["user"].get(url)).status_code == 403


@pytest_asyncio.fixture
async def foreign_prep_sample(ctx):
    """A second prep_sample, in a study the regular user holds no tier on."""
    pool, owner = ctx["pool"], ctx["principal_idx"]
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(pool, owner_idx=owner)
    study_idx = await _seed_study_with_sample(
        pool, owner_idx=owner, biosample_idx=bs_idx, prep_sample_idx=ps_idx
    )
    ctx["created"]["biosample"].append(bs_idx)
    ctx["created"]["prep_sample"].append(ps_idx)
    ctx["created"]["study"].append(study_idx)
    return ps_idx


async def _mint(ctx, prep_sample_idx: int) -> dict:
    resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={"prep_sample_idx": prep_sample_idx, "count": 3, "work_ticket_idx": _WORK_TICKET_IDX},
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def test_get_user_without_study_access_403_minted_or_not(ctx, foreign_prep_sample):
    """A user with no tier on the prep_sample's study is refused, and the
    refusal is the same status before and after the range exists — the 200/404
    split is not readable by a caller who fails the access gate."""
    url = URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=foreign_prep_sample)

    before = await ctx["user"].get(url)
    assert before.status_code == 403, before.text

    await _mint(ctx, foreign_prep_sample)
    after = await ctx["user"].get(url)
    assert after.status_code == 403, after.text
    assert "sequence_idx_start" not in after.text


async def test_get_user_unlinked_prep_sample_403(ctx):
    """A prep_sample with no active study link has no study to authorize
    against; the user is refused even though the range exists."""
    pool = ctx["pool"]
    bs_idx, ps_idx = await seed_biosample_with_sequenced_prep_sample(
        pool, owner_idx=ctx["principal_idx"]
    )
    ctx["created"]["biosample"].append(bs_idx)
    ctx["created"]["prep_sample"].append(ps_idx)
    await _mint(ctx, ps_idx)

    resp = await ctx["user"].get(URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ps_idx))
    assert resp.status_code == 403, resp.text


async def test_get_wet_lab_admin_bypasses_study_access(
    ctx, foreign_prep_sample, wet_lab_admin_session
):
    """wet_lab_admin reads a range in a study it holds no tier on."""
    from qiita_control_plane.main import app

    minted = await _mint(ctx, foreign_prep_sample)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
    ) as admin:
        resp = await admin.get(
            URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=foreign_prep_sample)
        )
    assert resp.status_code == 200, resp.text
    assert resp.json()["sequence_idx_start"] == minted["sequence_idx_start"]


async def test_get_sa_reads_range_in_study_no_human_granted(ctx, foreign_prep_sample):
    """The mint arm is not study-gated: the compute SA holds no study tier and
    reads the range back. `mint_or_reuse_sequence_range` depends on this."""
    minted = await _mint(ctx, foreign_prep_sample)
    resp = await ctx["sa"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=foreign_prep_sample)
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["sequence_idx_start"] == minted["sequence_idx_start"]


async def test_get_sa_with_mint_scope_returns_row(ctx):
    """The compute SA holds sequence_range:mint but NOT prep_sample:read; the
    GET's mint-scope arm lets it read back the range it minted. This is the
    ingest_reads reuse path — without it, the SA would 403 here."""
    post_resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 7,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    assert post_resp.status_code == 201, post_resp.text
    minted = post_resp.json()

    resp = await ctx["sa"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sequence_idx_start"] == minted["sequence_idx_start"]
    assert body["sequence_idx_stop"] == minted["sequence_idx_stop"]
    assert body["prep_sample_idx"] == ctx["prep_sample_idx"]


async def test_get_sa_without_mint_or_read_scope_403(ctx, sa_no_mint_client):
    """An SA holding neither sequence_range:mint nor prep_sample:read is
    rejected — the GET requires at least one of the two."""
    resp = await sa_no_mint_client.get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# Cascade behaviour through the HTTP surface
# ---------------------------------------------------------------------------


async def test_cascade_then_remint_yields_advanced_start(ctx):
    """Delete the parent prep_sample; the GET 404s; a fresh mint against
    a new prep_sample lands above the deleted range's stop (no recycle)."""
    first_resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={
            "prep_sample_idx": ctx["prep_sample_idx"],
            "count": 10,
            "work_ticket_idx": _WORK_TICKET_IDX,
        },
    )
    first = first_resp.json()

    # The study link RESTRICTs the prep_sample delete; drop it first.
    await ctx["pool"].execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = $1",
        ctx["prep_sample_idx"],
    )
    await ctx["pool"].execute(
        "DELETE FROM qiita.prep_sample WHERE idx = $1",
        ctx["prep_sample_idx"],
    )
    ctx["created"]["prep_sample"].remove(ctx["prep_sample_idx"])

    # GET against the cascaded prep_sample → 404. Asked as the SA: the user is
    # refused at the access gate once the prep_sample (and its study link) is gone.
    resp = await ctx["sa"].get(
        URL_SEQUENCE_RANGE_BY_PREP_SAMPLE.format(prep_sample_idx=ctx["prep_sample_idx"])
    )
    assert resp.status_code == 404, resp.text

    # Mint against a fresh prep_sample.
    _bs2, ps2 = await seed_biosample_with_sequenced_prep_sample(
        ctx["pool"], owner_idx=ctx["principal_idx"]
    )
    ctx["created"]["biosample"].append(_bs2)
    ctx["created"]["prep_sample"].append(ps2)
    second_resp = await ctx["sa"].post(
        URL_SEQUENCE_RANGE_PREFIX,
        json={"prep_sample_idx": ps2, "count": 5, "work_ticket_idx": _WORK_TICKET_IDX},
    )
    assert second_resp.status_code == 201, second_resp.text
    second = second_resp.json()
    assert second["sequence_idx_start"] > first["sequence_idx_stop"]
