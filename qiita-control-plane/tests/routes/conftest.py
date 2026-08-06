"""Shared fixtures and helpers for control-plane route tests.

Holds the three-role AsyncClient triple, a PAT-minting client factory used
by the per-route no-scope fixtures, a generic FK-reverse delete helper, and
the parametrise source + driver for the owner-eligibility 422 surface. Each
route test still owns its own `ctx` and `_cleanup_tracked` because the
tracked table set differs per route.
"""

import json
import secrets
import uuid
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import NamedTuple, get_args

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_BIOSAMPLE_STUDY_FIELD_BY_STUDY,
    URL_PREP_SAMPLE_STUDY_FIELD_BY_STUDY,
)
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope
from qiita_common.models import FieldDataType
from qiita_common.models.reference import Tier

from qiita_control_plane.repositories import UpdatableTable
from qiita_control_plane.routes import _helpers as route_helpers
from qiita_control_plane.testing.db_seeds import (
    disable_principal,
    retire_principal,
    seed_biosample_global_field,
    seed_biosample_to_study_link,
    seed_biosample_with_sequenced_prep_sample,
    seed_prep_sample_global_field,
    seed_sequenced_sample_subtype,
    seed_service_principal,
    seed_user_principal,
)
from qiita_control_plane.testing.unique_names import unique_field_name


@pytest.fixture(autouse=True)
def _route_settings():
    """Stash a route-sufficient Settings on the app before every route test.

    `qiita_control_plane.main.app` is a module-level SINGLETON, and the route
    fixtures below only ever set `app.state.pool` — never `app.state.settings`. Any
    route whose dependencies read Settings (e.g. `create_upload` →
    `get_flight_signing_key`) therefore only worked when some *other* test module on
    the same xdist worker had already stashed Settings on the shared app and leaked
    it (`test_landing.py` does exactly that). Under `pytest -n auto --dist worksteal`
    the worker assignment shifts with the test set, so that leak is load-bearing and
    invisible: add tests anywhere in the suite and a route test can start 500ing with
    "Settings not initialised" for reasons entirely unrelated to it.

    This makes the route tests own their Settings instead of inheriting one by
    accident. Save/restore the prior value so we don't leak in turn.
    """
    from qiita_control_plane.config import Settings
    from qiita_control_plane.main import app

    had_prior = hasattr(app.state, "settings")
    prior = getattr(app.state, "settings", None)
    app.state.settings = Settings(
        database_url="unused",
        flight_signing_key=b"\x00" * 32,
        data_plane_url="unused",
    )
    try:
        yield
    finally:
        if had_prior:
            app.state.settings = prior
        else:
            del app.state.settings


# ---------------------------------------------------------------------------
# Three-role AsyncClient triple
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def role_keyed_clients(
    postgres_pool,
    human_admin_session,
    regular_user_session,
    wet_lab_admin_session,
):
    """Yield {pool, admin, user, wet, admin_session, user_session, wet_session}.

    Sets app.state.pool so route-internal Depends(get_db_pool) resolves to
    the same pool the test uses for direct SQL, then opens three role-keyed
    AsyncClients sharing one ASGITransport. Imported by every route test's
    `ctx` fixture; per-route `ctx` adds its own `created` dict on top.
    """
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    transport = ASGITransport(app=app)
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
        AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {wet_lab_admin_session['token']}"},
        ) as wet,
    ):
        yield {
            "pool": postgres_pool,
            "admin": admin,
            "user": user,
            "wet": wet,
            "admin_session": human_admin_session,
            "user_session": regular_user_session,
            "wet_session": wet_lab_admin_session,
        }


# ---------------------------------------------------------------------------
# PAT-minting client factory for missing-scope tests
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def make_pat_client(postgres_pool, regular_user_session):
    """Factory: mint an ad-hoc PAT against the regular_user principal with the
    caller-supplied scope set, return an entered AsyncClient.

    Used by the per-route no-scope fixtures (e.g., no_study_write_client) to
    drive the require_scope guard with a token that omits one specific scope.
    The factory tracks every client it opens and closes them all at fixture
    teardown so individual fixtures need only call the factory.
    """
    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    opened: list[AsyncClient] = []

    async def _factory(*, label: str, scopes: list[Scope]) -> AsyncClient:
        # Mint a fresh PAT against the regular_user principal_idx.
        plaintext, _ = await mint_api_token(
            postgres_pool,
            principal_idx=regular_user_session["principal_idx"],
            label=label,
            scopes=scopes,
        )
        # Open the client and remember it for teardown.
        client = AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
            headers={"Authorization": f"Bearer {plaintext}"},
        )
        await client.__aenter__()
        opened.append(client)
        return client

    yield _factory

    # Close every client the factory handed out, in reverse order.
    for c in reversed(opened):
        await c.__aexit__(None, None, None)


# ---------------------------------------------------------------------------
# Shared sequencing-route fixtures and helpers
# ---------------------------------------------------------------------------
# Every sequencing-ingestion route gates on Scope.PREP_SAMPLE_WRITE and the
# WET_LAB_ADMIN role, so the three test files share one PAT-client pair and
# one instrument-run-id generator instead of redefining them per file.


@pytest_asyncio.fixture
async def no_prep_sample_write_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.PREP_SAMPLE_WRITE
    so the require_scope guard's missing-scope 403 surfaces."""
    return await make_pat_client(label="no-prep-sample-write", scopes=[Scope.SELF_PROFILE])


@pytest_asyncio.fixture
async def no_prep_sample_read_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.PREP_SAMPLE_READ
    so the require_scope guard's missing-scope 403 surfaces on the
    sequenced-sample read endpoints."""
    return await make_pat_client(label="no-prep-sample-read", scopes=[Scope.SELF_PROFILE])


@pytest_asyncio.fixture
async def regular_user_with_prep_sample_write_client(make_pat_client):
    """A regular_user PAT scoped to only SELF_PROFILE + PREP_SAMPLE_WRITE.

    Use when a test needs the *minimal* scope set to reach a downstream
    gate; the standard `ctx["user"]` client carries the full USER ceiling."""
    return await make_pat_client(
        label="user-with-prep-sample-write",
        scopes=[Scope.SELF_PROFILE, Scope.PREP_SAMPLE_WRITE],
    )


def unique_instrument_id(prefix: str) -> str:
    """Build a per-test sequencing_run.instrument_run_id with a caller-supplied
    prefix. Used by every test that seeds a sequencing_run so the unique
    constraint never collides across parallel test runs."""
    return f"{prefix}-{secrets.token_hex(6)}"


# ---------------------------------------------------------------------------
# Shared study-access fixtures and helpers
# ---------------------------------------------------------------------------
# The study-scoped roster reads (biosample and sequenced-sample list-idxs)
# share the same require_scope(STUDY_READ) gate and the same study_access
# seeding, so the missing-scope client and the grant helper live here
# instead of being redefined per file.


@pytest_asyncio.fixture
async def no_study_read_client(make_pat_client):
    """A regular_user PAT with a scope set that EXCLUDES Scope.STUDY_READ so
    the require_scope guard's missing-scope 403 surfaces on the study-scoped
    list-idxs routes."""
    return await make_pat_client(label="no-study-read", scopes=[Scope.SELF_PROFILE])


async def _grant_study_access(ctx, *, study_idx, principal_idx, tier, granted_by_idx):
    """Insert a study_access row at the named tier; track for cleanup.

    Appends (study_idx, principal_idx) to ctx['created']['study_access'];
    the consuming file's _cleanup_tracked deletes those rows before its
    own study delete.
    """
    await ctx["pool"].execute(
        "INSERT INTO qiita.study_access (study_idx, principal_idx, access_tier, granted_by_idx)"
        " VALUES ($1, $2, $3::qiita.tier, $4)",
        study_idx,
        principal_idx,
        tier,
        granted_by_idx,
    )
    ctx["created"]["study_access"].append((study_idx, principal_idx))


async def _seed_study(ctx, *, owner_idx: int, suffix: str) -> int:
    """Insert a minimal study owned by `owner_idx`, append its idx to
    `ctx['created']['study']` for FK-reverse teardown, and return the idx.

    The title is uniquified with `suffix` plus a random token so concurrent
    tests never collide.
    """
    study_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        owner_idx,
        f"route-study-{suffix}-{secrets.token_hex(4)}",
    )
    ctx["created"]["study"].append(study_idx)
    return study_idx


# ---------------------------------------------------------------------------
# Study-local field create: per-entity bindings and shared matrix drivers
# ---------------------------------------------------------------------------
# Every study-local field create route carries the same gate — require_scope,
# require_study_exists, and require_study_access at ADMIN with a wet_lab_admin
# bypass — and the same conflict semantics, so each entity supplies its
# bindings once and drives both matrices through the shared helpers below.


class StudyFieldCreateSurface(NamedTuple):
    """One entity's bindings for its study-local field create route.

    `global_fk_key` is the request/response key naming the global-field link,
    `idx_key` the response key naming the created row, and the two
    `*_created_key` names the ctx cleanup buckets the created rows belong in.
    """

    url_template: str
    idx_key: str
    created_key: str
    global_fk_key: str
    seed_global_field: Callable[..., Awaitable[int]]
    global_created_key: str
    global_field_table: str


BIOSAMPLE_FIELD_SURFACE = StudyFieldCreateSurface(
    url_template=URL_BIOSAMPLE_STUDY_FIELD_BY_STUDY,
    idx_key="biosample_study_field_idx",
    created_key="biosample_study_field",
    global_fk_key="biosample_global_field_idx",
    seed_global_field=seed_biosample_global_field,
    global_created_key="biosample_global_field",
    global_field_table="qiita.biosample_global_field",
)
PREP_SAMPLE_FIELD_SURFACE = StudyFieldCreateSurface(
    url_template=URL_PREP_SAMPLE_STUDY_FIELD_BY_STUDY,
    idx_key="prep_sample_study_field_idx",
    created_key="prep_sample_study_field",
    global_fk_key="prep_sample_global_field_idx",
    seed_global_field=seed_prep_sample_global_field,
    global_created_key="prep_sample_global_field",
    global_field_table="qiita.prep_sample_global_field",
)


async def post_study_field(
    ctx, *, surface: StudyFieldCreateSurface, client, study_idx: int, **body
):
    """POST one entity's create-field route and, on 201, track the created row."""
    resp = await client.post(surface.url_template.format(study_idx=study_idx), json=body)
    if resp.status_code == 201:
        ctx["created"][surface.created_key].append(resp.json()[surface.idx_key])
    return resp


async def _seed_field_global(ctx, *, surface: StudyFieldCreateSurface, label: str) -> int:
    """Seed one global field for `surface`'s entity and track it for cleanup."""
    suffix = secrets.token_hex(4)
    global_idx = await surface.seed_global_field(
        ctx["pool"],
        internal_name=f"{label}_{suffix}",
        display_name=f"Global {label} {suffix}",
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"][surface.global_created_key].append(global_idx)
    return global_idx


# case -> (study owner, tier granted to the regular user, calling client,
# expected status). A None owner means no study is seeded, so the path idx
# names a study that does not exist.
_STUDY_FIELD_CREATE_AUTHZ: dict[str, tuple[str | None, str | None, str, int]] = {
    "owner": ("user", None, "user", 201),
    "admin_grant": ("wet", "admin", "user", 201),
    "wet_lab_admin_bypass": ("user", None, "wet", 201),
    "no_access": ("wet", None, "user", 403),
    "below_admin": ("wet", "member", "user", 403),
    "missing_scope": ("user", None, "no_scope", 403),
    "nonexistent_study": (None, None, "wet", 404),
}
STUDY_FIELD_CREATE_AUTHZ_CASES = tuple(_STUDY_FIELD_CREATE_AUTHZ)


async def assert_study_field_create_authz(
    ctx,
    *,
    case: str,
    surface: StudyFieldCreateSurface,
    no_scope_client,
) -> None:
    """Drive one access case of a study-local field create and assert its status.

    `case` names a row of the access matrix, which fixes the study's ownership,
    any grant to the regular user, the calling client, and the expected status.
    `no_scope_client` is a PAT client lacking the route's write scope.
    """
    owner_key, grant_tier, client_key, expected_status = _STUDY_FIELD_CREATE_AUTHZ[case]
    client = {"user": ctx["user"], "wet": ctx["wet"], "no_scope": no_scope_client}[client_key]

    # A None owner exercises the missing-study 404, so no study is seeded and
    # the path carries an idx above every existing one.
    if owner_key is None:
        max_idx = await ctx["pool"].fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.study")
        study_idx = max_idx + 100_000
    else:
        study_idx = await _seed_study(
            ctx,
            owner_idx=ctx[f"{owner_key}_session"]["principal_idx"],
            suffix=f"authz-{case}",
        )
        if grant_tier is not None:
            await _grant_study_access(
                ctx,
                study_idx=study_idx,
                principal_idx=ctx["user_session"]["principal_idx"],
                tier=grant_tier,
                granted_by_idx=ctx["wet_session"]["principal_idx"],
            )

    resp = await post_study_field(
        ctx,
        surface=surface,
        client=client,
        study_idx=study_idx,
        display_name=unique_field_name("Authz"),
        data_type="text",
    )
    assert resp.status_code == expected_status, resp.text


# case -> expected status for the create's conflict / bad-reference surface.
_STUDY_FIELD_CREATE_CONFLICT: dict[str, int] = {
    "duplicate_name": 409,
    "different_global": 409,
    "unknown_global_fk": 422,
}
STUDY_FIELD_CREATE_CONFLICT_CASES = tuple(_STUDY_FIELD_CREATE_CONFLICT)


async def assert_study_field_create_conflict(
    ctx,
    *,
    case: str,
    surface: StudyFieldCreateSurface,
) -> None:
    """Drive one conflict case of a study-local field create and assert its status.

    Each case builds its own precondition on a caller-owned study: a name
    already minted on the study, that name already bound to a different global
    field, or a global-field link naming no row at all.
    """
    expected_status = _STUDY_FIELD_CREATE_CONFLICT[case]
    study_idx = await _seed_study(
        ctx,
        owner_idx=ctx["user_session"]["principal_idx"],
        suffix=f"conflict-{case}",
    )
    display_name = unique_field_name("Conflict")

    if case == "unknown_global_fk":
        # Table name comes from the frozen surface constant, never from input.
        max_idx = await ctx["pool"].fetchval(
            f"SELECT COALESCE(MAX(idx), 0) FROM {surface.global_field_table}"
        )
        body = {"display_name": display_name, surface.global_fk_key: max_idx + 100_000}
    elif case == "different_global":
        # The first create binds the name to global A; the retry aims the same
        # name at global B, which the propagate guard refuses.
        global_a = await _seed_field_global(ctx, surface=surface, label="cfa")
        global_b = await _seed_field_global(ctx, surface=surface, label="cfb")
        first = await post_study_field(
            ctx,
            surface=surface,
            client=ctx["user"],
            study_idx=study_idx,
            display_name=display_name,
            **{surface.global_fk_key: global_a},
        )
        assert first.status_code == 201, first.text
        body = {"display_name": display_name, surface.global_fk_key: global_b}
    else:
        first = await post_study_field(
            ctx,
            surface=surface,
            client=ctx["user"],
            study_idx=study_idx,
            display_name=display_name,
            data_type="text",
        )
        assert first.status_code == 201, first.text
        body = {"display_name": display_name, "data_type": "text"}

    resp = await post_study_field(
        ctx, surface=surface, client=ctx["user"], study_idx=study_idx, **body
    )
    assert resp.status_code == expected_status, resp.text
    if case == "duplicate_name":
        assert "already" in resp.json()["detail"]


# ---------------------------------------------------------------------------
# Generic FK-reverse delete helper
# ---------------------------------------------------------------------------


async def delete_idxs(pool, table: str, idxs) -> None:
    """Bulk-delete by idx; tolerates a bare int or an iterable; empty is a no-op.

    Used by per-route `_cleanup_tracked` to drop test-created rows in
    FK-reverse order. The table name is interpolated, so callers must pass
    a static schema-qualified suffix (e.g., 'study', not user-input).
    """
    if isinstance(idxs, int):
        idxs = [idxs]
    if not idxs:
        return
    await pool.execute(
        f"DELETE FROM qiita.{table} WHERE idx = ANY($1::bigint[])",
        idxs,
    )


async def etag_for_row(pool, *, table: UpdatableTable, row_idx: int) -> str:
    """Build the quoted ISO-8601 ETag a PATCH route emits for a row.

    Reads updated_at directly so the helper does not depend on the
    route's behavior; the on-the-wire wording matches the routes'.
    """
    # Python does not enforce Literal at runtime; the f-string below is raw SQL.
    if table not in get_args(UpdatableTable):
        raise ValueError(f"etag_for_row rejects non-updatable table: {table!r}")
    updated_at = await pool.fetchval(
        f"SELECT updated_at FROM qiita.{table} WHERE idx = $1", row_idx
    )
    return f'"{updated_at.isoformat()}"'


async def assert_submission_error_cleared_on_new_attempt(
    client, url: str, *, initial_etag: str
) -> dict:
    """Drive the shared clear-submission-error-on-new-attempt trigger and
    assert it fired, for any entity exposing the submission-tracking pair.

    Seeds a submission_error via one PATCH, then bumps last_submission_at
    alone via a second PATCH (using the first response's ETag as If-Match);
    asserts the trigger nulled the error. `initial_etag` is the row's current
    If-Match. Returns the final PATCH body so a caller can layer
    entity-specific assertions on top.
    """
    seed = await client.patch(
        url, json={"submission_error": "ENA timed out"}, headers={"If-Match": initial_etag}
    )
    assert seed.status_code == 200, seed.text
    assert seed.json()["submission_error"] == "ENA timed out"

    bump = await client.patch(
        url,
        json={"last_submission_at": "2026-02-01T08:30:00+00:00"},
        headers={"If-Match": seed.headers["ETag"]},
    )
    assert bump.status_code == 200, bump.text
    body = bump.json()
    assert body["last_submission_at"] is not None
    assert body["submission_error"] is None
    return body


# ---------------------------------------------------------------------------
# Owner-eligibility 422 cases
# ---------------------------------------------------------------------------
# All ineligibility paths collapse to one 422 detail by design (avoids leaking
# principal-state to callers probing arbitrary owner_idx values). Each case
# locks in that the matching backend code path emits 422 — a regression where
# one input accidentally yields 500 / 409 / 201 surfaces here.


class IneligibilityKind(StrEnum):
    """One per non-eligible owner_idx shape. Pytest renders the StrEnum value
    as the parametrized test id (e.g., test_x[system_principal])."""

    SYSTEM_PRINCIPAL = "system_principal"
    NONEXISTENT = "nonexistent"
    SERVICE_ACCOUNT = "service_account"
    DISABLED = "disabled"
    RETIRED = "retired"
    INCOMPLETE_PROFILE = "incomplete_profile"


OWNER_INELIGIBILITY_KINDS = list(IneligibilityKind)


async def resolve_ineligible_owner_idx(
    pool,
    *,
    kind: IneligibilityKind,
    prefix: str,
    created: dict,
) -> int:
    """Resolve the owner_idx for one ineligibility kind; track any seeded
    rows in `created` for FK-reverse cleanup at teardown.

    Caller passes the route-specific `prefix` (e.g., 'bs-route-elig',
    'st-route-elig') so seeded principal display_names stay scoped to the
    suite. Caller is also responsible for passing a `created` dict with the
    standard 'user_principals' / 'service_account_principals' keys used by
    the route's _cleanup_tracked.
    """
    # The system principal exists but has no qiita.user row → is_user=False.
    if kind is IneligibilityKind.SYSTEM_PRINCIPAL:
        return SYSTEM_PRINCIPAL_IDX

    # An idx past the highest existing principal → fetch_user_eligibility None.
    if kind is IneligibilityKind.NONEXISTENT:
        max_idx = await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal")
        return max_idx + 100_000

    # Service-account-kind principal → is_user=False.
    if kind is IneligibilityKind.SERVICE_ACCOUNT:
        idx = await seed_service_principal(pool, prefix=prefix, suffix=str(kind))
        created["service_account_principals"].append(idx)
        return idx

    # Live user, then mark disabled / retired / leave with incomplete profile.
    if kind is IneligibilityKind.DISABLED:
        idx = await seed_user_principal(pool, prefix=prefix, suffix=str(kind))
        created["user_principals"].append(idx)
        await disable_principal(pool, idx)
        return idx
    if kind is IneligibilityKind.RETIRED:
        idx = await seed_user_principal(pool, prefix=prefix, suffix=str(kind))
        created["user_principals"].append(idx)
        await retire_principal(pool, idx)
        return idx
    if kind is IneligibilityKind.INCOMPLETE_PROFILE:
        idx = await seed_user_principal(
            pool, prefix=prefix, suffix=str(kind), profile_complete=False
        )
        created["user_principals"].append(idx)
        return idx

    # Closed-set fallback so a future kind without a branch fails loudly.
    raise AssertionError(f"unhandled IneligibilityKind: {kind}")


async def assert_owner_ineligibility_422(
    *,
    post_with_owner_idx: Callable[[int], Awaitable],
    expected_detail: str,
    owner_idx: int,
) -> None:
    """Drive `post_with_owner_idx` with the resolved owner_idx and assert the
    response is 422 with the expected detail.

    The caller wires up the route specifics (URL, body shape, study seed,
    wet_lab_admin client) inside `post_with_owner_idx`; this driver just
    invokes it and checks the surface contract.
    """
    resp = await post_with_owner_idx(owner_idx)
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == expected_detail


# ---------------------------------------------------------------------------
# Atomicity-test fixtures
# ---------------------------------------------------------------------------
# Used by tests that monkeypatch a helper (e.g., record_event) to raise, then
# assert the route's primary write rolled back when the route returned 500.


@pytest.fixture
def audit_failure():
    """Return an async coroutine that always raises. Pass it to
    monkeypatch.setattr(..., audit_failure) to simulate a failing audit
    insert during an atomicity test."""

    async def _failing(*args, **kwargs):
        raise RuntimeError("intentional audit failure")

    return _failing


@pytest.fixture
def study_link_gate_reports_live(monkeypatch):
    """Force the pre-write study-link gate to report a live link.

    Reproduces the window a mid-request retirement opens: the caller clears the
    gate on a link the database will refuse by the time the write runs. Patching
    the read is what makes that ordering deterministic instead of a race the test
    would have to win.
    """

    async def _linked(*_args, **_kwargs):
        return True

    monkeypatch.setattr(route_helpers, "fetch_entity_is_linked_to_study", _linked)


@pytest_asyncio.fixture
async def fail_safe_client(postgres_pool):
    """Yield an AsyncClient whose transport surfaces 5xx responses to the
    test instead of re-raising the underlying exception. Use when the test
    deliberately drives a route to a 500 (e.g., atomicity tests injecting
    an audit failure). Depends on postgres_pool so the app's pool state is
    initialised before the request runs."""
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# One pool spanning two studies, with alignments over it
# ---------------------------------------------------------------------------
# Shared by the pool-alignment discovery reads and the human alignment mint,
# which are two halves of one contract: you discover a cohort, then sign a
# ticket for exactly it. Seeding them separately is how those two drift apart.


async def _mint_alignment(pool, *, owner_idx: int, tag: str) -> int:
    from qiita_common.hashing import canonical_params_hash

    params = {"reference_idx": 1, "aligner": "minimap2", "mask_idx": 1, "shard_ids": [0], "t": tag}
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT alignment_idx FROM qiita.mint_alignment_definition($1, $2::jsonb, $3)",
            canonical_params_hash(params),
            json.dumps(params),
            owner_idx,
        )
    return row["alignment_idx"]


async def _gate(pool, *, alignment_idx: int, prep_sample_idx: int, state: str) -> None:
    await pool.execute(
        "INSERT INTO qiita.alignment_sample (alignment_idx, prep_sample_idx, state)"
        " VALUES ($1, $2, $3)",
        alignment_idx,
        prep_sample_idx,
        state,
    )


@pytest_asyncio.fixture
async def pool_alignment_seed(role_keyed_clients):
    """One sequenced pool spanning two studies, deliberately awkward:

        ps_a → study_1            alignment_1: completed
        ps_b → study_2            alignment_1: completed,  alignment_2: completed
        ps_c → study_1            alignment_1: pending
        ps_d → study_1 (retired)  alignment_1: completed   ← orphaned by retirement

    `regular_user_session` holds Tier.VIEWER on study_1 only, so that caller may
    read ps_a and ps_c and nothing else. That single shape exercises every
    decision in the discovery/mint contract at once — per-study tier narrowing,
    the orphan denial, the completion gate, and whole-alignment invisibility
    (alignment_2 touches only ps_b).

    Tracks and tears down everything it creates, including its own studies and
    study_access rows, so it composes with any module's `ctx`.
    """
    db = role_keyed_clients["pool"]
    owner = role_keyed_clients["wet_session"]["principal_idx"]
    reader = role_keyed_clients["user_session"]["principal_idx"]
    tracker = {"pool": db, "created": {"study": [], "study_access": []}}

    study_1 = await _seed_study(tracker, owner_idx=owner, suffix="align-disc-1")
    study_2 = await _seed_study(tracker, owner_idx=owner, suffix="align-disc-2")
    # The reader can see study_1 and NOT study_2 — the whole point of the shape.
    await _grant_study_access(
        tracker, study_idx=study_1, principal_idx=reader, tier=Tier.VIEWER, granted_by_idx=owner
    )

    samples: list[tuple[int, int, int]] = []  # (biosample, prep_sample, sequenced_sample)
    run_idx = pool_idx = None
    for i in range(4):
        bs, ps = await seed_biosample_with_sequenced_prep_sample(db, owner_idx=owner)
        if run_idx is None:
            run_idx, pool_idx, ss = await seed_sequenced_sample_subtype(
                db, prep_sample_idx=ps, owner_idx=owner, sequenced_pool_item_id=f"disc-{i}"
            )
        else:
            ss = await db.fetchval(
                "INSERT INTO qiita.sequenced_sample"
                "  (prep_sample_idx, sequenced_pool_idx, sequenced_pool_item_id, created_by_idx)"
                " VALUES ($1, $2, $3, $4) RETURNING idx",
                ps,
                pool_idx,
                f"disc-{i}",
                owner,
            )
        samples.append((bs, ps, ss))
    (bs_a, ps_a, _), (bs_b, ps_b, _), (bs_c, ps_c, _), (bs_d, ps_d, _) = samples

    async def link(biosample_idx, prep_sample_idx, study_idx, *, retired=False):
        await seed_biosample_to_study_link(
            db, biosample_idx=biosample_idx, study_idx=study_idx, created_by_idx=owner
        )
        await db.execute(
            "INSERT INTO qiita.prep_sample_to_study"
            " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
            prep_sample_idx,
            study_idx,
            owner,
        )
        if retired:
            await db.execute(
                "UPDATE qiita.prep_sample_to_study"
                " SET retired = true, retired_at = now(), retired_by_idx = $3"
                " WHERE prep_sample_idx = $1 AND study_idx = $2",
                prep_sample_idx,
                study_idx,
                owner,
            )

    await link(bs_a, ps_a, study_1)
    await link(bs_b, ps_b, study_2)
    await link(bs_c, ps_c, study_1)
    await link(bs_d, ps_d, study_1, retired=True)

    align_1 = await _mint_alignment(db, owner_idx=owner, tag=f"one-{uuid.uuid4()}")
    align_2 = await _mint_alignment(db, owner_idx=owner, tag=f"two-{uuid.uuid4()}")
    await _gate(db, alignment_idx=align_1, prep_sample_idx=ps_a, state="completed")
    await _gate(db, alignment_idx=align_1, prep_sample_idx=ps_b, state="completed")
    await _gate(db, alignment_idx=align_1, prep_sample_idx=ps_c, state="pending")
    await _gate(db, alignment_idx=align_1, prep_sample_idx=ps_d, state="completed")
    await _gate(db, alignment_idx=align_2, prep_sample_idx=ps_b, state="completed")

    yield {
        "run_idx": run_idx,
        "pool_idx": pool_idx,
        "align_1": align_1,
        "align_2": align_2,
        "ps_a": ps_a,
        "ps_b": ps_b,
        "ps_c": ps_c,
        "ps_d": ps_d,
        "study_1": study_1,
        "study_2": study_2,
        "owner_idx": owner,
        "reader_idx": reader,
    }

    prep_idxs = [ps for _, ps, _ in samples]
    bio_idxs = [bs for bs, _, _ in samples]
    ss_idxs = [ss for _, _, ss in samples]
    await db.execute(
        "DELETE FROM qiita.alignment_sample WHERE alignment_idx = ANY($1::bigint[])",
        [align_1, align_2],
    )
    await db.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = ANY($1::bigint[])",
        [align_1, align_2],
    )
    await db.execute(
        "DELETE FROM qiita.prep_sample_to_study WHERE prep_sample_idx = ANY($1::bigint[])",
        prep_idxs,
    )
    await db.execute(
        "DELETE FROM qiita.biosample_to_study WHERE biosample_idx = ANY($1::bigint[])", bio_idxs
    )
    await db.execute("DELETE FROM qiita.sequenced_sample WHERE idx = ANY($1::bigint[])", ss_idxs)
    await db.execute("DELETE FROM qiita.sequenced_pool WHERE idx = $1", pool_idx)
    await db.execute("DELETE FROM qiita.sequencing_run WHERE idx = $1", run_idx)
    await db.execute("DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", prep_idxs)
    await db.execute("DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", bio_idxs)
    for study_idx, principal_idx in tracker["created"]["study_access"]:
        await db.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            principal_idx,
        )
    await db.execute(
        "DELETE FROM qiita.study WHERE idx = ANY($1::bigint[])", tracker["created"]["study"]
    )
