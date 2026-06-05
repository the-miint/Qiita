"""Shared fixtures and helpers for control-plane route tests.

Holds the three-role AsyncClient triple, a PAT-minting client factory used
by the per-route no-scope fixtures, a generic FK-reverse delete helper, and
the parametrise source + driver for the owner-eligibility 422 surface. Each
route test still owns its own `ctx` and `_cleanup_tracked` because the
tracked table set differs per route.
"""

import secrets
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import get_args

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX, Scope

from qiita_control_plane.repositories import UpdatableTable
from qiita_control_plane.testing.db_seeds import (
    disable_principal,
    retire_principal,
    seed_service_principal,
    seed_user_principal,
)

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
