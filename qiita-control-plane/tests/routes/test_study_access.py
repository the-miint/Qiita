"""Integration tests for /api/v1/study/{study_idx}/access.

The full permission table is unit-tested in tests/auth/test_study_access_policy.py;
these cover the wiring: each route applies the policy with the caller's real
standing, the email lookup and its 422s, the 409 on an existing row, the
owner-row rule, 404s, scope gates, and the auth_event each mutation records.

The regular-user client is the caller throughout; its standing on a study is
set by making it the owner or by inserting its row directly.
"""

import asyncio
import json
import secrets

import pytest
import pytest_asyncio
from qiita_common.api_paths import URL_STUDY_ACCESS, URL_STUDY_ACCESS_BY_PRINCIPAL
from qiita_common.auth_constants import AuthEventType, Scope
from qiita_common.models import Tier

from qiita_control_plane.routes.study_access import _MSG_ACCOUNT_INACTIVE, _MSG_CONCURRENT
from qiita_control_plane.testing.db_seeds import seed_user_principal

from .conftest import _grant_study_access, _seed_study, delete_idxs

pytestmark = pytest.mark.db

_PREFIX = "sa-route"


@pytest_asyncio.fixture
async def ctx(role_keyed_clients):
    created: dict = {"study_access": [], "study": [], "user_principals": []}
    yield {**role_keyed_clients, "created": created}
    await _cleanup(role_keyed_clients["pool"], created)


async def _cleanup(pool, created: dict) -> None:
    """study_access → the study_access_* auth_events on these studies → study →
    seeded principals. auth_event is append-only (a BEFORE DELETE trigger raises),
    so the trigger is disabled around the one DELETE and re-enabled after."""
    studies = created["study"]
    async with pool.acquire() as conn, conn.transaction():
        if studies:
            await conn.execute(
                "DELETE FROM qiita.study_access WHERE study_idx = ANY($1::bigint[])", studies
            )
        await conn.execute("ALTER TABLE qiita.auth_event DISABLE TRIGGER auth_event_no_delete")
        try:
            await conn.execute(
                "DELETE FROM qiita.auth_event"
                " WHERE (event_type LIKE 'study_access_%'"
                "        AND (detail->>'study_idx')::bigint = ANY($1::bigint[]))"
                "    OR principal_idx = ANY($2::bigint[])",
                studies,
                created["user_principals"],
            )
        finally:
            await conn.execute("ALTER TABLE qiita.auth_event ENABLE TRIGGER auth_event_no_delete")
    await delete_idxs(pool, "study", studies)
    if created["user_principals"]:
        await pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = ANY($1::bigint[])",
            created["user_principals"],
        )
    await delete_idxs(pool, "principal", created["user_principals"])


async def _seed_person(ctx, suffix: str, **kwargs) -> tuple[int, str]:
    """A user principal; returns (principal_idx, email)."""
    pidx = await seed_user_principal(ctx["pool"], prefix=_PREFIX, suffix=suffix, **kwargs)
    ctx["created"]["user_principals"].append(pidx)
    email = await ctx["pool"].fetchval(
        "SELECT email FROM qiita.user WHERE principal_idx = $1", pidx
    )
    return pidx, email


async def _study_with_caller_at(ctx, tier: str | None) -> int:
    """A study owned by someone else, the regular user holding `tier` on it
    (no row when None)."""
    owner, _ = await _seed_person(ctx, "owner")
    study_idx = await _seed_study(ctx, owner_idx=owner, suffix="sa")
    if tier is not None:
        await _grant_study_access(
            ctx,
            study_idx=study_idx,
            principal_idx=ctx["user_session"]["principal_idx"],
            tier=tier,
            granted_by_idx=owner,
        )
    return study_idx


async def _insert_row(ctx, study_idx: int, principal_idx: int, tier: str) -> None:
    await _grant_study_access(
        ctx, study_idx=study_idx, principal_idx=principal_idx, tier=tier, granted_by_idx=None
    )


async def _tier(ctx, study_idx: int, principal_idx: int) -> str | None:
    return await ctx["pool"].fetchval(
        "SELECT access_tier::text FROM qiita.study_access"
        " WHERE study_idx = $1 AND principal_idx = $2",
        study_idx,
        principal_idx,
    )


async def _events(ctx, study_idx: int) -> list[dict]:
    rows = await ctx["pool"].fetch(
        "SELECT event_type, principal_idx, actor_principal_idx, detail FROM qiita.auth_event"
        " WHERE event_type LIKE 'study_access_%' AND (detail->>'study_idx')::bigint = $1"
        " ORDER BY event_idx",
        study_idx,
    )
    return [
        {
            "event_type": r["event_type"],
            "principal_idx": r["principal_idx"],
            "actor": r["actor_principal_idx"],
            "detail": json.loads(r["detail"]) if isinstance(r["detail"], str) else r["detail"],
        }
        for r in rows
    ]


def _url(study_idx: int, principal_idx: int | None = None) -> str:
    if principal_idx is None:
        return URL_STUDY_ACCESS.format(study_idx=study_idx)
    return URL_STUDY_ACCESS_BY_PRINCIPAL.format(study_idx=study_idx, principal_idx=principal_idx)


# ---------------------------------------------------------------------------
# GET — list
# ---------------------------------------------------------------------------


async def test_member_lists_rows_admin_first(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    viewer, viewer_email = await _seed_person(ctx, "v")
    admin, _ = await _seed_person(ctx, "a")
    await _insert_row(ctx, study_idx, viewer, Tier.VIEWER)
    await _insert_row(ctx, study_idx, admin, Tier.ADMIN)

    resp = await ctx["user"].get(_url(study_idx))

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert [r["access_tier"] for r in body] == [Tier.ADMIN, Tier.MEMBER, Tier.VIEWER]
    assert body[2]["principal_idx"] == viewer
    assert body[2]["email"] == viewer_email


@pytest.mark.parametrize("tier", [Tier.VIEWER, None])
async def test_viewer_and_public_cannot_list(ctx, tier):
    study_idx = await _study_with_caller_at(ctx, tier)
    resp = await ctx["user"].get(_url(study_idx))
    assert resp.status_code == 403, resp.text


async def test_wet_lab_admin_lists_without_a_row(ctx):
    study_idx = await _study_with_caller_at(ctx, None)
    resp = await ctx["wet"].get(_url(study_idx))
    assert resp.status_code == 200, resp.text


async def test_list_missing_study_is_404(ctx):
    resp = await ctx["user"].get(_url(2**62))
    assert resp.status_code == 404


async def test_list_requires_study_read_scope(ctx, make_pat_client):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    client = await make_pat_client(label="sa-no-read", scopes=[Scope.SELF_PROFILE])
    resp = await client.get(_url(study_idx))
    assert resp.status_code == 403
    assert "study:read" in resp.text


# ---------------------------------------------------------------------------
# POST — grant
# ---------------------------------------------------------------------------


async def test_owner_grants_by_email_and_records_the_event(ctx):
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=caller, suffix="own")
    grantee, email = await _seed_person(ctx, "g")

    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email.upper(), "access_tier": Tier.ADMIN}
    )

    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["principal_idx"] == grantee
    assert body["access_tier"] == Tier.ADMIN
    assert body["granted_by_idx"] == caller
    assert await _tier(ctx, study_idx, grantee) == Tier.ADMIN
    assert await _events(ctx, study_idx) == [
        {
            "event_type": AuthEventType.STUDY_ACCESS_GRANT,
            "principal_idx": grantee,
            "actor": caller,
            "detail": {"study_idx": study_idx, "access_tier": Tier.ADMIN},
        }
    ]


async def test_member_cannot_grant_admin(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, email = await _seed_person(ctx, "g")
    resp = await ctx["user"].post(_url(study_idx), json={"email": email, "access_tier": Tier.ADMIN})
    assert resp.status_code == 403, resp.text
    assert await _tier(ctx, study_idx, grantee) is None
    assert await _events(ctx, study_idx) == []


async def test_member_grants_member(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, email = await _seed_person(ctx, "g")
    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email, "access_tier": Tier.MEMBER}
    )
    assert resp.status_code == 201, resp.text
    assert await _tier(ctx, study_idx, grantee) == Tier.MEMBER


async def test_viewer_cannot_grant(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.VIEWER)
    _, email = await _seed_person(ctx, "g")
    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email, "access_tier": Tier.VIEWER}
    )
    assert resp.status_code == 403, resp.text


async def test_grant_to_unknown_email_says_to_log_in_first(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    resp = await ctx["user"].post(
        _url(study_idx),
        json={"email": f"nobody-{secrets.token_hex(4)}@test.local", "access_tier": Tier.VIEWER},
    )
    assert resp.status_code == 422, resp.text
    assert "log in to Qiita once" in resp.json()["detail"]


@pytest.mark.parametrize("flag", ["disabled", "retired"])
async def test_grant_to_inactive_account_is_422(ctx, flag):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    grantee, email = await _seed_person(ctx, "g")
    # The *_consistent CHECKs require the timestamp and actor alongside the flag.
    await ctx["pool"].execute(
        f"UPDATE qiita.principal SET {flag} = true, {flag}_at = now(), {flag}_by_idx = $2"
        " WHERE idx = $1",
        grantee,
        ctx["user_session"]["principal_idx"],
    )
    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email, "access_tier": Tier.VIEWER}
    )
    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"] == _MSG_ACCOUNT_INACTIVE


async def test_grant_on_existing_row_is_409_naming_the_tier(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    grantee, email = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.VIEWER)
    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email, "access_tier": Tier.MEMBER}
    )
    assert resp.status_code == 409, resp.text
    assert "'viewer'" in resp.json()["detail"]
    assert await _tier(ctx, study_idx, grantee) == Tier.VIEWER


async def test_grant_public_is_422_before_the_db(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    _, email = await _seed_person(ctx, "g")
    resp = await ctx["user"].post(
        _url(study_idx), json={"email": email, "access_tier": Tier.PUBLIC}
    )
    assert resp.status_code == 422


async def test_grant_requires_study_write_scope(ctx, make_pat_client):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    _, email = await _seed_person(ctx, "g")
    client = await make_pat_client(label="sa-no-write", scopes=[Scope.STUDY_READ])
    resp = await client.post(_url(study_idx), json={"email": email, "access_tier": Tier.VIEWER})
    assert resp.status_code == 403
    assert "study:write" in resp.text


# ---------------------------------------------------------------------------
# PATCH — change tier
# ---------------------------------------------------------------------------


async def test_member_promotes_viewer_to_member(ctx):
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, _ = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.VIEWER)

    resp = await ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.MEMBER})

    assert resp.status_code == 200, resp.text
    assert resp.json()["access_tier"] == Tier.MEMBER
    assert await _tier(ctx, study_idx, grantee) == Tier.MEMBER
    assert await _events(ctx, study_idx) == [
        {
            "event_type": AuthEventType.STUDY_ACCESS_TIER_CHANGE,
            "principal_idx": grantee,
            "actor": caller,
            "detail": {"study_idx": study_idx, "from": Tier.VIEWER, "to": Tier.MEMBER},
        }
    ]


async def test_member_cannot_promote_to_admin(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, _ = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.VIEWER)
    resp = await ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.ADMIN})
    assert resp.status_code == 403, resp.text
    assert await _tier(ctx, study_idx, grantee) == Tier.VIEWER


async def test_study_admin_cannot_demote_an_admin_row(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    other_admin, _ = await _seed_person(ctx, "a")
    await _insert_row(ctx, study_idx, other_admin, Tier.ADMIN)
    resp = await ctx["user"].patch(_url(study_idx, other_admin), json={"access_tier": Tier.VIEWER})
    assert resp.status_code == 403, resp.text
    assert await _tier(ctx, study_idx, other_admin) == Tier.ADMIN


async def test_wet_lab_admin_demotes_an_admin_row(ctx):
    study_idx = await _study_with_caller_at(ctx, None)
    other_admin, _ = await _seed_person(ctx, "a")
    await _insert_row(ctx, study_idx, other_admin, Tier.ADMIN)
    resp = await ctx["wet"].patch(_url(study_idx, other_admin), json={"access_tier": Tier.VIEWER})
    assert resp.status_code == 200, resp.text
    assert await _tier(ctx, study_idx, other_admin) == Tier.VIEWER


async def test_same_tier_patch_returns_row_and_records_nothing(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    grantee, _ = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.MEMBER)
    resp = await ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.MEMBER})
    assert resp.status_code == 200, resp.text
    assert await _events(ctx, study_idx) == []


async def test_patch_missing_row_is_404(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    grantee, _ = await _seed_person(ctx, "g")
    resp = await ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.MEMBER})
    assert resp.status_code == 404, resp.text


async def test_viewer_gets_403_not_404_on_a_missing_row(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.VIEWER)
    grantee, _ = await _seed_person(ctx, "g")
    resp = await ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.VIEWER})
    assert resp.status_code == 403, resp.text


# ---------------------------------------------------------------------------
# DELETE — revoke
# ---------------------------------------------------------------------------


async def test_member_revokes_viewer_and_records_the_event(ctx):
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, _ = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.VIEWER)

    resp = await ctx["user"].delete(_url(study_idx, grantee))

    assert resp.status_code == 200, resp.text
    assert resp.json()["access_tier"] == Tier.VIEWER
    assert await _tier(ctx, study_idx, grantee) is None
    assert await _events(ctx, study_idx) == [
        {
            "event_type": AuthEventType.STUDY_ACCESS_REVOKE,
            "principal_idx": grantee,
            "actor": caller,
            "detail": {"study_idx": study_idx, "access_tier": Tier.VIEWER},
        }
    ]


async def test_member_revokes_own_row(ctx):
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    resp = await ctx["user"].delete(_url(study_idx, caller))
    assert resp.status_code == 200, resp.text
    assert await _tier(ctx, study_idx, caller) is None


async def test_owner_cannot_revoke_own_admin_row_but_wet_lab_admin_can(ctx):
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _seed_study(ctx, owner_idx=caller, suffix="own")
    await _insert_row(ctx, study_idx, caller, Tier.ADMIN)

    resp = await ctx["user"].delete(_url(study_idx, caller))
    assert resp.status_code == 403, resp.text

    resp = await ctx["wet"].delete(_url(study_idx, caller))
    assert resp.status_code == 200, resp.text
    # The owner still manages the study without the row (owner bypass).
    resp = await ctx["user"].get(_url(study_idx))
    assert resp.status_code == 200, resp.text


async def test_revoke_missing_study_is_404(ctx):
    resp = await ctx["user"].delete(_url(2**62, 1))
    assert resp.status_code == 404


async def test_same_tier_patch_is_still_403_when_the_caller_could_not_change_it(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    other_admin, _ = await _seed_person(ctx, "a")
    await _insert_row(ctx, study_idx, other_admin, Tier.ADMIN)
    resp = await ctx["user"].patch(_url(study_idx, other_admin), json={"access_tier": Tier.ADMIN})
    assert resp.status_code == 403, resp.text


async def test_grant_email_longer_than_320_is_422(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    email = "a" * 310 + "@test.local"
    resp = await ctx["user"].post(_url(study_idx), json={"email": email, "access_tier": "viewer"})
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["loc"] == ["body", "email"]


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


async def test_two_grants_to_the_same_account_yield_one_201_and_one_409(ctx):
    study_idx = await _study_with_caller_at(ctx, Tier.ADMIN)
    grantee, email = await _seed_person(ctx, "g")
    body = {"email": email, "access_tier": Tier.MEMBER}

    first, second = await asyncio.gather(
        ctx["user"].post(_url(study_idx), json=body),
        ctx["user"].post(_url(study_idx), json=body),
    )

    assert sorted([first.status_code, second.status_code]) == [201, 409]
    loser = first if first.status_code == 409 else second
    assert "'member'" in loser.json()["detail"]
    assert await _tier(ctx, study_idx, grantee) == Tier.MEMBER


# Long enough for a lock wait and Postgres's default 1 s deadlock_timeout; a test
# whose request never returns fails here instead of hanging the suite.
_REQUEST_TIMEOUT_S = 15


async def _finish(task: asyncio.Task):
    return await asyncio.wait_for(task, _REQUEST_TIMEOUT_S)


async def _blocked(task: asyncio.Task) -> bool:
    """Whether `task` is still waiting after the other side's lock has had time
    to be contended."""
    await asyncio.sleep(0.3)
    return not task.done()


async def test_change_waits_for_a_concurrent_change_and_checks_the_committed_tier(ctx):
    """A member's viewer→member change waits on a concurrent viewer→admin change;
    once that commits, the member may not change an admin row."""
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, _ = await _seed_person(ctx, "g")
    await _insert_row(ctx, study_idx, grantee, Tier.VIEWER)

    async with ctx["pool"].acquire() as other:
        tr = other.transaction()
        await tr.start()
        await other.execute(
            "UPDATE qiita.study_access SET access_tier = 'admin'"
            " WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            grantee,
        )
        task = asyncio.create_task(
            ctx["user"].patch(_url(study_idx, grantee), json={"access_tier": Tier.MEMBER})
        )
        assert await _blocked(task)
        await tr.commit()
        resp = await _finish(task)

    assert resp.status_code == 403, resp.text
    assert await _tier(ctx, study_idx, grantee) == Tier.ADMIN


async def test_grant_waits_for_a_concurrent_revoke_of_the_caller(ctx):
    """The caller's own row is share-locked before their standing is read, so a
    revoke of it that is in flight finishes first and the grant is refused."""
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    grantee, email = await _seed_person(ctx, "g")

    async with ctx["pool"].acquire() as other:
        tr = other.transaction()
        await tr.start()
        await other.execute(
            "DELETE FROM qiita.study_access WHERE study_idx = $1 AND principal_idx = $2",
            study_idx,
            caller,
        )
        task = asyncio.create_task(
            ctx["user"].post(_url(study_idx), json={"email": email, "access_tier": Tier.VIEWER})
        )
        assert await _blocked(task)
        await tr.commit()
        resp = await _finish(task)

    assert resp.status_code == 403, resp.text
    assert await _tier(ctx, study_idx, grantee) is None


async def test_crossed_changes_deadlock_and_the_request_gets_409(ctx):
    """The request share-locks the caller's row, then waits to update B's row,
    which another transaction has share-locked; that transaction then tries to
    update the caller's row. Postgres aborts one side; the request, which began
    waiting first, is the one that detects the cycle."""
    caller = ctx["user_session"]["principal_idx"]
    study_idx = await _study_with_caller_at(ctx, Tier.MEMBER)
    b, _ = await _seed_person(ctx, "b")
    await _insert_row(ctx, study_idx, b, Tier.MEMBER)

    async with ctx["pool"].acquire() as other:
        tr = other.transaction()
        await tr.start()
        try:
            await other.execute(
                "SELECT 1 FROM qiita.study_access"
                " WHERE study_idx = $1 AND principal_idx = $2 FOR SHARE",
                study_idx,
                b,
            )
            task = asyncio.create_task(ctx["user"].delete(_url(study_idx, b)))
            assert await _blocked(task)
            crossing = asyncio.create_task(
                other.execute(
                    "UPDATE qiita.study_access SET access_tier = 'viewer'"
                    " WHERE study_idx = $1 AND principal_idx = $2",
                    study_idx,
                    caller,
                )
            )
            resp = await _finish(task)
            await asyncio.wait_for(crossing, _REQUEST_TIMEOUT_S)
        finally:
            await tr.rollback()

    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"] == _MSG_CONCURRENT
    assert await _tier(ctx, study_idx, b) == Tier.MEMBER
    assert await _events(ctx, study_idx) == []
