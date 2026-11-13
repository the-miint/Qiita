"""DB tests for pair decisions: the `definition_pair` schema (constraints, detach on
a hand DELETE of a definition, append-only history), the /admin/definition-pair
routes including authorization, the deprecation refusal on each kind's status
route, and the group lock's try-lock.

The pure rules are unit-tested in tests/test_definition_similarity.py; these tests
check that the routes reach them and that Postgres holds what they write.
"""

import asyncio
import secrets
import time

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_ADMIN_DEFINITION_PAIR,
    URL_ADMIN_DEFINITION_PAIR_BY_IDX,
    URL_ADMIN_DEFINITION_PAIR_DECISION,
    URL_ADMIN_DEFINITION_PAIR_REVOKE,
    URL_ALIGNMENT_DEFINITION_STATUS,
    URL_MASK_DEFINITION_STATUS,
    URL_PROCESSING_STATUS,
)
from qiita_common.auth_constants import Scope

from qiita_control_plane.repositories import definition_pair as pair_repo
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.processing import mint_processing

pytestmark = pytest.mark.db

_REASON = "same host references, threshold change reviewed against the mock community"


@pytest_asyncio.fixture
async def clients(postgres_pool, human_admin_session, regular_user_session):
    """{admin, user, mask_only}: the system_admin's session token, a regular user's,
    and a system_admin PAT carrying only `mask_definition:lifecycle`."""
    from qiita_control_plane.auth.token import mint_api_token
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    mask_only_token, _ = await mint_api_token(
        postgres_pool,
        principal_idx=human_admin_session["principal_idx"],
        label="pair-mask-only",
        scopes=[Scope.MASK_DEFINITION_LIFECYCLE],
    )
    transport = ASGITransport(app=app)

    def _client(token):
        return AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {token}"},
        )

    async with (
        _client(human_admin_session["token"]) as admin,
        _client(regular_user_session["token"]) as user,
        _client(mask_only_token) as mask_only,
    ):
        yield {"admin": admin, "user": user, "mask_only": mask_only}


@pytest_asyncio.fixture
async def defs(postgres_pool, human_admin_session):
    """Mints definitions with unique params and removes them, and every pair naming
    them, afterwards."""
    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    made: dict[str, list[int]] = {"mask": [], "alignment": [], "processing": []}

    async def make_mask(*, workflow="read-mask", **params) -> int:
        async with postgres_pool.acquire() as conn:
            row = await mint_mask_definition(
                conn,
                filter_workflow=workflow,
                filter_version="1.0.0",
                params={"filter_workflow": workflow, "probe": suffix, **params},
                principal_idx=owner,
            )
        made["mask"].append(row["mask_idx"])
        return row["mask_idx"]

    async def make_assembly(mask_idx: int, *, assembler="myloasm") -> int:
        params = {
            "workflow": "long-read-assembly",
            "version": "1.0.0",
            "mask_idx": mask_idx,
            "assembler": assembler,
            "probe": suffix,
        }
        async with postgres_pool.acquire() as conn:
            row = await mint_processing(
                conn, workflow="long-read-assembly", version="1.0.0", params=params
            )
        made["processing"].append(row["processing_idx"])
        return row["processing_idx"]

    async def make_block(mask_idx: int, *, shard_ids=(0,), aligner="minimap2", **extra) -> int:
        params = {
            "reference_idx": 1,
            "aligner": aligner,
            "mask_idx": mask_idx,
            "shard_ids": list(shard_ids),
            "probe": suffix,
            **extra,
        }
        async with postgres_pool.acquire() as conn:
            row = await mint_alignment_definition(conn, params=params, principal_idx=owner)
        made["alignment"].append(row["alignment_idx"])
        return row["alignment_idx"]

    yield {
        "mask": make_mask,
        "assembly": make_assembly,
        "block": make_block,
        "owner": owner,
        "made": made,
    }

    for kind, idxs in made.items():
        await postgres_pool.execute(
            "DELETE FROM qiita.definition_pair WHERE definition_kind = $1"
            " AND (low_definition_ref = ANY($2::bigint[])"
            "      OR high_definition_ref = ANY($2::bigint[]))",
            kind,
            idxs,
        )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = ANY($1::bigint[])",
        made["alignment"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])",
        made["processing"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = ANY($1::bigint[])", made["mask"]
    )


async def _open(client, kind, a, b):
    return await client.post(
        URL_ADMIN_DEFINITION_PAIR, json={"definition_kind": kind, "definition_idx": [a, b]}
    )


async def _decide(client, pair_idx, verdict, reason=_REASON):
    return await client.post(
        URL_ADMIN_DEFINITION_PAIR_DECISION.format(definition_pair_idx=pair_idx),
        json={"verdict": verdict, "reason": reason},
    )


async def _revoke(client, pair_idx, reason="decided before the mock community was re-run"):
    return await client.post(
        URL_ADMIN_DEFINITION_PAIR_REVOKE.format(definition_pair_idx=pair_idx),
        json={"reason": reason},
    )


async def _opened(client, kind, a, b) -> int:
    resp = await _open(client, kind, a, b)
    assert resp.status_code == 201, resp.text
    return resp.json()["definition_pair_idx"]


def _refusal(resp, reason):
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == reason, detail
    return detail


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("kind", ["mask", "alignment", "processing"])
async def test_a_hand_delete_detaches_the_pair_and_keeps_its_refs(
    postgres_pool, clients, defs, kind
):
    m1 = await defs["mask"](rype=0.05)
    m2 = await defs["mask"](rype=0.1)
    if kind == "mask":
        low, high = m1, m2
        table, col = "qiita.mask_definition", "mask_idx"
    elif kind == "alignment":
        low, high = await defs["block"](m1), await defs["block"](m1, shard_ids=(0, 1))
        table, col = "qiita.alignment_definition", "alignment_idx"
    else:
        low, high = await defs["assembly"](m1), await defs["assembly"](m1, assembler="hifiasm")
        table, col = "qiita.processing", "processing_idx"
    pair_idx = await _opened(clients["admin"], kind, low, high)
    if kind != "alignment":
        assert (await _decide(clients["admin"], pair_idx, "not_similar")).status_code == 200

    await postgres_pool.execute(f"DELETE FROM {table} WHERE {col} = $1", high)

    row = await postgres_pool.fetchrow(
        f"SELECT low_{kind}_idx AS lo, high_{kind}_idx AS hi, low_definition_ref,"
        " high_definition_ref, detached_at, verdict"
        " FROM qiita.definition_pair WHERE definition_pair_idx = $1",
        pair_idx,
    )
    assert (row["lo"], row["hi"]) == (low, None)
    assert (row["low_definition_ref"], row["high_definition_ref"]) == (low, high)
    assert row["detached_at"] is not None
    assert row["verdict"] == ("pending" if kind == "alignment" else "not_similar")

    resp = await clients["admin"].get(
        URL_ADMIN_DEFINITION_PAIR_BY_IDX.format(definition_pair_idx=pair_idx)
    )
    assert resp.status_code == 200
    assert resp.json()["detached_at"] is not None
    _refusal(await _decide(clients["admin"], pair_idx, "not_similar"), "detached")


async def test_a_second_delete_keeps_the_first_detach_stamp(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", m1)
    first = await postgres_pool.fetchval(
        "SELECT detached_at FROM qiita.definition_pair WHERE definition_pair_idx = $1", pair_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", m2)
    row = await postgres_pool.fetchrow(
        "SELECT detached_at, low_mask_idx, high_mask_idx FROM qiita.definition_pair"
        " WHERE definition_pair_idx = $1",
        pair_idx,
    )
    assert row["detached_at"] == first
    assert (row["low_mask_idx"], row["high_mask_idx"]) == (None, None)


async def test_typed_columns_must_match_the_refs_and_the_kind(postgres_pool, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    insert = (
        "INSERT INTO qiita.definition_pair (definition_kind, low_mask_idx, high_mask_idx,"
        " low_alignment_idx, low_definition_ref, high_definition_ref, change, derived_only,"
        " opened_by_idx) VALUES ($1, $2, $3, $4, $5, $6, '{}', false, $7)"
    )
    cases = [
        ("mask", m1, m2, None, m1, m2 + 1000),  # high ref differs from the FK
        ("alignment", m1, m2, None, m1, m2),  # another kind's columns set
        ("mask", m1, None, None, m1, m2),  # a NULL FK on a live row
    ]
    for kind, lo, hi, al, lo_ref, hi_ref in cases:
        with pytest.raises(asyncpg.CheckViolationError, match="definition_pair_typed_refs"):
            await postgres_pool.execute(insert, kind, lo, hi, al, lo_ref, hi_ref, defs["owner"])
    with pytest.raises(asyncpg.CheckViolationError, match="definition_pair_ordered"):
        await postgres_pool.execute(insert, "mask", m2, m1, None, m2, m1, defs["owner"])


async def test_decision_fields_move_with_the_verdict(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    with pytest.raises(asyncpg.CheckViolationError, match="definition_pair_decision_fields"):
        await postgres_pool.execute(
            "UPDATE qiita.definition_pair SET verdict = 'similar' WHERE definition_pair_idx = $1",
            pair_idx,
        )


async def test_history_rows_are_not_updated(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    with pytest.raises(asyncpg.RaiseError, match="not updated"):
        await postgres_pool.execute(
            "UPDATE qiita.definition_pair_event SET reason = 'x' WHERE definition_pair_idx = $1",
            pair_idx,
        )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


async def test_open_records_the_change_and_an_opened_event(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    resp = await _open(clients["admin"], "mask", m2, m1)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert (body["low_definition_idx"], body["high_definition_idx"]) == (m1, m2)
    assert body["change"] == {"rype": [0.05, 0.1]}
    assert body["derived_only"] is False
    assert body["verdict"] == "pending"
    assert body["opened_by_idx"] == defs["owner"]

    again = await _open(clients["admin"], "mask", m1, m2)
    assert again.status_code == 200
    assert again.json()["definition_pair_idx"] == body["definition_pair_idx"]

    shown = await clients["admin"].get(
        URL_ADMIN_DEFINITION_PAIR_BY_IDX.format(definition_pair_idx=body["definition_pair_idx"])
    )
    assert [e["event"] for e in shown.json()["events"]] == ["opened"]


async def test_open_marks_an_upstream_only_change_derived_only(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m2)
    body = (await _open(clients["admin"], "processing", p1, p2)).json()
    assert body["change"] == {"mask_idx": [m1, m2]}
    assert body["derived_only"] is True
    _refusal(
        await _decide(clients["admin"], body["definition_pair_idx"], "similar"), "derived_only"
    )
    assert (
        await _decide(clients["admin"], body["definition_pair_idx"], "not_similar")
    ).status_code == 200


async def test_open_refuses_an_unknown_definition_and_a_cross_scope_pair(clients, defs):
    m1 = await defs["mask"](rype=0.05)
    other = await defs["mask"](workflow="fastq-to-parquet")
    assert (await _open(clients["admin"], "mask", m1, 2**40)).status_code == 404
    _refusal(await _open(clients["admin"], "mask", m1, other), "cross_scope")
    assert (await _open(clients["admin"], "mask", m1, m1)).status_code == 422


async def test_decide_then_revoke_returns_to_pending_and_keeps_the_history(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)

    decided = await _decide(clients["admin"], pair_idx, "similar")
    assert decided.status_code == 200, decided.text
    assert decided.json()["verdict"] == "similar"
    assert decided.json()["decision_reason"] == _REASON
    assert decided.json()["decided_by_idx"] == defs["owner"]

    revoked = await _revoke(clients["admin"], pair_idx)
    assert revoked.status_code == 200, revoked.text
    body = revoked.json()
    assert body["verdict"] == "pending"
    assert body["decided_by_idx"] is None and body["decision_reason"] is None

    _refusal(await _revoke(clients["admin"], pair_idx), "not_decided")

    events = (
        await clients["admin"].get(
            URL_ADMIN_DEFINITION_PAIR_BY_IDX.format(definition_pair_idx=pair_idx)
        )
    ).json()["events"]
    assert [(e["event"], e["verdict"]) for e in events] == [
        ("opened", "pending"),
        ("decided", "similar"),
        ("revoked", "pending"),
    ]
    assert events[1]["reason"] == _REASON


async def test_a_decision_rejects_pending_and_a_blank_reason(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    assert (await _decide(clients["admin"], pair_idx, "pending")).status_code == 422
    assert (await _decide(clients["admin"], pair_idx, "similar", reason="  ")).status_code == 422


async def test_revoking_a_mask_pair_a_processing_similar_relies_on_is_refused(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m2, assembler="hifiasm")
    mask_pair = await _opened(clients["admin"], "mask", m1, m2)
    proc_pair = await _opened(clients["admin"], "processing", p1, p2)

    _refusal(await _decide(clients["admin"], proc_pair, "similar"), "upstream_not_grouped")
    assert (await _decide(clients["admin"], mask_pair, "similar")).status_code == 200
    assert (await _decide(clients["admin"], proc_pair, "similar")).status_code == 200

    detail = _refusal(await _revoke(clients["admin"], mask_pair), "relied_on")
    assert detail["definition_pair_idx"] == [proc_pair]
    _refusal(await _decide(clients["admin"], mask_pair, "not_similar"), "relied_on")

    assert (await _revoke(clients["admin"], proc_pair)).status_code == 200
    assert (await _revoke(clients["admin"], mask_pair)).status_code == 200


async def test_list_filters_by_verdict_and_kind(clients, defs):
    m1, m2, m3 = [await defs["mask"](rype=r) for r in (0.05, 0.1, 0.2)]
    a1, a2 = await defs["block"](m1), await defs["block"](m1, shard_ids=(0, 1))
    mask_similar = await _opened(clients["admin"], "mask", m1, m2)
    mask_pending = await _opened(clients["admin"], "mask", m1, m3)
    alignment_pending = await _opened(clients["admin"], "alignment", a1, a2)
    assert (await _decide(clients["admin"], mask_similar, "similar")).status_code == 200

    async def listed(**params):
        resp = await clients["admin"].get(URL_ADMIN_DEFINITION_PAIR, params=params)
        assert resp.status_code == 200, resp.text
        return {p["definition_pair_idx"] for p in resp.json()["pairs"]}

    mine = {mask_similar, mask_pending, alignment_pending}
    assert mine <= await listed()
    assert (await listed(verdict="pending")) & mine == {mask_pending, alignment_pending}
    assert (await listed(definition_kind="mask")) & mine == {mask_similar, mask_pending}
    assert (await listed(definition_kind="mask", verdict="similar")) & mine == {mask_similar}


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


async def test_a_regular_user_is_refused_every_route(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    user = clients["user"]
    assert (await user.get(URL_ADMIN_DEFINITION_PAIR)).status_code == 403
    assert (await _open(user, "mask", m1, m2)).status_code == 403
    assert (
        await user.get(URL_ADMIN_DEFINITION_PAIR_BY_IDX.format(definition_pair_idx=pair_idx))
    ).status_code == 403
    assert (await _decide(user, pair_idx, "similar")).status_code == 403
    assert (await _revoke(user, pair_idx)).status_code == 403


async def test_each_pair_is_gated_on_its_own_kinds_lifecycle_scope(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    a1, a2 = await defs["block"](m1), await defs["block"](m1, shard_ids=(0, 1))
    mask_pair = await _opened(clients["admin"], "mask", m1, m2)
    alignment_pair = await _opened(clients["admin"], "alignment", a1, a2)
    narrow = clients["mask_only"]

    listed = await narrow.get(URL_ADMIN_DEFINITION_PAIR)
    assert listed.status_code == 200
    kinds = {p["definition_kind"] for p in listed.json()["pairs"]}
    assert kinds == {"mask"}
    assert (
        await narrow.get(URL_ADMIN_DEFINITION_PAIR, params={"definition_kind": "alignment"})
    ).status_code == 403

    for path in (URL_ADMIN_DEFINITION_PAIR_BY_IDX,):
        assert (
            await narrow.get(path.format(definition_pair_idx=alignment_pair))
        ).status_code == 403
        assert (await narrow.get(path.format(definition_pair_idx=mask_pair))).status_code == 200
    assert (await _open(narrow, "alignment", a1, a2)).status_code == 403
    assert (await _decide(narrow, alignment_pair, "not_similar")).status_code == 403
    assert (await _decide(narrow, mask_pair, "similar")).status_code == 200
    assert (await _revoke(narrow, mask_pair)).status_code == 200


# ---------------------------------------------------------------------------
# Deprecation refused while a similar pair names the definition
# ---------------------------------------------------------------------------


async def _deprecate(client, url):
    return await client.patch(url, json={"status": "deprecated", "reason": "scored wrongly"})


async def test_deprecating_a_mask_with_a_similar_pair_is_refused(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)
    assert (await _decide(clients["admin"], pair_idx, "similar")).status_code == 200

    url = URL_MASK_DEFINITION_STATUS.format(mask_idx=m2)
    detail = _refusal(await _deprecate(clients["admin"], url), "similar_pairs")
    assert detail["definition_pair_idx"] == [pair_idx]

    assert (await _decide(clients["admin"], pair_idx, "not_similar")).status_code == 200
    assert (await _deprecate(clients["admin"], url)).status_code == 200
    _refusal(await _decide(clients["admin"], pair_idx, "similar"), "deprecated_side")


async def test_deprecating_an_alignment_with_a_similar_pair_is_refused(clients, defs):
    m1 = await defs["mask"](rype=0.05)
    a1, a2 = await defs["block"](m1), await defs["block"](m1, aligner="minimap2", preset="sr")
    pair_idx = await _opened(clients["admin"], "alignment", a1, a2)
    assert (await _decide(clients["admin"], pair_idx, "similar")).status_code == 200

    url = URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=a1)
    _refusal(await _deprecate(clients["admin"], url), "similar_pairs")
    assert (await _revoke(clients["admin"], pair_idx)).status_code == 200
    assert (await _deprecate(clients["admin"], url)).status_code == 200


async def test_deprecating_a_processing_with_a_similar_pair_is_refused(clients, defs):
    m1 = await defs["mask"](rype=0.05)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m1, assembler="hifiasm")
    pair_idx = await _opened(clients["admin"], "processing", p1, p2)
    assert (await _decide(clients["admin"], pair_idx, "similar")).status_code == 200

    url = URL_PROCESSING_STATUS.format(processing_idx=p2)
    _refusal(await _deprecate(clients["admin"], url), "similar_pairs")
    assert (await _revoke(clients["admin"], pair_idx)).status_code == 200
    resp = await _deprecate(clients["admin"], url)
    assert resp.status_code == 200, resp.text
    assert resp.json()["status"] == "deprecated"
    reactivated = await clients["admin"].patch(url, json={"status": "active"})
    assert reactivated.status_code == 200
    assert (
        await clients["admin"].patch(
            URL_PROCESSING_STATUS.format(processing_idx=2**40), json={"status": "active"}
        )
    ).status_code == 404


async def test_a_definition_grouped_only_by_derivation_can_be_deprecated(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    p1 = await defs["assembly"](m1)
    await defs["assembly"](m2)
    mask_pair = await _opened(clients["admin"], "mask", m1, m2)
    assert (await _decide(clients["admin"], mask_pair, "similar")).status_code == 200
    url = URL_PROCESSING_STATUS.format(processing_idx=p1)
    assert (await _deprecate(clients["admin"], url)).status_code == 200


# ---------------------------------------------------------------------------
# Group lock
# ---------------------------------------------------------------------------


async def test_a_decision_does_not_queue_behind_a_shared_holder(
    postgres_pool, clients, defs, monkeypatch
):
    """A reader holding the group lock SHARED makes a decision answer 503 within its
    bounded retry, instead of waiting in the lock queue (where it would block every
    later shared request). Once the reader ends, the same decision goes through."""
    monkeypatch.setattr(pair_repo, "GROUP_LOCK_EXCLUSIVE_ATTEMPTS", 3)
    monkeypatch.setattr(pair_repo, "GROUP_LOCK_EXCLUSIVE_INTERVAL_S", 0.05)
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    pair_idx = await _opened(clients["admin"], "mask", m1, m2)

    async with postgres_pool.acquire() as reader, reader.transaction():
        await pair_repo.lock_groups_shared(reader)
        started = time.monotonic()
        resp = await asyncio.wait_for(_decide(clients["admin"], pair_idx, "similar"), timeout=10)
        elapsed = time.monotonic() - started
        assert resp.status_code == 503, resp.text
        assert resp.headers["Retry-After"]
        assert elapsed < 5
        # Nothing was left waiting in the lock queue.
        assert not await postgres_pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
            " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            " AND classid = $1)",
            pair_repo.DEFINITION_GROUP_LOCK_CLASS,
        )

    assert (await _decide(clients["admin"], pair_idx, "similar")).status_code == 200
