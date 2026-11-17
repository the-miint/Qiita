"""DB tests for output equivalences: the `output_equivalence` schema (constraints, detach on
a hand DELETE of a definition, append-only history), the /admin/output-equivalence
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
    URL_ADMIN_OUTPUT_EQUIVALENCE,
    URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,
    URL_ADMIN_OUTPUT_EQUIVALENCE_DECISION,
    URL_ADMIN_OUTPUT_EQUIVALENCE_REVOKE,
    URL_ALIGNMENT_DEFINITION_STATUS,
    URL_MASK_DEFINITION_STATUS,
    URL_PROCESSING_STATUS,
)
from qiita_common.auth_constants import Scope
from qiita_common.models import (
    JOINING_VERDICTS,
    NON_JOINING_VERDICTS,
    DefinitionKind,
    OutputEquivalenceEventType,
    OutputEquivalenceRefusal,
    OutputEquivalenceVerdict,
)

from qiita_control_plane.definition_similarity import decide_equivalence, load_groups
from qiita_control_plane.repositories import output_equivalence as equivalence_repo
from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_teardown import delete_output_equivalences
from qiita_control_plane.testing.postgres import until_advisory_lock_awaited

pytestmark = pytest.mark.db

_REASON = "same host references, threshold change reviewed against the mock community"
_JOINING = sorted(JOINING_VERDICTS)
_NON_JOINING = sorted(NON_JOINING_VERDICTS)


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
        label="equivalence-mask-only",
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
    """Mints definitions with unique params and removes them, and every equivalence naming
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
        await delete_output_equivalences(
            postgres_pool,
            await postgres_pool.fetchval(
                "SELECT coalesce(array_agg(output_equivalence_idx), '{}')"
                "  FROM qiita.output_equivalence"
                " WHERE definition_kind = $1 AND (low_definition_ref = ANY($2::bigint[])"
                "      OR high_definition_ref = ANY($2::bigint[]))",
                kind,
                idxs,
            ),
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
        URL_ADMIN_OUTPUT_EQUIVALENCE, json={"definition_kind": kind, "definition_idx": [a, b]}
    )


async def _decide(client, equivalence_idx, verdict, reason=_REASON):
    return await client.post(
        URL_ADMIN_OUTPUT_EQUIVALENCE_DECISION.format(output_equivalence_idx=equivalence_idx),
        json={"verdict": verdict, "reason": reason},
    )


async def _revoke(client, equivalence_idx, reason="decided before the mock community was re-run"):
    return await client.post(
        URL_ADMIN_OUTPUT_EQUIVALENCE_REVOKE.format(output_equivalence_idx=equivalence_idx),
        json={"reason": reason},
    )


async def _opened(client, kind, a, b) -> int:
    resp = await _open(client, kind, a, b)
    assert resp.status_code == 201, resp.text
    return resp.json()["output_equivalence_idx"]


def _refusal(resp, reason):
    assert resp.status_code == 409, resp.text
    detail = resp.json()["detail"]
    assert detail["reason"] == reason, detail
    return detail


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


async def _equivalence_of_kind(defs, kind):
    """(low, high, table, id column) for two definitions of `kind` that a joining
    decision accepts."""
    m1 = await defs["mask"](rype=0.05)
    if kind == DefinitionKind.MASK:
        return m1, await defs["mask"](rype=0.1), "qiita.mask_definition", "mask_idx"
    if kind == DefinitionKind.ALIGNMENT:
        low = await defs["block"](m1)
        high = await defs["block"](m1, aligner="minimap2", preset="sr")
        return low, high, "qiita.alignment_definition", "alignment_idx"
    low = await defs["assembly"](m1)
    high = await defs["assembly"](m1, assembler="hifiasm")
    return low, high, "qiita.processing", "processing_idx"


@pytest.mark.parametrize("kind", list(DefinitionKind))
@pytest.mark.parametrize("verdict", [OutputEquivalenceVerdict.PENDING, *_NON_JOINING], ids=str)
async def test_a_hand_delete_detaches_the_equivalence_and_keeps_its_refs(
    postgres_pool, clients, defs, kind, verdict
):
    low, high, table, col = await _equivalence_of_kind(defs, kind)
    equivalence_idx = await _opened(clients["admin"], kind, low, high)
    if verdict != OutputEquivalenceVerdict.PENDING:
        assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 200

    await postgres_pool.execute(f"DELETE FROM {table} WHERE {col} = $1", high)

    row = await postgres_pool.fetchrow(
        f"SELECT low_{kind}_idx AS lo, high_{kind}_idx AS hi, low_definition_ref,"
        " high_definition_ref, detached_at, verdict"
        " FROM qiita.output_equivalence WHERE output_equivalence_idx = $1",
        equivalence_idx,
    )
    assert (row["lo"], row["hi"]) == (low, None)
    assert (row["low_definition_ref"], row["high_definition_ref"]) == (low, high)
    assert row["detached_at"] is not None
    assert row["verdict"] == verdict

    resp = await clients["admin"].get(
        URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX.format(output_equivalence_idx=equivalence_idx)
    )
    assert resp.status_code == 200
    assert resp.json()["detached_at"] is not None
    _refusal(
        await _decide(clients["admin"], equivalence_idx, OutputEquivalenceVerdict.DIFFERENT),
        OutputEquivalenceRefusal.DETACHED,
    )


@pytest.mark.parametrize("kind", list(DefinitionKind))
@pytest.mark.parametrize("verdict", _JOINING, ids=str)
async def test_a_hand_delete_is_refused_while_a_joining_equivalence_names_the_definition(
    postgres_pool, clients, defs, kind, verdict
):
    low, high, table, col = await _equivalence_of_kind(defs, kind)
    equivalence_idx = await _opened(clients["admin"], kind, low, high)
    assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 200

    for idx in (low, high):
        with pytest.raises(asyncpg.ObjectInUseError) as exc:
            await postgres_pool.execute(f"DELETE FROM {table} WHERE {col} = $1", idx)
        assert exc.value.sqlstate == "55006"
        assert f"joining output equivalence(s) {equivalence_idx};" in str(exc.value)
        assert "revoke first" in str(exc.value)
    assert (
        await postgres_pool.fetchval(
            "SELECT detached_at FROM qiita.output_equivalence WHERE output_equivalence_idx = $1",
            equivalence_idx,
        )
        is None
    )

    assert (await _revoke(clients["admin"], equivalence_idx)).status_code == 200
    await postgres_pool.execute(f"DELETE FROM {table} WHERE {col} = $1", high)
    assert (
        await postgres_pool.fetchval(
            "SELECT detached_at FROM qiita.output_equivalence WHERE output_equivalence_idx = $1",
            equivalence_idx,
        )
        is not None
    )


async def test_a_second_delete_keeps_the_first_detach_stamp(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", m1)
    first = await postgres_pool.fetchval(
        "SELECT detached_at FROM qiita.output_equivalence WHERE output_equivalence_idx = $1",
        equivalence_idx,
    )
    await postgres_pool.execute("DELETE FROM qiita.mask_definition WHERE mask_idx = $1", m2)
    row = await postgres_pool.fetchrow(
        "SELECT detached_at, low_mask_idx, high_mask_idx FROM qiita.output_equivalence"
        " WHERE output_equivalence_idx = $1",
        equivalence_idx,
    )
    assert row["detached_at"] == first
    assert (row["low_mask_idx"], row["high_mask_idx"]) == (None, None)


async def test_typed_columns_must_match_the_refs_and_the_kind(postgres_pool, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    insert = (
        "INSERT INTO qiita.output_equivalence (definition_kind, low_mask_idx, high_mask_idx,"
        " low_alignment_idx, low_definition_ref, high_definition_ref, change, derived_only,"
        " opened_by_idx) VALUES ($1, $2, $3, $4, $5, $6, '{}', false, $7)"
    )
    cases = [
        (DefinitionKind.MASK, m1, m2, None, m1, m2 + 1000),  # high ref differs from the FK
        (DefinitionKind.ALIGNMENT, m1, m2, None, m1, m2),  # another kind's columns set
        (DefinitionKind.MASK, m1, None, None, m1, m2),  # a NULL FK on a live row
    ]
    for kind, lo, hi, al, lo_ref, hi_ref in cases:
        with pytest.raises(asyncpg.CheckViolationError, match="output_equivalence_typed_refs"):
            await postgres_pool.execute(insert, kind, lo, hi, al, lo_ref, hi_ref, defs["owner"])
    with pytest.raises(asyncpg.CheckViolationError, match="output_equivalence_ordered"):
        await postgres_pool.execute(
            insert, DefinitionKind.MASK, m2, m1, None, m2, m1, defs["owner"]
        )


async def test_decision_fields_move_with_the_verdict(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    with pytest.raises(asyncpg.CheckViolationError, match="output_equivalence_decision_fields"):
        await postgres_pool.execute(
            "UPDATE qiita.output_equivalence SET verdict = 'similar'"
            " WHERE output_equivalence_idx = $1",
            equivalence_idx,
        )


async def test_the_sql_joining_verdicts_are_the_python_ones(postgres_pool):
    """`qiita.output_equivalence_verdict_joins` is the SQL twin of `JOINING_VERDICTS`:
    the delete trigger, the settings lock, the derived-only CHECK and the repository's
    joining lookup all read it."""
    for verdict in OutputEquivalenceVerdict:
        joins = await postgres_pool.fetchval(
            "SELECT qiita.output_equivalence_verdict_joins($1)", verdict.value
        )
        assert joins is (verdict in JOINING_VERDICTS), verdict


async def test_the_verdict_column_takes_exactly_the_python_verdicts(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    decide = (
        "UPDATE qiita.output_equivalence SET verdict = $2, decided_by_idx = $3,"
        " decided_at = now(), decision_reason = 'x' WHERE output_equivalence_idx = $1"
    )
    for verdict in sorted(JOINING_VERDICTS | NON_JOINING_VERDICTS):
        await postgres_pool.execute(decide, equivalence_idx, verdict.value, defs["owner"])
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(decide, equivalence_idx, "not_similar", defs["owner"])


async def test_history_rows_are_not_updated_or_deleted_and_keep_their_equivalence(
    postgres_pool, clients, defs
):
    """A revoke nulls the equivalence's decision columns, so the history is the record of
    who decided what: its rows are neither updated nor deleted, and the equivalence they
    belong to cannot be deleted from under them."""
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    for statement, op in (
        ("UPDATE qiita.output_equivalence_event SET reason = 'x'", "UPDATE"),
        ("DELETE FROM qiita.output_equivalence_event", "DELETE"),
    ):
        with pytest.raises(asyncpg.RaiseError, match=f"append-only \\({op} refused\\)"):
            await postgres_pool.execute(
                f"{statement} WHERE output_equivalence_idx = $1", equivalence_idx
            )
    with pytest.raises(asyncpg.ForeignKeyViolationError, match="output_equivalence_event"):
        await postgres_pool.execute(
            "DELETE FROM qiita.output_equivalence WHERE output_equivalence_idx = $1",
            equivalence_idx,
        )
    assert (
        await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.output_equivalence_event WHERE output_equivalence_idx = $1",
            equivalence_idx,
        )
        == 1
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


async def test_open_records_the_change_and_an_opened_event(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    resp = await _open(clients["admin"], DefinitionKind.MASK, m2, m1)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert (body["low_definition_idx"], body["high_definition_idx"]) == (m1, m2)
    assert body["change"] == {"/rype": [0.05, 0.1]}
    assert body[OutputEquivalenceRefusal.DERIVED_ONLY] is False
    assert body["verdict"] == OutputEquivalenceVerdict.PENDING
    assert body["opened_by_idx"] == defs["owner"]

    again = await _open(clients["admin"], DefinitionKind.MASK, m1, m2)
    assert again.status_code == 200
    assert again.json()["output_equivalence_idx"] == body["output_equivalence_idx"]

    shown = await clients["admin"].get(
        URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX.format(
            output_equivalence_idx=body["output_equivalence_idx"]
        )
    )
    assert [e["event"] for e in shown.json()["events"]] == [OutputEquivalenceEventType.OPENED]


async def test_open_marks_an_upstream_only_change_derived_only(postgres_pool, clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m2)
    body = (await _open(clients["admin"], DefinitionKind.PROCESSING, p1, p2)).json()
    assert body["change"] == {"/mask_idx": [m1, m2]}
    assert body[OutputEquivalenceRefusal.DERIVED_ONLY] is True
    for verdict in _JOINING:
        _refusal(
            await _decide(clients["admin"], body["output_equivalence_idx"], verdict),
            OutputEquivalenceRefusal.DERIVED_ONLY,
        )
    for verdict in _NON_JOINING:
        decided = await _decide(clients["admin"], body["output_equivalence_idx"], verdict)
        assert decided.status_code == 200, decided.text
    with pytest.raises(asyncpg.CheckViolationError, match="derived_only_not_joining"):
        await postgres_pool.execute(
            "UPDATE qiita.output_equivalence SET verdict = 'same'"
            " WHERE output_equivalence_idx = $1",
            body["output_equivalence_idx"],
        )


async def test_open_refuses_an_unknown_definition_and_a_cross_scope_equivalence(clients, defs):
    m1 = await defs["mask"](rype=0.05)
    other = await defs["mask"](workflow="fastq-to-parquet")
    assert (await _open(clients["admin"], DefinitionKind.MASK, m1, 2**40)).status_code == 404
    _refusal(
        await _open(clients["admin"], DefinitionKind.MASK, m1, other),
        OutputEquivalenceRefusal.CROSS_SCOPE,
    )
    assert (await _open(clients["admin"], DefinitionKind.MASK, m1, m1)).status_code == 422


@pytest.mark.parametrize("verdict", [*_JOINING, *_NON_JOINING], ids=str)
async def test_decide_then_revoke_returns_to_pending_and_keeps_the_history(clients, defs, verdict):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)

    decided = await _decide(clients["admin"], equivalence_idx, verdict)
    assert decided.status_code == 200, decided.text
    assert decided.json()["verdict"] == verdict
    assert decided.json()["decision_reason"] == _REASON
    assert decided.json()["decided_by_idx"] == defs["owner"]

    revoked = await _revoke(clients["admin"], equivalence_idx)
    assert revoked.status_code == 200, revoked.text
    body = revoked.json()
    assert body["verdict"] == OutputEquivalenceVerdict.PENDING
    assert body["decided_by_idx"] is None and body["decision_reason"] is None

    _refusal(await _revoke(clients["admin"], equivalence_idx), OutputEquivalenceRefusal.NOT_DECIDED)

    events = (
        await clients["admin"].get(
            URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX.format(output_equivalence_idx=equivalence_idx)
        )
    ).json()["events"]
    assert [(e["event"], e["verdict"]) for e in events] == [
        (OutputEquivalenceEventType.OPENED, OutputEquivalenceVerdict.PENDING),
        (OutputEquivalenceEventType.DECIDED, verdict),
        (OutputEquivalenceEventType.REVOKED, OutputEquivalenceVerdict.PENDING),
    ]
    assert events[1]["reason"] == _REASON


async def test_a_decision_rejects_pending_an_unknown_verdict_and_a_blank_reason(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    for verdict in (OutputEquivalenceVerdict.PENDING, "not_similar", "equivalent"):
        assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 422
    assert (
        await _decide(
            clients["admin"], equivalence_idx, OutputEquivalenceVerdict.SIMILAR, reason="  "
        )
    ).status_code == 422


async def test_revoking_a_mask_equivalence_a_processing_similar_relies_on_is_refused(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m2, assembler="hifiasm")
    mask_equivalence = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    proc_equivalence = await _opened(clients["admin"], DefinitionKind.PROCESSING, p1, p2)

    _refusal(
        await _decide(clients["admin"], proc_equivalence, OutputEquivalenceVerdict.SIMILAR),
        OutputEquivalenceRefusal.UPSTREAM_NOT_GROUPED,
    )
    assert (
        await _decide(clients["admin"], mask_equivalence, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200
    assert (
        await _decide(clients["admin"], proc_equivalence, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200

    detail = _refusal(
        await _revoke(clients["admin"], mask_equivalence), OutputEquivalenceRefusal.RELIED_ON
    )
    assert detail["output_equivalence_idx"] == [proc_equivalence]
    _refusal(
        await _decide(clients["admin"], mask_equivalence, OutputEquivalenceVerdict.DIFFERENT),
        OutputEquivalenceRefusal.RELIED_ON,
    )

    assert (await _revoke(clients["admin"], proc_equivalence)).status_code == 200
    assert (await _revoke(clients["admin"], mask_equivalence)).status_code == 200


async def test_list_pages_newest_first_and_follows_the_cursor(clients, defs):
    """Pages of `limit`, newest first; the cursor reaches every equivalence, the oldest
    included, and the last page carries none."""
    m1, m2, m3 = [await defs["mask"](rype=r) for r in (0.05, 0.1, 0.2)]
    mine = [
        await _opened(clients["admin"], DefinitionKind.MASK, a, b)
        for a, b in ((m1, m2), (m1, m3), (m2, m3))
    ]
    seen: list[int] = []
    params: dict = {"definition_kind": DefinitionKind.MASK, "limit": 2}
    while True:
        resp = await clients["admin"].get(URL_ADMIN_OUTPUT_EQUIVALENCE, params=params)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert len(body["output_equivalences"]) <= 2 and body["count"] == len(
            body["output_equivalences"]
        )
        seen.extend(p["output_equivalence_idx"] for p in body["output_equivalences"])
        if body["next_after_output_equivalence_idx"] is None:
            break
        assert body["next_after_output_equivalence_idx"] == seen[-1]
        params["after_output_equivalence_idx"] = body["next_after_output_equivalence_idx"]
    assert seen == sorted(seen, reverse=True)
    assert set(mine) <= set(seen)
    over = await clients["admin"].get(URL_ADMIN_OUTPUT_EQUIVALENCE, params={"limit": 1001})
    assert over.status_code == 422, over.text


async def test_list_filters_by_verdict_and_kind(clients, defs):
    m1, m2, m3 = [await defs["mask"](rype=r) for r in (0.05, 0.1, 0.2)]
    a1, a2 = await defs["block"](m1), await defs["block"](m1, shard_ids=(0, 1))
    mask_similar = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    mask_pending = await _opened(clients["admin"], DefinitionKind.MASK, m1, m3)
    alignment_pending = await _opened(clients["admin"], DefinitionKind.ALIGNMENT, a1, a2)
    assert (
        await _decide(clients["admin"], mask_similar, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200

    async def listed(**params):
        resp = await clients["admin"].get(URL_ADMIN_OUTPUT_EQUIVALENCE, params=params)
        assert resp.status_code == 200, resp.text
        return {p["output_equivalence_idx"] for p in resp.json()["output_equivalences"]}

    mine = {mask_similar, mask_pending, alignment_pending}
    assert mine <= await listed()
    assert (await listed(verdict=OutputEquivalenceVerdict.PENDING)) & mine == {
        mask_pending,
        alignment_pending,
    }
    assert (await listed(definition_kind=DefinitionKind.MASK)) & mine == {
        mask_similar,
        mask_pending,
    }
    assert (
        await listed(definition_kind=DefinitionKind.MASK, verdict=OutputEquivalenceVerdict.SIMILAR)
    ) & mine == {mask_similar}


# ---------------------------------------------------------------------------
# Authorization
# ---------------------------------------------------------------------------


async def test_a_regular_user_is_refused_every_route(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    user = clients["user"]
    assert (await user.get(URL_ADMIN_OUTPUT_EQUIVALENCE)).status_code == 403
    assert (await _open(user, DefinitionKind.MASK, m1, m2)).status_code == 403
    assert (
        await user.get(
            URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX.format(output_equivalence_idx=equivalence_idx)
        )
    ).status_code == 403
    assert (
        await _decide(user, equivalence_idx, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 403
    assert (await _revoke(user, equivalence_idx)).status_code == 403


async def test_each_equivalence_is_gated_on_its_own_kinds_lifecycle_scope(clients, defs):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    a1, a2 = await defs["block"](m1), await defs["block"](m1, shard_ids=(0, 1))
    mask_equivalence = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    alignment_equivalence = await _opened(clients["admin"], DefinitionKind.ALIGNMENT, a1, a2)
    narrow = clients["mask_only"]

    listed = await narrow.get(URL_ADMIN_OUTPUT_EQUIVALENCE)
    assert listed.status_code == 200
    kinds = {p["definition_kind"] for p in listed.json()["output_equivalences"]}
    assert kinds == {DefinitionKind.MASK}
    assert (
        await narrow.get(
            URL_ADMIN_OUTPUT_EQUIVALENCE, params={"definition_kind": DefinitionKind.ALIGNMENT}
        )
    ).status_code == 403

    for path in (URL_ADMIN_OUTPUT_EQUIVALENCE_BY_IDX,):
        assert (
            await narrow.get(path.format(output_equivalence_idx=alignment_equivalence))
        ).status_code == 403
        assert (
            await narrow.get(path.format(output_equivalence_idx=mask_equivalence))
        ).status_code == 200
    assert (await _open(narrow, DefinitionKind.ALIGNMENT, a1, a2)).status_code == 403
    assert (
        await _decide(narrow, alignment_equivalence, OutputEquivalenceVerdict.DIFFERENT)
    ).status_code == 403
    assert (
        await _decide(narrow, mask_equivalence, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200
    assert (await _revoke(narrow, mask_equivalence)).status_code == 200


# ---------------------------------------------------------------------------
# Deprecation refused while a joining output equivalence names the definition
# ---------------------------------------------------------------------------


async def _deprecate(client, url):
    return await client.patch(url, json={"status": "deprecated", "reason": "scored wrongly"})


@pytest.mark.parametrize("verdict", _JOINING, ids=str)
async def test_deprecating_a_mask_with_a_joining_equivalence_is_refused(clients, defs, verdict):
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 200

    url = URL_MASK_DEFINITION_STATUS.format(mask_idx=m2)
    detail = _refusal(
        await _deprecate(clients["admin"], url), OutputEquivalenceRefusal.JOINING_EQUIVALENCES
    )
    assert detail["output_equivalence_idx"] == [equivalence_idx]

    assert (
        await _decide(clients["admin"], equivalence_idx, OutputEquivalenceVerdict.DIFFERENT)
    ).status_code == 200
    assert (await _deprecate(clients["admin"], url)).status_code == 200
    # A deprecated side still takes either non-joining verdict, and neither joining one.
    assert (
        await _decide(clients["admin"], equivalence_idx, OutputEquivalenceVerdict.NOT_BENCHMARKED)
    ).status_code == 200
    _refusal(
        await _decide(clients["admin"], equivalence_idx, verdict),
        OutputEquivalenceRefusal.DEPRECATED_SIDE,
    )


@pytest.mark.parametrize("verdict", _JOINING, ids=str)
async def test_deprecating_an_alignment_with_a_joining_equivalence_is_refused(
    clients, defs, verdict
):
    m1 = await defs["mask"](rype=0.05)
    a1, a2 = await defs["block"](m1), await defs["block"](m1, aligner="minimap2", preset="sr")
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.ALIGNMENT, a1, a2)
    assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 200

    url = URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=a1)
    _refusal(await _deprecate(clients["admin"], url), OutputEquivalenceRefusal.JOINING_EQUIVALENCES)
    assert (await _revoke(clients["admin"], equivalence_idx)).status_code == 200
    assert (await _deprecate(clients["admin"], url)).status_code == 200


@pytest.mark.parametrize("verdict", _JOINING, ids=str)
async def test_deprecating_a_processing_with_a_joining_equivalence_is_refused(
    clients, defs, verdict
):
    m1 = await defs["mask"](rype=0.05)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m1, assembler="hifiasm")
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.PROCESSING, p1, p2)
    assert (await _decide(clients["admin"], equivalence_idx, verdict)).status_code == 200

    url = URL_PROCESSING_STATUS.format(processing_idx=p2)
    _refusal(await _deprecate(clients["admin"], url), OutputEquivalenceRefusal.JOINING_EQUIVALENCES)
    assert (await _revoke(clients["admin"], equivalence_idx)).status_code == 200
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
    mask_equivalence = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    assert (
        await _decide(clients["admin"], mask_equivalence, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200
    url = URL_PROCESSING_STATUS.format(processing_idx=p1)
    assert (await _deprecate(clients["admin"], url)).status_code == 200


# ---------------------------------------------------------------------------
# Group lock
# ---------------------------------------------------------------------------


async def test_a_decision_does_not_queue_behind_a_shared_holder(
    postgres_pool, clients, defs, monkeypatch
):
    """A deprecation holding the group lock SHARED makes a decision answer 503 within its
    bounded retry, instead of waiting in the lock queue (where it would block every
    later shared request). Once the deprecation ends, the same decision goes through."""
    monkeypatch.setattr(equivalence_repo, "GROUP_LOCK_EXCLUSIVE_ATTEMPTS", 3)
    monkeypatch.setattr(equivalence_repo, "GROUP_LOCK_EXCLUSIVE_INTERVAL_S", 0.05)
    m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
    equivalence_idx = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)

    async with postgres_pool.acquire() as deprecation, deprecation.transaction():
        await equivalence_repo.lock_groups_shared(deprecation)
        started = time.monotonic()
        resp = await asyncio.wait_for(
            _decide(clients["admin"], equivalence_idx, OutputEquivalenceVerdict.SIMILAR), timeout=10
        )
        elapsed = time.monotonic() - started
        assert resp.status_code == 503, resp.text
        assert resp.headers["Retry-After"]
        assert elapsed < 5
        # Nothing was left waiting in the lock queue.
        assert not await postgres_pool.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
            " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            " AND classid = $1)",
            equivalence_repo.DEFINITION_GROUP_LOCK_CLASS,
        )

    assert (
        await _decide(clients["admin"], equivalence_idx, OutputEquivalenceVerdict.SIMILAR)
    ).status_code == 200


async def _similar_candidates(defs, kind: DefinitionKind) -> tuple[int, int, str]:
    """Two definitions of `kind` that can be decided similar, and the status URL that
    deprecates the second."""
    if kind == DefinitionKind.MASK:
        m1, m2 = await defs["mask"](rype=0.05), await defs["mask"](rype=0.1)
        return m1, m2, URL_MASK_DEFINITION_STATUS.format(mask_idx=m2)
    m1 = await defs["mask"](rype=0.05)
    if kind == DefinitionKind.ALIGNMENT:
        a1 = await defs["block"](m1)
        a2 = await defs["block"](m1, aligner="minimap2", preset="sr")
        return a1, a2, URL_ALIGNMENT_DEFINITION_STATUS.format(alignment_idx=a2)
    p1, p2 = await defs["assembly"](m1), await defs["assembly"](m1, assembler="hifiasm")
    return p1, p2, URL_PROCESSING_STATUS.format(processing_idx=p2)


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_a_deprecation_waits_for_a_decision_in_progress_and_sees_it(
    postgres_pool, clients, defs, kind
):
    """A deprecation PATCH arriving while a `similar` decision on its definition holds
    the group lock EXCLUSIVE waits for it to commit, then refuses with
    `joining_equivalences`. Without its SHARED group lock the PATCH would read no similar
    equivalence (the decision is uncommitted) and deprecate the definition."""
    low, high, url = await _similar_candidates(defs, kind)
    equivalence_idx = await _opened(clients["admin"], kind, low, high)

    async with postgres_pool.acquire() as decider:
        tx = decider.transaction()
        await tx.start()
        try:
            await decide_equivalence(
                decider,
                output_equivalence_idx=equivalence_idx,
                verdict=OutputEquivalenceVerdict.SIMILAR,
                reason="same filter, reviewed",
                principal_idx=defs["owner"],
            )
            patch = asyncio.create_task(_deprecate(clients["admin"], url))
            await asyncio.wait_for(
                until_advisory_lock_awaited(
                    postgres_pool,
                    lock_class=equivalence_repo.DEFINITION_GROUP_LOCK_CLASS,
                    key=equivalence_repo.DEFINITION_GROUP_LOCK_KEY,
                    task=patch,
                ),
                timeout=10,
            )
            assert not patch.done()
        except BaseException:
            await tx.rollback()
            raise
        await tx.commit()

    detail = _refusal(
        await asyncio.wait_for(patch, timeout=10), OutputEquivalenceRefusal.JOINING_EQUIVALENCES
    )
    assert detail["output_equivalence_idx"] == [equivalence_idx]


async def test_the_shared_group_lock_refuses_a_repeatable_read_transaction(postgres_pool):
    """At REPEATABLE READ the lock statement takes the snapshot, so a deprecation
    would not see a decision that it waited for."""
    async with (
        postgres_pool.acquire() as conn,
        conn.transaction(isolation="repeatable_read"),
    ):
        with pytest.raises(RuntimeError, match="READ COMMITTED"):
            await equivalence_repo.lock_groups_shared(conn)


async def test_the_exclusive_group_lock_refuses_a_repeatable_read_transaction(postgres_pool):
    """At REPEATABLE READ the first try-lock fixes the snapshot, so a decision that
    retried while another committed would be checked against the groups before it.
    load_groups refuses the equivalence too, for a lock taken by other means."""
    async with (
        postgres_pool.acquire() as conn,
        conn.transaction(isolation="repeatable_read"),
    ):
        with pytest.raises(RuntimeError, match="READ COMMITTED"):
            await equivalence_repo.lock_groups_exclusive(conn)
    async with (
        postgres_pool.acquire() as conn,
        conn.transaction(isolation="repeatable_read"),
    ):
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1, $2)",
            equivalence_repo.DEFINITION_GROUP_LOCK_CLASS,
            equivalence_repo.DEFINITION_GROUP_LOCK_KEY,
        )
        with pytest.raises(RuntimeError, match="needs a READ COMMITTED"):
            await load_groups(conn)


async def test_a_decision_that_waited_out_another_sees_it_and_is_refused(
    postgres_pool, clients, defs, monkeypatch
):
    """different(M1, M3) stands. Decision A (M1 ~ M2) holds the group lock; decision
    B (M2 ~ M3) retries the try-lock meanwhile. Once A commits, B takes the lock, reads
    A, and is refused: with A, it would put M1 and M3 in one group."""
    monkeypatch.setattr(equivalence_repo, "GROUP_LOCK_EXCLUSIVE_ATTEMPTS", 200)
    monkeypatch.setattr(equivalence_repo, "GROUP_LOCK_EXCLUSIVE_INTERVAL_S", 0.05)
    m1, m2, m3 = [await defs["mask"](rype=r) for r in (0.05, 0.1, 0.2)]
    p13 = await _opened(clients["admin"], DefinitionKind.MASK, m1, m3)
    p12 = await _opened(clients["admin"], DefinitionKind.MASK, m1, m2)
    p23 = await _opened(clients["admin"], DefinitionKind.MASK, m2, m3)
    assert (
        await _decide(clients["admin"], p13, OutputEquivalenceVerdict.DIFFERENT)
    ).status_code == 200

    async with postgres_pool.acquire() as decider:
        tx = decider.transaction()
        await tx.start()
        try:
            await decide_equivalence(
                decider,
                output_equivalence_idx=p12,
                verdict=OutputEquivalenceVerdict.SIMILAR,
                reason="same filter, reviewed",
                principal_idx=defs["owner"],
            )
            second = asyncio.create_task(
                _decide(clients["admin"], p23, OutputEquivalenceVerdict.SIMILAR)
            )
            await asyncio.sleep(0.5)
            assert not second.done()
        except BaseException:
            await tx.rollback()
            raise
        await tx.commit()

    detail = _refusal(
        await asyncio.wait_for(second, timeout=20), OutputEquivalenceRefusal.CONTRADICTION
    )
    assert detail["output_equivalence_idx"] == [p13]


async def test_load_groups_refuses_read_committed_without_the_exclusive_lock(postgres_pool):
    async with postgres_pool.acquire() as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="EXCLUSIVE or a REPEATABLE READ"):
            await load_groups(conn)
    # The shared lock is not enough: it is the deprecations' side.
    async with postgres_pool.acquire() as conn, conn.transaction():
        await equivalence_repo.lock_groups_shared(conn)
        with pytest.raises(RuntimeError, match="EXCLUSIVE or a REPEATABLE READ"):
            await load_groups(conn)
    async with postgres_pool.acquire() as conn, conn.transaction():
        await equivalence_repo.lock_groups_exclusive(conn)
        await load_groups(conn)
