"""DB tests for the settings lock on mask_definition, alignment_definition and
processing, and its one audited path, PATCH /admin/definition-settings/{kind}/{idx}."""

import json
import secrets

import asyncpg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from qiita_common.api_paths import (
    URL_ADMIN_DEFINITION_PAIR,
    URL_ADMIN_DEFINITION_PAIR_DECISION,
    URL_ADMIN_DEFINITION_PAIR_REVOKE,
    URL_ADMIN_DEFINITION_SETTINGS,
)
from qiita_common.hashing import canonical_params_hash
from qiita_common.models import (
    DefinitionKind,
    DefinitionPairRefusal,
    DefinitionPairVerdict,
    DefinitionSettingsRefusal,
)

from qiita_control_plane.repositories.alignment_definition import mint_alignment_definition
from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.repositories.processing import mint_processing
from qiita_control_plane.testing.db_teardown import delete_definition_pairs

pytestmark = pytest.mark.db

_REASON = "the params were minted with a mistyped threshold"
_TABLE = {
    DefinitionKind.MASK: ("qiita.mask_definition", "mask_idx"),
    DefinitionKind.ALIGNMENT: ("qiita.alignment_definition", "alignment_idx"),
    DefinitionKind.PROCESSING: ("qiita.processing", "processing_idx"),
}
# One locked column per table besides params / params_hash, and a value for it.
_OTHER_LOCKED = {
    DefinitionKind.MASK: [("filter_workflow", "'other'"), ("adapter_hash_scheme", "'x'")],
    DefinitionKind.ALIGNMENT: [],
    DefinitionKind.PROCESSING: [("workflow", "'other'"), ("version", "'9.9.9'")],
}


@pytest_asyncio.fixture
async def clients(postgres_pool, human_admin_session, wet_lab_admin_session):
    from qiita_control_plane.main import app

    app.state.pool = postgres_pool
    transport = ASGITransport(app=app)

    def _client(token):
        return AsyncClient(
            transport=transport,
            base_url="http://test",
            headers={"Authorization": f"Bearer {token}"},
        )

    async with (
        _client(human_admin_session["token"]) as admin,
        _client(wet_lab_admin_session["token"]) as wet,
    ):
        yield {"admin": admin, "wet": wet}


@pytest_asyncio.fixture
async def defs(postgres_pool, human_admin_session):
    """`make(kind, **params)` mints a definition with unique params; every definition
    and every pair naming one is removed afterwards."""
    owner = human_admin_session["principal_idx"]
    suffix = secrets.token_hex(4)
    made: dict[DefinitionKind, list[int]] = {k: [] for k in DefinitionKind}

    async def make(kind: DefinitionKind, **extra) -> int:
        async with postgres_pool.acquire() as conn:
            if kind is DefinitionKind.MASK:
                params = {"filter_workflow": "read-mask", "probe": suffix, **extra}
                row = await mint_mask_definition(
                    conn,
                    filter_workflow="read-mask",
                    filter_version="1.0.0",
                    params=params,
                    principal_idx=owner,
                )
                idx = row["mask_idx"]
            elif kind is DefinitionKind.ALIGNMENT:
                params = {
                    "reference_idx": 1,
                    "aligner": "minimap2",
                    "mask_idx": None,
                    "shard_ids": [0],
                    "probe": suffix,
                    **extra,
                }
                row = await mint_alignment_definition(conn, params=params, principal_idx=owner)
                idx = row["alignment_idx"]
            else:
                params = {
                    "workflow": "long-read-assembly",
                    "version": "1.0.0",
                    "assembler": "myloasm",
                    "probe": suffix,
                    **extra,
                }
                row = await mint_processing(
                    conn, workflow="long-read-assembly", version="1.0.0", params=params
                )
                idx = row["processing_idx"]
        made[kind].append(idx)
        return idx

    yield {"make": make, "owner": owner}

    for kind, idxs in made.items():
        await delete_definition_pairs(
            postgres_pool,
            await postgres_pool.fetchval(
                "SELECT coalesce(array_agg(definition_pair_idx), '{}') FROM qiita.definition_pair"
                " WHERE definition_kind = $1 AND (low_definition_ref = ANY($2::bigint[])"
                "      OR high_definition_ref = ANY($2::bigint[]))",
                kind.value,
                idxs,
            ),
        )
        table, col = _TABLE[kind]
        await postgres_pool.execute(f"DELETE FROM {table} WHERE {col} = ANY($1::bigint[])", idxs)


async def _params(pool, kind, idx) -> dict:
    table, col = _TABLE[kind]
    return json.loads(await pool.fetchval(f"SELECT params FROM {table} WHERE {col} = $1", idx))


def _url(kind, idx) -> str:
    return URL_ADMIN_DEFINITION_SETTINGS.format(definition_kind=kind.value, definition_idx=idx)


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_a_plain_update_of_a_locked_column_is_refused(postgres_pool, defs, kind):
    idx = await defs["make"](kind, threshold=1)
    table, col = _TABLE[kind]
    assignments = [
        ("params", "params || '{\"threshold\": 2}'::jsonb"),
        ("params_hash", "sha256('other'::bytea)"),
        *_OTHER_LOCKED[kind],
    ]
    for column, value in assignments:
        with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError) as exc:
            await postgres_pool.execute(
                f"UPDATE {table} SET {column} = {value} WHERE {col} = $1", idx
            )
        assert "locked after its mint" in str(exc.value)


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_lifecycle_columns_stay_editable(postgres_pool, defs, kind):
    idx = await defs["make"](kind)
    table, col = _TABLE[kind]
    await postgres_pool.execute(
        f"UPDATE {table} SET status = 'deprecated', deprecated_at = now(),"
        f" deprecated_by_idx = $2, deprecation_reason = 'superseded' WHERE {col} = $1",
        idx,
        defs["owner"],
    )
    await postgres_pool.execute(
        f"UPDATE {table} SET status = 'active', deprecated_at = NULL,"
        f" deprecated_by_idx = NULL, deprecation_reason = NULL WHERE {col} = $1",
        idx,
    )


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_the_admin_edit_changes_the_settings_and_records_them(
    postgres_pool, clients, defs, kind
):
    idx = await defs["make"](kind, threshold=1)
    new_params = {**await _params(postgres_pool, kind, idx), "threshold": 2}

    resp = await clients["admin"].patch(
        _url(kind, idx), json={"params": new_params, "reason": _REASON}
    )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["actor_principal_idx"] == defs["owner"]
    assert body["reason"] == _REASON
    assert body["old_settings"]["params"]["threshold"] == 1
    assert body["new_settings"]["params"]["threshold"] == 2
    assert body["new_settings"]["params_hash"] == "\\x" + canonical_params_hash(new_params).hex()
    table, col = _TABLE[kind]
    row = await postgres_pool.fetchrow(f"SELECT params_hash FROM {table} WHERE {col} = $1", idx)
    assert bytes(row["params_hash"]) == canonical_params_hash(new_params)
    assert (await _params(postgres_pool, kind, idx))["threshold"] == 2
    audit = await postgres_pool.fetchrow(
        "SELECT * FROM qiita.definition_settings_edit WHERE definition_settings_edit_idx = $1",
        body["definition_settings_edit_idx"],
    )
    assert (audit["definition_kind"], audit["definition_ref"], audit["edit_kind"]) == (
        kind.value,
        idx,
        "admin_edit",
    )
    with pytest.raises(asyncpg.RaiseError):
        await postgres_pool.execute(
            "DELETE FROM qiita.definition_settings_edit WHERE definition_settings_edit_idx = $1",
            audit["definition_settings_edit_idx"],
        )


async def test_a_non_admin_is_refused_by_the_route_and_by_the_database(
    postgres_pool, clients, defs, wet_lab_admin_session
):
    idx = await defs["make"](DefinitionKind.MASK, threshold=1)
    new_params = {**await _params(postgres_pool, DefinitionKind.MASK, idx), "threshold": 2}

    resp = await clients["wet"].patch(
        _url(DefinitionKind.MASK, idx), json={"params": new_params, "reason": _REASON}
    )
    assert resp.status_code == 403, resp.text

    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await postgres_pool.execute(
            "SELECT qiita.edit_definition_settings('mask', $1, $2::jsonb, $3, $4)",
            idx,
            '{"filter_version": "2.0.0"}',
            wet_lab_admin_session["principal_idx"],
            _REASON,
        )


async def test_a_blank_reason_is_refused(clients, defs, postgres_pool):
    idx = await defs["make"](DefinitionKind.MASK, threshold=1)
    params = await _params(postgres_pool, DefinitionKind.MASK, idx)
    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, idx), json={"params": params, "reason": "  "}
    )
    assert resp.status_code == 422


async def test_an_edit_onto_another_definitions_params_is_refused(clients, defs, postgres_pool):
    a = await defs["make"](DefinitionKind.MASK, threshold=1)
    b = await defs["make"](DefinitionKind.MASK, threshold=2)
    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, a),
        json={"params": await _params(postgres_pool, DefinitionKind.MASK, b), "reason": _REASON},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["reason"] == DefinitionSettingsRefusal.PARAMS_TAKEN


@pytest.mark.parametrize(
    ("kind", "columns"),
    [
        (DefinitionKind.MASK, {"workflow": "other-mask"}),
        (DefinitionKind.PROCESSING, {"version": "1.1.0"}),
        (DefinitionKind.ALIGNMENT, {"version": "1.1.0"}),
    ],
)
async def test_a_column_that_disagrees_with_params_is_refused(
    clients, defs, postgres_pool, kind, columns
):
    """A mask's filter_workflow / filter_version and a processing's workflow / version
    are written with the same-named params keys at the mint, so an edit that would set
    the column to a value other than the params key is refused; an alignment has no
    such columns."""
    idx = await defs["make"](kind, threshold=1)
    params = {**await _params(postgres_pool, kind, idx), "threshold": 2}
    resp = await clients["admin"].patch(
        _url(kind, idx), json={"params": params, "reason": _REASON, **columns}
    )
    assert resp.status_code == 422, resp.text
    assert (await _params(postgres_pool, kind, idx))["threshold"] == 1


async def test_a_processing_version_edit_with_matching_params_is_accepted(
    clients, defs, postgres_pool
):
    idx = await defs["make"](DefinitionKind.PROCESSING, threshold=1)
    params = {**await _params(postgres_pool, DefinitionKind.PROCESSING, idx), "version": "1.1.0"}
    resp = await clients["admin"].patch(
        _url(DefinitionKind.PROCESSING, idx),
        json={"params": params, "reason": _REASON, "version": "1.1.0"},
    )
    assert resp.status_code == 200, resp.text
    assert (
        await postgres_pool.fetchval(
            "SELECT version FROM qiita.processing WHERE processing_idx = $1", idx
        )
        == "1.1.0"
    )


async def test_a_params_hash_taken_after_the_check_is_params_taken(
    clients, defs, postgres_pool, monkeypatch
):
    """A mint of the same params that lands between the check and the UPDATE hits the
    params_hash UNIQUE constraint; the check is skipped here to stand in for that
    mint, and the violation is refused as `params_taken`, not a 500."""
    from qiita_control_plane import definition_settings

    async def _missed(*_args):
        return None

    monkeypatch.setattr(definition_settings, "_params_taken_by", _missed)
    a = await defs["make"](DefinitionKind.MASK, threshold=1)
    b = await defs["make"](DefinitionKind.MASK, threshold=2)
    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, a),
        json={"params": await _params(postgres_pool, DefinitionKind.MASK, b), "reason": _REASON},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["reason"] == DefinitionSettingsRefusal.PARAMS_TAKEN


async def test_an_edit_that_changes_nothing_is_refused(clients, defs, postgres_pool):
    a = await defs["make"](DefinitionKind.MASK, threshold=1)
    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, a),
        json={"params": await _params(postgres_pool, DefinitionKind.MASK, a), "reason": _REASON},
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["reason"] == DefinitionSettingsRefusal.NO_CHANGE


async def test_an_edit_is_refused_while_a_similar_pair_names_it_and_recomputes_others(
    postgres_pool, clients, defs
):
    a = await defs["make"](DefinitionKind.MASK, threshold=1)
    b = await defs["make"](DefinitionKind.MASK, threshold=2)
    opened = await clients["admin"].post(
        URL_ADMIN_DEFINITION_PAIR, json={"definition_kind": "mask", "definition_idx": [a, b]}
    )
    pair_idx = opened.json()["definition_pair_idx"]
    decided = await clients["admin"].post(
        URL_ADMIN_DEFINITION_PAIR_DECISION.format(definition_pair_idx=pair_idx),
        json={"verdict": DefinitionPairVerdict.SIMILAR, "reason": _REASON},
    )
    assert decided.status_code == 200, decided.text
    new_params = {**await _params(postgres_pool, DefinitionKind.MASK, a), "threshold": 3}

    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, a), json={"params": new_params, "reason": _REASON}
    )
    assert resp.status_code == 409, resp.text
    assert resp.json()["detail"]["reason"] == DefinitionPairRefusal.SIMILAR_PAIRS
    assert resp.json()["detail"]["definition_pair_idx"] == [pair_idx]
    # The database refuses it too, even with a matching record.
    with pytest.raises(asyncpg.ObjectInUseError):
        await postgres_pool.execute(
            "SELECT qiita.edit_definition_settings('mask', $1, $2::jsonb, $3, $4)",
            a,
            '{"filter_version": "2.0.0"}',
            defs["owner"],
            _REASON,
        )

    revoked = await clients["admin"].post(
        URL_ADMIN_DEFINITION_PAIR_REVOKE.format(definition_pair_idx=pair_idx),
        json={"reason": _REASON},
    )
    assert revoked.status_code == 200, revoked.text
    resp = await clients["admin"].patch(
        _url(DefinitionKind.MASK, a), json={"params": new_params, "reason": _REASON}
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["repaired_pair_idx"] == [pair_idx]
    change = await postgres_pool.fetchval(
        "SELECT change FROM qiita.definition_pair WHERE definition_pair_idx = $1", pair_idx
    )
    assert json.loads(change) == {"/threshold": [3, 2]}


async def test_the_adapter_hash_rekey_passes_and_is_recorded(postgres_pool, defs):
    """The mint's legacy path and the backfill re-key a mask in place; the lock lets
    exactly that transition through and records it with no actor."""
    idx = await defs["make"](DefinitionKind.MASK, threshold=1)
    await postgres_pool.execute(
        "UPDATE qiita.mask_definition"
        "   SET params = params || '{\"rekeyed\": true}'::jsonb,"
        "       params_hash = sha256(('rekey' || mask_idx)::bytea),"
        "       adapter_hash_scheme = 'sequence_hash_v1'"
        " WHERE mask_idx = $1",
        idx,
    )
    row = await postgres_pool.fetchrow(
        "SELECT edit_kind, actor_principal_idx FROM qiita.definition_settings_edit"
        " WHERE definition_kind = 'mask' AND definition_ref = $1",
        idx,
    )
    assert (row["edit_kind"], row["actor_principal_idx"]) == ("adapter_hash_rekey", None)


_TICKET_ACTION = "settings-edit-live-work-probe"
_TICKET_VERSION = "0.0.0-settings-edit"


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_an_edit_is_refused_while_work_runs_under_the_definition(
    postgres_pool, clients, defs, kind
):
    """A pending ticket under the definition refuses the edit (`live_work`); once it
    is terminal the edit goes through."""
    from qiita_common.models import ScopeTargetKind, WorkTicketState

    from qiita_control_plane.repositories.block import create_block
    from qiita_control_plane.testing.db_seeds import (
        delete_action_if_created,
        seed_action_if_absent,
    )

    idx = await defs["make"](kind, threshold=1)
    created = await seed_action_if_absent(
        postgres_pool,
        action_id=_TICKET_ACTION,
        version=_TICKET_VERSION,
        target_kind=ScopeTargetKind.BLOCK.value,
    )
    async with postgres_pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
    _, col = _TABLE[kind]
    ticket = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version, originator_principal_idx,"
        "  scope_target_kind, block_idx, state, " + col + ")"
        " VALUES ($1, $2, $3, 'block', $4, $5::qiita.work_ticket_state, $6)"
        " RETURNING work_ticket_idx",
        _TICKET_ACTION,
        _TICKET_VERSION,
        defs["owner"],
        block_idx,
        WorkTicketState.PENDING.value,
        idx,
    )
    try:
        new_params = {**await _params(postgres_pool, kind, idx), "threshold": 2}
        body = {"params": new_params, "reason": _REASON}
        resp = await clients["admin"].patch(_url(kind, idx), json=body)
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert detail["reason"] == DefinitionSettingsRefusal.LIVE_WORK
        assert str(ticket) in detail["detail"]
        assert (await _params(postgres_pool, kind, idx))["threshold"] == 1

        await postgres_pool.execute(
            "UPDATE qiita.work_ticket SET state = 'cancelled' WHERE work_ticket_idx = $1",
            ticket,
        )
        resp = await clients["admin"].patch(_url(kind, idx), json=body)
        assert resp.status_code == 200, resp.text
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        await postgres_pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", block_idx)
        await delete_action_if_created(
            postgres_pool, action_id=_TICKET_ACTION, version=_TICKET_VERSION, created=created
        )


@pytest.mark.parametrize("kind", list(DefinitionKind))
async def test_an_edit_waits_for_a_ticket_write_holding_the_definition_and_then_sees_it(
    postgres_pool, clients, defs, kind
):
    """A transaction that writes a ticket under the definition blocks the edit until it
    commits; the edit then sees the ticket and refuses (`live_work`). The write holds
    no explicit lock: setting the column's FK takes a key-share lock on the definition
    row, which conflicts with the edit's `FOR UPDATE`. That lock is what every ticket
    writer's recording relies on."""
    import asyncio

    from qiita_common.models import ScopeTargetKind, WorkTicketState

    from qiita_control_plane.repositories.block import create_block
    from qiita_control_plane.testing.db_seeds import (
        delete_action_if_created,
        seed_action_if_absent,
    )

    idx = await defs["make"](kind, threshold=1)
    created = await seed_action_if_absent(
        postgres_pool,
        action_id=_TICKET_ACTION,
        version=_TICKET_VERSION,
        target_kind=ScopeTargetKind.BLOCK.value,
    )
    async with postgres_pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
    _, col = _TABLE[kind]
    ticket = None
    try:
        body = {
            "params": {**await _params(postgres_pool, kind, idx), "threshold": 2},
            "reason": _REASON,
        }
        writer = await postgres_pool.acquire()
        try:
            tx = writer.transaction()
            await tx.start()
            ticket = await writer.fetchval(
                "INSERT INTO qiita.work_ticket (action_id, action_version,"
                "  originator_principal_idx, scope_target_kind, block_idx, state, " + col + ")"
                " VALUES ($1, $2, $3, 'block', $4, $5::qiita.work_ticket_state, $6)"
                " RETURNING work_ticket_idx",
                _TICKET_ACTION,
                _TICKET_VERSION,
                defs["owner"],
                block_idx,
                WorkTicketState.PENDING.value,
                idx,
            )
            edit = asyncio.create_task(clients["admin"].patch(_url(kind, idx), json=body))
            await asyncio.sleep(0.5)
            assert not edit.done()
            await tx.commit()
        finally:
            await postgres_pool.release(writer)
        resp = await edit
        assert resp.status_code == 409, resp.text
        assert resp.json()["detail"]["reason"] == DefinitionSettingsRefusal.LIVE_WORK
        assert (await _params(postgres_pool, kind, idx))["threshold"] == 1
    finally:
        if ticket is not None:
            await postgres_pool.execute(
                "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
            )
        await postgres_pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", block_idx)
        await delete_action_if_created(
            postgres_pool, action_id=_TICKET_ACTION, version=_TICKET_VERSION, created=created
        )


async def test_an_edit_between_a_processing_mint_and_its_hold_re_mints(
    postgres_pool, clients, monkeypatch
):
    """An edit that commits between the runner's mint of a processing and its hold of
    the row re-keys the row the mint resolved. The runner re-mints, so the ticket is
    recorded under a processing whose settings are the ones it hashed, and the edited
    row keeps the edit."""
    from qiita_common.models import ScopeTargetKind, WorkTicketState

    from qiita_control_plane.repositories.block import create_block
    from qiita_control_plane.runner import _processing
    from qiita_control_plane.testing.db_seeds import (
        delete_action_if_created,
        seed_action_if_absent,
    )

    bound = {"mask_idx": secrets.randbelow(10**9) + 10**9}

    async def mint(work_ticket_idx=None):
        return (
            await _processing._mint_processing_idx(
                postgres_pool,
                action_id="long-read-assembly",
                action_version="1.0.0",
                bound=bound,
                assembler_default="hifiasm_meta",
                work_ticket_idx=work_ticket_idx,
            )
        )["processing_idx"]

    edited = await mint()
    minted = [edited]
    created = await seed_action_if_absent(
        postgres_pool,
        action_id=_TICKET_ACTION,
        version=_TICKET_VERSION,
        target_kind=ScopeTargetKind.BLOCK.value,
    )
    async with postgres_pool.acquire() as conn, conn.transaction():
        block_idx = await create_block(conn)
    ticket = await postgres_pool.fetchval(
        "INSERT INTO qiita.work_ticket (action_id, action_version,"
        "  originator_principal_idx, scope_target_kind, block_idx, state)"
        " VALUES ($1, $2, (SELECT MIN(idx) FROM qiita.principal), 'block', $3,"
        "         $4::qiita.work_ticket_state)"
        " RETURNING work_ticket_idx",
        _TICKET_ACTION,
        _TICKET_VERSION,
        block_idx,
        WorkTicketState.COMPLETED.value,
    )
    params = await _params(postgres_pool, DefinitionKind.PROCESSING, edited)
    real_mint = _processing.mint_processing
    edits = []

    async def mint_then_edit(conn, **kw):
        row = await real_mint(conn, **kw)
        if not edits:
            resp = await clients["admin"].patch(
                _url(DefinitionKind.PROCESSING, edited),
                json={"params": {**params, "assembler": "edited"}, "reason": _REASON},
            )
            edits.append(resp)
        return row

    monkeypatch.setattr(_processing, "mint_processing", mint_then_edit)
    try:
        recorded = await mint(work_ticket_idx=ticket)
        minted.append(recorded)
        assert edits[0].status_code == 200, edits[0].text
        assert recorded != edited
        assert (
            await postgres_pool.fetchval(
                "SELECT processing_idx FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
            )
            == recorded
        )
        assert await _params(postgres_pool, DefinitionKind.PROCESSING, recorded) == params
        assert (await _params(postgres_pool, DefinitionKind.PROCESSING, edited))[
            "assembler"
        ] == "edited"
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.work_ticket WHERE work_ticket_idx = $1", ticket
        )
        await postgres_pool.execute("DELETE FROM qiita.block WHERE block_idx = $1", block_idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.processing WHERE processing_idx = ANY($1::bigint[])", minted
        )
        await delete_action_if_created(
            postgres_pool, action_id=_TICKET_ACTION, version=_TICKET_VERSION, created=created
        )


@pytest.mark.parametrize("kind", [DefinitionKind.MASK, DefinitionKind.ALIGNMENT])
async def test_an_edit_is_refused_while_a_public_handle_cites_the_definition(
    postgres_pool, clients, defs, kind
):
    """A live `QP<n>` naming the definition refuses the edit (`published`); once the
    handle is retired the edit goes through."""
    idx = await defs["make"](kind, threshold=1)
    _, col = _TABLE[kind]
    handle = await postgres_pool.fetchval(
        f"INSERT INTO qiita.exported_processing ({col}, created_by_idx) VALUES ($1, $2)"
        " RETURNING idx",
        idx,
        defs["owner"],
    )
    try:
        new_params = {**await _params(postgres_pool, kind, idx), "threshold": 2}
        body = {"params": new_params, "reason": _REASON}
        resp = await clients["admin"].patch(_url(kind, idx), json=body)
        assert resp.status_code == 409, resp.text
        detail = resp.json()["detail"]
        assert detail["reason"] == DefinitionSettingsRefusal.PUBLISHED
        assert "exported_processing" in detail["detail"]

        await postgres_pool.execute(
            "UPDATE qiita.exported_processing SET retired = true, retired_at = now(),"
            " retire_reason = 'withdrawn in a test' WHERE idx = $1",
            handle,
        )
        resp = await clients["admin"].patch(_url(kind, idx), json=body)
        assert resp.status_code == 200, resp.text
    finally:
        await postgres_pool.execute("DELETE FROM qiita.exported_processing WHERE idx = $1", handle)
