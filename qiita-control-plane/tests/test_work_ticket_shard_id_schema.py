"""Schema + behavioral invariants for `qiita.work_ticket.shard_id`.

`shard_id` is the fan-out discriminant that lets N concurrent same-action
build tickets coexist for one reference (one ticket per analysis-index shard),
without weakening the one-in-flight guarantee for every existing action.

Locks in:

* the `shard_id` column (nullable INTEGER) + its CHECK — a non-NULL shard_id
  is only legal on a `reference`-scoped ticket, and must be >= 0.
* the re-partitioned `work_ticket_one_in_flight_per_reference` (now
  `... AND shard_id IS NULL`) — the exact one-per-reference guarantee is
  preserved for `shard_id NULL` tickets (every existing action).
* the new `work_ticket_one_in_flight_per_shard` — at most one non-terminal
  ticket per (action, reference, shard).

The behavioral tests INSERT real tickets to prove the two partial-unique
indexes gate correctly: two shard-distinct tickets coexist, two same-shard
tickets collide (23505), and two unsharded tickets still collide.

These fail until 20260710000000_work_ticket_shard_id.sql lands.
"""

import secrets

import asyncpg
import pytest

from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


async def _columns(postgres_pool, relname: str) -> dict[str, tuple[str, bool]]:
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type,"
        "       a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND c.relname = $1"
        "   AND a.attnum > 0"
        "   AND NOT a.attisdropped"
        " ORDER BY a.attnum",
        relname,
    )
    return {r["attname"]: (r["type"], r["attnotnull"]) for r in rows}


async def _index_def(postgres_pool, indexname: str) -> str | None:
    return await postgres_pool.fetchval(
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'qiita' AND tablename = 'work_ticket'"
        "   AND indexname = $1",
        indexname,
    )


# ---------------------------------------------------------------------------
# Catalog-level shape
# ---------------------------------------------------------------------------


async def test_work_ticket_shard_id_column(postgres_pool):
    cols = await _columns(postgres_pool, "work_ticket")
    assert "shard_id" in cols, "qiita.work_ticket.shard_id is missing"
    # Nullable INTEGER — only sharded reference build tickets carry a value.
    assert cols["shard_id"] == ("integer", False)


async def test_work_ticket_shard_id_check(postgres_pool):
    """The CHECK ties a non-NULL shard_id to reference scope + non-negativity."""
    defs = await postgres_pool.fetch(
        "SELECT pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'work_ticket'",
    )
    joined = " ".join(r["def"] for r in defs)
    assert "shard_id" in joined, f"no CHECK references shard_id: {joined}"
    assert "shard_id >= 0" in joined, joined
    assert "'reference'" in joined, joined


async def test_one_in_flight_per_reference_now_excludes_sharded(postgres_pool):
    """The pre-existing per-reference index must gain `AND shard_id IS NULL`,
    so it no longer applies to sharded build tickets (which fan out N-wide)."""
    idxdef = await _index_def(postgres_pool, "work_ticket_one_in_flight_per_reference")
    assert idxdef is not None, "work_ticket_one_in_flight_per_reference is missing"
    assert "shard_id IS NULL" in idxdef, idxdef


async def test_one_in_flight_per_shard_index(postgres_pool):
    idxdef = await _index_def(postgres_pool, "work_ticket_one_in_flight_per_shard")
    assert idxdef is not None, "work_ticket_one_in_flight_per_shard is missing"
    assert "UNIQUE" in idxdef, idxdef
    for col in ("action_id", "action_version", "reference_idx", "shard_id"):
        assert col in idxdef, f"{col} missing from per-shard index: {idxdef}"
    assert "shard_id IS NOT NULL" in idxdef, idxdef


# ---------------------------------------------------------------------------
# Behavioral: the indexes actually gate INSERTs
# ---------------------------------------------------------------------------


@pytest.fixture
async def scaffold(postgres_pool):
    """Seed a principal, a reference, and a reference-scoped action. Yields the
    ids + a `ticket(shard_id=...)` INSERT helper; tears down after."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="wt-shard", suffix=suffix)
    reference_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ($1, '1', 'sequence_reference', $2) RETURNING reference_idx",
        f"wt-shard-ref-{suffix}",
        principal_idx,
    )
    study_idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
        " VALUES ($1, $2, $1) RETURNING idx",
        principal_idx,
        f"wt-shard-study-{suffix}",
    )
    action_id = f"wt-shard-act-{suffix}"
    version = "1.0.0"
    await postgres_pool.execute(
        "INSERT INTO qiita.action"
        " (action_id, version, target_kind, scopes, audience, context_schema, steps,"
        "  cpu_ceiling, mem_ceiling_gb, walltime_ceiling, success_status, failure_status)"
        " VALUES ($1, $2, 'reference', '{}'::text[], $3::jsonb, '{}'::jsonb, '[]'::jsonb,"
        "         1, 1, '1 minute', 'active', 'failed')",
        action_id,
        version,
        '{"service": false, "human_roles": ["system_admin"]}',
    )

    async def ticket(*, shard_id, state="pending"):
        return await postgres_pool.fetchval(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  reference_idx, shard_id, state)"
            " VALUES ($1, $2, $3, 'reference', $4, $5, $6) RETURNING work_ticket_idx",
            action_id,
            version,
            principal_idx,
            reference_idx,
            shard_id,
            state,
        )

    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "reference_idx": reference_idx,
        "study_idx": study_idx,
        "action_id": action_id,
        "version": version,
        "ticket": ticket,
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.work_ticket WHERE action_id = $1 AND action_version = $2",
        action_id,
        version,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.action WHERE action_id = $1 AND version = $2", action_id, version
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.reference WHERE reference_idx = $1", reference_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.study WHERE idx = $1", study_idx)
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def test_distinct_shards_coexist_in_flight(scaffold):
    """Two in-flight tickets for the same (action, reference) with different
    shard_ids are the whole point of the fan-out — they must NOT collide."""
    a = await scaffold["ticket"](shard_id=0)
    b = await scaffold["ticket"](shard_id=1)
    assert a != b


async def test_same_shard_collides(scaffold):
    """Two in-flight tickets for the same (action, reference, shard) collide —
    the per-shard disallow-without-delete backstop."""
    await scaffold["ticket"](shard_id=0)
    with pytest.raises(asyncpg.UniqueViolationError):
        await scaffold["ticket"](shard_id=0)


async def test_terminal_shard_ticket_frees_slot(scaffold):
    """A COMPLETED shard ticket no longer occupies the in-flight slot, so a
    redrive for the same shard can be inserted."""
    await scaffold["ticket"](shard_id=0, state="completed")
    # Not in ('pending','queued','processing') → partial index doesn't apply.
    again = await scaffold["ticket"](shard_id=0)
    assert again > 0


async def test_unsharded_reference_tickets_still_collide(scaffold):
    """The per-reference guarantee is intact for shard_id NULL tickets: two
    unsharded in-flight tickets for one (action, reference) still collide."""
    await scaffold["ticket"](shard_id=None)
    with pytest.raises(asyncpg.UniqueViolationError):
        await scaffold["ticket"](shard_id=None)


async def test_unsharded_coexists_with_sharded(scaffold):
    """An unsharded ticket and a sharded ticket for the same (action,
    reference) live in different indexes — they do not collide."""
    unsharded = await scaffold["ticket"](shard_id=None)
    sharded = await scaffold["ticket"](shard_id=0)
    assert unsharded != sharded


async def test_shard_ticket_round_trips_through_model(scaffold):
    """The route SELECT + row-shaper + WorkTicket model carry shard_id end to
    end (a value on a sharded ticket, None on an unsharded one) — proving the
    _WORK_TICKET_COLUMNS / model plumbing added alongside the migration."""
    from qiita_control_plane.routes import work_ticket as wt_routes

    async def fetch_model(wt_idx):
        row = await scaffold["pool"].fetchrow(
            f"SELECT {wt_routes._WORK_TICKET_COLUMNS}{wt_routes._WORK_TICKET_FROM}"
            " WHERE wt.work_ticket_idx = $1",
            wt_idx,
        )
        return wt_routes._row_to_work_ticket(row)

    sharded = await fetch_model(await scaffold["ticket"](shard_id=3))
    assert sharded.shard_id == 3
    assert sharded.scope_target.reference_idx == scaffold["reference_idx"]

    unsharded = await fetch_model(await scaffold["ticket"](shard_id=None))
    assert unsharded.shard_id is None


async def test_check_rejects_negative_shard_id(scaffold):
    """shard_id must be >= 0 (a valid reference scope isolates the >= 0 clause)."""
    with pytest.raises(asyncpg.CheckViolationError):
        await scaffold["ticket"](shard_id=-1)


async def test_check_rejects_shard_id_on_non_reference_scope(scaffold):
    """A non-NULL shard_id is only legal on reference scope. This ticket is an
    otherwise-valid study_prep ticket — only the shard_id trips the CHECK, so
    the failure isolates the shard_id constraint (not scope-consistency)."""
    with pytest.raises(asyncpg.CheckViolationError):
        await scaffold["pool"].execute(
            "INSERT INTO qiita.work_ticket"
            " (action_id, action_version, originator_principal_idx, scope_target_kind,"
            "  study_idx, prep_idx, shard_id)"
            " VALUES ($1, $2, $3, 'study_prep', $4, 1, 0)",
            scaffold["action_id"],
            scaffold["version"],
            scaffold["principal_idx"],
            scaffold["study_idx"],
        )
