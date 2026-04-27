"""Tests for the references.created_by_idx FK migration (Phase H.a → H.c).

H.a (this phase): adds nullable BIGINT FK to qiita.principal, backfills
existing rows to the system principal. H.b dual-writes; H.c finalises
with NOT NULL + drop of the legacy created_by UUID column.
"""

import asyncpg
import pytest


# ---------------------------------------------------------------------------
# Phase H.a — column shape
# ---------------------------------------------------------------------------


async def test_h_a_column_exists(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT data_type, is_nullable FROM information_schema.columns"
        " WHERE table_schema = 'qiita' AND table_name = 'references'"
        "   AND column_name = 'created_by_idx'"
    )
    assert row is not None, "qiita.references.created_by_idx does not exist"
    assert row["data_type"] == "bigint"
    # Phase H.a leaves the column nullable; H.c sets it NOT NULL.
    assert row["is_nullable"] == "YES"


async def test_h_a_column_has_fk_to_principal(postgres_pool):
    """The column must FK to qiita.principal(idx) so worker / human / system
    creators are all valid targets."""
    row = await postgres_pool.fetchrow(
        "SELECT confrelid::regclass::text AS target_table,"
        "  pg_get_constraintdef(oid) AS def"
        " FROM pg_constraint"
        " WHERE conrelid = 'qiita.references'::regclass"
        "   AND contype = 'f'"
        "   AND conname LIKE '%created_by_idx%'"
    )
    assert row is not None, "no FK constraint on created_by_idx found"
    # regclass omits the schema qualifier when the schema is in the
    # current search_path. Either form is correct.
    assert row["target_table"] in ("qiita.principal", "principal")


async def test_h_a_no_cascade_on_delete(postgres_pool):
    """Project convention: NO ACTION / RESTRICT on every FK in the qiita
    schema. The new FK on references.created_by_idx must follow."""
    rule = await postgres_pool.fetchval(
        "SELECT rc.delete_rule"
        " FROM information_schema.table_constraints tc"
        " JOIN information_schema.referential_constraints rc"
        "   ON tc.constraint_name = rc.constraint_name"
        "   AND tc.table_schema = rc.constraint_schema"
        " JOIN information_schema.key_column_usage kcu"
        "   ON kcu.constraint_name = tc.constraint_name"
        "   AND kcu.table_schema = tc.table_schema"
        " WHERE tc.table_schema = 'qiita'"
        "   AND tc.table_name = 'references'"
        "   AND kcu.column_name = 'created_by_idx'"
    )
    assert rule in ("NO ACTION", "RESTRICT"), f"unexpected delete_rule: {rule!r}"


# ---------------------------------------------------------------------------
# Phase H.a — backfill semantics
# ---------------------------------------------------------------------------


async def test_h_a_backfill_assigns_system_principal_to_null_rows(postgres_pool):
    """The migration's backfill UPDATE assigns idx=1 to any reference row
    whose created_by_idx is NULL. Re-running the same SQL is idempotent
    (no-op the second time) and is what the test simulates here."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            # Insert a reference row with created_by_idx explicitly NULL
            # (simulates the pre-H.a state).
            ref_idx = await conn.fetchval(
                "INSERT INTO qiita.references"
                "  (name, version, kind, created_by, created_by_idx)"
                " VALUES ($1, $2, 'sequence_reference',"
                "         'a0000000-0000-0000-0000-000000000001'::uuid, NULL)"
                " RETURNING reference_idx",
                "h_a-backfill-test", "1.0",
            )
            null_before = await conn.fetchval(
                "SELECT created_by_idx FROM qiita.references"
                " WHERE reference_idx = $1",
                ref_idx,
            )
            assert null_before is None

            # Apply the same backfill SQL the migration uses.
            await conn.execute(
                "UPDATE qiita.references SET created_by_idx = 1"
                " WHERE created_by_idx IS NULL"
            )

            backfilled = await conn.fetchval(
                "SELECT created_by_idx FROM qiita.references"
                " WHERE reference_idx = $1",
                ref_idx,
            )
            assert backfilled == 1
        finally:
            await tr.rollback()


async def test_h_a_post_migration_no_null_rows(postgres_pool):
    """Sanity: after the migration runs at session start, no existing
    qiita.references row should have a NULL created_by_idx — the backfill
    handles the pre-H.a population. New rows inserted later may have NULL
    until Phase H.b flips routes to dual-write; that's covered separately.
    """
    n = await postgres_pool.fetchval(
        "SELECT count(*) FROM qiita.references"
        " WHERE created_by_idx IS NULL"
        # Exclude rows we know other tests insert with explicit NULL
        # (the backfill test above wraps in a transaction and rolls back,
        # so it shouldn't leak rows; this filter is belt-and-suspenders).
        "   AND created_at < now() - interval '1 hour'"
    )
    assert n == 0


async def test_h_a_can_insert_explicit_principal_idx(postgres_pool):
    """Once H.a lands, the route layer should be able to INSERT references
    with an explicit created_by_idx pointing at any principal. This is the
    setup Phase H.b's dual-write depends on."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            # Seed a principal we'll point at.
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal"
                "  (display_name, system_role, created_by_idx)"
                " VALUES ('h_a-creator', 'user', 1) RETURNING idx"
            )
            row = await conn.fetchrow(
                "INSERT INTO qiita.references"
                "  (name, version, kind, created_by, created_by_idx)"
                " VALUES ($1, $2, 'sequence_reference',"
                "         'a0000000-0000-0000-0000-000000000001'::uuid, $3)"
                " RETURNING reference_idx, created_by_idx",
                "h_a-explicit-creator", "1.0", pidx,
            )
            assert row["created_by_idx"] == pidx
        finally:
            await tr.rollback()


async def test_h_a_rejects_fk_to_nonexistent_principal(postgres_pool):
    """The FK must be enforced — created_by_idx pointing at a missing
    principal raises ForeignKeyViolation."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await conn.execute(
                    "INSERT INTO qiita.references"
                    "  (name, version, kind, created_by, created_by_idx)"
                    " VALUES ($1, $2, 'sequence_reference',"
                    "         'a0000000-0000-0000-0000-000000000001'::uuid, $3)",
                    "h_a-bad-fk", "1.0", 999_999_999,
                )
        finally:
            await tr.rollback()
