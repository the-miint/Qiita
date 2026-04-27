"""Tests for the references.created_by_idx FK migration (Phase H.a → H.c).

H.a added the nullable BIGINT FK and backfilled. H.b dual-wrote both columns
from the route layer. H.c (this commit) finalised: NOT NULL on created_by_idx
and DROP of the legacy created_by UUID column. By the time this test runs,
both migrations are applied and the assertions reflect the final state.
"""

import asyncpg
import pytest


# ---------------------------------------------------------------------------
# Final column shape (post-H.c)
# ---------------------------------------------------------------------------


async def test_created_by_idx_is_not_null(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT data_type, is_nullable FROM information_schema.columns"
        " WHERE table_schema = 'qiita' AND table_name = 'references'"
        "   AND column_name = 'created_by_idx'"
    )
    assert row is not None, "qiita.references.created_by_idx does not exist"
    assert row["data_type"] == "bigint"
    assert row["is_nullable"] == "NO"


async def test_legacy_created_by_column_dropped(postgres_pool):
    """Phase H.c dropped the legacy created_by UUID column."""
    row = await postgres_pool.fetchval(
        "SELECT 1 FROM information_schema.columns"
        " WHERE table_schema = 'qiita' AND table_name = 'references'"
        "   AND column_name = 'created_by'"
    )
    assert row is None, "qiita.references.created_by should have been dropped in H.c"


async def test_column_has_fk_to_principal(postgres_pool):
    """The column must FK to qiita.principal(idx) so worker / human / system
    creators are all valid targets."""
    row = await postgres_pool.fetchrow(
        "SELECT confrelid::regclass::text AS target_table"
        " FROM pg_constraint"
        " WHERE conrelid = 'qiita.references'::regclass"
        "   AND contype = 'f'"
        "   AND conname LIKE '%created_by_idx%'"
    )
    assert row is not None, "no FK constraint on created_by_idx found"
    # regclass omits the schema qualifier when the schema is in the
    # current search_path. Either form is correct.
    assert row["target_table"] in ("qiita.principal", "principal")


async def test_no_cascade_on_delete(postgres_pool):
    """Project convention: NO ACTION / RESTRICT on every FK in the qiita
    schema."""
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
# Behavior post-H.c
# ---------------------------------------------------------------------------


async def test_insert_with_explicit_principal_idx(postgres_pool):
    """The route layer INSERTs references with an explicit created_by_idx."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            pidx = await conn.fetchval(
                "INSERT INTO qiita.principal"
                "  (display_name, system_role, created_by_idx)"
                " VALUES ('h_c-creator', 'user', 1) RETURNING idx"
            )
            row = await conn.fetchrow(
                "INSERT INTO qiita.references"
                "  (name, version, kind, created_by_idx)"
                " VALUES ($1, $2, 'sequence_reference', $3)"
                " RETURNING reference_idx, created_by_idx",
                "h_c-explicit-creator",
                "1.0",
                pidx,
            )
            assert row["created_by_idx"] == pidx
        finally:
            await tr.rollback()


async def test_rejects_fk_to_nonexistent_principal(postgres_pool):
    """The FK must be enforced — created_by_idx pointing at a missing
    principal raises ForeignKeyViolation."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await conn.execute(
                    "INSERT INTO qiita.references"
                    "  (name, version, kind, created_by_idx)"
                    " VALUES ($1, $2, 'sequence_reference', $3)",
                    "h_c-bad-fk",
                    "1.0",
                    999_999_999,
                )
        finally:
            await tr.rollback()


async def test_insert_without_created_by_idx_rejected(postgres_pool):
    """Phase H.c made the column NOT NULL — omitting it raises."""
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            with pytest.raises(asyncpg.NotNullViolationError):
                await conn.execute(
                    "INSERT INTO qiita.references (name, version, kind)"
                    " VALUES ($1, $2, 'sequence_reference')",
                    "h_c-no-fk",
                    "1.0",
                )
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# get_current_user / get_current_principal_idx removal
# ---------------------------------------------------------------------------


def test_get_current_user_no_longer_importable():
    """Phase H.c deleted the mock auth helpers from deps.py."""
    import importlib

    deps = importlib.import_module("qiita_control_plane.deps")
    assert not hasattr(deps, "get_current_user"), (
        "get_current_user should be removed from deps.py in Phase H.c"
    )
    assert not hasattr(deps, "get_current_principal_idx"), (
        "get_current_principal_idx should be removed from deps.py in Phase H.c"
    )
