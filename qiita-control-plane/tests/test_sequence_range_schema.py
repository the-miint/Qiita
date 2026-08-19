"""Schema-level invariants for qiita.sequence_range and its mint function.

The composite-FK + GENERATED contract is covered for free by the
parametrize loop in test_prep_sample_subtype_invariants.py
(KIND_PINNED_TABLES). This file locks in the remaining invariants that
the loop does not see:

  - the free-standing qiita.sequence_idx_seq exists with the expected
    type and bounds
  - the table's composite FK is ON DELETE CASCADE (so a prep_sample
    delete drops the range row), not the default RESTRICT
  - the CHECK constraints on the range bounds exist
  - the qiita.mint_sequence_range(bigint, bigint, bigint, bigint) function
    exists with the expected return type
  - minted_by_work_ticket_idx exists, is nullable, and carries NO foreign key
    (it is an identity token compared for equality, not a navigable relationship)

These tests fail with a clear "does not exist" message until the
migration in this PR lands.
"""

import pytest

pytestmark = pytest.mark.db


async def test_sequence_idx_seq_exists(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT s.seqtypid::regtype::text AS type,"
        "       s.seqmin AS min_value,"
        "       s.seqcycle AS cycles"
        "  FROM pg_sequence s"
        "  JOIN pg_class c ON c.oid = s.seqrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita' AND c.relname = 'sequence_idx_seq'"
    )
    assert row is not None, "qiita.sequence_idx_seq is missing"
    assert row["type"] == "bigint", row["type"]
    assert row["min_value"] == 1, row["min_value"]
    assert row["cycles"] is False, "sequence_idx_seq must be NO CYCLE"


async def test_sequence_idx_seq_is_free_standing(postgres_pool):
    # The sequence is allocated explicitly by qiita.mint_sequence_range;
    # it must not be owned by a serial/identity column or it would be
    # dropped if that column went away.
    owned_by = await postgres_pool.fetchval(
        "SELECT pg_get_serial_sequence('qiita.sequence_range', 'idx')"
    )
    # qiita.sequence_range.idx has its own IDENTITY sequence; the
    # sequence_idx_seq must be a different object.
    assert owned_by != "qiita.sequence_idx_seq", (
        "sequence_idx_seq must not be owned by sequence_range.idx"
    )


async def test_sequence_range_table_columns(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type,"
        "       a.attnotnull"
        "  FROM pg_attribute a"
        "  JOIN pg_class c ON c.oid = a.attrelid"
        "  JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = 'qiita'"
        "   AND c.relname = 'sequence_range'"
        "   AND a.attnum > 0"
        "   AND NOT a.attisdropped"
        " ORDER BY a.attnum"
    )
    cols = {r["attname"]: (r["type"], r["attnotnull"]) for r in rows}
    assert cols, "qiita.sequence_range is missing"
    assert cols["idx"] == ("bigint", True)
    assert cols["prep_sample_idx"] == ("bigint", True)
    # format_type drops the schema qualifier when 'qiita' is on the search
    # path, so the formatted name comes back unqualified.
    assert cols["processing_kind"][0] == "processing_kind"
    assert cols["sequence_idx_start"] == ("bigint", True)
    assert cols["sequence_idx_stop"] == ("bigint", True)
    assert cols["created_by_idx"] == ("bigint", True)
    assert cols["created_at"][0].startswith("timestamp with time zone")
    assert cols["created_at"][1] is True


async def test_sequence_range_fk_is_on_delete_cascade(postgres_pool):
    # confdeltype: 'c' = CASCADE, 'r' = RESTRICT, 'a' = NO ACTION, etc.
    confdeltype = await postgres_pool.fetchval(
        "SELECT c.confdeltype"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        "  JOIN pg_class pt ON pt.oid = c.confrelid"
        "  JOIN pg_namespace pn ON pn.oid = pt.relnamespace"
        " WHERE c.contype = 'f'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'sequence_range'"
        "   AND pn.nspname = 'qiita' AND pt.relname = 'prep_sample'"
    )
    assert confdeltype is not None, "qiita.sequence_range is missing its FK to qiita.prep_sample"
    # asyncpg surfaces the Postgres "char" type as a single-byte bytes
    # value — same idiom as test_subtype_processing_kind_is_generated_literal.
    assert confdeltype == b"c", (
        f"sequence_range FK must be ON DELETE CASCADE (got confdeltype={confdeltype!r})"
    )


async def test_sequence_range_check_constraints(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT c.conname, pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = 'sequence_range'"
    )
    defs_by_name = {r["conname"]: r["def"] for r in rows}
    # Names are fixed by the migration; the test asserts on them so the
    # error message points at the right constraint when one is missing.
    assert "sequence_range_start_lte_stop" in defs_by_name, defs_by_name
    assert "sequence_idx_start" in defs_by_name["sequence_range_start_lte_stop"]
    assert "sequence_idx_stop" in defs_by_name["sequence_range_start_lte_stop"]

    assert "sequence_range_positive" in defs_by_name, defs_by_name
    assert "sequence_idx_start" in defs_by_name["sequence_range_positive"]


async def test_mint_sequence_range_function_exists(postgres_pool):
    row = await postgres_pool.fetchrow(
        "SELECT p.pronargs,"
        "       p.proargtypes::regtype[] AS argtypes,"
        "       pg_get_function_result(p.oid) AS result"
        "  FROM pg_proc p"
        "  JOIN pg_namespace n ON n.oid = p.pronamespace"
        " WHERE n.nspname = 'qiita' AND p.proname = 'mint_sequence_range'"
    )
    assert row is not None, "qiita.mint_sequence_range is missing"
    # Signature: (bigint, bigint, bigint) -> qiita.sequence_range. The
    # types-only check avoids depending on the in_parameter names which
    # are intentional in the migration but not part of the contract.
    # 4 args: prep_sample_idx, count, principal_idx, work_ticket_idx. The ticket is
    # what lets a reads job prove an orphaned range is its OWN before reusing it.
    assert row["pronargs"] == 4, row["pronargs"]
    assert list(row["argtypes"]) == ["bigint", "bigint", "bigint", "bigint"], row["argtypes"]
    # pg_get_function_result drops the schema qualifier when 'qiita' is
    # on the search path; same idiom as the format_type case above.
    assert row["result"] == "sequence_range", row["result"]


async def test_minted_by_work_ticket_idx_column(postgres_pool):
    """The range records WHICH ticket minted it, and the column is NULLABLE.

    Nullable is load-bearing: rows that predate this column (every Illumina sample
    ingested so far) have no provenance, and callers read NULL as "not mine" —
    fail closed, which is exactly disallow-without-delete."""
    row = await postgres_pool.fetchrow(
        "SELECT data_type, is_nullable"
        "  FROM information_schema.columns"
        " WHERE table_schema = 'qiita' AND table_name = 'sequence_range'"
        "   AND column_name = 'minted_by_work_ticket_idx'"
    )
    assert row is not None, "qiita.sequence_range.minted_by_work_ticket_idx is missing"
    assert row["data_type"] == "bigint", row["data_type"]
    assert row["is_nullable"] == "YES", "minted_by_work_ticket_idx must be nullable"


async def test_minted_by_work_ticket_has_no_fk(postgres_pool):
    """No FK, on purpose. The column is an identity token compared for equality, not
    a relationship we navigate. A FK would make a mint whose ticket row is absent
    raise ForeignKeyViolationError — which the route maps to a misleading 404
    ("prep_sample not found") — and would force a delete rule on work_ticket that
    must never cascade (the range's sequence_idx values are already in the lake).
    A dangling value already reads as "not mine" and fails closed."""
    fks = await postgres_pool.fetchval(
        "SELECT count(*)"
        "  FROM pg_constraint c"
        "  JOIN pg_class t ON t.oid = c.conrelid"
        "  JOIN pg_namespace n ON n.oid = t.relnamespace"
        "  JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY (c.conkey)"
        " WHERE n.nspname = 'qiita' AND t.relname = 'sequence_range'"
        "   AND c.contype = 'f' AND a.attname = 'minted_by_work_ticket_idx'"
    )
    assert fks == 0, "minted_by_work_ticket_idx must carry no FK (see the migration)"
