"""Schema-level invariants for the block-compute core.

Locks in the shapes the block-compute migrations introduce for bulk-block
read masking:

* `qiita.block` — the generic, why-agnostic compute-unit record (one ~10M-read
  block = one work ticket), with a nullable back-filled `work_ticket_idx` and a
  TEXT+CHECK lifecycle `state`.
* `qiita.block_member` — the block↔sample cover-map: per (block, prep_sample)
  the contiguous `[min_sequence_idx, max_sequence_idx]` sub-range this block
  covers.
* `qiita.mask_sample` — the per-`(mask_idx, prep_sample)` completion gate the
  masked-read export path reads (absence of a `read_mask` row must never be
  read as "pass"; the gate is the first-class COMPLETED signal).
* `qiita.work_ticket.block_idx` — the `block` scope-target arm, its scope-target
  CHECK, and the `work_ticket_one_in_flight_per_block` partial unique index.

These fail with a clear "is missing" message until the block migrations land.
"""

import asyncpg
import pytest

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


async def _check_defs(postgres_pool, relname: str) -> dict[str, str]:
    rows = await postgres_pool.fetch(
        "SELECT c.conname, pg_get_constraintdef(c.oid) AS def"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'c'"
        "   AND cn.nspname = 'qiita' AND ct.relname = $1",
        relname,
    )
    return {r["conname"]: r["def"] for r in rows}


async def _pk_def(postgres_pool, relname: str) -> str | None:
    return await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE c.contype = 'p'"
        "   AND cn.nspname = 'qiita' AND ct.relname = $1",
        relname,
    )


async def _fk_deltype(postgres_pool, relname: str, parent: str) -> bytes | None:
    return await postgres_pool.fetchval(
        "SELECT c.confdeltype"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        "  JOIN pg_class pt ON pt.oid = c.confrelid"
        "  JOIN pg_namespace pn ON pn.oid = pt.relnamespace"
        " WHERE c.contype = 'f'"
        "   AND cn.nspname = 'qiita' AND ct.relname = $1"
        "   AND pn.nspname = 'qiita' AND pt.relname = $2",
        relname,
        parent,
    )


# ---------------------------------------------------------------------------
# qiita.block
# ---------------------------------------------------------------------------


async def test_block_table_columns(postgres_pool):
    cols = await _columns(postgres_pool, "block")
    assert cols, "qiita.block is missing"
    assert cols["block_idx"] == ("bigint", True)
    # work_ticket_idx is nullable — minted before the ticket, back-filled after.
    assert cols["work_ticket_idx"] == ("bigint", False)
    assert cols["state"] == ("text", True)
    assert cols["created_at"][0].startswith("timestamp with time zone")
    assert cols["created_at"][1] is True
    assert cols["updated_at"][0].startswith("timestamp with time zone")
    assert cols["updated_at"][1] is True


async def test_block_state_check(postgres_pool):
    defs = await _check_defs(postgres_pool, "block")
    joined = " ".join(defs.values())
    for state in ("pending", "processing", "completed", "failed"):
        assert state in joined, f"block.state CHECK is missing {state!r}: {defs}"


async def test_block_state_check_rejects_bad_state(postgres_pool):
    """The block.state CHECK actually rejects an out-of-set value at INSERT time,
    not merely present in the catalog. block has no required FK (work_ticket_idx
    is nullable), so a bare INSERT isolates the CHECK."""
    async with postgres_pool.acquire() as conn:
        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute("INSERT INTO qiita.block (state) VALUES ('bogus')")


async def test_block_work_ticket_idx_unique(postgres_pool):
    """block.work_ticket_idx carries a partial UNIQUE index (excluding the
    pre-back-fill NULLs): a block runs on exactly one ticket, so at most one
    block may point at any given work_ticket. Without it, the mutual-FK cascade
    can abort on a divergent back-fill."""
    idxdef = await postgres_pool.fetchval(
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'qiita' AND tablename = 'block'"
        "   AND indexname = 'block_work_ticket_idx'"
    )
    assert idxdef is not None, "block_work_ticket_idx index is missing"
    assert "UNIQUE" in idxdef, idxdef
    assert "work_ticket_idx" in idxdef, idxdef


async def test_block_work_ticket_fk_cascade(postgres_pool):
    """block.work_ticket_idx → work_ticket ON DELETE CASCADE: deleting the
    ticket removes its block. The reverse edge (work_ticket.block_idx) is a
    deferred NO ACTION so the mutual reference does not deadlock on delete."""
    confdeltype = await _fk_deltype(postgres_pool, "block", "work_ticket")
    assert confdeltype is not None, "qiita.block is missing its FK to qiita.work_ticket"
    assert confdeltype == b"c", (
        f"block.work_ticket_idx FK must be ON DELETE CASCADE (got {confdeltype!r})"
    )


# ---------------------------------------------------------------------------
# qiita.block_member
# ---------------------------------------------------------------------------


async def test_block_member_table_columns(postgres_pool):
    cols = await _columns(postgres_pool, "block_member")
    assert cols, "qiita.block_member is missing"
    assert cols["block_idx"] == ("bigint", True)
    assert cols["prep_sample_idx"] == ("bigint", True)
    assert cols["min_sequence_idx"] == ("bigint", True)
    assert cols["max_sequence_idx"] == ("bigint", True)


async def test_block_member_primary_key(postgres_pool):
    pk = await _pk_def(postgres_pool, "block_member")
    assert pk is not None, "qiita.block_member has no primary key"
    assert "block_idx" in pk and "prep_sample_idx" in pk, pk


async def test_block_member_min_max_check(postgres_pool):
    """Assert the ordering relation itself, not just that both column names
    appear — an inverted (max <= min) or two-unrelated-bounds CHECK would pass a
    mere name-presence assertion while the invariant is wrong."""
    defs = await _check_defs(postgres_pool, "block_member")
    joined = " ".join(defs.values())
    assert "min_sequence_idx <= max_sequence_idx" in joined, defs


async def test_block_member_fk_to_block_cascade(postgres_pool):
    confdeltype = await _fk_deltype(postgres_pool, "block_member", "block")
    assert confdeltype is not None, "qiita.block_member is missing its FK to qiita.block"
    assert confdeltype == b"c", (
        f"block_member.block_idx FK must be ON DELETE CASCADE (got {confdeltype!r})"
    )


async def test_block_member_prep_sample_index(postgres_pool):
    """A plain index on prep_sample_idx backs the reconcile cover-map lookup
    ('which blocks cover this sample?')."""
    idxdefs = await postgres_pool.fetch(
        "SELECT indexname, indexdef FROM pg_indexes"
        " WHERE schemaname = 'qiita' AND tablename = 'block_member'"
    )
    assert any("prep_sample_idx" in r["indexdef"] for r in idxdefs), [
        r["indexname"] for r in idxdefs
    ]


# ---------------------------------------------------------------------------
# qiita.mask_sample (the completion gate)
# ---------------------------------------------------------------------------


async def test_mask_sample_table_columns(postgres_pool):
    cols = await _columns(postgres_pool, "mask_sample")
    assert cols, "qiita.mask_sample is missing"
    assert cols["mask_idx"] == ("bigint", True)
    assert cols["prep_sample_idx"] == ("bigint", True)
    assert cols["state"] == ("text", True)
    assert cols["created_at"][0].startswith("timestamp with time zone")
    assert cols["updated_at"][0].startswith("timestamp with time zone")


async def test_mask_sample_primary_key(postgres_pool):
    pk = await _pk_def(postgres_pool, "mask_sample")
    assert pk is not None, "qiita.mask_sample has no primary key"
    assert "mask_idx" in pk and "prep_sample_idx" in pk, pk


async def test_mask_sample_state_check(postgres_pool):
    defs = await _check_defs(postgres_pool, "mask_sample")
    joined = " ".join(defs.values())
    for state in ("pending", "completed"):
        assert state in joined, f"mask_sample.state CHECK is missing {state!r}: {defs}"


async def test_mask_sample_fk_to_mask_definition(postgres_pool):
    """The gate rows are derived from the mask, so the FK is ON DELETE CASCADE —
    deleting a mask removes its per-sample gate rows. Assert the delete action,
    not just the FK's existence."""
    confdeltype = await _fk_deltype(postgres_pool, "mask_sample", "mask_definition")
    assert confdeltype is not None, "qiita.mask_sample is missing its FK to qiita.mask_definition"
    assert confdeltype == b"c", (
        f"mask_sample.mask_idx FK must be ON DELETE CASCADE (got {confdeltype!r})"
    )


# ---------------------------------------------------------------------------
# qiita.work_ticket.block_idx + scope target + one-in-flight index + enum
# ---------------------------------------------------------------------------


async def test_work_ticket_block_idx_column(postgres_pool):
    cols = await _columns(postgres_pool, "work_ticket")
    assert "block_idx" in cols, "qiita.work_ticket.block_idx is missing"
    # Nullable — only block-scoped tickets carry it (mirrors the other arms).
    assert cols["block_idx"] == ("bigint", False)


async def test_work_ticket_block_idx_fk_to_block(postgres_pool):
    confdeltype = await _fk_deltype(postgres_pool, "work_ticket", "block")
    assert confdeltype is not None, "qiita.work_ticket.block_idx is missing its FK to qiita.block"


async def test_work_ticket_scope_target_check_has_block_arm(postgres_pool):
    """The scope-target consistency CHECK must gain a block arm (block_idx set,
    every other scope arm NULL) and must add block_idx IS NULL to the others."""
    con = await postgres_pool.fetchval(
        "SELECT pg_get_constraintdef(c.oid)"
        "  FROM pg_constraint c"
        "  JOIN pg_class ct ON ct.oid = c.conrelid"
        "  JOIN pg_namespace cn ON cn.oid = ct.relnamespace"
        " WHERE cn.nspname = 'qiita' AND ct.relname = 'work_ticket'"
        "   AND c.conname = 'work_ticket_scope_target_consistent'"
    )
    assert con is not None, "work_ticket_scope_target_consistent CHECK is missing"
    assert "block_idx" in con, con
    assert "'block'" in con, con


async def test_work_ticket_one_in_flight_per_block_index(postgres_pool):
    idxdef = await postgres_pool.fetchval(
        "SELECT indexdef FROM pg_indexes"
        " WHERE schemaname = 'qiita' AND tablename = 'work_ticket'"
        "   AND indexname = 'work_ticket_one_in_flight_per_block'"
    )
    assert idxdef is not None, "work_ticket_one_in_flight_per_block index is missing"
    assert "UNIQUE" in idxdef, idxdef
    assert "block_idx" in idxdef, idxdef
    assert "action_id" in idxdef and "action_version" in idxdef, idxdef


async def test_scope_target_kind_enum_has_block(postgres_pool):
    rows = await postgres_pool.fetch(
        "SELECT e.enumlabel"
        "  FROM pg_enum e"
        "  JOIN pg_type t ON t.oid = e.enumtypid"
        "  JOIN pg_namespace n ON n.oid = t.typnamespace"
        " WHERE n.nspname = 'qiita' AND t.typname = 'scope_target_kind'"
    )
    labels = {r["enumlabel"] for r in rows}
    assert "block" in labels, labels
