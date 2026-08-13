"""Schema-level invariants for `qiita.exported_processing`.

The sibling of `test_exported_identifier_schema.py`, for the handle that names
the PROCESSING rather than one processed sample. The guarantees are the ones a
caller cannot reach and a future migration could quietly drop:

* `export_processing_id` is authored by Postgres and unwritable, so a public
  identifier cannot be forged, cannot drift from `idx`, and cannot be edited
  after publication.
* Exactly one processing identifier on a live row (`num_nonnulls`), which is the
  constraint a new processing type must extend.
* One LIVE identifier per processing, so a manifest written twice names the
  processing the same way both times.
* An identifier OUTLIVES the processing it names: purging an alignment detaches
  and retires it instead of deleting it. The trigger that does this is what makes
  the FK's `ON DELETE SET NULL` satisfy the check at all, so without it an
  alignment purge fails outright.

There is no accession half here, unlike `exported_feature`: a processing is
something we performed, so nobody else has a name for it. The namespace is
entirely minted, which is why its unique index is total rather than partial —
`'QP' || idx` cannot recur.
"""

import hashlib
import uuid

import asyncpg
import pytest

from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


async def _seed(postgres_pool):
    """A principal and an alignment for it to have processed. No samples: this
    handle names the processing, not what went through it."""
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="expproc-schema", suffix=str(uuid.uuid4())[:8]
    )
    alignment_idx = await postgres_pool.fetchval(
        "SELECT (qiita.mint_alignment_definition($1, $2, $3)).alignment_idx",
        hashlib.sha256(f"exported-processing-probe-{uuid.uuid4()}".encode()).digest(),
        '{"probe": "exported_processing"}',
        principal_idx,
    )
    return alignment_idx, principal_idx


@pytest.fixture
async def probe(postgres_pool):
    alignment_idx, principal_idx = await _seed(postgres_pool)
    yield {"alignment_idx": alignment_idx, "principal_idx": principal_idx}
    await postgres_pool.execute(
        "DELETE FROM qiita.exported_processing WHERE alignment_idx = $1 OR created_by_idx = $2",
        alignment_idx,
        principal_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def _insert(postgres_pool, probe):
    return await postgres_pool.fetchrow(
        "INSERT INTO qiita.exported_processing (alignment_idx, created_by_idx)"
        " VALUES ($1, $2) RETURNING idx, export_processing_id",
        probe["alignment_idx"],
        probe["principal_idx"],
    )


async def test_the_handle_is_minted_from_the_row_identity(postgres_pool, probe):
    row = await _insert(postgres_pool, probe)
    assert row["export_processing_id"] == f"QP{row['idx']}"


async def test_the_handle_cannot_be_supplied_by_a_caller(postgres_pool, probe):
    with pytest.raises(asyncpg.PostgresError) as exc:
        await postgres_pool.execute(
            "INSERT INTO qiita.exported_processing"
            "       (alignment_idx, created_by_idx, export_processing_id)"
            " VALUES ($1, $2, 'FORGED')",
            probe["alignment_idx"],
            probe["principal_idx"],
        )
    assert "export_processing_id" in str(exc.value)


async def test_a_live_row_names_exactly_one_processing(postgres_pool, probe):
    """The constraint a second processing type extends. A row naming nothing is an
    identifier for nothing."""
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "INSERT INTO qiita.exported_processing (created_by_idx) VALUES ($1)",
            probe["principal_idx"],
        )


async def test_one_live_identifier_per_processing(postgres_pool, probe):
    """So a manifest written twice for one alignment names it the same way both
    times — this index is what makes the mint an idempotent upsert."""
    await _insert(postgres_pool, probe)
    with pytest.raises(asyncpg.UniqueViolationError):
        await _insert(postgres_pool, probe)


async def test_a_retired_row_does_not_block_a_fresh_identifier(postgres_pool, probe):
    """Partial on `NOT retired`, for the deliberate-retirement case (published in
    error, an embargo) where alignment_idx stays attached. The purge path nulls the
    column and so could never collide anyway."""
    first = await _insert(postgres_pool, probe)
    await postgres_pool.execute(
        "UPDATE qiita.exported_processing"
        "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
        " WHERE idx = $1",
        first["idx"],
    )
    second = await _insert(postgres_pool, probe)
    assert second["idx"] != first["idx"]
    assert second["export_processing_id"] != first["export_processing_id"]


async def test_an_identifier_outlives_the_processing_it_names(postgres_pool, probe):
    """The alignment purge hard-DELETEs the definition row. Without the
    detach-and-retire trigger either that purge fails on the one-processing CHECK or
    a published citation stops resolving."""
    row = await _insert(postgres_pool, probe)
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", probe["alignment_idx"]
    )
    after = await postgres_pool.fetchrow(
        "SELECT alignment_idx, retired, retired_at, retire_reason, export_processing_id"
        "  FROM qiita.exported_processing WHERE idx = $1",
        row["idx"],
    )
    assert after is not None, "the identifier was deleted with the alignment"
    assert after["alignment_idx"] is None
    assert after["retired"] is True
    assert after["retired_at"] is not None
    assert str(probe["alignment_idx"]) in after["retire_reason"]
    assert after["export_processing_id"] == row["export_processing_id"]


async def test_a_retired_handle_still_occupies_the_namespace(postgres_pool, probe):
    """Unlike `exported_feature`, whose namespace index is partial for the genome
    kind so a re-loaded genome can reclaim its accession. Nothing to reclaim here —
    the handle is ours and per-row — so the index is total and a citation to QP7
    resolves forever."""
    row = await _insert(postgres_pool, probe)
    await postgres_pool.execute(
        "UPDATE qiita.exported_processing"
        "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
        " WHERE idx = $1",
        row["idx"],
    )
    still_there = await postgres_pool.fetchval(
        "SELECT export_processing_id FROM qiita.exported_processing WHERE idx = $1", row["idx"]
    )
    assert still_there == row["export_processing_id"]


async def test_retirement_columns_cannot_disagree(postgres_pool, probe):
    row = await _insert(postgres_pool, probe)
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "UPDATE qiita.exported_processing SET retired = true WHERE idx = $1", row["idx"]
        )
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute(
            "UPDATE qiita.exported_processing SET retire_reason = 'no' WHERE idx = $1", row["idx"]
        )
