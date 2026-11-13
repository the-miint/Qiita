"""Schema-level invariants for the mask kind of `qiita.exported_processing` and
`qiita.exported_identifier` (migration 20261113000001_exported_processing_mask.sql).

* A live row names exactly one processing: an alignment or a mask, not both and
  not neither.
* One live row per mask (exported_processing) and per (mask, prep_sample)
  (exported_identifier), and a row of the alignment kind does not collide with one
  of the mask kind.
* Deleting a mask_definition row by hand detaches and retires the rows that named
  it, with the reason naming the deleted mask_idx.
"""

import hashlib
import uuid

import asyncpg
import pytest

from qiita_control_plane.repositories.mask_definition import mint_mask_definition
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)
from qiita_control_plane.testing.db_teardown import delete_principal, teardown_entity_graph

pytestmark = pytest.mark.db


@pytest.fixture
async def probe(postgres_pool):
    token = str(uuid.uuid4())[:8]
    principal_idx = await seed_user_principal(postgres_pool, prefix="expmask-schema", suffix=token)
    async with postgres_pool.acquire() as conn:
        mask = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"probe": f"exported-mask-{token}"},
            principal_idx=principal_idx,
        )
    alignment_idx = await postgres_pool.fetchval(
        "SELECT (qiita.mint_alignment_definition($1, $2, $3)).alignment_idx",
        hashlib.sha256(f"exported-mask-probe-{token}".encode()).digest(),
        '{"probe": "exported_mask"}',
        principal_idx,
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    yield {
        "mask_idx": mask["mask_idx"],
        "alignment_idx": alignment_idx,
        "prep_sample_idx": prep_sample_idx,
        "principal_idx": principal_idx,
    }
    await postgres_pool.execute(
        "DELETE FROM qiita.exported_identifier WHERE created_by_idx = $1", principal_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.exported_processing WHERE created_by_idx = $1", principal_idx
    )
    await teardown_entity_graph(
        postgres_pool,
        study_idxs=[],
        biosample_idxs=[biosample_idx],
        prep_sample_idxs=[prep_sample_idx],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", alignment_idx
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", mask["mask_idx"]
    )
    await delete_principal(postgres_pool, [principal_idx])


async def _insert_processing(pool, probe, *, alignment=False, mask=True):
    return await pool.fetchrow(
        "INSERT INTO qiita.exported_processing (alignment_idx, mask_idx, created_by_idx)"
        " VALUES ($1, $2, $3) RETURNING idx, export_processing_id",
        probe["alignment_idx"] if alignment else None,
        probe["mask_idx"] if mask else None,
        probe["principal_idx"],
    )


async def _insert_identifier(pool, probe, *, alignment=False, mask=True):
    return await pool.fetchrow(
        "INSERT INTO qiita.exported_identifier"
        "       (alignment_idx, mask_idx, prep_sample_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx, export_id",
        probe["alignment_idx"] if alignment else None,
        probe["mask_idx"] if mask else None,
        probe["prep_sample_idx"],
        probe["principal_idx"],
    )


@pytest.mark.parametrize("insert", [_insert_processing, _insert_identifier])
async def test_a_live_row_cannot_name_both_an_alignment_and_a_mask(postgres_pool, probe, insert):
    with pytest.raises(asyncpg.CheckViolationError):
        await insert(postgres_pool, probe, alignment=True, mask=True)


@pytest.mark.parametrize("insert", [_insert_processing, _insert_identifier])
async def test_a_live_row_must_name_a_processing(postgres_pool, probe, insert):
    with pytest.raises(asyncpg.CheckViolationError):
        await insert(postgres_pool, probe, alignment=False, mask=False)


async def test_one_live_handle_per_mask(postgres_pool, probe):
    row = await _insert_processing(postgres_pool, probe)
    assert row["export_processing_id"] == f"QP{row['idx']}"
    with pytest.raises(asyncpg.UniqueViolationError):
        await _insert_processing(postgres_pool, probe)


async def test_one_live_identifier_per_mask_and_prep_sample(postgres_pool, probe):
    row = await _insert_identifier(postgres_pool, probe)
    assert row["export_id"] == f"QM{row['idx']}"
    with pytest.raises(asyncpg.UniqueViolationError):
        await _insert_identifier(postgres_pool, probe)


@pytest.mark.parametrize("insert", [_insert_processing, _insert_identifier])
async def test_the_alignment_and_mask_kinds_do_not_collide(postgres_pool, probe, insert):
    """Each kind's partial unique index ignores the other kind's rows."""
    first = await insert(postgres_pool, probe, alignment=True, mask=False)
    second = await insert(postgres_pool, probe, alignment=False, mask=True)
    assert first["idx"] != second["idx"]


@pytest.mark.parametrize(
    ("insert", "table"),
    [(_insert_processing, "exported_processing"), (_insert_identifier, "exported_identifier")],
)
async def test_a_retired_mask_row_does_not_block_a_fresh_one(postgres_pool, probe, insert, table):
    first = await insert(postgres_pool, probe)
    await postgres_pool.execute(
        f"UPDATE qiita.{table}"
        "   SET retired = true, retired_at = now(), retire_reason = 'published in error'"
        " WHERE idx = $1",
        first["idx"],
    )
    second = await insert(postgres_pool, probe)
    assert second["idx"] != first["idx"]


async def test_deleting_the_mask_retires_the_rows_that_named_it(postgres_pool, probe):
    """A hand DELETE of the mask_definition nulls both FKs. The detach trigger
    retires each row in the same statement; without it the one_processing CHECK
    rejects the FK action and the DELETE fails."""
    handle = await _insert_processing(postgres_pool, probe)
    identifier = await _insert_identifier(postgres_pool, probe)
    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE mask_idx = $1", probe["mask_idx"]
    )
    expected_reason = (
        f"mask_definition {probe['mask_idx']} was deleted; the processing this"
        " identifier named no longer exists"
    )
    for table, idx, handle_column, published in (
        ("exported_processing", handle["idx"], "export_processing_id", handle),
        ("exported_identifier", identifier["idx"], "export_id", identifier),
    ):
        after = await postgres_pool.fetchrow(
            f"SELECT mask_idx, retired, retired_at, retire_reason, {handle_column} AS handle"
            f"  FROM qiita.{table} WHERE idx = $1",
            idx,
        )
        assert after is not None, f"the {table} row was deleted with the mask"
        assert after["mask_idx"] is None
        assert after["retired"] is True
        assert after["retired_at"] is not None
        assert after["retire_reason"] == expected_reason
        assert after["handle"] == published[handle_column]


async def test_deleting_an_alignment_still_retires_with_the_alignment_wording(postgres_pool, probe):
    """The extended trigger keeps the alignment branch's reason text."""
    row = await _insert_processing(postgres_pool, probe, alignment=True, mask=False)
    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE alignment_idx = $1", probe["alignment_idx"]
    )
    reason = await postgres_pool.fetchval(
        "SELECT retire_reason FROM qiita.exported_processing WHERE idx = $1", row["idx"]
    )
    assert reason == (
        f"alignment_definition {probe['alignment_idx']} was deleted; the processing"
        " this identifier named no longer exists"
    )
