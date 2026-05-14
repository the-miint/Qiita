"""Repository-layer tests for qiita.sequence_range.

These tests exercise the two repository entry points
(mint_sequence_range, fetch_sequence_range_by_prep_sample_idx) and the
underlying qiita.mint_sequence_range plpgsql function. Each test seeds
its own principal -> user -> biosample -> prep_sample chain so cleanup
runs in FK-reverse order and the suite can run in parallel against the
shared postgres_pool fixture.
"""

import asyncio
import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX

from qiita_control_plane.repositories.biosample import insert_biosample
from qiita_control_plane.repositories.prep_sample import insert_prep_sample
from qiita_control_plane.repositories.sequence_range import (
    fetch_sequence_range_by_prep_sample_idx,
    mint_sequence_range,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Parent-chain seeding
# ---------------------------------------------------------------------------


async def _seed_user_principal(pool, *, suffix: str) -> int:
    """Seed a principal + user row, return the principal_idx.

    The owner_idx columns on biosample / prep_sample carry a role-typed
    FK trigger that rejects non-user-kind principals; this helper
    promotes the fresh principal to user-kind so it can serve as
    owner_idx.
    """
    principal_idx = await pool.fetchval(
        "INSERT INTO qiita.principal (display_name, created_by_idx) VALUES ($1, $2) RETURNING idx",
        f"sr-test-{suffix}",
        SYSTEM_PRINCIPAL_IDX,
    )
    await pool.execute(
        "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
        principal_idx,
        f"sr-test-{suffix}@test.local",
    )
    return principal_idx


async def _seed_prep_sample(pool, *, owner_idx: int) -> tuple[int, int]:
    """Seed a biosample + sequenced prep_sample, return (bs_idx, ps_idx).

    Uses the migration-seeded `short_read_metagenomics` prep_protocol so
    no extra protocol row needs to land. processing_kind='sequenced' is
    required for the composite FK on sequence_range to succeed.
    """
    protocol_idx = await pool.fetchval(
        "SELECT idx FROM qiita.prep_protocol WHERE name = $1",
        "short_read_metagenomics",
    )
    assert protocol_idx is not None, "short_read_metagenomics protocol not seeded"
    async with pool.acquire() as conn:
        async with conn.transaction():
            bs_idx = await insert_biosample(
                conn,
                owner_idx=owner_idx,
                created_by_idx=owner_idx,
            )
            ps_idx = await insert_prep_sample(
                conn,
                biosample_idx=bs_idx,
                owner_idx=owner_idx,
                prep_protocol_idx=protocol_idx,
                processing_kind="sequenced",
                created_by_idx=owner_idx,
            )
    return bs_idx, ps_idx


@pytest_asyncio.fixture
async def parent_chain(postgres_pool):
    """Seed one principal + one prep_sample for the test; FK-reverse cleanup.

    Tests that need a SECOND prep_sample (e.g., concurrent / disjoint
    range tests) call `_seed_prep_sample` again with the same
    `principal_idx` and append to `created["prep_sample"]`.
    """
    suffix = secrets.token_hex(4)
    principal_idx = await _seed_user_principal(postgres_pool, suffix=suffix)
    bs_idx, ps_idx = await _seed_prep_sample(postgres_pool, owner_idx=principal_idx)
    created: dict[str, list[int]] = {
        "biosample": [bs_idx],
        "prep_sample": [ps_idx],
    }
    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idx": ps_idx,
        "biosample_idx": bs_idx,
        "created": created,
    }

    # FK-reverse cleanup. sequence_range rows cascade with prep_sample,
    # so no explicit sweep is needed for them.
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])",
        created["prep_sample"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])",
        created["biosample"],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = $1",
        principal_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = $1",
        principal_idx,
    )


# ---------------------------------------------------------------------------
# mint_sequence_range happy paths
# ---------------------------------------------------------------------------


async def test_mint_sequence_range_returns_row_with_expected_shape(parent_chain):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        row = await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=10,
            principal_idx=parent_chain["principal_idx"],
        )
    assert row["prep_sample_idx"] == parent_chain["prep_sample_idx"]
    assert row["sequence_idx_stop"] - row["sequence_idx_start"] + 1 == 10
    assert row["sequence_idx_start"] >= 1
    assert row["created_by_idx"] == parent_chain["principal_idx"]
    # Verify the durable row matches the returned record.
    db_row = await pool.fetchrow(
        "SELECT sequence_idx_start, sequence_idx_stop FROM qiita.sequence_range"
        " WHERE prep_sample_idx = $1",
        parent_chain["prep_sample_idx"],
    )
    assert db_row["sequence_idx_start"] == row["sequence_idx_start"]
    assert db_row["sequence_idx_stop"] == row["sequence_idx_stop"]


async def test_mint_sequence_range_count_of_one_is_single_idx(parent_chain):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        row = await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=1,
            principal_idx=parent_chain["principal_idx"],
        )
    assert row["sequence_idx_start"] == row["sequence_idx_stop"]


async def test_mint_sequence_range_sequential_disjoint(parent_chain):
    """Two sequential mints on different prep_samples produce disjoint
    contiguous ranges with no overlap."""
    pool = parent_chain["pool"]
    _bs2, ps2 = await _seed_prep_sample(pool, owner_idx=parent_chain["principal_idx"])
    parent_chain["created"]["biosample"].append(_bs2)
    parent_chain["created"]["prep_sample"].append(ps2)

    async with pool.acquire() as conn:
        first = await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=100,
            principal_idx=parent_chain["principal_idx"],
        )
        second = await mint_sequence_range(
            conn,
            prep_sample_idx=ps2,
            count=50,
            principal_idx=parent_chain["principal_idx"],
        )

    assert first["sequence_idx_stop"] < second["sequence_idx_start"]
    assert second["sequence_idx_stop"] - second["sequence_idx_start"] + 1 == 50


async def test_mint_sequence_range_concurrent_disjoint(parent_chain):
    """asyncio.gather two mints on separate connections — the advisory
    lock in qiita.mint_sequence_range must serialise them so the two
    returned ranges never overlap."""
    pool = parent_chain["pool"]
    # Seed a second prep_sample so the two mints don't collide on the
    # UNIQUE (prep_sample_idx) constraint.
    _bs2, ps2 = await _seed_prep_sample(pool, owner_idx=parent_chain["principal_idx"])
    parent_chain["created"]["biosample"].append(_bs2)
    parent_chain["created"]["prep_sample"].append(ps2)

    async def _mint(ps_idx: int):
        async with pool.acquire() as conn:
            return await mint_sequence_range(
                conn,
                prep_sample_idx=ps_idx,
                count=1000,
                principal_idx=parent_chain["principal_idx"],
            )

    row_a, row_b = await asyncio.gather(
        _mint(parent_chain["prep_sample_idx"]),
        _mint(ps2),
    )
    # Whichever landed first carries the lower start; the other must
    # start strictly above the first one's stop. The actual invariant
    # the advisory lock protects is non-overlap, not zero-gap — a
    # concurrent test against the same Postgres instance could advance
    # the sequence between these two mints, so don't assert contiguous.
    if row_a["sequence_idx_start"] < row_b["sequence_idx_start"]:
        lo, hi = row_a, row_b
    else:
        lo, hi = row_b, row_a
    assert lo["sequence_idx_stop"] < hi["sequence_idx_start"], (lo, hi)


# ---------------------------------------------------------------------------
# mint_sequence_range failure paths
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_count", [0, -1, -1000])
async def test_mint_sequence_range_rejects_nonpositive_count(parent_chain, bad_count):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        # The migration raises with USING ERRCODE = '22023', which asyncpg
        # maps to InvalidParameterValueError (a subclass of PostgresError).
        with pytest.raises(asyncpg.InvalidParameterValueError) as exc_info:
            await mint_sequence_range(
                conn,
                prep_sample_idx=parent_chain["prep_sample_idx"],
                count=bad_count,
                principal_idx=parent_chain["principal_idx"],
            )
    assert exc_info.value.sqlstate == "22023"


async def test_mint_sequence_range_rejects_duplicate_prep_sample(parent_chain):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=10,
            principal_idx=parent_chain["principal_idx"],
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await mint_sequence_range(
                conn,
                prep_sample_idx=parent_chain["prep_sample_idx"],
                count=10,
                principal_idx=parent_chain["principal_idx"],
            )


async def test_mint_sequence_range_rejects_unknown_prep_sample(parent_chain):
    pool = parent_chain["pool"]
    bogus_idx = (
        await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 1_000_000
    )
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await mint_sequence_range(
                conn,
                prep_sample_idx=bogus_idx,
                count=10,
                principal_idx=parent_chain["principal_idx"],
            )


# ---------------------------------------------------------------------------
# fetch_sequence_range_by_prep_sample_idx
# ---------------------------------------------------------------------------


async def test_fetch_returns_row_after_mint(parent_chain):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        minted = await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=7,
            principal_idx=parent_chain["principal_idx"],
        )
    fetched = await fetch_sequence_range_by_prep_sample_idx(pool, parent_chain["prep_sample_idx"])
    assert fetched is not None
    assert fetched["sequence_idx_start"] == minted["sequence_idx_start"]
    assert fetched["sequence_idx_stop"] == minted["sequence_idx_stop"]


async def test_fetch_returns_none_when_unminted(parent_chain):
    fetched = await fetch_sequence_range_by_prep_sample_idx(
        parent_chain["pool"], parent_chain["prep_sample_idx"]
    )
    assert fetched is None


async def test_fetch_returns_none_for_unknown_prep_sample(parent_chain):
    pool = parent_chain["pool"]
    bogus_idx = await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.prep_sample") + 999
    fetched = await fetch_sequence_range_by_prep_sample_idx(pool, bogus_idx)
    assert fetched is None


# ---------------------------------------------------------------------------
# Cascade behavior
# ---------------------------------------------------------------------------


async def test_sequence_range_cascade_on_prep_sample_delete(parent_chain):
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=10,
            principal_idx=parent_chain["principal_idx"],
        )
    # Direct delete of the parent prep_sample should cascade.
    await pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = $1",
        parent_chain["prep_sample_idx"],
    )
    # Don't double-delete in teardown.
    parent_chain["created"]["prep_sample"].remove(parent_chain["prep_sample_idx"])
    fetched = await fetch_sequence_range_by_prep_sample_idx(pool, parent_chain["prep_sample_idx"])
    assert fetched is None


async def test_sequence_idx_not_reused_after_cascade_delete(parent_chain):
    """Once consumed by qiita.sequence_idx_seq, an idx range is never
    returned to the free pool — even when the row was cascaded away by
    a parent delete."""
    pool = parent_chain["pool"]
    async with pool.acquire() as conn:
        first = await mint_sequence_range(
            conn,
            prep_sample_idx=parent_chain["prep_sample_idx"],
            count=10,
            principal_idx=parent_chain["principal_idx"],
        )

    # Cascade-delete the parent, then mint against a fresh prep_sample
    # and assert the new range starts strictly after the old stop.
    await pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = $1",
        parent_chain["prep_sample_idx"],
    )
    parent_chain["created"]["prep_sample"].remove(parent_chain["prep_sample_idx"])

    _bs2, ps2 = await _seed_prep_sample(pool, owner_idx=parent_chain["principal_idx"])
    parent_chain["created"]["biosample"].append(_bs2)
    parent_chain["created"]["prep_sample"].append(ps2)
    async with pool.acquire() as conn:
        second = await mint_sequence_range(
            conn,
            prep_sample_idx=ps2,
            count=5,
            principal_idx=parent_chain["principal_idx"],
        )
    assert second["sequence_idx_start"] > first["sequence_idx_stop"]
