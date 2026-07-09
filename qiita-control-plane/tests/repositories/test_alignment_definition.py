"""Repository-layer tests for qiita.alignment_definition.

Exercises mint_alignment_definition / lookup_alignment_idx_by_params /
fetch_alignment_definition_by_idx and the underlying
qiita.mint_alignment_definition plpgsql upsert. The defining property is
idempotency: the same `params` (canonical-JSON hashed) must return the same
alignment_idx, while a different `params` — critically, a different shard-set —
mints a new one (the growth foundation). Twin of test_mask_definition.py.

Each test seeds its own principal so cleanup runs in FK-reverse order and the
suite can run in parallel against the shared postgres_pool fixture.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.repositories.alignment_definition import (
    fetch_alignment_definition_by_idx,
    lookup_alignment_idx_by_params,
    mint_alignment_definition,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def principal(postgres_pool):
    """Seed one principal; FK-reverse cleanup of any alignments it minted."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="align-test", suffix=suffix)
    yield {"pool": postgres_pool, "principal_idx": principal_idx}

    await postgres_pool.execute(
        "DELETE FROM qiita.alignment_definition WHERE created_by_idx = $1",
        principal_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.user WHERE principal_idx = $1",
        principal_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.principal WHERE idx = $1",
        principal_idx,
    )


def _params(**kw):
    return {
        "reference_idx": 7,
        "aligner": "minimap2",
        "mask_idx": 3,
        "shard_ids": [0, 1, 2],
        **kw,
    }


# ---------------------------------------------------------------------------
# mint idempotency
# ---------------------------------------------------------------------------


async def test_mint_returns_row_with_expected_shape(principal):
    pool = principal["pool"]
    params = _params()
    async with pool.acquire() as conn:
        row = await mint_alignment_definition(
            conn,
            params=params,
            principal_idx=principal["principal_idx"],
        )
    assert row["alignment_idx"] > 0
    assert row["created_by_idx"] == principal["principal_idx"]
    # params_hash matches the canonical Python hash.
    assert bytes(row["params_hash"]) == canonical_params_hash(params)


async def test_mint_same_params_is_idempotent(principal):
    """Same params (even with keys in a different insertion order) → same idx."""
    pool = principal["pool"]
    async with pool.acquire() as conn:
        first = await mint_alignment_definition(
            conn,
            params={"reference_idx": 1, "aligner": "bowtie2", "mask_idx": 2, "shard_ids": [0, 1]},
            principal_idx=principal["principal_idx"],
        )
        # Reordered keys must hash identically (canonical JSON sorts keys).
        second = await mint_alignment_definition(
            conn,
            params={"shard_ids": [0, 1], "mask_idx": 2, "aligner": "bowtie2", "reference_idx": 1},
            principal_idx=principal["principal_idx"],
        )
    assert first["alignment_idx"] == second["alignment_idx"]


async def test_mint_different_shard_set_mints_new_idx(principal):
    """The growth foundation: a grown reference has a different DISTINCT
    shard_id set, so an align run over the new shard-set mints a NEW idx."""
    pool = principal["pool"]
    async with pool.acquire() as conn:
        a = await mint_alignment_definition(
            conn,
            params=_params(shard_ids=[0, 1, 2]),
            principal_idx=principal["principal_idx"],
        )
        b = await mint_alignment_definition(
            conn,
            params=_params(shard_ids=[0, 1, 2, 3]),  # a grown reference
            principal_idx=principal["principal_idx"],
        )
    assert a["alignment_idx"] != b["alignment_idx"]


async def test_mint_different_aligner_mints_new_idx(principal):
    pool = principal["pool"]
    async with pool.acquire() as conn:
        a = await mint_alignment_definition(
            conn,
            params=_params(aligner="minimap2"),
            principal_idx=principal["principal_idx"],
        )
        b = await mint_alignment_definition(
            conn,
            params=_params(aligner="bowtie2"),
            principal_idx=principal["principal_idx"],
        )
    assert a["alignment_idx"] != b["alignment_idx"]


# ---------------------------------------------------------------------------
# failure paths
# ---------------------------------------------------------------------------


async def test_mint_rejects_unknown_principal(principal):
    pool = principal["pool"]
    bogus_idx = await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal") + 1_000_000
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await mint_alignment_definition(
                conn,
                params=_params(),
                principal_idx=bogus_idx,
            )


# ---------------------------------------------------------------------------
# lookup + fetch
# ---------------------------------------------------------------------------


async def test_lookup_returns_idx_after_mint_and_none_otherwise(principal):
    pool = principal["pool"]
    params = _params(reference_idx=42)
    # Not minted yet → None.
    assert await lookup_alignment_idx_by_params(pool, params) is None
    async with pool.acquire() as conn:
        minted = await mint_alignment_definition(
            conn, params=params, principal_idx=principal["principal_idx"]
        )
    assert await lookup_alignment_idx_by_params(pool, params) == minted["alignment_idx"]


async def test_fetch_returns_row_after_mint(principal):
    pool = principal["pool"]
    async with pool.acquire() as conn:
        minted = await mint_alignment_definition(
            conn, params=_params(reference_idx=99), principal_idx=principal["principal_idx"]
        )
    fetched = await fetch_alignment_definition_by_idx(pool, minted["alignment_idx"])
    assert fetched is not None
    assert fetched["alignment_idx"] == minted["alignment_idx"]


async def test_fetch_returns_none_for_unknown_alignment(principal):
    pool = principal["pool"]
    bogus_idx = (
        await pool.fetchval(
            "SELECT COALESCE(MAX(alignment_idx), 0) FROM qiita.alignment_definition"
        )
        + 999
    )
    assert await fetch_alignment_definition_by_idx(pool, bogus_idx) is None
