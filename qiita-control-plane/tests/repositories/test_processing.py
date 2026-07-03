"""Repository-layer tests for qiita.processing_method and processed_prep_sample.

The property under test is idempotency: the same `params` returns the same processing_idx and a
re-mint over the same cohort returns the same leaf idxs, while different `params` mint a new method.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.repositories.processing import (
    lookup_processing_idx_by_params,
    mint_processed_prep_samples,
    mint_processing_method,
)
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def fixture(postgres_pool):
    """Seed one principal and two prep_samples, then clean up in FK-reverse order."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="proc-test", suffix=suffix)
    bio1, prep1 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    bio2, prep2 = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    yield {
        "pool": postgres_pool,
        "principal_idx": principal_idx,
        "prep_sample_idxs": [prep1, prep2],
    }

    await postgres_pool.execute(
        "DELETE FROM qiita.processed_prep_sample WHERE prep_sample_idx = ANY($1::bigint[])",
        [prep1, prep2],
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.processing_method WHERE created_by_idx = $1",
        principal_idx,
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.prep_sample WHERE idx = ANY($1::bigint[])", [prep1, prep2]
    )
    await postgres_pool.execute(
        "DELETE FROM qiita.biosample WHERE idx = ANY($1::bigint[])", [bio1, bio2]
    )
    await postgres_pool.execute("DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx)
    await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


def _params(**kw):
    return {"workflow": "amplicon", "version": "1.0.0", "primer": "GTGYCAGCMGCCGCGGTAA", **kw}


# ---------------------------------------------------------------------------
# mint_processing_method idempotency
# ---------------------------------------------------------------------------


async def test_mint_returns_row_with_expected_shape(fixture):
    pool = fixture["pool"]
    params = _params()
    async with pool.acquire() as conn:
        row = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=params,
            principal_idx=fixture["principal_idx"],
        )
    assert row["processing_idx"] > 0
    assert row["workflow_name"] == "amplicon"
    assert row["workflow_version"] == "1.0.0"
    assert row["created_by_idx"] == fixture["principal_idx"]
    assert bytes(row["params_hash"]) == canonical_params_hash(params)


async def test_mint_same_params_is_idempotent(fixture):
    """Same params in any key order resolve to the same processing_idx."""
    pool = fixture["pool"]
    async with pool.acquire() as conn:
        first = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params={"a": 1, "b": [3, 4], "c": "x"},
            principal_idx=fixture["principal_idx"],
        )
        second = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params={"c": "x", "b": [3, 4], "a": 1},
            principal_idx=fixture["principal_idx"],
        )
    assert first["processing_idx"] == second["processing_idx"]


async def test_mint_different_params_distinct_processing_idx(fixture):
    pool = fixture["pool"]
    async with pool.acquire() as conn:
        a = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=_params(trim=150),
            principal_idx=fixture["principal_idx"],
        )
        b = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=_params(trim=250),
            principal_idx=fixture["principal_idx"],
        )
    assert a["processing_idx"] != b["processing_idx"]


async def test_mint_rejects_unknown_principal(fixture):
    pool = fixture["pool"]
    bogus_idx = await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal") + 1_000_000
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await mint_processing_method(
                conn,
                workflow_name="amplicon",
                workflow_version="1.0.0",
                params={"k": "v"},
                principal_idx=bogus_idx,
            )


async def test_lookup_by_params(fixture):
    pool = fixture["pool"]
    params = _params(trim=123)
    async with pool.acquire() as conn:
        minted = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=params,
            principal_idx=fixture["principal_idx"],
        )
    found = await lookup_processing_idx_by_params(pool, params)
    assert found == minted["processing_idx"]
    missing = await lookup_processing_idx_by_params(pool, _params(trim=999_999))
    assert missing is None


# ---------------------------------------------------------------------------
# mint_processed_prep_samples
# ---------------------------------------------------------------------------


async def test_mint_processed_prep_samples_returns_full_map(fixture):
    pool = fixture["pool"]
    preps = fixture["prep_sample_idxs"]
    async with pool.acquire() as conn:
        method = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=_params(),
            principal_idx=fixture["principal_idx"],
        )
        mapping = await mint_processed_prep_samples(
            conn,
            processing_idx=method["processing_idx"],
            prep_sample_idxs=preps,
        )
    assert set(mapping.keys()) == set(preps)
    assert all(v > 0 for v in mapping.values())
    # distinct leaf idx per sample
    assert len(set(mapping.values())) == len(preps)


async def test_mint_processed_prep_samples_is_idempotent(fixture):
    """A re-mint over the same processing and cohort returns the same leaf idxs."""
    pool = fixture["pool"]
    preps = fixture["prep_sample_idxs"]
    async with pool.acquire() as conn:
        method = await mint_processing_method(
            conn,
            workflow_name="amplicon",
            workflow_version="1.0.0",
            params=_params(),
            principal_idx=fixture["principal_idx"],
        )
        first = await mint_processed_prep_samples(
            conn, processing_idx=method["processing_idx"], prep_sample_idxs=preps
        )
        second = await mint_processed_prep_samples(
            conn, processing_idx=method["processing_idx"], prep_sample_idxs=preps
        )
    assert first == second


async def test_mint_processed_prep_samples_rejects_unknown_processing(fixture):
    pool = fixture["pool"]
    bogus = (
        await pool.fetchval("SELECT COALESCE(MAX(processing_idx), 0) FROM qiita.processing_method")
        + 1_000_000
    )
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await mint_processed_prep_samples(
                conn, processing_idx=bogus, prep_sample_idxs=fixture["prep_sample_idxs"]
            )
