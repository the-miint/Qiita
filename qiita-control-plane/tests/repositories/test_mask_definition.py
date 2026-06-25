"""Repository-layer tests for qiita.mask_definition.

Exercises mint_mask_definition / fetch_mask_definition_by_idx and the
underlying qiita.mint_mask_definition plpgsql upsert. The defining property
is idempotency: the same `params` (canonical-JSON hashed) must return the same
mask_idx, while a different `params` mints a new one.

Each test seeds its own principal so cleanup runs in FK-reverse order and the
suite can run in parallel against the shared postgres_pool fixture.
"""

import secrets

import asyncpg
import pytest
import pytest_asyncio
from qiita_common.hashing import canonical_params_hash

from qiita_control_plane.repositories.mask_definition import (
    fetch_mask_definition_by_idx,
    mint_mask_definition,
)
from qiita_control_plane.testing.db_seeds import seed_user_principal

pytestmark = pytest.mark.db


@pytest_asyncio.fixture
async def principal(postgres_pool):
    """Seed one principal; FK-reverse cleanup of any masks it minted."""
    suffix = secrets.token_hex(4)
    principal_idx = await seed_user_principal(postgres_pool, prefix="mask-test", suffix=suffix)
    yield {"pool": postgres_pool, "principal_idx": principal_idx}

    await postgres_pool.execute(
        "DELETE FROM qiita.mask_definition WHERE created_by_idx = $1",
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
    return {"workflow": "host_filter", "version": "1.0.0", "host_refs": [1, 2], **kw}


# ---------------------------------------------------------------------------
# mint idempotency
# ---------------------------------------------------------------------------


async def test_mint_returns_row_with_expected_shape(principal):
    pool = principal["pool"]
    params = _params()
    async with pool.acquire() as conn:
        row = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params=params,
            principal_idx=principal["principal_idx"],
        )
    assert row["mask_idx"] > 0
    assert row["filter_workflow"] == "host_filter"
    assert row["filter_version"] == "1.0.0"
    assert row["created_by_idx"] == principal["principal_idx"]
    # params_hash matches the canonical Python hash.
    assert bytes(row["params_hash"]) == canonical_params_hash(params)


async def test_mint_same_params_is_idempotent(principal):
    """Same params (even with keys in a different insertion order) → same mask_idx."""
    pool = principal["pool"]
    async with pool.acquire() as conn:
        first = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"a": 1, "b": [3, 4], "c": "x"},
            principal_idx=principal["principal_idx"],
        )
        # Reordered keys must hash identically (canonical JSON sorts keys).
        second = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"c": "x", "b": [3, 4], "a": 1},
            principal_idx=principal["principal_idx"],
        )
    assert first["mask_idx"] == second["mask_idx"]


async def test_mint_different_params_distinct_mask_idx(principal):
    pool = principal["pool"]
    async with pool.acquire() as conn:
        a = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params=_params(host_refs=[1]),
            principal_idx=principal["principal_idx"],
        )
        b = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params=_params(host_refs=[2]),
            principal_idx=principal["principal_idx"],
        )
    assert a["mask_idx"] != b["mask_idx"]


async def test_mint_dedup_on_params_only_not_descriptive_columns(principal):
    """Dedup key is the params hash; same params with a different descriptive
    filter_version still collapses to the existing row (the version, when it
    matters, belongs inside params so it changes the hash)."""
    pool = principal["pool"]
    async with pool.acquire() as conn:
        first = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params={"k": "v"},
            principal_idx=principal["principal_idx"],
        )
        second = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="9.9.9",  # different descriptive column, same params
            params={"k": "v"},
            principal_idx=principal["principal_idx"],
        )
    assert first["mask_idx"] == second["mask_idx"]
    # The stored row keeps the FIRST mint's descriptive columns (upsert is
    # DO NOTHING on conflict, so the loser's values are discarded).
    assert second["filter_version"] == "1.0.0"


# ---------------------------------------------------------------------------
# failure paths
# ---------------------------------------------------------------------------


async def test_mint_rejects_unknown_principal(principal):
    pool = principal["pool"]
    bogus_idx = await pool.fetchval("SELECT COALESCE(MAX(idx), 0) FROM qiita.principal") + 1_000_000
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await mint_mask_definition(
                conn,
                filter_workflow="host_filter",
                filter_version="1.0.0",
                params={"k": "v"},
                principal_idx=bogus_idx,
            )


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


async def test_fetch_returns_row_after_mint(principal):
    pool = principal["pool"]
    async with pool.acquire() as conn:
        minted = await mint_mask_definition(
            conn,
            filter_workflow="host_filter",
            filter_version="1.0.0",
            params=_params(),
            principal_idx=principal["principal_idx"],
        )
    fetched = await fetch_mask_definition_by_idx(pool, minted["mask_idx"])
    assert fetched is not None
    assert fetched["mask_idx"] == minted["mask_idx"]


async def test_fetch_returns_none_for_unknown_mask(principal):
    pool = principal["pool"]
    bogus_idx = (
        await pool.fetchval("SELECT COALESCE(MAX(mask_idx), 0) FROM qiita.mask_definition") + 999
    )
    fetched = await fetch_mask_definition_by_idx(pool, bogus_idx)
    assert fetched is None
