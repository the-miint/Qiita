"""DB tests for the register-index library primitive.

`register_index` records a built index (path + JSONB build params) in
qiita.reference_index. It is the in-process action the host-reference-add
workflow runs after build-rype-index.
"""

import json

import pytest

from qiita_control_plane.actions.library import register_index

pytestmark = pytest.mark.db


async def _make_reference(pool, name):
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', true,"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
        name,
    )


async def _cleanup(pool, idx):
    await pool.execute("DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx)
    await pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_register_index_inserts_row(postgres_pool):
    idx = await _make_reference(postgres_pool, "regidx-insert")
    try:
        rii = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path="/srv/qiita/references/x/rype/index.ryxdi",
            params={"k": 64, "w": 25, "bucket_name": f"reference_{idx}"},
        )
        assert rii > 0
        row = await postgres_pool.fetchrow(
            "SELECT reference_idx, index_type, fs_path, params"
            " FROM qiita.reference_index WHERE reference_index_idx = $1",
            rii,
        )
        assert row["reference_idx"] == idx
        assert row["index_type"] == "rype"
        assert row["fs_path"].endswith("index.ryxdi")
        # params is stored as JSONB; asyncpg returns it as a JSON string.
        assert json.loads(row["params"])["k"] == 64
    finally:
        await _cleanup(postgres_pool, idx)


async def test_register_index_is_idempotent_on_same_path(postgres_pool):
    """A re-run (e.g. workflow retried from the start) must not duplicate the
    row for the same (reference_idx, index_type, fs_path); it returns the
    existing id."""
    idx = await _make_reference(postgres_pool, "regidx-idempotent")
    try:
        path = "/srv/qiita/references/y/rype/index.ryxdi"
        first = await register_index(
            postgres_pool, reference_idx=idx, index_type="rype", fs_path=path, params={"k": 64}
        )
        second = await register_index(
            postgres_pool, reference_idx=idx, index_type="rype", fs_path=path, params={"k": 64}
        )
        assert first == second
        count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        assert count == 1
    finally:
        await _cleanup(postgres_pool, idx)


async def test_register_index_records_shard_id(postgres_pool):
    """A sharded analysis index registration records its `shard_id` verbatim;
    omitting it (the host / unsharded path) leaves the column NULL."""
    idx = await _make_reference(postgres_pool, "regidx-shard")
    try:
        rii_shard = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=f"/srv/qiita/references/{idx}/shards/2/index.ryxdi",
            params={"k": 64},
            shard_id=2,
        )
        rii_unsharded = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=f"/srv/qiita/references/{idx}/rype/index.ryxdi",
            params={"k": 64},
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT shard_id FROM qiita.reference_index WHERE reference_index_idx = $1",
                rii_shard,
            )
            == 2
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT shard_id FROM qiita.reference_index WHERE reference_index_idx = $1",
                rii_unsharded,
            )
            is None
        )
    finally:
        await _cleanup(postgres_pool, idx)


async def test_register_index_distinct_shards_are_distinct_rows(postgres_pool):
    """Two shards of the same (reference, index_type) write two distinct rows —
    the idempotency key is (reference_idx, index_type, fs_path) and each shard's
    fs_path is distinct (`.../shards/{shard_id}/...`), so they never collide."""
    idx = await _make_reference(postgres_pool, "regidx-two-shards")
    try:
        rii0 = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=f"/srv/qiita/references/{idx}/shards/0/index.ryxdi",
            params={"k": 64},
            shard_id=0,
        )
        rii1 = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=f"/srv/qiita/references/{idx}/shards/1/index.ryxdi",
            params={"k": 64},
            shard_id=1,
        )
        assert rii0 != rii1
        count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        assert count == 2
    finally:
        await _cleanup(postgres_pool, idx)


async def test_register_index_same_shard_is_idempotent(postgres_pool):
    """Re-registering the same shard (same reference/type/path) returns the
    existing id and does not duplicate the row — the replay path."""
    idx = await _make_reference(postgres_pool, "regidx-shard-replay")
    try:
        path = f"/srv/qiita/references/{idx}/shards/0/index.ryxdi"
        first = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=path,
            params={"k": 64},
            shard_id=0,
        )
        second = await register_index(
            postgres_pool,
            reference_idx=idx,
            index_type="rype",
            fs_path=path,
            params={"k": 64},
            shard_id=0,
        )
        assert first == second
        count = await postgres_pool.fetchval(
            "SELECT count(*) FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        assert count == 1
    finally:
        await _cleanup(postgres_pool, idx)
