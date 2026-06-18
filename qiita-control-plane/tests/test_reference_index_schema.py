"""Schema-level DB tests for the host-reference / reference_index migrations.

Talks directly to the integration Postgres pool (no routes) to pin the shape
the higher layers depend on: the `is_host` column, the `indexing` status value,
and the `qiita.reference_index` table.
"""

import pytest

pytestmark = pytest.mark.db


async def _make_reference(pool, name, *, is_host=False):
    principal_idx = await pool.fetchval("SELECT MIN(idx) FROM qiita.principal")
    return await pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, is_host, created_by_idx)"
        " VALUES ($1, '1.0', 'sequence_reference', $2, $3) RETURNING reference_idx",
        name,
        is_host,
        principal_idx,
    )


async def test_is_host_defaults_false(postgres_pool):
    idx = await postgres_pool.fetchval(
        "INSERT INTO qiita.reference (name, version, kind, created_by_idx)"
        " VALUES ('schema-default-host', '1.0', 'sequence_reference',"
        "         (SELECT MIN(idx) FROM qiita.principal)) RETURNING reference_idx",
    )
    try:
        assert (
            await postgres_pool.fetchval(
                "SELECT is_host FROM qiita.reference WHERE reference_idx = $1", idx
            )
            is False
        )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_status_check_accepts_indexing(postgres_pool):
    idx = await _make_reference(postgres_pool, "schema-indexing")
    try:
        await postgres_pool.execute(
            "UPDATE qiita.reference SET status = 'indexing' WHERE reference_idx = $1", idx
        )
        assert (
            await postgres_pool.fetchval(
                "SELECT status FROM qiita.reference WHERE reference_idx = $1", idx
            )
            == "indexing"
        )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_status_check_still_rejects_bogus(postgres_pool):
    idx = await _make_reference(postgres_pool, "schema-bogus-status")
    try:
        with pytest.raises(Exception):  # asyncpg.CheckViolationError
            await postgres_pool.execute(
                "UPDATE qiita.reference SET status = 'bogus' WHERE reference_idx = $1", idx
            )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_reference_index_table_accepts_row(postgres_pool):
    idx = await _make_reference(postgres_pool, "schema-refindex", is_host=True)
    try:
        rii = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
            " VALUES ($1, 'rype', '/srv/x.ryxdi', $2::jsonb) RETURNING reference_index_idx",
            idx,
            '{"k": 64, "w": 25}',
        )
        row = await postgres_pool.fetchrow(
            "SELECT reference_idx, index_type, fs_path, params, created_at"
            " FROM qiita.reference_index WHERE reference_index_idx = $1",
            rii,
        )
        assert row["reference_idx"] == idx
        assert row["index_type"] == "rype"
        assert row["created_at"] is not None
    finally:
        # RESTRICT FK: index rows must go before the reference.
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_reference_index_accepts_minimap2_type(postgres_pool):
    """The CHECK allow-list includes 'minimap2' (the sidecar host-filter index)
    alongside 'rype'."""
    idx = await _make_reference(postgres_pool, "schema-refindex-minimap2", is_host=True)
    try:
        rii = await postgres_pool.fetchval(
            "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
            " VALUES ($1, 'minimap2', '/srv/x/minimap2/index.mmi', $2::jsonb)"
            " RETURNING reference_index_idx",
            idx,
            '{"preset": "sr", "source_chunks": "/data/host/grch38.chunks", "num_subjects": 1}',
        )
        row = await postgres_pool.fetchrow(
            "SELECT index_type FROM qiita.reference_index WHERE reference_index_idx = $1",
            rii,
        )
        assert row["index_type"] == "minimap2"
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_reference_index_rejects_unknown_type(postgres_pool):
    idx = await _make_reference(postgres_pool, "schema-refindex-badtype")
    try:
        with pytest.raises(Exception):  # asyncpg.CheckViolationError
            await postgres_pool.execute(
                "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
                " VALUES ($1, 'bowtie3', '/srv/x', '{}'::jsonb)",
                idx,
            )
    finally:
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)


async def test_reference_index_restricts_reference_delete(postgres_pool):
    """RESTRICT FK (schema-wide convention): a reference with an index row
    cannot be deleted until the index row is removed first."""
    idx = await _make_reference(postgres_pool, "schema-refindex-restrict")
    await postgres_pool.execute(
        "INSERT INTO qiita.reference_index (reference_idx, index_type, fs_path, params)"
        " VALUES ($1, 'rype', '/srv/x.ryxdi', '{}'::jsonb)",
        idx,
    )
    try:
        with pytest.raises(Exception):  # asyncpg.ForeignKeyViolationError
            await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.reference_index WHERE reference_idx = $1", idx
        )
        await postgres_pool.execute("DELETE FROM qiita.reference WHERE reference_idx = $1", idx)
