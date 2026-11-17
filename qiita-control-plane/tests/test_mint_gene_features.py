"""DB test for `mint-gene-features`: a feature_idx per called gene's hash, written
where the resume path looks for it."""

from __future__ import annotations

import uuid

import duckdb
import pytest

from qiita_control_plane.actions import library as lib

pytestmark = pytest.mark.db


async def test_mints_one_feature_per_gene_hash_at_its_basename(postgres_pool, tmp_path):
    """Two genes with the same bytes share one feature_idx; the map is keyed by hash
    and lands at `MINT_GENE_FEATURES_OUTPUT_BASENAME`."""
    shared, other = uuid.uuid4(), uuid.uuid4()
    manifest = tmp_path / "gene.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES ('{shared}'::UUID), ('{shared}'::UUID),"
            f" ('{other}'::UUID)) t(sequence_hash)) TO '{manifest}' (FORMAT PARQUET)"
        )
    out_dir = tmp_path / "ws"
    out_dir.mkdir()
    try:
        path, minted, reused = await lib.mint_gene_features(postgres_pool, manifest, out_dir)

        assert path == out_dir / lib.MINT_GENE_FEATURES_OUTPUT_BASENAME
        assert (minted, reused) == (2, 0)
        with duckdb.connect(":memory:") as conn:
            rows = dict(
                conn.execute(
                    f"SELECT sequence_hash, feature_idx FROM read_parquet('{path}')"
                ).fetchall()
            )
        stored = {
            r["sequence_hash"]: r["feature_idx"]
            for r in await postgres_pool.fetch(
                "SELECT sequence_hash, feature_idx FROM qiita.feature"
                " WHERE sequence_hash = ANY($1::uuid[])",
                [shared, other],
            )
        }
        assert rows == stored
    finally:
        await postgres_pool.execute(
            "DELETE FROM qiita.feature WHERE sequence_hash = ANY($1::uuid[])", [shared, other]
        )
