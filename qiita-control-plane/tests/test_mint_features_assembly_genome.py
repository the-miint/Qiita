"""DB tests for `mint-features` over an ASSEMBLY genome map — the source='qiita' half.

The reference workflows' maps name external repositories, and the integration tier
covers that shape. `long-read-assembly` opens the other branch of the same code: a
map whose every row is `genome_source='qiita'` and therefore must carry the origin
`prep_sample_idx` the `genome_qiita_origin_check` biconditional requires. What is
covered here is the join between the two components — that the Parquet
`assembly_hash` writes is one `_validate_genome_map` accepts and `_associate_genomes`
resolves into `qiita.genome` + `qiita.feature_genome`.

The genome-map SCHEMA is restated below rather than imported, so a producer-side
column rename fails here instead of at runtime on the deploy host.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import GenomeSource

from qiita_control_plane.actions.library import mint_features
from qiita_control_plane.testing.db_seeds import (
    seed_biosample_with_sequenced_prep_sample,
    seed_user_principal,
)

pytestmark = pytest.mark.db

# What assembly_hash writes: manifest.parquet and genome_map.parquet, joined on read_id.
_MANIFEST_SCHEMA = "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT"
_GENOME_MAP_SCHEMA = (
    "read_id VARCHAR, genome_source VARCHAR, genome_source_id VARCHAR, prep_sample_idx BIGINT"
)


def _write_parquet(path: Path, schema_sql: str, rows: list[tuple]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema_sql})")
        conn.executemany(f"INSERT INTO t VALUES ({', '.join('?' for _ in rows[0])})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")
    return path


async def test_an_assembly_genome_map_lands_one_genome_per_kind_and_bin(postgres_pool, tmp_path):
    """One LCG contig, a two-contig MAG, and one unbinned contig: three genomes, four
    feature_genome rows, each genome carrying the origin prep_sample.

    The MAG's two contigs share a `genome_source_id`, which is the case
    `_write_genome_associations` dedupes before its upsert — Postgres rejects an
    `ON CONFLICT DO UPDATE` that touches one conflict target twice.
    """
    principal_idx = await seed_user_principal(
        postgres_pool, prefix="asm-genome", suffix=str(uuid.uuid4())[:8]
    )
    biosample_idx, prep_sample_idx = await seed_biosample_with_sequenced_prep_sample(
        postgres_pool, owner_idx=principal_idx
    )
    # Distinct per run, so a re-run does not resolve onto the previous one's genomes.
    run = str(uuid.uuid4())[:8]
    lcg_id, mag_id, unbinned_id = f"lcg-{run}", f"mag-{run}", f"unb-{run}"
    hashes = {name: uuid.uuid4() for name in ("c1", "b1", "b2", "u1")}

    manifest = _write_parquet(
        tmp_path / "manifest.parquet",
        _MANIFEST_SCHEMA,
        [
            ("LCG:c1:c1", str(hashes["c1"]), 16),
            ("MAG:bin.1:b1", str(hashes["b1"]), 16),
            ("MAG:bin.1:b2", str(hashes["b2"]), 16),
            ("UNBINNED:u1:u1", str(hashes["u1"]), 16),
        ],
    )
    genome_map = _write_parquet(
        tmp_path / "genome_map.parquet",
        _GENOME_MAP_SCHEMA,
        [
            ("LCG:c1:c1", GenomeSource.QIITA.value, lcg_id, prep_sample_idx),
            ("MAG:bin.1:b1", GenomeSource.QIITA.value, mag_id, prep_sample_idx),
            ("MAG:bin.1:b2", GenomeSource.QIITA.value, mag_id, prep_sample_idx),
            ("UNBINNED:u1:u1", GenomeSource.QIITA.value, unbinned_id, prep_sample_idx),
        ],
    )

    genome_idxs: list[int] = []
    try:
        await mint_features(postgres_pool, manifest, tmp_path / "out", genome_map_path=genome_map)

        rows = await postgres_pool.fetch(
            "SELECT g.genome_idx, g.source, g.source_id, g.prep_sample_idx,"
            "       count(fg.feature_idx) AS features"
            "  FROM qiita.genome g"
            "  JOIN qiita.feature_genome fg ON fg.genome_idx = g.genome_idx"
            " WHERE g.source_id = ANY($1::text[])"
            " GROUP BY g.genome_idx, g.source, g.source_id, g.prep_sample_idx"
            " ORDER BY g.source_id",
            [lcg_id, mag_id, unbinned_id],
        )
        genome_idxs = [r["genome_idx"] for r in rows]
        assert [(r["source_id"], r["features"]) for r in rows] == [
            (lcg_id, 1),
            (mag_id, 2),
            (unbinned_id, 1),
        ]
        assert {r["source"] for r in rows} == {GenomeSource.QIITA.value}
        assert {r["prep_sample_idx"] for r in rows} == {prep_sample_idx}
    finally:
        if genome_idxs:
            await postgres_pool.execute(
                "DELETE FROM qiita.feature_genome WHERE genome_idx = ANY($1::bigint[])", genome_idxs
            )
            await postgres_pool.execute(
                "DELETE FROM qiita.genome WHERE genome_idx = ANY($1::bigint[])", genome_idxs
            )
        await postgres_pool.execute("DELETE FROM qiita.prep_sample WHERE idx = $1", prep_sample_idx)
        await postgres_pool.execute("DELETE FROM qiita.biosample WHERE idx = $1", biosample_idx)
        await postgres_pool.execute(
            "DELETE FROM qiita.user WHERE principal_idx = $1", principal_idx
        )
        await postgres_pool.execute("DELETE FROM qiita.principal WHERE idx = $1", principal_idx)


async def test_a_qiita_genome_map_without_the_origin_sample_is_rejected(postgres_pool, tmp_path):
    """The map assembly_hash writes always sets prep_sample_idx; a producer that
    stopped fails before any DB write rather than at the `genome_qiita_origin_check`
    biconditional, which would leave the features already minted."""
    sequence_hash = uuid.uuid4()
    manifest = _write_parquet(
        tmp_path / "manifest.parquet",
        _MANIFEST_SCHEMA,
        [("LCG:c1:c1", str(sequence_hash), 16)],
    )
    genome_map = _write_parquet(
        tmp_path / "genome_map.parquet",
        _GENOME_MAP_SCHEMA,
        [("LCG:c1:c1", GenomeSource.QIITA.value, "no-origin", None)],
    )
    with pytest.raises(ValueError, match="qiita-origin rule"):
        await mint_features(postgres_pool, manifest, tmp_path / "out", genome_map_path=genome_map)
