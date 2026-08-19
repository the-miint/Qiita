"""DB test for `mint-features` over an ASSEMBLY genome map.

`tests/integration/test_action_library.py` covers the primitive's genome-map
behaviour one property at a time, on external-source maps. This covers the one
combination `long-read-assembly` produces and none of those tests do: three
`genome_source='qiita'` genomes in a single batch, one of them shared by two
features, every row carrying the origin `prep_sample_idx` that
`genome_qiita_origin_check` requires of that source.

The genome-map columns are spelled out here rather than imported: the producer is
`qiita_compute_orchestrator`, which is not a control-plane dependency. What each
side requires is named by `_validate_genome_map` here and by the job's own tests
there; nothing pins the two spellings to each other.
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

    The MAG's two contigs share a `genome_source_id` — the repeated conflict target
    `_write_genome_associations` dedupes before its upsert — while carrying a
    non-NULL prep_sample_idx.
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
