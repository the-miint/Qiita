"""tests for the amplicon_load job.

call execute() directly on synthesized asv_counts + feature_map + manifest +
asv_chunks, and assert the three staging parquets (membership + the two ASV
sequence tables). no miint; a plain in-memory DuckDB writes the fixtures and
reads the results back.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from qiita_compute_orchestrator.jobs.amplicon_load import Inputs, execute


def _hash(seq: str) -> UUID:
    return UUID(hashlib.md5(seq.encode()).hexdigest())


def _write(path: Path, schema: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema})")
        if rows:
            placeholders = ", ".join("?" for _ in rows[0])
            conn.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def _rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT prep_sample_idx, processing_idx, feature_idx, count "
            f"FROM read_parquet('{path}') ORDER BY prep_sample_idx, feature_idx"
        ).fetchall()


@pytest.fixture
def inputs(tmp_path):
    """Two samples over three ASVs; sample 11 and 12 share ASV `a` (same feature)."""
    ws = tmp_path / "ws"
    ws.mkdir()
    ha, hb, hc = _hash("A"), _hash("B"), _hash("C")
    _write(
        tmp_path / "asv_counts.parquet",
        "prep_sample_idx BIGINT, sequence_hash UUID, count BIGINT",
        [(11, ha, 7), (11, hb, 3), (12, ha, 5), (12, hc, 9)],
    )
    _write(
        tmp_path / "feature_map.parquet",
        "sequence_hash UUID, feature_idx BIGINT",
        [(ha, 100), (hb, 200), (hc, 300)],
    )
    _write(
        tmp_path / "manifest.parquet",
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [("A", ha, 1), ("B", hb, 1), ("C", hc, 1)],
    )
    chunks = tmp_path / "asv_chunks"
    _write(
        chunks / "part_00000.parquet",
        "sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR",
        [(ha, 0, "A"), (hb, 0, "B"), (hc, 0, "C")],
    )
    return Inputs(
        asv_counts=tmp_path / "asv_counts.parquet",
        feature_map=tmp_path / "feature_map.parquet",
        manifest=tmp_path / "manifest.parquet",
        asv_chunks=chunks,
        processing_idx=42,
        work_ticket_idx=1,
    ), ws


def test_writes_amplicon_membership(inputs):
    """The join maps each (prep_sample, sequence_hash, count) to its feature_idx,
    stamps processing_idx, and lands in amplicon_membership.parquet (the stem is
    the DuckLake table name register-files loads by)."""
    inp, ws = inputs
    out = asyncio.run(execute(inp, ws))
    staging = out["staging_dir"]
    membership = staging / "amplicon_membership.parquet"
    assert membership.exists()
    assert _rows(membership) == [
        (11, 42, 100, 7),
        (11, 42, 200, 3),
        (12, 42, 100, 5),
        (12, 42, 300, 9),
    ]


def test_shared_asv_keeps_per_sample_rows(inputs):
    """An ASV present in two samples yields one row per sample (same feature_idx),
    not a collapsed one — abundance is per prep_sample."""
    inp, ws = inputs
    out = asyncio.run(execute(inp, ws))
    rows = _rows(out["staging_dir"] / "amplicon_membership.parquet")
    feature_100 = [r for r in rows if r[2] == 100]
    assert feature_100 == [(11, 42, 100, 7), (12, 42, 100, 5)]


def test_writes_asv_sequence_tables(inputs):
    """The ASV bytes are stored: amplicon_sequence carries one row per feature_idx
    (with length), and amplicon_sequence_chunks re-keys the deblur chunks from
    sequence_hash to feature_idx so an ASV never has to be recomputed to read."""
    inp, ws = inputs
    out = asyncio.run(execute(inp, ws))
    staging = out["staging_dir"]
    ha, hb, hc = _hash("A"), _hash("B"), _hash("C")

    with duckdb.connect(":memory:") as conn:
        seqs = conn.execute(
            f"SELECT feature_idx, sequence_hash, sequence_length_bp "
            f"FROM read_parquet('{staging / 'amplicon_sequence.parquet'}') ORDER BY feature_idx"
        ).fetchall()
        chunks = conn.execute(
            f"SELECT feature_idx, chunk_index, chunk_data "
            f"FROM read_parquet('{staging / 'amplicon_sequence_chunks' / 'part_*.parquet'}') "
            f"ORDER BY feature_idx, chunk_index"
        ).fetchall()

    assert seqs == [(100, ha, 1), (200, hb, 1), (300, hc, 1)]
    assert chunks == [(100, 0, "A"), (200, 0, "B"), (300, 0, "C")]


def test_unmapped_hash_fails_loud(inputs):
    """A manifest hash with no minted feature_idx is a permanent upstream error —
    build_feature_id_map raises rather than silently dropping the ASV."""
    inp, ws = inputs
    # drop feature 300 from the map so hash C has no feature_idx.
    _write(
        inp.feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(_hash("A"), 100), (_hash("B"), 200)],
    )
    with pytest.raises(ValueError, match="unmapped sequence hash"):
        asyncio.run(execute(inp, ws))


def test_missing_input_raises_file_not_found(tmp_path):
    inp = Inputs(
        asv_counts=tmp_path / "nope.parquet",
        feature_map=tmp_path / "nope2.parquet",
        manifest=tmp_path / "nope3.parquet",
        asv_chunks=tmp_path / "nope_chunks",
        processing_idx=1,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(execute(inp, tmp_path / "ws"))
