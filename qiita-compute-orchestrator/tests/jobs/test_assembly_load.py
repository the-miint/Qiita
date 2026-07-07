"""Tests for the assembly_load native job.

Calls execute() directly. Synthesizes assembly_hash's outputs (manifest,
hash-keyed assembly_chunks, bin_map) + mint-features' feature_map + the container
CheckM / DAS_Tool TSVs, and asserts the four DuckLake-shape staging Parquets:
assembled_sequence (reused reference_load writer), assembled_sequence_chunks
(reused writer, re-keyed to feature_idx), assembly_membership (the DuckLake copy),
and bin_quality (CSV-read). No FASTA and no real hashing here — fixtures carry the
sequence_hash/feature_idx directly so the test isolates the re-key + lift logic.
"""

from __future__ import annotations

import asyncio
import hashlib
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

# Three contigs across two bins + one circular genome. bin.1 has two contigs;
# bin.2 shares bin.1:x1's bytes (same hash -> same feature_idx) to exercise the
# distinct-membership / dedup path.
_SEQUENCES = {
    "LCG:circ1:c1": ("AAAACCCCGGGGTTTT", 100),
    "MAG:bin.1:x1": ("ACGTACGTACGTACGT", 200),
    "MAG:bin.1:x2": ("TTTTGGGGCCCCAAAA", 300),
    "MAG:bin.2:y1": ("ACGTACGTACGTACGT", 200),  # identical bytes to x1
}


def _hash(seq: str) -> UUID:
    return UUID(hashlib.md5(seq.encode()).hexdigest())


def _bin_kind(read_id: str) -> tuple[str, str]:
    kind, bin_id, _contig = read_id.split(":")
    return kind, bin_id


def _write(path: Path, schema: str, rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(":memory:") as conn:
        conn.execute(f"CREATE TEMP TABLE t ({schema})")
        if rows:
            placeholders = ", ".join("?" for _ in rows[0])
            conn.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def _run(inputs, workspace) -> dict:
    from qiita_compute_orchestrator.jobs.assembly_load import execute

    return asyncio.run(execute(inputs, workspace))


@pytest.fixture
def staging_inputs(tmp_path):
    """Synthesize assembly_hash's manifest / assembly_chunks / bin_map +
    mint-features' feature_map. Returns the dict spread into Inputs."""
    manifest = tmp_path / "manifest.parquet"
    _write(
        manifest,
        "read_id VARCHAR, sequence_hash UUID, sequence_length_bp BIGINT",
        [(rid, str(_hash(seq)), len(seq)) for rid, (seq, _f) in _SEQUENCES.items()],
    )

    # feature_map: one row per DISTINCT sequence_hash -> feature_idx.
    distinct = {_hash(seq): fidx for _rid, (seq, fidx) in _SEQUENCES.items()}
    feature_map = tmp_path / "feature_map.parquet"
    _write(
        feature_map,
        "sequence_hash UUID, feature_idx BIGINT",
        [(str(h), fidx) for h, fidx in distinct.items()],
    )

    # assembly_chunks: hash-keyed dir of part_*.parquet, one row per distinct hash.
    chunks = tmp_path / "assembly_chunks"
    chunks.mkdir()
    _write(
        chunks / "part_00000.parquet",
        "sequence_hash UUID, chunk_index INTEGER, chunk_data VARCHAR",
        [
            (str(h), 0, seq)
            for seq, h in {seq: _hash(seq) for seq, _ in _SEQUENCES.values()}.items()
        ],
    )

    bin_map = tmp_path / "bin_map.parquet"
    _write(
        bin_map,
        "read_id VARCHAR, kind VARCHAR, bin_id VARCHAR",
        [(rid, *_bin_kind(rid)) for rid in _SEQUENCES],
    )

    return {
        "manifest": manifest,
        "feature_map": feature_map,
        "assembly_chunks": chunks,
        "bin_map": bin_map,
    }


def _inputs(tmp_path, staging_inputs, *, checkm_rows=None, das_rows=None):
    from qiita_compute_orchestrator.jobs.assembly_load import Inputs

    checkm_dir = tmp_path / "checkm"
    refined_dir = tmp_path / "refined"
    checkm_dir.mkdir(exist_ok=True)
    refined_dir.mkdir(exist_ok=True)
    if checkm_rows is not None:
        header = (
            "genome_local_id\tmarker_lineage\tcompleteness\tcontamination\t"
            "strain_heterogeneity\tgenome_size\tn_contigs\n"
        )
        (checkm_dir / "checkm_quality.tsv").write_text(
            header + "".join("\t".join(str(c) for c in r) + "\n" for r in checkm_rows)
        )
    if das_rows is not None:
        (refined_dir / "das_tool_scores.tsv").write_text(
            "genome_local_id\tdas_tool_score\tsource_binner\n"
            + "".join("\t".join(str(c) for c in r) + "\n" for r in das_rows)
        )
    return Inputs(
        checkm_dir=checkm_dir,
        refined_bins_dir=refined_dir,
        processing_idx=77,
        prep_sample_idx=42,
        work_ticket_idx=7,
        **staging_inputs,
    )


def _rows(pq, cols, order):
    with duckdb.connect(":memory:") as con:
        return con.execute(f"SELECT {cols} FROM read_parquet('{pq}') ORDER BY {order}").fetchall()


def _schema(pq):
    with duckdb.connect(":memory:") as con:
        return {c[0]: c[1] for c in con.execute(f"DESCRIBE SELECT * FROM '{pq}'").fetchall()}


def test_reused_writers_emit_feature_keyed_sequences_and_chunks(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    staging = out["staging_dir"]

    # assembled_sequence.parquet — one row per DISTINCT feature_idx (reused writer).
    seq = _rows(
        staging / "assembled_sequence.parquet",
        "feature_idx, CAST(sequence_hash AS VARCHAR), sequence_length_bp",
        "feature_idx",
    )
    assert seq == [
        (100, str(_hash("AAAACCCCGGGGTTTT")), 16),
        (200, str(_hash("ACGTACGTACGTACGT")), 16),
        (300, str(_hash("TTTTGGGGCCCCAAAA")), 16),
    ]

    # assembled_sequence_chunks/ — directory of part files, keyed by feature_idx.
    chunks_dir = staging / "assembled_sequence_chunks"
    assert chunks_dir.is_dir()
    glob = str(chunks_dir / "part_*.parquet")
    assert _schema(chunks_dir / "part_00000.parquet") == {
        "feature_idx": "BIGINT",
        "chunk_index": "INTEGER",
        "chunk_data": "VARCHAR",
    }
    with duckdb.connect(":memory:") as con:
        reassembled = dict(
            con.execute(
                "SELECT feature_idx, string_agg(chunk_data, '' ORDER BY chunk_index) "
                "FROM read_parquet(?) GROUP BY feature_idx",
                [glob],
            ).fetchall()
        )
    assert reassembled == {
        100: "AAAACCCCGGGGTTTT",
        200: "ACGTACGTACGTACGT",
        300: "TTTTGGGGCCCCAAAA",
    }


def test_assembly_membership_parquet_lifts_bins_to_feature_idx(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "assembly_membership.parquet"
    assert _schema(pq) == {
        "prep_sample_idx": "BIGINT",
        "processing_idx": "BIGINT",
        "kind": "VARCHAR",
        "bin_id": "VARCHAR",
        "feature_idx": "BIGINT",
    }
    rows = _rows(pq, "kind, bin_id, feature_idx", "kind, bin_id, feature_idx")
    # bin.2 shares x1's feature (200) but keeps its own distinct membership row.
    assert rows == [
        ("LCG", "circ1", 100),
        ("MAG", "bin.1", 200),
        ("MAG", "bin.1", 300),
        ("MAG", "bin.2", 200),
    ]
    stamps = _rows(pq, "DISTINCT prep_sample_idx, processing_idx", "1")
    assert stamps == [(42, 77)]


def test_bin_quality_joins_checkm_and_das(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=[("bin.1", 0.87, "metabat2")],
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    assert _schema(pq) == {
        "prep_sample_idx": "BIGINT",
        "processing_idx": "BIGINT",
        "kind": "VARCHAR",
        "bin_id": "VARCHAR",
        "marker_lineage": "VARCHAR",
        "completeness": "DOUBLE",
        "contamination": "DOUBLE",
        "strain_heterogeneity": "DOUBLE",
        "genome_size": "BIGINT",
        "n_contigs": "BIGINT",
        "das_tool_score": "DOUBLE",
        "source_binner": "VARCHAR",
    }
    rows = _rows(
        pq,
        "prep_sample_idx, processing_idx, kind, bin_id, completeness, contamination, "
        "genome_size, n_contigs, das_tool_score, source_binner",
        "bin_id",
    )
    assert rows == [(42, 77, "MAG", "bin.1", 95.5, 1.2, 10000, 2, 0.87, "metabat2")]


def test_bin_quality_without_das_scores_is_null(tmp_path, staging_inputs):
    inputs = _inputs(
        tmp_path,
        staging_inputs,
        checkm_rows=[("bin.1", "k__Bacteria", 95.5, 1.2, 0.0, 10000, 2)],
        das_rows=None,  # no das_tool_scores.tsv
    )
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    rows = _rows(pq, "bin_id, completeness, das_tool_score, source_binner", "bin_id")
    assert rows == [("bin.1", 95.5, None, None)]


def test_lcg_only_writes_empty_bin_quality(tmp_path, staging_inputs):
    """No CheckM table (LCG-only, or CheckM DB absent) -> valid empty bin_quality
    with the right schema; the sequences/membership still store."""
    inputs = _inputs(tmp_path, staging_inputs, checkm_rows=None, das_rows=None)
    out = _run(inputs, tmp_path / "ws")
    pq = out["staging_dir"] / "bin_quality.parquet"
    # Schema present, zero rows.
    assert _schema(pq)["completeness"] == "DOUBLE"
    with duckdb.connect(":memory:") as con:
        n = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    assert n == 0
    # Membership still written.
    assert (out["staging_dir"] / "assembly_membership.parquet").exists()


def test_missing_manifest_raises_file_not_found(tmp_path, staging_inputs):
    si = dict(staging_inputs)
    si["manifest"] = tmp_path / "nope.parquet"
    with pytest.raises(FileNotFoundError):
        _run(_inputs(tmp_path, si), tmp_path / "ws")
