"""real-miint smoke tests for the amplicon_deblur reference-agnostic core.

The full chain (SortMeRNA → UCHIME → MAFFT → deblur) needs the GPL-boundary
tools and real 16S input, and is validated on a live stack; here we pin the
deterministic output writer (`_write_outputs`) with real miint scalars:
per-sample counts, the mint-features manifest, and the hash-keyed chunk
directory carrying each ASV's bytes.
"""

from __future__ import annotations

import duckdb

from qiita_compute_orchestrator.jobs.amplicon_deblur import _write_outputs
from qiita_compute_orchestrator.miint import open_miint_conn


def test_write_outputs_emits_counts_manifest_and_chunks(tmp_path):
    """`_write_outputs` writes per-sample counts, a (read_id, sequence_hash,
    sequence_length_bp) manifest, and a hash-keyed chunk directory whose chunks
    reassemble to each ASV's bytes."""
    counts_out = tmp_path / "asv_counts.parquet"
    manifest_out = tmp_path / "manifest.parquet"
    chunks_dir = tmp_path / "asv_chunks"
    with open_miint_conn() as conn:
        # the chunked-parquet writer needs this (the job's apply_duckdb_settings
        # sets it); without it the ROW_GROUP_SIZE_BYTES option errors.
        conn.execute("SET preserve_insertion_order=false")
        conn.execute(
            "CREATE TABLE asv AS SELECT * FROM (VALUES "
            "  (11, md5('AAAA')::uuid, 7), (12, md5('CCCC')::uuid, 3)"
            ") AS t(prep_sample_idx, sequence_hash, count)"
        )
        conn.execute(
            "CREATE TABLE asv_seq AS SELECT * FROM (VALUES "
            "  (md5('AAAA')::uuid, 'AAAA', 4), (md5('CCCC')::uuid, 'CCCC', 4)"
            ") AS t(sequence_hash, sequence1, sequence_length_bp)"
        )
        _write_outputs(
            conn, counts_out=counts_out, manifest_out=manifest_out, chunks_dir=chunks_dir
        )

    with duckdb.connect(":memory:") as conn:
        counts = conn.execute(
            f"SELECT prep_sample_idx, count FROM read_parquet('{counts_out}') "
            f"ORDER BY prep_sample_idx"
        ).fetchall()
        manifest_cols = [
            r[0]
            for r in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{manifest_out}')"
            ).fetchall()
        ]
        reassembled = conn.execute(
            f"SELECT string_agg(chunk_data, '' ORDER BY chunk_index) "
            f"FROM read_parquet('{chunks_dir / 'part_*.parquet'}') GROUP BY sequence_hash "
            f"ORDER BY min(chunk_data)"
        ).fetchall()

    assert counts == [(11, 7), (12, 3)]
    assert manifest_cols == ["read_id", "sequence_hash", "sequence_length_bp"]
    assert [r[0] for r in reassembled] == ["AAAA", "CCCC"]
    assert not (tmp_path / "asv_chunks.partial").exists()
