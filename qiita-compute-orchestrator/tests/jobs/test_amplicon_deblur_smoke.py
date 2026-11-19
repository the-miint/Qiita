"""real-miint smoke tests for the amplicon_deblur reference-agnostic core.

The full chain (SortMeRNA → UCHIME → MAFFT → deblur) needs the GPL-boundary
tools and real 16S input, and is validated on a live stack; here we pin the
deterministic output writer (`_write_outputs`) with real miint scalars:
per-sample counts, the mint-features manifest, and asv_sequences carrying each
ASV's bytes inline.
"""

from __future__ import annotations

import duckdb

from qiita_compute_orchestrator.jobs.amplicon_deblur import _write_outputs
from qiita_compute_orchestrator.miint import open_miint_conn


def test_write_outputs_emits_counts_manifest_and_sequences(tmp_path):
    """`_write_outputs` writes per-sample counts, a (read_id, sequence_hash,
    sequence_length_bp) manifest, and asv_sequences (sequence_hash, sequence) with each
    ASV's bytes inline."""
    counts_out = tmp_path / "asv_counts.parquet"
    manifest_out = tmp_path / "manifest.parquet"
    sequences_out = tmp_path / "asv_sequences.parquet"
    with open_miint_conn() as conn:
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
            conn, counts_out=counts_out, manifest_out=manifest_out, sequences_out=sequences_out
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
        sequences = conn.execute(
            f"SELECT sequence FROM read_parquet('{sequences_out}') ORDER BY sequence"
        ).fetchall()

    assert counts == [(11, 7), (12, 3)]
    assert manifest_cols == ["read_id", "sequence_hash", "sequence_length_bp"]
    assert [r[0] for r in sequences] == ["AAAA", "CCCC"]
    assert not (tmp_path / "asv_sequences.parquet.partial").exists()
