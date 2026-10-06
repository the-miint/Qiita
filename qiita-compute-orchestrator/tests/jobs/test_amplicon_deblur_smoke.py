"""real-miint smoke tests for the amplicon_deblur reference-agnostic core.

The full chain (SortMeRNA → UCHIME → MAFFT → deblur) needs the GPL-boundary
tools and real 16S input, and is validated on a live stack; here we pin the two
deterministic pieces this PR touches with real miint scalars:

  * the primer-orient step (`_set_session_vars` + `_ORIENT_SQL`) — strips the
    primer, orients both strands, and keeps the full read after the primer
    (ambiguity codes, mixed case) rather than truncating it;
  * the output writer (`_write_outputs`) — per-sample counts, the mint-features
    manifest, and the hash-keyed chunk directory carrying each ASV's bytes.
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from qiita_compute_orchestrator.jobs.amplicon_deblur import (
    _ORIENT_SQL,
    _set_session_vars,
    _write_outputs,
)
from qiita_compute_orchestrator.miint import open_miint_conn

# a concrete instantiation of the EMP 515F primer (Y->C, M->A).
_PRIMER = "GTGYCAGCMGCCGCGGTAA"
_PRIMER_CONCRETE = "GTGCCAGCAGCCGCGGTAA"
_REF = Path("/tmp/ref.fasta")


def _revcomp(conn, seq: str) -> str:
    return conn.execute("SELECT sequence_dna_reverse_complement(?)", [seq]).fetchone()[0]


def _oriented(conn, reads: list[tuple[int, int, str]]) -> list[str]:
    conn.execute(
        "CREATE OR REPLACE TABLE _inputs "
        "(sample_id BIGINT, sequence_index BIGINT, sequence1 VARCHAR)"
    )
    conn.executemany("INSERT INTO _inputs VALUES (?, ?, ?)", reads)
    conn.execute(_ORIENT_SQL)
    return [
        r[0]
        for r in conn.execute(
            "SELECT sequence1 FROM _inputs_oriented ORDER BY sequence_index"
        ).fetchall()
    ]


def test_orient_strips_primer_on_both_strands():
    """A forward read and its reverse complement both orient to the same
    primer-stripped body."""
    body = "ACGTACGTACGTACGTACGT"
    with open_miint_conn() as conn:
        _set_session_vars(conn, primer=_PRIMER, trim=150, sortmerna_ref=_REF, orient=True)
        fwd = _PRIMER_CONCRETE + body
        rev = _revcomp(conn, fwd)
        out = _oriented(conn, [(1, 0, fwd), (1, 1, rev)])
    assert out == [body, body]


def test_orient_keeps_ambiguity_and_lowercase():
    """`(.+)` keeps an N after the primer (which `[ATGC]+` would truncate), and
    upper-casing the read lets a lowercase primer region still match."""
    with open_miint_conn() as conn:
        _set_session_vars(conn, primer=_PRIMER, trim=150, sortmerna_ref=_REF, orient=True)
        with_n = _PRIMER_CONCRETE + "ACGTNACGT"
        lowercase = (_PRIMER_CONCRETE + "ACGTACGT").lower()
        out = _oriented(conn, [(1, 0, with_n), (1, 1, lowercase)])
    assert out == ["ACGTNACGT", "ACGTACGT"]


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
