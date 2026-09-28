"""real-miint smoke for golay_demux's demux half (`_run_demux`).

exercise the in-job Golay cloud, the revcomp, and the barcode→prep_sample
assignment on tiny synthetic FASTQ, using miint's read_fastx and
sequence_dna_reverse_complement. the ingest half is the shared
mint_or_reuse_sequence_range, already covered by test_ingest_reads.
"""

from __future__ import annotations

import gzip
from pathlib import Path

import duckdb
import pytest

from qiita_compute_orchestrator.jobs.golay_demux import (
    Inputs,
    _bits_to_dna,
    _golay_codeword,
    _run_demux,
)
from qiita_compute_orchestrator.miint import duckdb_tmp_dir, open_miint_conn


def _write_fastq_gz(path: Path, records: list[tuple[str, str]]) -> None:
    body = "".join(f"@{rid}\n{seq}\n+\n{'I' * len(seq)}\n" for rid, seq in records)
    path.write_bytes(gzip.compress(body.encode()))


def _rc(seq: str) -> str:
    """miint's hardened reverse-complement (not a hand-rolled map)."""
    with open_miint_conn() as conn:
        return conn.execute("SELECT sequence_dna_reverse_complement(?)", [seq]).fetchone()[0]


def _write_barcode_map(path: Path, roster: list[tuple[int, str, bool]]) -> None:
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "CREATE TEMP TABLE t (prep_sample_idx BIGINT, barcode VARCHAR, barcodes_are_rc BOOLEAN)"
        )
        conn.executemany("INSERT INTO t VALUES (?,?,?)", roster)
        conn.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


def test_run_demux_assigns_reads_by_golay_barcode(tmp_path):
    """Two valid Golay barcodes (codewords) → two samples. Each I1 read carries the
    RC of its sample's barcode (Illumina submits I1 RC to the prep), so after the
    demux RCs it back and joins the in-job cloud, its paired R1 read lands under the
    right prep_sample."""
    bc0 = _bits_to_dna(_golay_codeword(0))  # "CCCCCCCCCCCC"
    bc1 = _bits_to_dna(_golay_codeword(1))
    assert bc0 != bc1

    # I1 reads = RC(barcode): the demux RCs I1 back to the codeword before matching.
    i1 = tmp_path / "I1.fastq.gz"
    r1 = tmp_path / "R1.fastq.gz"
    _write_fastq_gz(i1, [("read0", _rc(bc0)), ("read1", _rc(bc1))])
    _write_fastq_gz(r1, [("read0", "ACGTACGTACGTACGT"), ("read1", "TTTTGGGGCCCCAAAA")])

    bc_map = tmp_path / "barcode_map.parquet"
    _write_barcode_map(bc_map, [(11, bc0, False), (12, bc1, False)])

    inputs = Inputs(
        index_reads_path=i1,
        forward_reads_path=r1,
        barcode_map=bc_map,
        golay_error_threshold=1.5,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )
    demuxed = tmp_path / "_demuxed.parquet"
    with duckdb_tmp_dir(tmp_path / "ws") as duckdb_tmp:
        _run_demux(inputs, demuxed, duckdb_tmp, memory_gb=2)

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, sequence1 FROM read_parquet('{demuxed}') "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert rows == [(11, "ACGTACGTACGTACGT"), (12, "TTTTGGGGCCCCAAAA")]


def test_run_demux_drops_unmatched_index(tmp_path):
    """An I1 read that decodes to no prep barcode (too many errors) is dropped —
    non-matching reads simply don't appear in the demuxed output."""
    bc0 = _bits_to_dna(_golay_codeword(0))
    i1 = tmp_path / "I1.fastq.gz"
    r1 = tmp_path / "R1.fastq.gz"
    # A garbage index (all A → far from the all-C codeword) matches nothing.
    _write_fastq_gz(i1, [("read0", _rc(bc0)), ("bad", "AAAAAAAAAAAA")])
    _write_fastq_gz(r1, [("read0", "ACGTACGTACGTACGT"), ("bad", "GGGGGGGGGGGGGGGG")])
    bc_map = tmp_path / "barcode_map.parquet"
    _write_barcode_map(bc_map, [(11, bc0, False)])

    inputs = Inputs(
        index_reads_path=i1,
        forward_reads_path=r1,
        barcode_map=bc_map,
        golay_error_threshold=1.5,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )
    demuxed = tmp_path / "_demuxed.parquet"
    with duckdb_tmp_dir(tmp_path / "ws") as duckdb_tmp:
        _run_demux(inputs, demuxed, duckdb_tmp, memory_gb=2)

    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(f"SELECT prep_sample_idx FROM read_parquet('{demuxed}')").fetchall()
    assert rows == [(11,)]


def _inputs(tmp_path, i1, r1, bc_map, *, r2=None):
    return Inputs(
        index_reads_path=i1,
        forward_reads_path=r1,
        reverse_reads_path=r2,
        barcode_map=bc_map,
        golay_error_threshold=1.5,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=1,
        sequencing_run_idx=1,
        work_ticket_idx=1,
    )


def test_run_demux_raises_on_barcode_that_is_not_a_codeword(tmp_path):
    """A barcode decoding to no codeword fails loud, not silent-drop. Here a real
    codeword flagged barcodes_are_rc, whose RC is (almost always) not a codeword."""
    bc1 = _bits_to_dna(_golay_codeword(1))
    i1, r1 = tmp_path / "I1.fastq.gz", tmp_path / "R1.fastq.gz"
    _write_fastq_gz(i1, [("r", _rc(bc1))])
    _write_fastq_gz(r1, [("r", "ACGTACGTACGTACGT")])
    bc_map = tmp_path / "barcode_map.parquet"
    _write_barcode_map(bc_map, [(11, bc1, True)])  # wrong rc flag
    with (
        duckdb_tmp_dir(tmp_path / "ws") as duckdb_tmp,
        pytest.raises(ValueError, match="no Golay codeword"),
    ):
        _run_demux(
            _inputs(tmp_path, i1, r1, bc_map),
            tmp_path / "_demuxed.parquet",
            duckdb_tmp,
            memory_gb=2,
        )


def test_run_demux_carries_r2(tmp_path):
    """R2 rides through as sequence2 when reverse_reads_path is set."""
    bc0 = _bits_to_dna(_golay_codeword(0))
    i1, r1, r2 = tmp_path / "I1.fastq.gz", tmp_path / "R1.fastq.gz", tmp_path / "R2.fastq.gz"
    _write_fastq_gz(i1, [("read0", _rc(bc0))])
    _write_fastq_gz(r1, [("read0", "ACGTACGTACGTACGT")])
    _write_fastq_gz(r2, [("read0", "TTTTGGGGCCCCAAAA")])
    bc_map = tmp_path / "barcode_map.parquet"
    _write_barcode_map(bc_map, [(11, bc0, False)])
    demuxed = tmp_path / "_demuxed.parquet"
    with duckdb_tmp_dir(tmp_path / "ws") as duckdb_tmp:
        _run_demux(_inputs(tmp_path, i1, r1, bc_map, r2=r2), demuxed, duckdb_tmp, memory_gb=2)
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(
            f"SELECT prep_sample_idx, sequence1, sequence2 FROM read_parquet('{demuxed}')"
        ).fetchall()
    assert rows == [(11, "ACGTACGTACGTACGT", "TTTTGGGGCCCCAAAA")]
