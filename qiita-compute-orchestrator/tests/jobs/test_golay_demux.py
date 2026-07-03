"""Unit tests for the golay_demux job (ported demux_qiita.sql + per-sample ingest).

synthetic I1/R1 FASTQ and a tiny identity Golay table exercise the real demux SQL on
the staged miint build; the sequence-range mint is monkeypatched. pins: each read is
assigned to the sample whose barcode its RC'd I1 index matches, unmatched reads are
dropped, and each sample's reads land in read/<prep_sample_idx>.parquet with contiguous
minted sequence_idx and R1 sequence+quality carried as sequence1/qual1.
"""

from __future__ import annotations

import asyncio

import duckdb
import pytest

from qiita_compute_orchestrator.jobs import golay_demux as gd
from qiita_compute_orchestrator.jobs.golay_demux import Inputs, execute
from qiita_compute_orchestrator.sequence_range import MintedSequenceRange

_COMPLEMENT = str.maketrans("ACGT", "TGCA")


def _rc(seq: str) -> str:
    return seq.translate(_COMPLEMENT)[::-1]


# two 12-nt sample barcodes
_BC1 = "AAAACCCCGGGG"  # prep_sample 11
_BC2 = "ACACACACACAC"  # prep_sample 12


@pytest.fixture
def fake_mint(monkeypatch):
    """monkeypatch mint_sequence_range; records the (prep_sample_idx, count) calls."""
    calls: list[tuple[int, int]] = []

    async def _fake(*, http, prep_sample_idx, count):
        calls.append((prep_sample_idx, count))
        return MintedSequenceRange(
            prep_sample_idx=prep_sample_idx,
            sequence_idx_start=1000,
            sequence_idx_stop=1000 + count - 1,
        )

    monkeypatch.setattr(gd, "mint_sequence_range", _fake)
    return calls


def _write_fastq(path, records: list[tuple[str, str, str]]) -> None:
    """records: (read_id, sequence, qual)."""
    with open(path, "w") as fh:
        for rid, seq, qual in records:
            fh.write(f"@{rid}\n{seq}\n+\n{qual}\n")


def _tiny_golay(path, barcodes: list[str]) -> None:
    """identity Golay table (raw==corrected, errors 0) plus one distractor code."""
    values = ", ".join(f"('{b}', '{b}', 0)" for b in [*barcodes, "GGGGGGGGGGGG"])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t(raw, corrected, errors)) "
            f"TO '{path}' (FORMAT PARQUET)"
        )


def _barcode_map(path, mapping: dict[int, str], *, barcodes_are_rc: bool = False) -> None:
    """roster Parquet `(prep_sample_idx, barcode, barcodes_are_rc)`; the RC flag is per-barcode."""
    rc = "TRUE" if barcodes_are_rc else "FALSE"
    values = ", ".join(f"({p}::BIGINT, '{b}', {rc})" for p, b in mapping.items())
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) "
            "AS t(prep_sample_idx, barcode, barcodes_are_rc)) "
            f"TO '{path}' (FORMAT PARQUET)"
        )


def _inputs(tmp_path, i1_records, r1_records) -> Inputs:
    i1 = tmp_path / "I1.fastq"
    r1 = tmp_path / "R1.fastq"
    _write_fastq(i1, i1_records)
    _write_fastq(r1, r1_records)
    golay = tmp_path / "golay.parquet"
    _tiny_golay(golay, [_BC1, _BC2])
    bc = tmp_path / "barcode_map.parquet"
    _barcode_map(bc, {11: _BC1, 12: _BC2}, barcodes_are_rc=False)
    return Inputs(
        index_reads_path=i1,
        forward_reads_path=r1,
        golay_table_path=golay,
        barcode_map=bc,
        golay_error_threshold=1.5,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=7,
        sequencing_run_idx=3,
        work_ticket_idx=1,
    )


def _read(path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT prep_sample_idx, sequence_idx, read_id, sequence1, "
            "       qual1 IS NOT NULL AS has_qual, sequence2, qual2 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def test_demux_assigns_reads_to_samples(fake_mint, tmp_path):
    """two reads to sample 11, one to sample 12, one junk index dropped."""
    # I1-RC == barcode (identity Golay table), so I1 = RC(barcode)
    i1 = [
        ("r1", _rc(_BC1), "I" * 12),
        ("r2", _rc(_BC1), "I" * 12),
        ("r3", _rc(_BC2), "I" * 12),
        ("r4", "TTTTTTTTTTTT", "I" * 12),  # RC not a barcode, dropped
    ]
    r1 = [
        ("r1", "TACGGAGGGTGCAAGCGTTA", "I" * 20),
        ("r2", "TACGGAGGGTGCAAGCGTTC", "I" * 20),
        ("r3", "GACGGAGGGTGCAAGCGTTG", "I" * 20),
        ("r4", "CCCCGGGGAAAATTTTACGT", "I" * 20),
    ]
    outputs = asyncio.run(execute(_inputs(tmp_path, i1, r1), tmp_path / "ws"))
    ws = outputs["read_staging_dir"]

    # mint called per sample with the demuxed count, ascending prep idx
    assert sorted(fake_mint) == [(11, 2), (12, 1)]

    s11 = _read(ws / "read" / "11.parquet")
    s12 = _read(ws / "read" / "12.parquet")
    assert [r[1] for r in s11] == [1000, 1001]  # contiguous from minted start
    assert {r[3] for r in s11} == {"TACGGAGGGTGCAAGCGTTA", "TACGGAGGGTGCAAGCGTTC"}
    assert all(r[0] == 11 and r[4] and r[5] is None and r[6] is None for r in s11)
    assert [r[1] for r in s12] == [1000]
    assert s12[0][3] == "GACGGAGGGTGCAAGCGTTG"

    # demux-stats sidecar reports per-sample counts
    with duckdb.connect(":memory:") as conn:
        stats = conn.execute(
            f"SELECT prep_sample_idx, demultiplexed_read_count "
            f"FROM read_csv('{ws / 'demultiplex-stats.tsv'}', delim='\t', header=true) "
            "ORDER BY prep_sample_idx"
        ).fetchall()
    assert stats == [(11, 2), (12, 1)]

    # the durable per-sample copy (what read-mask consumes) exists too
    from qiita_common.api_paths import compute_reads_staging_path

    assert compute_reads_staging_path(tmp_path / "staging", 11).exists()


def test_revcomp_barcodes_flag(fake_mint, tmp_path):
    """a barcode's barcodes_are_rc=True RC's it before the Golay join."""
    i1 = tmp_path / "I1.fastq"
    r1 = tmp_path / "R1.fastq"
    _write_fastq(i1, [("r1", _BC1, "I" * 12)])  # I1 = BC1 forward
    _write_fastq(r1, [("r1", "TACGGAGGGTGCAAGCGTTA", "I" * 20)])
    golay = tmp_path / "golay.parquet"
    _tiny_golay(golay, [_rc(_BC1)])  # corrected == raw == RC(BC1)
    bc = tmp_path / "barcode_map.parquet"
    _barcode_map(bc, {11: _BC1}, barcodes_are_rc=True)  # recorded forward, RC'd at run time

    inputs = Inputs(
        index_reads_path=i1,
        forward_reads_path=r1,
        golay_table_path=golay,
        barcode_map=bc,
        golay_error_threshold=1.5,
        reads_staging_root=tmp_path / "staging",
        sequenced_pool_idx=7,
        sequencing_run_idx=3,
        work_ticket_idx=1,
    )
    outputs = asyncio.run(execute(inputs, tmp_path / "ws"))
    assert fake_mint == [(11, 1)]
    assert len(_read(outputs["read_staging_dir"] / "read" / "11.parquet")) == 1


def test_no_barcode_match_is_no_data(fake_mint, tmp_path):
    """every read's index matches no prep barcode, so StepNoData and no mint."""
    from qiita_common.backend_failure import StepNoData

    i1 = [("r1", "TTTTTTTTTTTT", "I" * 12), ("r2", "TTTTTTTTTTTT", "I" * 12)]
    r1 = [("r1", "TACGGAGGGTGCAAGCGTTA", "I" * 20), ("r2", "TACGGAGGGTGCAAGCGTTC", "I" * 20)]
    with pytest.raises(StepNoData):
        asyncio.run(execute(_inputs(tmp_path, i1, r1), tmp_path / "ws"))
    assert fake_mint == []


def test_missing_input_raises(tmp_path):
    inputs = _inputs(tmp_path, [("r1", _rc(_BC1), "I" * 12)], [("r1", "ACGT", "IIII")])
    inputs.index_reads_path = tmp_path / "absent.fastq"
    with pytest.raises(FileNotFoundError):
        asyncio.run(execute(inputs, tmp_path / "ws"))
