"""Real-miint smoke tests for `qc.execute` (seams NOT stubbed).

Runs the actual `trim_adapters` / `trim_adapters_pe` / `trim_polyg` /
`filter_read` chain end-to-end and pins the behavior the stubbed unit tests
cannot see:

  - SE: a 3' adapter is removed, and a read that falls below min_length (100)
    AFTER trimming is dropped while a long one survives with the insert recovered;
  - PE: a pair drops when EITHER mate is below min_length after trimming;
  - polyG is applied ONLY for a 2-color instrument — the same low-quality 3'
    G-run is trimmed on a NextSeq run but retained on a MiSeq run.

Runs against the team-mirror miint build (conftest stages it). The
`write_reads_q` fixture (tests/jobs/conftest.py) owns the quality-carrying
reads.parquet schema.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb

# A clean 120 nt insert (>= the QC min_length of 100). Deliberately G-FREE: a G
# in the insert tail would let fastp's polyG run extend into high-quality bases
# and suppress trimming, confusing the polyG case — a real fastp behavior, but
# not what this fixture is isolating.
_INSERT = "ACTACTACTA" * 12
assert len(_INSERT) == 120 and "G" not in _INSERT
# A 40 nt insert: below min_length once it stands alone (after adapter trim).
_SHORT = "ACTACTACTA" * 4
assert len(_SHORT) == 40
_ADAPTER = "AGATCGGAAGAGC"


def _revcomp(seq: str) -> str:
    return seq.translate(str.maketrans("ACGT", "TGCA"))[::-1]


def _q(seq: str, val: int = 35) -> list[int]:
    return [val] * len(seq)


def _adapter_parquet(tmp_path: Path) -> Path:
    """The runner-staged adapter Parquet (columns feature_idx, sequence) the qc
    job reads via read_parquet."""
    p = tmp_path / "adapters.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE a(feature_idx BIGINT, sequence VARCHAR)")
        conn.execute("INSERT INTO a VALUES (0, ?)", [_ADAPTER])
        conn.execute(f"COPY a TO '{p}' (FORMAT PARQUET)")
    return p


def _read_seqs(path: Path) -> dict[int, str]:
    """Map surviving sequence_idx -> sequence1."""
    with duckdb.connect(":memory:") as conn:
        return {
            r[0]: r[1]
            for r in conn.execute(
                f"SELECT sequence_idx, sequence1 FROM read_parquet('{path}')"
            ).fetchall()
        }


def test_qc_smoke_se_adapter_trim_and_length_filter(tmp_path, write_reads_q, read_survivors):
    """SE: the adaptered long read survives with the insert recovered; the
    adaptered short read drops below min_length=100 after trimming."""
    from qiita_compute_orchestrator.jobs import qc

    long_read = _INSERT + _ADAPTER
    short_read = _SHORT + _ADAPTER
    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (10, "long", long_read, _q(long_read), None, None),
            (20, "short", short_read, _q(short_read), None, None),
        ],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        instrument_model="Illumina MiSeq",  # not 2-color: no polyG
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["reads"]) == [10]
    # adapter removed -> the bare insert is recovered.
    assert _read_seqs(out["reads"])[10] == _INSERT


def test_qc_smoke_pe_pair_drop_when_one_mate_short(tmp_path, write_reads_q, read_survivors):
    """PE: a pair with both mates long survives; a pair with one mate short (after
    adapter trim) is dropped whole."""
    from qiita_compute_orchestrator.jobs import qc

    r1 = _INSERT + _ADAPTER
    r2 = _revcomp(_INSERT) + _ADAPTER
    short2 = _SHORT + _ADAPTER
    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (30, "both_ok", r1, _q(r1), r2, _q(r2)),
            (40, "r2_short", r1, _q(r1), short2, _q(short2)),
        ],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert read_survivors(out["reads"]) == [30]


def test_qc_smoke_polyg_gated_on_instrument(tmp_path, write_reads_q, read_survivors):
    """The SAME low-quality 3' G-run is trimmed on a 2-color (NextSeq) run but
    retained on a non-2-color (MiSeq) run — proving polyG is gated on the
    instrument model."""
    from qiita_compute_orchestrator.jobs import qc

    g_run = "G" * 16
    seq = _INSERT + g_run  # no adapter; isolates polyG behavior
    qual = _q(_INSERT) + _q(g_run, 2)  # low quality on the G-run (2-color no-signal)
    rows = [(50, "polyg", seq, qual, None, None)]

    nextseq = qc.Inputs(
        reads=write_reads_q(tmp_path / "ns.parquet", rows),
        adapter_parquet=_adapter_parquet(tmp_path),
        instrument_model="NextSeq 550",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    miseq = qc.Inputs(
        reads=write_reads_q(tmp_path / "ms.parquet", rows),
        adapter_parquet=_adapter_parquet(tmp_path),
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    ns_out = asyncio.run(qc.execute(nextseq, tmp_path / "ws_ns"))
    ms_out = asyncio.run(qc.execute(miseq, tmp_path / "ws_ms"))

    # Both survive (>= 100 nt either way), but only the 2-color run had its G-run
    # trimmed back to the bare insert.
    assert read_survivors(ns_out["reads"]) == [50]
    assert read_survivors(ms_out["reads"]) == [50]
    assert _read_seqs(ns_out["reads"])[50] == _INSERT
    assert _read_seqs(ms_out["reads"])[50] == seq
