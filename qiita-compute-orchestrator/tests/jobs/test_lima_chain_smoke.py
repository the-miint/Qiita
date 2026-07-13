"""Real-miint tests for the long-read adapter chain (`lima_export` / `lima_mask`).

The container step between them is not exercised here (lima is a binary in a SIF;
`LocalBackend` refuses container steps and `make test-workflows` is Linux-only).
Instead lima is SIMULATED by writing the FASTQ it would emit — which is the point:
the fragile part of this chain is not lima itself but the `sequence_idx` round-trip
through lima's output, and the `infer_trim` contract layered on it.

Pinned here:
  - `lima_export` writes `sequence_idx` (not `read_id`) as the FASTQ record name,
    CAST to VARCHAR — miint's FASTQ writer rejects a BIGINT name;
  - lima's appended BAM tags (`bc=`/`bl=`/…) land in `read_fastx`'s `comment`
    column and never pollute the key;
  - emitted trims are relative to the RAW read, so applying them to the raw read
    recovers the insert;
  - a read lima omitted becomes `twist_no_adaptor` with zero trims;
  - an EMPTY lima output (every read failed adapter detection) is a legitimate
    all-`twist_no_adaptor` mask, not a crash — `read_fastx` rejects an empty file;
  - a record name that is not an input `sequence_idx`, or a duplicated one, fails
    loud rather than silently dropping / duplicating a mask row;
  - lima editing internal bases (not pure end-trimming) fails loud via `infer_trim`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

# G-free insert, comfortably long; the flanks stand in for the Twist adaptor ends.
_INSERT = "ACTACTACTA" * 6
_LEAD = "TTTTTTTTTT"
_TRAIL = "CCCCCCCC"
_LIMA_ARGS = "--hifi-preset ASYMMETRIC --neighbors --peek-guess"


def _q(seq: str, val: int = 35) -> list[int]:
    return [val] * len(seq)


def _mask_rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _write_lima_output(path: Path, records: list[tuple[int, str]], *, tags: bool = True) -> Path:
    """Simulate lima: record NAME preserved verbatim, BAM tags appended after one
    space, sequence end-clipped. Reads lima dropped simply do not appear."""
    suffix = " bc=3,3 bl=AACC bq=100" if tags else ""
    path.write_text(
        "".join(f"@{idx}{suffix}\n{seq}\n+\n{'I' * len(seq)}\n" for idx, seq in records)
    )
    return path


def _reads(tmp_path: Path, rows: list[tuple[int, str]], *, paired: bool = False) -> Path:
    """A raw read.parquet in the fastq_to_parquet 7-column shape."""
    out = tmp_path / "reads.parquet"
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(? AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([5, sidx, f"r{sidx}", seq, _q(seq), seq if paired else None])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{out}' (FORMAT PARQUET)",
            params,
        )
    return out


def _export(tmp_path: Path, reads: Path, args: str = _LIMA_ARGS):
    from qiita_compute_orchestrator.jobs import lima_export

    return asyncio.run(
        lima_export.execute(
            lima_export.Inputs(reads=reads, lima_args=args, work_ticket_idx=1),
            tmp_path / "we",
        )
    )


def _mask(tmp_path: Path, reads: Path, lima_out: Path):
    from qiita_compute_orchestrator.jobs import lima_mask

    return asyncio.run(
        lima_mask.execute(
            lima_mask.Inputs(reads=reads, lima_out_fastq=lima_out, work_ticket_idx=1),
            tmp_path / "wm",
        )
    )


def test_export_writes_sequence_idx_as_the_record_name(tmp_path):
    """The join key must survive lima. `read_fastx`'s own `sequence_index` is
    positional and resets per file, so `sequence_idx` rides in the record NAME."""
    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    out = _export(tmp_path, reads)
    names = [ln[1:] for ln in out["lima_in_fastq"].read_text().splitlines() if ln.startswith("@")]
    # sequence_idx, not read_id ("r11"/"r22"). SORTED, not compared in emission order:
    # the FASTQ is written unsorted on purpose (a blocking sort over sequence1+qual1
    # would spill tens of GB), and no consumer reads record order — lima_mask recovers
    # the key from the record NAME. Asserting the order here would re-impose a contract
    # the job deliberately does not offer.
    assert sorted(names) == ["11", "22"]


def test_export_writes_the_cp_resolved_args_to_a_file(tmp_path):
    """A scalar cannot ride a container step's inputs (the runner treats every
    container input as a bind-mount path), so lima's args arrive as a file."""
    import json

    reads = _reads(tmp_path, [(11, _INSERT)])
    out = _export(tmp_path, reads)
    assert json.loads(out["lima_config"].read_text()) == {"args": _LIMA_ARGS}


def test_export_rejects_paired_end(tmp_path):
    reads = _reads(tmp_path, [(11, _INSERT)], paired=True)
    with pytest.raises(ValueError, match="paired-end"):
        _export(tmp_path, reads)


def test_export_rejects_empty_lima_args(tmp_path):
    reads = _reads(tmp_path, [(11, _INSERT)])
    with pytest.raises(ValueError, match="lima_args"):
        _export(tmp_path, reads, args="   ")


def test_mask_trims_are_relative_to_the_raw_read(tmp_path):
    """lima clipped _LEAD off the 5' end and _TRAIL off the 3'. The emitted trims
    must reconstruct the insert from the RAW read — `qc`, `host_filter`, and the
    `read_masked` view all apply mask trims to `read.sequence1`."""
    raw = _LEAD + _INSERT + _TRAIL
    reads = _reads(tmp_path, [(11, raw)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT)])
    rows = _mask_rows(_mask(tmp_path, reads, lima_out)["partial_mask"])
    assert rows == [(11, ReadMaskReason.PASS.value, len(_LEAD), len(_TRAIL), None, None)]
    left, right = rows[0][2], rows[0][3]
    assert raw[left : len(raw) - right] == _INSERT


def test_mask_marks_reads_lima_dropped_as_twist_no_adaptor(tmp_path):
    """A HiFi read with no Twist adaptor is artifactual, not a library molecule."""
    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT)])
    rows = _mask_rows(_mask(tmp_path, reads, lima_out)["partial_mask"])
    assert rows[0][1] == ReadMaskReason.PASS.value
    assert rows[1] == (22, ReadMaskReason.TWIST_NO_ADAPTOR.value, 0, 0, None, None)


def test_mask_handles_an_empty_lima_output(tmp_path):
    """Every read failed adapter detection. `read_fastx` REJECTS an empty file, so
    the step must route around it rather than crash: the mask is all-twist_no_adaptor."""
    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    empty = tmp_path / "empty.fastq"
    empty.write_text("")
    rows = _mask_rows(_mask(tmp_path, reads, empty)["partial_mask"])
    assert [r[1] for r in rows] == [ReadMaskReason.TWIST_NO_ADAPTOR.value] * 2
    assert all(r[2] == 0 and r[3] == 0 for r in rows)


def test_mask_tolerates_limas_appended_bam_tags(tmp_path):
    """lima appends `bc=`/`bl=`/`bq=` after one space; `read_fastx` parses those
    into `comment`, leaving `read_id` the bare sequence_idx."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    with_tags = _write_lima_output(tmp_path / "a.fastq", [(11, _INSERT)], tags=True)
    without = _write_lima_output(tmp_path / "b.fastq", [(11, _INSERT)], tags=False)
    assert _mask_rows(_mask(tmp_path, reads, with_tags)["partial_mask"]) == _mask_rows(
        _mask(tmp_path / "x", reads, without)["partial_mask"]
    )


def test_mask_rejects_a_record_name_that_is_not_an_input_read(tmp_path):
    """infer_trim LEFT JOINs original→clipped, so an unknown clipped key would be
    silently dropped — a stale or mismatched lima output must fail loud."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(999, _INSERT)])
    with pytest.raises(ValueError, match="not"):
        _mask(tmp_path, reads, lima_out)


def test_mask_rejects_a_duplicated_record_name(tmp_path):
    """A duplicate key fans the join out and emits two mask rows for one read."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT), (11, _INSERT)])
    with pytest.raises(ValueError, match="duplicate"):
        _mask(tmp_path, reads, lima_out)


def test_mask_fails_loud_when_lima_edited_internal_bases(tmp_path):
    """`infer_trim` requires the clipped read be a contiguous substring of its
    original. lima is a pure end-trimmer, so a violation means something is wrong —
    do not suppress it."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, "GGGGGG")])
    with pytest.raises(duckdb.Error, match="infer_trim"):
        _mask(tmp_path, reads, lima_out)


def test_mask_emits_exactly_one_row_per_read_even_when_lima_drops_some(tmp_path):
    """THE BIJECTION, pinned at the producer. `qc` JOINs its incoming mask against the
    reads, so a missing row silently drops a read and a duplicate double-counts.
    `infer_trim` returns one row per ORIGINAL read (NULL/NULL for one the tool omitted),
    so the bijection survives lima dropping reads — which is the interesting case, and
    the reason the consumers do not re-check it at runtime (see jobs/_partial_mask)."""
    reads = _reads(tmp_path, [(11, _LEAD + _INSERT), (22, _INSERT), (33, _INSERT + _TRAIL)])
    # lima kept only 11 and 33; 22 carried no adaptor and was dropped.
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT), (33, _INSERT)])
    rows = _mask_rows(_mask(tmp_path, reads, lima_out)["partial_mask"])
    assert [r[0] for r in rows] == [11, 22, 33], "one row per ORIGINAL read, dropped or not"
    assert len({r[0] for r in rows}) == 3


def test_mask_trims_never_exceed_the_raw_read(tmp_path):
    """The other invariant the consumers rely on rather than re-check. `infer_trim`
    locates the clipped read as a contiguous substring of the original and fails loud
    otherwise, so `left + right <= length` holds by construction. If it ever did not,
    the failure would be SILENT downstream: DuckDB's substr with a negative length
    walks backwards and returns bases instead of erroring."""
    raw = _LEAD + _INSERT + _TRAIL
    reads = _reads(tmp_path, [(11, raw), (22, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT)])
    for sidx, reason, left, right, _lt2, _rt2 in _mask_rows(
        _mask(tmp_path, reads, lima_out)["partial_mask"]
    ):
        length = len(raw) if sidx == 11 else len(_INSERT)
        assert left + right <= length, f"{sidx} ({reason}) trims {left}+{right} > {length}"
