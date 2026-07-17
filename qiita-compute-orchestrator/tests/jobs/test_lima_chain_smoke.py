"""Real-miint tests for the long-read adapter chain (`lima_export` / `lima_mask`).

The container step between them is not exercised here (lima is a binary in a SIF;
`LocalBackend` refuses container steps and `make test-workflows` is Linux-only).
Instead lima is SIMULATED by writing the FASTQ it would emit — which is the point:
the fragile part of this chain is not lima itself but the `sequence_idx` round-trip
through lima's output, and the `infer_trim` contract layered on it.

The simulation mirrors what lima 2.13.0 was PROBED to do, not what it plausibly
does (`test_sam_bam_writer_miint_contract.py` pins why that BAM is written with
pysam rather than a miint COPY): it emits `<movie>/<zmw>/ccs` names, rewritten from
each record's `zm` tag, with its BAM tags appended after one space.

Pinned here:
  - `lima_export` writes a CCS uBAM whose record names carry a DENSE ZMW counter,
    with `lima_zmw_map` carrying `zmw -> sequence_idx` — the key channel, since a
    lake-wide `sequence_idx` cannot ride in lima's int32 `zm` tag;
  - the ZMW counter is dense even when `sequence_idx` is astronomically large —
    the case that silently corrupted the mask if the idx were used as the ZMW;
  - lima's appended BAM tags (`bc=`/`bl=`/…) land in `read_fastx`'s `comment`
    column and never pollute the key;
  - emitted trims are relative to the RAW read, so applying them to the raw read
    recovers the insert;
  - a read lima omitted becomes `twist_no_adaptor` with zero trims;
  - an EMPTY lima output is handled rather than crashing `read_fastx` — a guard on
    an external tool's file, NOT the adapter-free-sample path (lima FATALs there);
  - a ZMW that is not in the map, a resolved key that is not an input read, or a
    duplicated one, fails loud rather than silently dropping / duplicating a row;
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


def _zmw_map(path: Path) -> dict[int, int]:
    """`sequence_idx -> zmw`, the inverse of what lima_export writes."""
    with duckdb.connect(":memory:") as conn:
        rows = conn.execute(f"SELECT sequence_idx, zmw FROM read_parquet('{path}')").fetchall()
    return {sidx: zmw for sidx, zmw in rows}


def _write_lima_output(path: Path, records: list[tuple[int, str]], *, tags: bool = True) -> Path:
    """Simulate lima: name rewritten from the record's `zm` tag as
    `<movie>/<zmw>/ccs`, BAM tags appended after one space, sequence end-clipped.
    Reads lima dropped simply do not appear. `records` is (zmw, clipped_sequence)."""
    from qiita_compute_orchestrator.jobs.lima_export import _MOVIE

    suffix = " bc=3,3 bl=AACC bq=100" if tags else ""
    path.write_text(
        "".join(
            f"@{_MOVIE}/{zmw}/ccs{suffix}\n{seq}\n+\n{'I' * len(seq)}\n" for zmw, seq in records
        )
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


def _mask(tmp_path: Path, reads: Path, lima_out: Path, zmw_map: Path):
    from qiita_compute_orchestrator.jobs import lima_mask

    return asyncio.run(
        lima_mask.execute(
            lima_mask.Inputs(
                reads=reads,
                lima_out_fastq=lima_out,
                lima_zmw_map=zmw_map,
                work_ticket_idx=1,
            ),
            tmp_path / "wm",
        )
    )


def _roundtrip(tmp_path: Path, rows: list[tuple[int, str]], kept: list[tuple[int, str]], **kw):
    """Export -> simulate lima keeping `kept` (sequence_idx, clipped) -> mask."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    reads = _reads(tmp_path, rows)
    out = _export(tmp_path, reads)
    zmw_of = _zmw_map(out["lima_zmw_map"])
    lima_out = _write_lima_output(
        tmp_path / "lima_out.fastq", [(zmw_of[sidx], seq) for sidx, seq in kept], **kw
    )
    return _mask(tmp_path, reads, lima_out, out["lima_zmw_map"])


def test_export_names_records_movie_zmw_ccs_not_a_bare_sequence_idx(tmp_path):
    """lima requires PacBio's `<movie>/<zmw>/ccs` convention. A bare-integer record
    name — the obvious way to carry `sequence_idx` — does not merely degrade: lima
    HANGS on it (probed). The name shape is therefore a hard contract, not a style."""
    import pysam

    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    out = _export(tmp_path, reads)
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        names = [rec.query_name for rec in bam]
    assert all(n.count("/") == 2 and n.endswith("/ccs") for n in names), names
    assert {n.split("/")[1] for n in names} == {"0", "1"}


def test_export_bam_carries_the_ccs_read_type(tmp_path):
    """`DS:READTYPE=CCS` is the field lima keys on: probed, an @RG whose DS says
    READTYPE=UNKNOWN is accepted but demoted to SubreadSets. (What selects the CCS
    path in the first place is the input FORMAT being BAM at all; PL is asserted
    because we ship it, not because it was independently varied.)"""
    import pysam

    reads = _reads(tmp_path, [(11, _INSERT)])
    out = _export(tmp_path, reads)
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        (rg,) = bam.header.to_dict()["RG"]
    assert "READTYPE=CCS" in rg["DS"]
    assert rg["PL"] == "PACBIO"


def test_export_sets_the_zm_tag_lima_rewrites_the_name_from(tmp_path):
    """lima does not preserve the input name: it REBUILDS each emitted record's name
    from the `zm` tag. With no `zm` the ZMW comes back as `?` and the key is gone."""
    import pysam

    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    out = _export(tmp_path, reads)
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        for rec in bam:
            assert rec.get_tag("zm") == int(rec.query_name.split("/")[1])


def test_export_bam_carries_the_reads_themselves(tmp_path):
    """The anti-vacuity check on the writer swap. miint's `COPY ... (FORMAT BAM)`
    silently emits `*` for SEQ/QUAL — a BAM that lima accepts and finds no reads in
    would look like a working chain that masks everything `twist_no_adaptor`."""
    import pysam

    raw = _LEAD + _INSERT
    reads = _reads(tmp_path, [(11, raw)])
    out = _export(tmp_path, reads)
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        (rec,) = list(bam)
    assert rec.query_sequence == raw
    assert list(rec.query_qualities) == _q(raw)


def test_export_bam_quals_track_their_own_read(tmp_path):
    """The phred scores are sliced out of Arrow's values buffer by a running offset
    rather than materialized per read (`to_pylist` on HiFi quals is the step's whole
    runtime). A mis-walked offset would not crash — it would hand read N read N-1's
    tail, silently. So: reads of DIFFERENT lengths, each with a distinct score
    pattern, every one checked. Constant-quality reads could not fail this."""
    import pysam

    rows = [(11, "ACGT" * 3), (22, "TTTT" * 7), (33, "GG"), (44, "ACGTAC" * 5)]
    quals = {sidx: [(sidx + i) % 94 for i in range(len(seq))] for sidx, seq in rows}
    reads = tmp_path / "reads.parquet"
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(? AS UTINYINT[]), CAST(NULL AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([5, sidx, f"r{sidx}", seq, quals[sidx]])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{reads}' (FORMAT PARQUET)",
            params,
        )
    out = _export(tmp_path, reads)
    zmw_of = _zmw_map(out["lima_zmw_map"])
    by_zmw = {zmw: sidx for sidx, zmw in zmw_of.items()}
    seen = 0
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        for rec in bam:
            sidx = by_zmw[int(rec.query_name.split("/")[1])]
            assert list(rec.query_qualities) == quals[sidx], f"quals crossed reads at {sidx}"
            seen += 1
    assert seen == len(rows)


def test_export_rejects_a_read_with_no_qualities(tmp_path):
    """A NULL `qual1` would break the running offset for every read after it, so it
    is rejected rather than allowed to shift the file silently."""
    reads = tmp_path / "reads.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT 5::BIGINT AS prep_sample_idx, 11::BIGINT AS sequence_idx, "
            "'r11' AS read_id, 'ACGT' AS sequence1, NULL::UTINYINT[] AS qual1, "
            "NULL::VARCHAR AS sequence2, NULL::UTINYINT[] AS qual2) "
            f"TO '{reads}' (FORMAT PARQUET)"
        )
    with pytest.raises(ValueError, match="qual1 contains NULL"):
        _export(tmp_path, reads)


def test_export_zmw_is_dense_even_for_a_huge_sequence_idx(tmp_path):
    """THE corruption guard. `zm` is an int32 BAM tag and `sequence_idx` is
    lake-wide-unique BIGINT. Using the idx as the ZMW does not raise — lima returns
    it TRUNCATED (probed: 5000000000 -> 705032704), i.e. a mask silently attributed
    to the wrong read. The ZMW must be a per-file counter, mapped back via the map."""
    import pysam

    huge = 5_000_000_000
    reads = _reads(tmp_path, [(huge, _INSERT), (huge + 1, _INSERT)])
    out = _export(tmp_path, reads)
    with pysam.AlignmentFile(out["lima_in_bam"], "rb", check_sq=False) as bam:
        zmws = [rec.get_tag("zm") for rec in bam]
    assert sorted(zmws) == [0, 1], "ZMW must be a dense counter, never the sequence_idx"
    mapping = _zmw_map(out["lima_zmw_map"])
    assert sorted(mapping) == [huge, huge + 1]
    assert sorted(mapping.values()) == [0, 1]


def test_export_zmw_map_round_trips_every_read(tmp_path):
    """The map IS the key channel — every exported read must be in it exactly once."""
    rows = [(11, _INSERT), (22, _INSERT), (33, _INSERT)]
    reads = _reads(tmp_path, rows)
    out = _export(tmp_path, reads)
    mapping = _zmw_map(out["lima_zmw_map"])
    assert sorted(mapping) == [11, 22, 33]
    assert sorted(mapping.values()) == [0, 1, 2], "dense, collision-free"


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
    rows = _mask_rows(_roundtrip(tmp_path, [(11, raw)], [(11, _INSERT)])["partial_mask"])
    assert rows == [(11, ReadMaskReason.PASS.value, len(_LEAD), len(_TRAIL), None, None)]
    left, right = rows[0][2], rows[0][3]
    assert raw[left : len(raw) - right] == _INSERT


def test_mask_marks_reads_lima_dropped_as_twist_no_adaptor(tmp_path):
    """A HiFi read with no Twist adaptor is artifactual, not a library molecule."""
    rows = _mask_rows(
        _roundtrip(tmp_path, [(11, _INSERT), (22, _INSERT)], [(11, _INSERT)])["partial_mask"]
    )
    assert rows[0][1] == ReadMaskReason.PASS.value
    assert rows[1] == (22, ReadMaskReason.TWIST_NO_ADAPTOR.value, 0, 0, None, None)


def test_mask_resolves_a_huge_sequence_idx_through_the_map(tmp_path):
    """The end-to-end of the int32 guard: an idx far past 2^31 must come back as
    ITSELF, not as a truncated near-miss that would mask an unrelated read."""
    huge = 5_000_000_000
    raw = _LEAD + _INSERT
    rows = _mask_rows(_roundtrip(tmp_path, [(huge, raw)], [(huge, _INSERT)])["partial_mask"])
    assert rows == [(huge, ReadMaskReason.PASS.value, len(_LEAD), 0, None, None)]


def test_mask_handles_an_empty_lima_output(tmp_path):
    """`read_fastx` REJECTS an empty file, so the guard must route around it rather
    than crash. NOTE this is NOT the adapter-free-sample path: probed, lima FATALs on
    a BAM whose reads carry no adaptor rather than emitting an empty output, so the
    step fails before lima_mask runs (see the job's module docstring). This pins the
    guard's behavior on an external tool's file, not a documented lima outcome."""
    rows = _mask_rows(_roundtrip(tmp_path, [(11, _INSERT), (22, _INSERT)], [])["partial_mask"])
    assert [r[1] for r in rows] == [ReadMaskReason.TWIST_NO_ADAPTOR.value] * 2
    assert all(r[2] == 0 and r[3] == 0 for r in rows)


def test_mask_tolerates_limas_appended_bam_tags(tmp_path):
    """lima appends `bc=`/`bl=`/`bq=` after one space; `read_fastx` parses those
    into `comment`, leaving `read_id` the bare `<movie>/<zmw>/ccs` name."""
    with_tags = _roundtrip(tmp_path / "a", [(11, _INSERT)], [(11, _INSERT)], tags=True)
    without = _roundtrip(tmp_path / "b", [(11, _INSERT)], [(11, _INSERT)], tags=False)
    assert _mask_rows(with_tags["partial_mask"]) == _mask_rows(without["partial_mask"])


def test_mask_rejects_a_zmw_that_is_not_in_the_map(tmp_path):
    """The sharpest failure: lima names records from the `zm` tag, so an unmappable
    ZMW means the key channel itself broke (a truncated ZMW, a map from another run).
    The LEFT JOIN must surface it, never let the read vanish into twist_no_adaptor."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    out = _export(tmp_path, reads)
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(4242, _INSERT)])
    with pytest.raises(ValueError, match="lima_zmw_map"):
        _mask(tmp_path, reads, lima_out, out["lima_zmw_map"])


def test_mask_rejects_a_resolved_key_that_is_not_an_input_read(tmp_path):
    """infer_trim LEFT JOINs original→clipped, so an unknown clipped key would be
    silently dropped — a stale or mismatched map must fail loud."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    # A map that resolves ZMW 0 to a sequence_idx the reads do not contain.
    bad_map = tmp_path / "bad_map.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            "COPY (SELECT 0::UINTEGER AS zmw, 999::BIGINT AS sequence_idx) "
            f"TO '{bad_map}' (FORMAT PARQUET)"
        )
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(0, _INSERT)])
    with pytest.raises(ValueError, match="not an input read"):
        _mask(tmp_path, reads, lima_out, bad_map)


def test_mask_rejects_a_duplicated_record_name(tmp_path):
    """A duplicate key fans the join out and emits two mask rows for one read."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    out = _export(tmp_path, reads)
    zmw = _zmw_map(out["lima_zmw_map"])[11]
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(zmw, _INSERT), (zmw, _INSERT)])
    with pytest.raises(ValueError, match="duplicate"):
        _mask(tmp_path, reads, lima_out, out["lima_zmw_map"])


def test_mask_fails_loud_when_lima_edited_internal_bases(tmp_path):
    """`infer_trim` requires the clipped read be a contiguous substring of its
    original. lima is a pure end-trimmer (probed: it does not even reverse-complement
    to orient), so a violation means something is wrong — do not suppress it."""
    with pytest.raises(duckdb.Error, match="infer_trim"):
        _roundtrip(tmp_path, [(11, _INSERT)], [(11, "GGGGGG")])


def test_mask_emits_exactly_one_row_per_read_even_when_lima_drops_some(tmp_path):
    """THE BIJECTION, pinned at the producer. `qc` JOINs its incoming mask against the
    reads, so a missing row silently drops a read and a duplicate double-counts.
    `infer_trim` returns one row per ORIGINAL read (NULL/NULL for one the tool omitted),
    so the bijection survives lima dropping reads — which is the interesting case, and
    the reason the consumers do not re-check it at runtime (see jobs/_partial_mask)."""
    rows = _mask_rows(
        _roundtrip(
            tmp_path,
            [(11, _LEAD + _INSERT), (22, _INSERT), (33, _INSERT + _TRAIL)],
            # lima kept only 11 and 33; 22 carried no adaptor and was dropped.
            [(11, _INSERT), (33, _INSERT)],
        )["partial_mask"]
    )
    assert [r[0] for r in rows] == [11, 22, 33], "one row per ORIGINAL read, dropped or not"
    assert len({r[0] for r in rows}) == 3


def test_mask_trims_never_exceed_the_raw_read(tmp_path):
    """The other invariant the consumers rely on rather than re-check. `infer_trim`
    locates the clipped read as a contiguous substring of the original and fails loud
    otherwise, so `left + right <= length` holds by construction. If it ever did not,
    the failure would be SILENT downstream: DuckDB's substr with a negative length
    walks backwards and returns bases instead of erroring."""
    raw = _LEAD + _INSERT + _TRAIL
    rows = _mask_rows(
        _roundtrip(tmp_path, [(11, raw), (22, _INSERT)], [(11, _INSERT)])["partial_mask"]
    )
    for sidx, reason, left, right, _lt2, _rt2 in rows:
        length = len(raw) if sidx == 11 else len(_INSERT)
        assert left + right <= length, f"{sidx} ({reason}) trims {left}+{right} > {length}"
