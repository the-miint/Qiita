"""Real-miint tests for the long-read adapter chain (`lima_export` / `lima_mask`).

The container step between them is not exercised here (lima is a binary in a SIF;
`LocalBackend` refuses container steps and `make test-workflows` is Linux-only).
Instead lima is SIMULATED by writing the FASTQ it would emit — which is the point:
the fragile part of this chain is not lima itself but the read-key round-trip
through lima's output, and the `infer_trim` contract layered on it.

The simulation mirrors what lima 2.13.0 was PROBED to do, not what it plausibly
does: it emits the record name VERBATIM (which for our BAM is the lake's
`read_id`), with its own BAM tags appended after one space.

The produced BAM is read back with miint's `read_sequences_sam` (not pysam): it
gives the record name, sequence, and quality, which is what these tests assert.
The `@RG DS:READTYPE=CCS` and the `zm` tag are miint's `FORMAT UBAM` contract, not
ours to re-test — they are proved by lima accepting the BAM (the end-to-end probe
in ~/claude/tmp/lima-bam-fix), not here.

Pinned here:
  - `lima_export` writes the lake's `read_id` as the record name — the key channel,
    since lima round-trips the name and `lima_mask` joins straight back on it;
  - the ZMW rides in an int32 `zm` tag, so a `read_id` whose hole number exceeds
    int32, a read set spanning >1 movie, or a non-PacBio `read_id` each fail LOUD
    rather than silently masking the wrong read;
  - lima's appended BAM tags (`bc=`/`bl=`/…) land in `read_fastx`'s `comment`
    column and never pollute the key;
  - emitted trims are relative to the RAW read, so applying them to the raw read
    recovers the insert;
  - a read lima omitted becomes `twist_no_adaptor` with zero trims;
  - an EMPTY lima output is handled rather than crashing `read_fastx` — a guard on
    an external tool's file, NOT the adapter-free-sample path (lima FATALs there);
  - a name that is not a read_id sent to lima, or a duplicated one, fails loud
    rather than silently dropping / duplicating a mask row;
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

# A realistic PacBio movie, and a hole-number base kept DISTINCT from sequence_idx
# (and int32-safe for the small sidx values here) so the join is genuinely on
# `read_id` and never on a sidx==hole coincidence.
_MOVIE = "m84137_260623_040906_s1"
_HOLE_BASE = 100_000_000


def _rid(sidx: int, *, hole: int | None = None, movie: str = _MOVIE) -> str:
    """The `read_id` for a given sequence_idx — PacBio's `<movie>/<zmw>/ccs`, the
    shape `bam_to_parquet` keeps verbatim from the instrument BAM."""
    return f"{movie}/{_HOLE_BASE + sidx if hole is None else hole}/ccs"


def _q(seq: str, val: int = 35) -> list[int]:
    return [val] * len(seq)


def _mask_rows(path: Path) -> list[tuple]:
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            "SELECT sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2 "
            f"FROM read_parquet('{path}') ORDER BY sequence_idx"
        ).fetchall()


def _write_lima_output(
    path: Path, kept: list[tuple[int, str]], *, tags: bool = True, read_id_of=_rid
) -> Path:
    """Simulate lima: record name emitted VERBATIM (lima round-trips it, probed),
    BAM tags appended after one space, sequence end-clipped. Reads lima dropped
    simply do not appear. `kept` is (sequence_idx, clipped_sequence)."""
    suffix = " bc=3,3 bl=AACC bq=100" if tags else ""
    path.write_text(
        "".join(f"@{read_id_of(sidx)}{suffix}\n{seq}\n+\n{'I' * len(seq)}\n" for sidx, seq in kept)
    )
    return path


def _reads(
    tmp_path: Path, rows: list[tuple[int, str]], *, paired: bool = False, read_id_of=_rid
) -> Path:
    """A raw read.parquet in the fastq_to_parquet 7-column shape, `read_id` in
    PacBio `<movie>/<zmw>/ccs` form (what `bam_to_parquet` produces)."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    out = tmp_path / "reads.parquet"
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(? AS UTINYINT[]), CAST(? AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([5, sidx, read_id_of(sidx), seq, _q(seq), seq if paired else None])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{out}' (FORMAT PARQUET)",
            params,
        )
    return out


def _bam_reads(path: Path) -> list[tuple[str, str, list[int]]]:
    """The produced uBAM read back via miint (`read_sequences_sam`) — `(read_id,
    sequence, qual)` per record. No pysam: the reads reader gives what these tests
    assert (name, bases, quality); the @RG/`zm` tag are FORMAT UBAM's contract,
    proved by lima accepting the BAM, not here."""
    from qiita_compute_orchestrator.miint import open_miint_conn

    with open_miint_conn() as conn:
        return conn.execute(
            f"SELECT read_id, sequence1, qual1 FROM read_sequences_sam('{path}') ORDER BY read_id"
        ).fetchall()


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


def _roundtrip(tmp_path: Path, rows: list[tuple[int, str]], kept: list[tuple[int, str]], **kw):
    """`_reads` -> simulate lima keeping `kept` (sequence_idx, clipped) -> `_mask`.
    The mask side is plain DuckDB, so this runs without the uBAM writer."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    reads = _reads(tmp_path, rows)
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", kept, **kw)
    return _mask(tmp_path, reads, lima_out)


# --------------------------------------------------------------------------- #
# lima_export — the produced BAM                                               #
# --------------------------------------------------------------------------- #


def test_export_bam_carries_each_read_keyed_by_its_read_id(tmp_path):
    """The record name IS `read_id` (the key lima round-trips and `lima_mask` joins
    back on), and each name carries ITS OWN sequence and quality. This is the
    anti-vacuity check on the writer: a BAM lima reads and finds no bases in would
    look like a chain that masks everything `twist_no_adaptor`, and a mis-bound TAGS
    or column would put read N's bases on read M. Reads of DIFFERENT lengths, each a
    distinct sequence AND a distinct quality pattern, every one checked — constant
    data could not fail this."""
    rows = [(11, "ACGTACGT"), (22, "TTTTGGGGCC"), (33, "GG"), (44, "ACGTACACGT")]
    quals = {sidx: [(sidx + i) % 94 for i in range(len(seq))] for sidx, seq in rows}
    reads = tmp_path / "reads.parquet"
    values = ", ".join(
        "(CAST(? AS BIGINT), CAST(? AS BIGINT), CAST(? AS VARCHAR), CAST(? AS VARCHAR), "
        "CAST(? AS UTINYINT[]), CAST(NULL AS VARCHAR), CAST(NULL AS UTINYINT[]))"
        for _ in rows
    )
    params: list = []
    for sidx, seq in rows:
        params.extend([5, sidx, _rid(sidx), seq, quals[sidx]])
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES {values}) AS t("
            "prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)) "
            f"TO '{reads}' (FORMAT PARQUET)",
            params,
        )
    out = _export(tmp_path, reads)
    seq_of = {seq: sidx for sidx, seq in rows}
    got = _bam_reads(out["lima_in_bam"])
    assert {rid for rid, _, _ in got} == {_rid(sidx) for sidx, _ in rows}, "names are read_id"
    for rid, seq, qual in got:
        sidx = seq_of[seq]  # seq present & non-empty (anti-vacuity)
        assert rid == _rid(sidx), "sequence is on its own read_id"
        assert list(qual) == quals[sidx], f"quality crossed reads at {sidx}"


def test_export_writes_the_cp_resolved_args_to_a_file(tmp_path):
    """A scalar cannot ride a container step's inputs (the runner treats every
    container input as a bind-mount path), so lima's args arrive as a file."""
    import json

    reads = _reads(tmp_path, [(11, _INSERT)])
    out = _export(tmp_path, reads)
    assert json.loads(out["lima_config"].read_text()) == {"args": _LIMA_ARGS}


# --------------------------------------------------------------------------- #
# lima_export — input guards (raise BEFORE the COPY, so they run without it)   #
# --------------------------------------------------------------------------- #


def test_export_rejects_paired_end(tmp_path):
    reads = _reads(tmp_path, [(11, _INSERT)], paired=True)
    with pytest.raises(ValueError, match="paired-end"):
        _export(tmp_path, reads)


def test_export_rejects_empty_lima_args(tmp_path):
    reads = _reads(tmp_path, [(11, _INSERT)])
    with pytest.raises(ValueError, match="lima_args"):
        _export(tmp_path, reads, args="   ")


@pytest.mark.parametrize(
    "bad_read_id",
    [
        "plain_read_11",  # FASTQ-ingested: no movie/zmw/ccs shape
        "/123/ccs",  # empty movie
        "mov/123",  # no /ccs suffix
        "a/b/c/ccs",  # non-numeric hole + extra field
        "mov/-3000000000/ccs",  # negative hole (would wrap in the int32 zm tag)
        "m'x/123/ccs",  # SINGLE QUOTE in movie — would break/inject the COPY @RG SQL
    ],
)
def test_export_rejects_a_read_id_that_is_not_pacbio_ccs(tmp_path, bad_read_id):
    """The read_id shape is enforced strictly. lima needs `<movie>/<zmw>/ccs` and
    hangs without it, so a FASTQ-ingested / malformed name fails loud at export. The
    strict movie charset (`[A-Za-z0-9_]`) is also what keeps the movie safe to
    interpolate into the COPY's @RG — a `'` in the movie must NOT reach the SQL."""
    reads = _reads(tmp_path, [(11, _INSERT)], read_id_of=lambda s: bad_read_id)
    with pytest.raises(ValueError, match="not PacBio"):
        _export(tmp_path, reads)


def test_export_rejects_reads_spanning_more_than_one_movie(tmp_path):
    """A single @RG stamps ONE movie on every record, and lima names each record from
    `zm` + that read group — so a second movie's reads would come back under the
    first movie's name, a wrong-but-plausible read_id. Fail loud rather than
    mis-join."""
    reads = _reads(
        tmp_path,
        [(11, _INSERT), (22, _INSERT)],
        read_id_of=lambda s: _rid(s, movie=("movieA" if s == 11 else "movieB")),
    )
    with pytest.raises(ValueError, match="span"):
        _export(tmp_path, reads)


def test_export_rejects_a_hole_number_over_int32(tmp_path):
    """The `zm` tag is int32. A hole number past it does not error in the tag — it
    TRUNCATES into a valid-looking ZMW (probed: 5000000000 -> 705032704) and the mask
    lands on the wrong read. A real PacBio hole cannot exceed int32; a corrupt one is
    rejected rather than trusted."""
    reads = _reads(tmp_path, [(11, _INSERT)], read_id_of=lambda s: _rid(s, hole=5_000_000_000))
    with pytest.raises(ValueError, match="over the"):
        _export(tmp_path, reads)


def test_export_handles_an_empty_source(tmp_path):
    """An all-spike-in sample exports ZERO reads (partial_mask leaves nothing `pass`).
    `_resolve_movie` must not crash unpacking a missing row — it yields a header-only
    BAM. (lima FATALs on it downstream, an empty-input outcome not settled here; the
    point of this test is only that EXPORT does not raise a TypeError.)"""
    reads = _reads(tmp_path, [(11, _INSERT)])
    # A partial_mask that marks the one read non-`pass`, so no read reaches lima.
    partial = tmp_path / "partial.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT 11::BIGINT AS sequence_idx, "
            f"'{ReadMaskReason.SPIKEIN_SYNDNA.value}' AS reason, 0::UINTEGER AS left_trim1, "
            "0::UINTEGER AS right_trim1, NULL::UINTEGER AS left_trim2, NULL::UINTEGER AS "
            f"right_trim2) TO '{partial}' (FORMAT PARQUET)"
        )
    from qiita_compute_orchestrator.jobs import lima_export

    out = asyncio.run(
        lima_export.execute(
            lima_export.Inputs(
                reads=reads, lima_args=_LIMA_ARGS, partial_mask=partial, work_ticket_idx=1
            ),
            tmp_path / "we",
        )
    )
    assert out["lima_in_bam"].exists(), "a header-only BAM is written, not a crash"
    assert _bam_reads(out["lima_in_bam"]) == [], "no reads exported"


# --------------------------------------------------------------------------- #
# lima_mask — the read_id round-trip and infer_trim contract (plain DuckDB)    #
# --------------------------------------------------------------------------- #


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


def test_mask_resolves_a_huge_sequence_idx(tmp_path):
    """A `sequence_idx` far past 2^31 is FINE now — it is a lookup value keyed by
    `read_id`, never a value that rides in the int32 `zm` tag. It must come back as
    ITSELF on the mask."""
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
    """lima appends `bc=`/`bl=`/`bq=` after one space; `read_fastx` parses those into
    `comment`, leaving `read_id` the bare name."""
    with_tags = _roundtrip(tmp_path / "a", [(11, _INSERT)], [(11, _INSERT)], tags=True)
    without = _roundtrip(tmp_path / "b", [(11, _INSERT)], [(11, _INSERT)], tags=False)
    assert _mask_rows(with_tags["partial_mask"]) == _mask_rows(without["partial_mask"])


def test_mask_rejects_a_name_that_is_not_a_read_id_sent_to_lima(tmp_path):
    """A name that is not in what we SENT lima means the key channel broke (a
    corrupted name, a stale output). The LEFT JOIN against `_ORIG` must surface it as
    a NULL key, never let the read vanish into twist_no_adaptor."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    # A record whose name is not any input read's read_id.
    lima_out = _write_lima_output(
        tmp_path / "lima_out.fastq", [(999, _INSERT)], read_id_of=lambda s: _rid(s, hole=424242)
    )
    with pytest.raises(ValueError, match="read_id round-trip is broken"):
        _mask(tmp_path, reads, lima_out)


def test_mask_rejects_a_read_lima_should_not_have_emitted(tmp_path):
    """The SAME check catches a read we EXCLUDED. With an upstream mask bound, only
    its `pass` reads are exported (`_ORIG` is pass-only), so a spike-in read_id in
    lima's output resolves against the full reads but not against `_ORIG` — it lands
    as a NULL key, same as a corrupted name. It must fail loud, not overwrite the
    spike-in verdict. (This is why `_QCD` joins `_ORIG`, not all reads.)"""
    from qiita_compute_orchestrator.jobs import lima_mask

    reads = _reads(tmp_path, [(11, _INSERT), (22, _INSERT)])
    # partial_mask: 11 pass (exported), 22 spikein_syndna (NOT exported to lima).
    partial = tmp_path / "partial.parquet"
    with duckdb.connect(":memory:") as conn:
        conn.execute(
            f"COPY (SELECT * FROM (VALUES "
            f"  (11::BIGINT, '{ReadMaskReason.PASS.value}', 0::UINTEGER, 0::UINTEGER, "
            "   NULL::UINTEGER, NULL::UINTEGER), "
            f"  (22::BIGINT, '{ReadMaskReason.SPIKEIN_SYNDNA.value}', 0::UINTEGER, 0::UINTEGER, "
            "   NULL::UINTEGER, NULL::UINTEGER)) AS t("
            "sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)) "
            f"TO '{partial}' (FORMAT PARQUET)"
        )
    # lima wrongly emits BOTH reads, including the excluded spike-in 22.
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT), (22, _INSERT)])
    with pytest.raises(ValueError, match="round-trip is broken"):
        asyncio.run(
            lima_mask.execute(
                lima_mask.Inputs(
                    reads=reads,
                    lima_out_fastq=lima_out,
                    partial_mask=partial,
                    work_ticket_idx=1,
                ),
                tmp_path / "wm",
            )
        )


def test_mask_rejects_a_duplicated_record_name(tmp_path):
    """A duplicate key fans the join out and emits two mask rows for one read."""
    reads = _reads(tmp_path, [(11, _INSERT)])
    lima_out = _write_lima_output(tmp_path / "lima_out.fastq", [(11, _INSERT), (11, _INSERT)])
    with pytest.raises(ValueError, match="duplicate"):
        _mask(tmp_path, reads, lima_out)


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
