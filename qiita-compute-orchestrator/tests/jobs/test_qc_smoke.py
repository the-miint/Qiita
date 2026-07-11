"""Real-miint smoke tests for `qc.execute` (seams NOT stubbed).

Runs the actual `trim_adapters` / `trim_adapters_pe` / `trim_polyg` /
`filter_read` chain end-to-end and pins the MASK behavior the stubbed unit tests
cannot see. `qc` no longer drops reads — it emits one `qc_mask.parquet` row per
read `(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)`:

  - SE: a 3' adapter is recorded as `right_trim1` and a read that falls below
    min_length (100) AFTER trimming is reason `qc_too_short` while a long one is
    `pass`; applying the recorded trims to the raw read recovers the insert;
  - PE: a pair is `pass` only when BOTH mates survive; one short mate -> the
    pair's reason is qc_too_short;
  - polyG is applied ONLY for a 2-color instrument — the same low-quality 3'
    G-run inflates `right_trim1` on a NextSeq run but not on a MiSeq run.

The trim-length invariant the read_masked view relies on (a `pass` read's
`left_trim + right_trim <= length`) is verified by reconstructing the trimmed
sequence from the raw read + recorded trims.

Runs against the team-mirror miint build (conftest stages it). The
`write_reads_q` fixture (tests/jobs/conftest.py) owns the quality-carrying
reads.parquet schema.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from qiita_common.models import ReadMaskReason

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


def _mask(path: Path) -> dict[int, dict]:
    """Map sequence_idx -> the mask row as a dict."""
    with duckdb.connect(":memory:") as conn:
        cur = conn.execute(
            "SELECT sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2 "
            f"FROM read_parquet('{path}')"
        )
        cols = [d[0] for d in cur.description]
        return {r[0]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


def _apply_se_trim(raw: str, m: dict) -> str:
    """Reconstruct the SE trimmed sequence the read_masked view would serve."""
    return raw[m["left_trim1"] : len(raw) - m["right_trim1"]]


def test_qc_smoke_se_adapter_trim_and_length_filter(tmp_path, write_reads_q):
    """SE: the adaptered long read is `pass` with the insert recoverable from the
    recorded trims; the adaptered short read is `qc_too_short` after trimming."""
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
    mask = _mask(out["qc_mask"])
    assert mask[10]["reason"] == ReadMaskReason.PASS.value
    assert mask[20]["reason"] == ReadMaskReason.QC_TOO_SHORT.value
    # The recorded trims on the pass read recover the bare insert (adapter is 3').
    assert _apply_se_trim(long_read, mask[10]) == _INSERT
    # SE leaves the mate trims NULL.
    assert mask[10]["left_trim2"] is None and mask[10]["right_trim2"] is None


def test_qc_smoke_pe_pair_reason_when_one_mate_short(tmp_path, write_reads_q):
    """PE: both mates long -> `pass`; one mate short (after adapter trim) -> the
    pair is qc_too_short (not pass). PE trims are 3'-only (left trims 0)."""
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
    mask = _mask(out["qc_mask"])
    assert mask[30]["reason"] == ReadMaskReason.PASS.value
    assert mask[40]["reason"] == ReadMaskReason.QC_TOO_SHORT.value
    # PE never populates the left pair (3'-only trimming).
    assert mask[30]["left_trim1"] == 0 and mask[30]["left_trim2"] == 0


def test_qc_smoke_n_base_and_low_quality_reasons(tmp_path, write_reads_q):
    """SE non-length fail reasons map correctly: a read with too many N bases is
    `qc_too_many_n` (filter_read max_n=5) and a read with too many sub-q15 bases is
    `qc_low_quality` (filter_read max_unqualified_pct=40). Both stay >= min_length
    (120 nt) so they cannot trip `length` first; no adapter/N so trimming is inert.
    """
    from qiita_compute_orchestrator.jobs import qc

    # 120 nt, 10 N bases (> max_n=5) but all high quality -> n_base.
    n_read = ("N" * 10) + _INSERT[10:]
    assert len(n_read) == 120 and n_read.count("N") == 10
    # 120 nt, no N, but 60 bases (50% > 40%) below qualified_q=15 -> quality.
    lowq_read = _INSERT
    lowq_qual = _q(_INSERT[:60], 2) + _q(_INSERT[60:], 35)
    assert len(lowq_qual) == 120 and sum(1 for q in lowq_qual if q < 15) == 60

    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (60, "n_base", n_read, _q(n_read), None, None),
            (70, "low_quality", lowq_read, lowq_qual, None, None),
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
    mask = _mask(out["qc_mask"])
    assert mask[60]["reason"] == ReadMaskReason.QC_TOO_MANY_N.value
    assert mask[70]["reason"] == ReadMaskReason.QC_LOW_QUALITY.value


def test_qc_smoke_polyg_gated_on_instrument(tmp_path, write_reads_q):
    """The SAME low-quality 3' G-run inflates right_trim1 on a 2-color (NextSeq)
    run but not on a non-2-color (MiSeq) run — proving polyG is gated on the
    instrument model. Both reads are `pass` (>= 100 nt either way)."""
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
    ns_mask = _mask(asyncio.run(qc.execute(nextseq, tmp_path / "ws_ns"))["qc_mask"])
    ms_mask = _mask(asyncio.run(qc.execute(miseq, tmp_path / "ws_ms"))["qc_mask"])

    assert ns_mask[50]["reason"] == ReadMaskReason.PASS.value
    assert ms_mask[50]["reason"] == ReadMaskReason.PASS.value
    # Only the 2-color run trimmed the G-run (right_trim1 recovers the bare insert).
    assert _apply_se_trim(seq, ns_mask[50]) == _INSERT
    assert ms_mask[50]["right_trim1"] == 0
    assert _apply_se_trim(seq, ms_mask[50]) == seq


# A 5' block an upstream mask-emitting step (lima) already removed, and a 3' one.
# Neither is in the QC adapter set, so QC alone would never trim them — which is
# what makes the cumulative-trim assertions below meaningful.
_LEAD = "TTTTTTTTTTTTTTTTTTTT"
_TRAIL = "CCCCCCCCCCCCCCC"
assert len(_LEAD) == 20 and len(_TRAIL) == 15


def test_qc_smoke_incoming_mask_trims_are_cumulative_from_raw(
    tmp_path, write_reads_q, write_partial_mask
):
    """An incoming `pass` row's trims are ADDED to what QC removes, not replaced.

    The raw read is `_LEAD + _INSERT + _ADAPTER + _TRAIL`. lima is simulated as
    having stripped `_LEAD` (5') and `_TRAIL` (3'); QC then sees `_INSERT +
    _ADAPTER` and trims the adapter off the 3' end. The emitted trims must be
    cumulative from the RAW read, so applying them to the raw read recovers the
    bare insert. Emitting QC's substring-relative trims instead would leave
    `_LEAD` in the sequence `host_filter` and the `read_masked` view serve.
    """
    from qiita_compute_orchestrator.jobs import qc

    raw = _LEAD + _INSERT + _ADAPTER + _TRAIL
    reads = write_reads_q(tmp_path / "reads.parquet", [(10, "r", raw, _q(raw), None, None)])
    adapter_mask = write_partial_mask(
        tmp_path / "adapter_mask.parquet",
        [(10, ReadMaskReason.PASS.value, len(_LEAD), len(_TRAIL))],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        partial_mask=adapter_mask,
        instrument_model="Illumina MiSeq",  # not 2-color: no polyG
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    mask = _mask(asyncio.run(qc.execute(inputs, tmp_path / "ws"))["qc_mask"])

    assert mask[10]["reason"] == ReadMaskReason.PASS.value
    # left: lima's 20 + QC's 0 (the adapter is 3'-only).
    assert mask[10]["left_trim1"] == len(_LEAD)
    # right: lima's 15 + QC's 13 (the adapter).
    assert mask[10]["right_trim1"] == len(_TRAIL) + len(_ADAPTER)
    # The whole point: raw read + emitted trims == the bare insert.
    assert _apply_se_trim(raw, mask[10]) == _INSERT


def test_qc_smoke_incoming_mask_filter_read_sees_the_trimmed_insert(
    tmp_path, write_reads_q, write_partial_mask
):
    """`filter_read` judges the INSERT, not the raw read.

    The raw read is well over min_length(100), but the insert that survives
    lima's trims is only `_SHORT` (40 nt). QC must call it `qc_too_short`. A qc
    that filtered the raw read would call this `pass`.
    """
    from qiita_compute_orchestrator.jobs import qc

    # 20 + 40 + 13 + 62 = 135 nt raw; lima strips the lead and the whole tail.
    tail = _TRAIL + "C" * 47
    raw = _LEAD + _SHORT + _ADAPTER + tail
    assert len(raw) > 100  # over QC's min_length, so only the INSERT can fail it
    reads = write_reads_q(tmp_path / "reads.parquet", [(20, "r", raw, _q(raw), None, None)])
    adapter_mask = write_partial_mask(
        tmp_path / "adapter_mask.parquet",
        [(20, ReadMaskReason.PASS.value, len(_LEAD), len(tail))],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        partial_mask=adapter_mask,
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    mask = _mask(asyncio.run(qc.execute(inputs, tmp_path / "ws"))["qc_mask"])
    assert mask[20]["reason"] == ReadMaskReason.QC_TOO_SHORT.value


def test_qc_smoke_incoming_non_pass_row_is_carried_verbatim(
    tmp_path, write_reads_q, write_partial_mask
):
    """A read an earlier step already rejected is never re-classified.

    Its reason and trims survive QC untouched, even though the read would
    otherwise sail through (`_INSERT` is 120 nt, adapter-free). The lima commit
    introduces `twist_no_adaptor` as the reason this carries in production; any
    non-`pass` value takes the same branch.
    """
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [
            (30, "rejected", _INSERT, _q(_INSERT), None, None),
            (40, "kept", _INSERT, _q(_INSERT), None, None),
        ],
    )
    adapter_mask = write_partial_mask(
        tmp_path / "adapter_mask.parquet",
        [
            (30, ReadMaskReason.QC_TOO_SHORT.value, 7, 3),
            (40, ReadMaskReason.PASS.value, 0, 0),
        ],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        partial_mask=adapter_mask,
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    mask = _mask(asyncio.run(qc.execute(inputs, tmp_path / "ws"))["qc_mask"])

    # Carried verbatim: reason AND trims, with no QC verdict applied.
    assert mask[30]["reason"] == ReadMaskReason.QC_TOO_SHORT.value
    assert (mask[30]["left_trim1"], mask[30]["right_trim1"]) == (7, 3)
    # The still-pass read is classified normally, and every read is accounted for.
    assert mask[40]["reason"] == ReadMaskReason.PASS.value
    assert sorted(mask) == [30, 40]


def test_qc_smoke_unbound_incoming_mask_is_unchanged(tmp_path, write_reads_q, write_partial_mask):
    """QC with no incoming mask == QC with an all-`pass`, all-zero-trim one."""
    from qiita_compute_orchestrator.jobs import qc

    raw = _INSERT + _ADAPTER
    rows = [(50, "r", raw, _q(raw), None, None)]
    common = dict(
        adapter_parquet=_adapter_parquet(tmp_path),
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    without = qc.Inputs(reads=write_reads_q(tmp_path / "a.parquet", rows), **common)
    with_identity = qc.Inputs(
        reads=write_reads_q(tmp_path / "b.parquet", rows),
        partial_mask=write_partial_mask(
            tmp_path / "identity.parquet", [(50, ReadMaskReason.PASS.value, 0, 0)]
        ),
        **common,
    )
    a = _mask(asyncio.run(qc.execute(without, tmp_path / "ws_a"))["qc_mask"])
    b = _mask(asyncio.run(qc.execute(with_identity, tmp_path / "ws_b"))["qc_mask"])
    assert a == b


def test_qc_smoke_incoming_mask_consuming_the_whole_read_is_qc_too_short(
    tmp_path, write_reads_q, write_partial_mask
):
    """`_assert_trims_within_read` rejects left+right > length but PERMITS
    left+right == length (infer_trim can produce it only in the limit). The read
    then presents to QC as an empty insert with an empty phred array: the chain
    must survive and call it qc_too_short, not crash and not pass."""
    from qiita_compute_orchestrator.jobs import qc

    raw = _LEAD + _TRAIL  # nothing between the two blocks lima strips
    reads = write_reads_q(tmp_path / "reads.parquet", [(60, "r", raw, _q(raw), None, None)])
    adapter_mask = write_partial_mask(
        tmp_path / "adapter_mask.parquet",
        [(60, ReadMaskReason.PASS.value, len(_LEAD), len(_TRAIL))],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path),
        partial_mask=adapter_mask,
        instrument_model="Illumina MiSeq",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    mask = _mask(asyncio.run(qc.execute(inputs, tmp_path / "ws"))["qc_mask"])
    assert mask[60]["reason"] == ReadMaskReason.QC_TOO_SHORT.value
    # The trims stay cumulative-from-raw: the whole read was consumed upstream.
    assert mask[60]["left_trim1"] == len(_LEAD)
    assert mask[60]["right_trim1"] == len(_TRAIL)
