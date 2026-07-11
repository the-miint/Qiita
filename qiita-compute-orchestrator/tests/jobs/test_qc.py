"""Isolated unit tests for `qc.execute` (miint chain seams stubbed).

`qc` is a `read.parquet -> qc_mask.parquet` transform keyed by the
already-minted `sequence_idx`. It drops NOTHING: it emits one mask row per read
`(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)`.

The real `trim_adapters` / `trim_adapters_pe` / `trim_polyg` / `filter_read`
calls need the miint extension, so they're exercised in `test_qc_smoke.py`; here
the two layout seams (`_qc_se_select`, `_qc_pe_select`) are stubbed to return
canned mask-shaped SELECT SQL and we assert the orchestration:

  - single-end (`sequence2 IS NULL`) and paired-end rows are routed to the
    matching seam (its source view), then UNION ALL'd into one streaming COPY;
  - the output is the union of both seams' rows, sorted by `sequence_idx`, with
    the 6-column mask schema;
  - polyG gating: `apply_polyg` is True only for 2-color instruments;
  - the parsed adapter set is threaded to both seams as a constant `VARCHAR[]`;
  - fail-fast on missing reads / missing or empty adapter set.

The `write_reads` fixture (tests/jobs/conftest.py) owns the read schema.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest
from qiita_common.models import ReadMaskReason

# Independent oracle for the emitted mask schema (deliberately NOT shared with the
# job, so a drift in either is caught).
_MASK_SCHEMA = [
    "sequence_idx",
    "reason",
    "left_trim1",
    "right_trim1",
    "left_trim2",
    "right_trim2",
]

# A canned 0-row, correctly-named mask SELECT a stubbed seam returns when the
# test doesn't care about its rows. Fully aliased so the COPY's UNION ALL (which
# takes Parquet column names from its first branch) yields the _MASK_SCHEMA.
_EMPTY_SEAM_SELECT = (
    "SELECT NULL::BIGINT AS sequence_idx, NULL::VARCHAR AS reason, "
    "NULL::UINTEGER AS left_trim1, NULL::UINTEGER AS right_trim1, "
    "NULL::UINTEGER AS left_trim2, NULL::UINTEGER AS right_trim2 WHERE false"
)


def _schema(path: Path) -> list[str]:
    with duckdb.connect(":memory:") as conn:
        return [
            d[0] for d in conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description
        ]


def _rows(path: Path) -> list[tuple]:
    """(sequence_idx, reason, left_trim2 IS NULL) per row, sorted — confirms SE
    rows keep NULL mate trims and PE rows non-NULL, through the union/COPY."""
    with duckdb.connect(":memory:") as conn:
        return conn.execute(
            f"SELECT sequence_idx, reason, left_trim2 IS NULL FROM read_parquet('{path}') "
            "ORDER BY sequence_idx"
        ).fetchall()


def _adapter_parquet(tmp_path: Path, *adapters: str, name: str = "adapters.parquet") -> Path:
    """Write the runner-staged adapter Parquet (columns feature_idx, sequence)
    the qc job reads via read_parquet. With no adapters -> a valid 0-row file
    (the empty-set fail-fast case)."""
    p = tmp_path / name
    with duckdb.connect(":memory:") as conn:
        conn.execute("CREATE TABLE a(feature_idx BIGINT, sequence VARCHAR)")
        if adapters:
            conn.executemany("INSERT INTO a VALUES (?, ?)", list(enumerate(adapters)))
        conn.execute(f"COPY a TO '{p}' (FORMAT PARQUET)")
    return p


_AD = "AGATCGGAAGAGC"  # standard TruSeq adapter prefix
_PASS = ReadMaskReason.PASS.value


def test_qc_routes_se_pe_and_unions(tmp_path, monkeypatch, write_reads):
    """SE rows go to the SE seam (its source view), PE rows to the PE seam; the
    COPY output is the union of both seams' rows, sorted, with the 6-col mask
    schema. SE rows carry NULL mate trims; PE rows carry non-NULL (0) mate trims."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "se1", "AAAA", None),  # SE
            (20, "pe1", "AAAA", "TTTT"),  # PE
            (30, "se2", "CCCC", None),  # SE
            (40, "pe2", "GGGG", "CCCC"),  # PE
        ],
    )

    captured: dict = {}

    def fake_se(src_view, *, adapters_sql, apply_polyg):
        captured["se_view"] = src_view
        captured["se_adapters_sql"] = adapters_sql
        captured["se_polyg"] = apply_polyg
        return (
            f"SELECT sequence_idx, '{_PASS}' AS reason, "
            "0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
            "NULL::UINTEGER AS left_trim2, NULL::UINTEGER AS right_trim2 "
            f"FROM {src_view}"
        )

    def fake_pe(src_view, *, adapters_sql, apply_polyg):
        captured["pe_view"] = src_view
        return (
            f"SELECT sequence_idx, '{_PASS}' AS reason, "
            "0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
            "0::UINTEGER AS left_trim2, 0::UINTEGER AS right_trim2 "
            f"FROM {src_view}"
        )

    monkeypatch.setattr(qc, "_qc_se_select", fake_se)
    monkeypatch.setattr(qc, "_qc_pe_select", fake_pe)

    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        instrument_model=None,
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))

    assert captured["se_view"] == qc._SE
    assert captured["pe_view"] == qc._PE
    assert _schema(out["qc_mask"]) == _MASK_SCHEMA
    # 10, 30 SE (mate trims NULL); 20, 40 PE (mate trims non-NULL) — proves routing.
    assert _rows(out["qc_mask"]) == [
        (10, _PASS, True),
        (20, _PASS, False),
        (30, _PASS, True),
        (40, _PASS, False),
    ]


def test_qc_multi_sample_block_processes_all_reads(tmp_path, monkeypatch, write_reads):
    """A BLOCK's reads parquet carries several prep_samples, but QC keys the mask
    on the globally-unique sequence_idx and never reads prep_sample_idx — so a
    multi-sample block yields one mask row per read exactly as a single-sample
    input would (the extra column is inert; SE/PE routing is unaffected)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet",
        [
            (10, "se1", "AAAA", None),  # sample 5, SE
            (20, "pe1", "AAAA", "TTTT"),  # sample 5, PE
            (30, "se2", "CCCC", None),  # sample 7, SE
            (40, "pe2", "GGGG", "CCCC"),  # sample 7, PE
        ],
        prep_sample_idx=[5, 5, 7, 7],
    )

    def fake_se(src_view, *, adapters_sql, apply_polyg):
        return (
            f"SELECT sequence_idx, '{_PASS}' AS reason, "
            "0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
            "NULL::UINTEGER AS left_trim2, NULL::UINTEGER AS right_trim2 "
            f"FROM {src_view}"
        )

    def fake_pe(src_view, *, adapters_sql, apply_polyg):
        return (
            f"SELECT sequence_idx, '{_PASS}' AS reason, "
            "0::UINTEGER AS left_trim1, 0::UINTEGER AS right_trim1, "
            "0::UINTEGER AS left_trim2, 0::UINTEGER AS right_trim2 "
            f"FROM {src_view}"
        )

    monkeypatch.setattr(qc, "_qc_se_select", fake_se)
    monkeypatch.setattr(qc, "_qc_pe_select", fake_pe)

    # A block ticket flows no prep_sample_idx scalar (block scope has many); the
    # Inputs field is optional and QC ignores it either way.
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        instrument_model=None,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))

    # All four reads masked, SE/PE routing preserved across the two samples.
    assert _schema(out["qc_mask"]) == _MASK_SCHEMA
    assert _rows(out["qc_mask"]) == [
        (10, _PASS, True),
        (20, _PASS, False),
        (30, _PASS, True),
        (40, _PASS, False),
    ]


@pytest.mark.parametrize(
    "model,expected",
    [
        ("NextSeq 550", True),
        ("Illumina NovaSeq 6000", True),
        ("MiniSeq", True),
        ("nextseq2000", True),  # case-insensitive
        ("Illumina MiSeq", False),
        ("HiSeq 2500", False),
        (None, False),
        ("", False),
    ],
)
def test_is_two_color(model, expected):
    from qiita_compute_orchestrator.jobs import qc

    assert qc._is_two_color(model) is expected


def test_qc_polyg_gate_threaded_to_seams(tmp_path, monkeypatch, write_reads):
    """`apply_polyg` reflects `_is_two_color(instrument_model)` and is passed to
    both seams (here: a 2-color NextSeq -> True)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(10, "se", "AAAA", None), (20, "pe", "AA", "TT")]
    )
    seen: dict = {}

    def rec(key):
        def _f(src_view, *, adapters_sql, apply_polyg):
            seen[key] = apply_polyg
            return _EMPTY_SEAM_SELECT

        return _f

    monkeypatch.setattr(qc, "_qc_se_select", rec("se"))
    monkeypatch.setattr(qc, "_qc_pe_select", rec("pe"))

    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        instrument_model="NextSeq 550",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert seen == {"se": True, "pe": True}


def test_qc_adapters_threaded_as_constant(tmp_path, monkeypatch, write_reads):
    """Both adapter sequences from the staged Parquet reach the seams as a
    constant `VARCHAR[]` literal."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    seen: dict = {}

    def rec(src_view, *, adapters_sql, apply_polyg):
        seen["adapters_sql"] = adapters_sql
        return _EMPTY_SEAM_SELECT

    monkeypatch.setattr(qc, "_qc_se_select", rec)
    monkeypatch.setattr(qc, "_qc_pe_select", lambda *a, **k: _EMPTY_SEAM_SELECT)

    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, "AGATCGGAAGAGC", "CTGTCTCTTATA"),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    sql = seen["adapters_sql"]
    assert "AGATCGGAAGAGC" in sql
    assert "CTGTCTCTTATA" in sql
    assert sql.endswith("::VARCHAR[]")


def test_qc_empty_output_when_no_reads(tmp_path, monkeypatch, write_reads):
    """A reads file with no SE/PE rows yields an empty (0-row) but well-formed
    qc_mask.parquet, schema intact (the seams contribute no rows)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    monkeypatch.setattr(qc, "_qc_se_select", lambda *a, **k: _EMPTY_SEAM_SELECT)
    monkeypatch.setattr(qc, "_qc_pe_select", lambda *a, **k: _EMPTY_SEAM_SELECT)

    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert out["qc_mask"].exists()
    assert _rows(out["qc_mask"]) == []
    assert _schema(out["qc_mask"]) == _MASK_SCHEMA


def test_qc_missing_reads_raises(tmp_path):
    from qiita_compute_orchestrator.jobs import qc

    inputs = qc.Inputs(
        reads=tmp_path / "nope.parquet",
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_missing_adapter_parquet_raises(tmp_path, write_reads):
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=tmp_path / "nope.parquet",
        prep_sample_idx=5,
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_empty_adapter_parquet_raises(tmp_path, write_reads):
    """An adapter set with no records is a misconfiguration — fail fast (QC is
    always-on with a required adapter set)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(10, "se", "AAAA", None)])
    empty = _adapter_parquet(tmp_path, name="empty.parquet")  # 0-row, valid Parquet
    inputs = qc.Inputs(reads=reads, adapter_parquet=empty, prep_sample_idx=5, work_ticket_idx=1)
    with pytest.raises(ValueError):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_read_adapter_parquet_returns_sequences_in_order(tmp_path):
    """`_read_adapter_parquet` returns the `sequence` column, one per row.
    Pins the read_parquet contract the job relies on (real DuckDB)."""
    from qiita_compute_orchestrator.jobs import qc
    from qiita_compute_orchestrator.miint import open_miint_conn

    p = _adapter_parquet(tmp_path, "AGATCGGAAGAGC", "CTGTCTCTTATA")
    with open_miint_conn() as conn:
        assert qc._read_adapter_parquet(conn, p) == ["AGATCGGAAGAGC", "CTGTCTCTTATA"]


def test_read_adapter_parquet_empty_raises(tmp_path):
    """A 0-row adapter Parquet is a misconfiguration; the reader raises ValueError
    (-> BAD_INPUT) rather than returning an empty adapter list."""
    from qiita_compute_orchestrator.jobs import qc
    from qiita_compute_orchestrator.miint import open_miint_conn

    p = _adapter_parquet(tmp_path, name="empty.parquet")
    with open_miint_conn() as conn, pytest.raises(ValueError):
        qc._read_adapter_parquet(conn, p)


def test_qc_plan_sizes_walltime_from_read_count(tmp_path, write_reads_q):
    """qc.plan() sizes WALLTIME (not memory — qc streams) from the read count,
    and returns only that axis (cpu/mem left to the YAML baseline)."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads_q(
        tmp_path / "reads.parquet",
        [(i, "se", "AAAA", [40, 40, 40, 40], None, None) for i in range(5)],
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=tmp_path / "adapters.parquet",  # unread by plan()
        prep_sample_idx=1,
        work_ticket_idx=1,
    )
    job_plan = qc.plan(inputs)
    r = job_plan.resources
    assert r is not None
    assert r.mem_gb is None and r.cpu is None
    # 5 reads: base 300 s + ceil(5/1e6 * 30) = 301 s.
    assert r.walltime.total_seconds() == 301


def test_qc_plan_walltime_grows_with_read_count(tmp_path):
    """More reads -> a larger walltime estimate (monotonic in cardinality).

    Builds the reads Parquet cheaply via DuckDB `range()` — qc.plan() only does
    a footer `count(*)`, so the row *count* is all that matters and we avoid
    materializing millions of literal rows (a `VALUES` list of 2M tuples is a
    multi-GB SQL string, not a test)."""
    from qiita_compute_orchestrator.jobs import qc

    def _reads(n: int) -> Path:
        path = tmp_path / f"reads_{n}.parquet"
        with duckdb.connect(":memory:") as conn:
            conn.execute(
                f"COPY (SELECT i AS sequence_idx FROM range({n}) t(i)) TO '{path}' (FORMAT PARQUET)"
            )
        return path

    def _plan_for(n: int):
        inputs = qc.Inputs(
            reads=_reads(n),
            adapter_parquet=tmp_path / "a.parquet",
            prep_sample_idx=1,
            work_ticket_idx=1,
        )
        return qc.plan(inputs).resources.walltime

    # 2M reads adds ceil(2 * 30) = 60 s over the base; 10 reads adds 1 s.
    assert _plan_for(2_000_000) > _plan_for(10)


# --- optional incoming mask (`adapter_mask`) ---------------------------------
#
# The trim MATH is pinned in test_qc_smoke.py against real miint. Here we assert
# the orchestration around it: the boundary guards, and that the incoming mask's
# non-`pass` rows reach the output via the carry branch without touching a seam.


def test_qc_missing_adapter_mask_raises(tmp_path, write_reads):
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(1, "a", "ACGT", None)])
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=tmp_path / "nope.parquet",
        work_ticket_idx=1,
    )
    with pytest.raises(FileNotFoundError, match="partial_mask not found"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_incoming_mask_rejects_paired_end(tmp_path, write_reads, write_partial_mask):
    """PE + incoming mask is unsupported (only long reads carry one) -> fail loud,
    rather than run the SE seam's cumulative-trim math on half a pair."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(1, "a", "ACGT", "TGCA")])
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet", [(1, ReadMaskReason.PASS.value, 0, 0)]
        ),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="paired-end"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_incoming_mask_must_cover_every_read(tmp_path, write_reads, write_partial_mask):
    """A short mask would silently DROP reads through the SE seam's inner join,
    understating the sample's raw read count downstream."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(1, "a", "ACGT", None), (2, "b", "ACGT", None)]
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet", [(1, ReadMaskReason.PASS.value, 0, 0)]
        ),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="exactly one row per read"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_incoming_mask_rejects_overlong_trims(tmp_path, write_reads, write_partial_mask):
    """left+right > length would make DuckDB's substr walk BACKWARDS (returning
    bases) while the qual slice returns [] — a silent sequence/qual desync."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(tmp_path / "reads.parquet", [(1, "a", "ACGT", None)])
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet", [(1, ReadMaskReason.PASS.value, 3, 3)]
        ),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="exceeds the read length"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_carry_branch_passes_non_pass_rows_through(
    tmp_path, monkeypatch, write_reads, write_partial_mask
):
    """Non-`pass` incoming rows reach the output verbatim without visiting a seam.

    Both seams are stubbed to emit nothing, so every row in the output came from
    the carry branch — and the `pass` row is absent precisely because its seam
    produced no rows.
    """
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(1, "kept", "ACGT", None), (2, "dropped", "ACGT", None)]
    )
    monkeypatch.setattr(qc, "_qc_se_select", lambda *a, **k: _EMPTY_SEAM_SELECT)
    monkeypatch.setattr(qc, "_qc_pe_select", lambda *a, **k: _EMPTY_SEAM_SELECT)
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet",
            [(1, ReadMaskReason.PASS.value, 0, 0), (2, ReadMaskReason.QC_TOO_MANY_N.value, 4, 6)],
        ),
        work_ticket_idx=1,
    )
    out = asyncio.run(qc.execute(inputs, tmp_path / "ws"))
    assert _schema(out["qc_mask"]) == _MASK_SCHEMA
    # _rows -> (sequence_idx, reason, left_trim2 IS NULL): only the carried row.
    assert _rows(out["qc_mask"]) == [(2, ReadMaskReason.QC_TOO_MANY_N.value, True)]
    # ...and its trims survived verbatim.
    with duckdb.connect(":memory:") as conn:
        trims = conn.execute(
            f"SELECT left_trim1, right_trim1 FROM read_parquet('{out['qc_mask']}')"
        ).fetchall()
    assert trims == [(4, 6)]


def test_qc_incoming_mask_rejects_duplicate_sequence_idx(tmp_path, write_reads, write_partial_mask):
    """Equal row counts are not enough: reads {1,2} vs a mask {1,1} would fan the
    join out (read 1 twice) and drop read 2. Both legs must fail loud."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(1, "a", "ACGT", None), (2, "b", "ACGT", None)]
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet", [(1, _PASS, 0, 0), (1, _PASS, 0, 0)]
        ),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="duplicate sequence_idx"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_incoming_mask_rejects_unmatched_read(tmp_path, write_reads, write_partial_mask):
    """Same cardinality, disjoint keys: read 2 has no mask row and would vanish."""
    from qiita_compute_orchestrator.jobs import qc

    reads = write_reads(
        tmp_path / "reads.parquet", [(1, "a", "ACGT", None), (2, "b", "ACGT", None)]
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(
            tmp_path / "m.parquet", [(1, _PASS, 0, 0), (3, _PASS, 0, 0)]
        ),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="covering 1 of 2 read"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))


def test_qc_incoming_mask_rejects_trims_longer_than_the_qual_array(
    tmp_path, write_reads_q, write_partial_mask
):
    """The sequence slice bounds on length(sequence1), the qual slice on
    length(qual1). A guard that trusts one to speak for the other is exactly how
    the two silently desync — so both are checked."""
    from qiita_compute_orchestrator.jobs import qc

    # A hand-corrupted read: qual shorter than the sequence. Trims fit the sequence
    # but overrun the qual array.
    reads = write_reads_q(
        tmp_path / "reads.parquet", [(1, "a", "ACGTACGTAC", [35, 35, 35], None, None)]
    )
    inputs = qc.Inputs(
        reads=reads,
        adapter_parquet=_adapter_parquet(tmp_path, _AD),
        partial_mask=write_partial_mask(tmp_path / "m.parquet", [(1, _PASS, 2, 2)]),
        work_ticket_idx=1,
    )
    with pytest.raises(ValueError, match="exceeds the read length"):
        asyncio.run(qc.execute(inputs, tmp_path / "ws"))
