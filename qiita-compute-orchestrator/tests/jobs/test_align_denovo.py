"""Real-miint contract test for `align_denovo` (align_minimap2 NOT stubbed).

The two data seams are monkeypatched, and what replaces them is a real
`pyarrow.RecordBatchReader` registered on the connection — the same object
`open_doget_stream` registers. That makes the module's single-consumption constraint
testable rather than assertable: every assertion below about a read reaching the
output is also an assertion that nothing scanned the read stream a second time, since
a second scan returns zero rows with no error.

The fixture is a 20 kb contig held linearised, the shape an assembler emits a circular
contig in. Its origin-spanning read is present on BOTH strands, because the reference
interval the job derives is on the contig's forward axis and a strand-blind derivation
reports the reverse copy as an ordinary interval instead of a wrap.
"""

from __future__ import annotations

import asyncio
import random
from contextlib import asynccontextmanager

import duckdb
import pyarrow as pa
import pytest

from qiita_compute_orchestrator.jobs import align_denovo
from qiita_compute_orchestrator.jobs.align_denovo import Inputs, execute
from qiita_compute_orchestrator.miint import open_miint_conn

_ALIGNMENT_IDX = 4242
_PREP_SAMPLE_IDX = 77
_PROCESSING_IDX = 88
_MASK_IDX = 99

# feature_idx of the two contigs. Distinct magnitudes so a row attributed to the wrong
# one is visible rather than plausible.
_CIRCULAR = 901
_LINEAR = 902

_CONTIG_LENGTH = 20_000
_READ_LENGTH = 6_000


def _rand_seq(rng: random.Random, n: int) -> str:
    return "".join(rng.choice("ACGT") for _ in range(n))


def _reverse_complement(seq: str) -> str:
    """miint's scalar, not a `str.maketrans` table: a hand-rolled one misses the IUPAC
    ambiguity codes and is case-sensitive where the scalar is not, and a test oracle is
    the worst place to carry a second implementation."""
    with open_miint_conn() as conn:
        return conn.execute("SELECT sequence_dna_reverse_complement(?)", [seq]).fetchone()[0]


@pytest.fixture
def genome():
    """The two contigs and the five reads, with what each read is FOR."""
    rng = random.Random(20260823)
    circular = _rand_seq(rng, _CONTIG_LENGTH)
    linear = _rand_seq(rng, 12_000)
    spanning = circular[_CONTIG_LENGTH - 3_000 :] + circular[:3_000]
    return {
        "contigs": {_CIRCULAR: circular, _LINEAR: linear},
        "reads": {
            # Crosses the origin: the case the circular gate exists for.
            11: spanning,
            # Does not: the control that says a split is a property of the origin and
            # not of the read length.
            12: circular[5_000 : 5_000 + _READ_LENGTH],
            # The same origin-crossing read on the other strand.
            13: _reverse_complement(spanning),
            # A second contig, so a row attributed to the wrong feature is visible.
            14: linear[2_000 : 2_000 + _READ_LENGTH],
            # 40% contig, 60% unrelated: pooled coverage lands far below the default
            # floor, and well above a lowered one.
            15: circular[9_000:11_400] + _rand_seq(rng, 3_600),
        },
    }


def _reader(schema: pa.Schema, rows: list[dict]) -> pa.RecordBatchReader:
    """A single-consumption reader over `rows` — what `open_doget_stream` registers.

    Batched at 2 rows so the fixture crosses batch boundaries; a single-batch reader
    would not exercise the streaming path the production seam uses.
    """
    table = pa.Table.from_pylist(rows, schema=schema)
    return pa.RecordBatchReader.from_batches(schema, table.to_batches(max_chunksize=2))


_CHUNK_SCHEMA = pa.schema(
    [
        ("feature_idx", pa.int64()),
        ("chunk_index", pa.int64()),
        ("chunk_data", pa.string()),
    ]
)

# The `read_masked` macro's own column set. Carried whole rather than narrowed to what
# the job reads, so the test exercises the projection the job actually applies — in
# particular that `sequence2` is present on the wire and is not selected.
_READ_SCHEMA = pa.schema(
    [
        ("mask_idx", pa.int64()),
        ("prep_sample_idx", pa.int64()),
        ("sequence_idx", pa.int64()),
        ("read_id", pa.string()),
        ("sequence1", pa.string()),
        ("qual1", pa.list_(pa.uint8())),
        ("sequence2", pa.string()),
        ("qual2", pa.list_(pa.uint8())),
    ]
)

# Contig chunking, so reassembly is exercised rather than assumed: a whole contig in
# one chunk would pass even if `string_agg`'s ORDER BY were dropped.
_CHUNK_BP = 4_096


def _install_streams(monkeypatch, genome, *, contigs=None, reads=None) -> None:
    """Replace both data seams with registered readers over `genome`.

    `contigs` / `reads` override what the corresponding stream carries, for the cases
    that need an empty or reduced one.
    """
    contig_rows = [
        {
            "feature_idx": idx,
            "chunk_index": i,
            "chunk_data": seq[i * _CHUNK_BP : (i + 1) * _CHUNK_BP],
        }
        for idx, seq in (genome["contigs"] if contigs is None else contigs).items()
        for i in range((len(seq) + _CHUNK_BP - 1) // _CHUNK_BP)
    ]
    read_rows = [
        {
            "mask_idx": _MASK_IDX,
            "prep_sample_idx": _PREP_SAMPLE_IDX,
            "sequence_idx": idx,
            "read_id": f"m84001/{idx}/ccs",
            "sequence1": seq,
            "qual1": None,
            "sequence2": None,
            "qual2": None,
        }
        for idx, seq in (genome["reads"] if reads is None else reads).items()
    ]

    @asynccontextmanager
    async def _chunks(conn, *, prep_sample_idx, processing_idx, relation="assembly_chunks"):
        assert (prep_sample_idx, processing_idx) == (_PREP_SAMPLE_IDX, _PROCESSING_IDX)
        conn.register(relation, _reader(_CHUNK_SCHEMA, contig_rows))
        try:
            yield relation
        finally:
            conn.unregister(relation)

    @asynccontextmanager
    async def _reads(conn, *, prep_sample_idx, mask_idx, relation="masked_reads"):
        assert (prep_sample_idx, mask_idx) == (_PREP_SAMPLE_IDX, _MASK_IDX)
        conn.register(relation, _reader(_READ_SCHEMA, read_rows))
        try:
            yield relation
        finally:
            conn.unregister(relation)

    monkeypatch.setattr(align_denovo, "open_assembly_chunk_stream", _chunks)
    monkeypatch.setattr(align_denovo, "open_read_masked_stream", _reads)


def _inputs(**overrides) -> Inputs:
    return Inputs(
        **{
            "prep_sample_idx": _PREP_SAMPLE_IDX,
            "work_ticket_idx": 5,
            "assembly_processing_idx": _PROCESSING_IDX,
            "align_mask_idx": _MASK_IDX,
            "alignment_idx": _ALIGNMENT_IDX,
            "preset": "map-hifi",
            "min_identity": 0.95,
            "min_query_coverage": 0.90,
            **overrides,
        }
    )


def _run(monkeypatch, genome, tmp_path, **overrides):
    """Run the job and read both outputs back as lists of dicts."""
    _install_streams(
        monkeypatch, genome, **{k: overrides.pop(k) for k in ("contigs", "reads") if k in overrides}
    )
    outputs = asyncio.run(execute(_inputs(**overrides), tmp_path / "ws"))
    with duckdb.connect(":memory:") as conn:
        return outputs, {
            key: conn.execute(f"SELECT * FROM read_parquet('{outputs[key]}')").fetchall()
            for key in ("alignment", "alignment_origin_spanning")
        }


def _columns(path) -> list[tuple[str, str]]:
    with duckdb.connect(":memory:") as conn:
        return [
            (name, str(dtype))
            for name, dtype in zip(
                *[
                    (rel.columns, rel.types)
                    for rel in [conn.sql(f"SELECT * FROM read_parquet('{path}')")]
                ][0],
                strict=True,
            )
        ]


def test_every_gate_clearing_read_reaches_the_alignment_output(monkeypatch, genome, tmp_path):
    """The end-to-end pass, and the single-consumption invariant with it.

    Reads 11 and 13 are the origin-spanning pair the per-record floor would drop, 12
    and 14 the whole-read controls, and 15 the one that genuinely fails the coverage
    threshold. Every read but 15 must be present — which is also what fails if
    anything scanned the registered read stream a second time, since the second scan
    returns zero rows with no error.
    """
    _, out = _run(monkeypatch, genome, tmp_path)
    by_read: dict[int, list] = {}
    for row in out["alignment"]:
        by_read.setdefault(row[2], []).append(row)

    assert sorted(by_read) == [11, 12, 13, 14]
    # The split reads keep BOTH of their records — the gate admits the group, not a
    # representative of it.
    assert [len(by_read[read]) for read in (11, 12, 13, 14)] == [2, 1, 2, 1]
    assert {row[0] for row in out["alignment"]} == {_ALIGNMENT_IDX}
    assert {row[1] for row in out["alignment"]} == {_PREP_SAMPLE_IDX}
    assert by_read[14][0][3] == _LINEAR
    assert {row[3] for read in (11, 12, 13) for row in by_read[read]} == {_CIRCULAR}


def test_a_read_below_the_coverage_floor_is_dropped_by_the_threshold(monkeypatch, genome, tmp_path):
    """Read 15 is absent above, and this is the control that says the THRESHOLD is why.

    Same fixture, same aligner call, only the floor moved: at 0.30 the read appears.
    Without this, "15 is missing" would be equally consistent with it never aligning.
    """
    _, out = _run(monkeypatch, genome, tmp_path, min_query_coverage=0.30)
    assert 15 in {row[2] for row in out["alignment"]}


def test_an_origin_spanning_read_is_recorded_with_a_wrapping_interval(
    monkeypatch, genome, tmp_path
):
    """The side table's rows, and the strand-independence `_origin_spanning_sql`
    derives: both strands of the same origin-crossing read report the SAME reference
    interval, and only `is_reverse` differs."""
    _, out = _run(monkeypatch, genome, tmp_path)
    rows = {row[2]: row for row in out["alignment_origin_spanning"]}

    # Only the split reads are recorded — a whole-read alignment is not evidence of
    # anything, and the DDL says the table holds only reads with the evidence.
    assert sorted(rows) == [11, 13]
    for read_id, is_reverse in ((11, False), (13, True)):
        (
            alignment_idx,
            prep_sample_idx,
            sequence_idx,
            feature_idx,
            query_start,
            query_stop,
            feature_start,
            feature_stop,
            row_is_reverse,
            pooled_identity,
            pooled_coverage,
            fragment_count,
        ) = rows[read_id]
        assert (alignment_idx, prep_sample_idx, sequence_idx, feature_idx) == (
            _ALIGNMENT_IDX,
            _PREP_SAMPLE_IDX,
            read_id,
            _CIRCULAR,
        )
        assert (query_start, query_stop) == (0, _READ_LENGTH)
        assert feature_start > feature_stop, "an origin-crossing read must wrap"
        assert (feature_start, feature_stop) == (_CONTIG_LENGTH - 3_000 + 1, 3_000 + 1)
        assert row_is_reverse is is_reverse
        assert (pooled_identity, pooled_coverage, fragment_count) == (1.0, 1.0, 2)


def test_the_fragments_of_a_recorded_read_stay_in_the_alignment_output(
    monkeypatch, genome, tmp_path
):
    """The side table records the MERGED read; `alignment` keeps one row per SAM
    record, unchanged. That split is what the DuckLake DDL's consumer contract rests
    on — a consumer LEFT JOINs the side table to rescore reads whose fragments it can
    see in `alignment`."""
    _, out = _run(monkeypatch, genome, tmp_path)
    spanning = [row for row in out["alignment"] if row[2] == 11]
    assert len(spanning) == 2
    # Each fragment keeps its own CIGAR, covering its own half of the read.
    with open_miint_conn() as conn:
        per_record = [
            conn.execute("SELECT cigar_query_coverage(?)", [row[9]]).fetchone()[0]
            for row in spanning
        ]
    assert per_record == [0.5, 0.5], "a fragment must still score as a fragment"


# Four multi-fragment groups the gate clears, of which exactly ONE crossed the origin.
# Rows are hand-built SAM records (the shape `test_circular_gate_contract.py` uses), so
# `_origin_spanning_sql` is under test rather than the aligner's placement choices.
# `position` is 1-based inclusive, `stop_position` exclusive, contig length 20 kb.
#   (sequence_idx, flags, position, stop_position, cigar)
_GROUPS: dict[int, list[tuple[int, int, int, str]]] = {
    # A single origin crossing, forward. The one shape the table is defined for.
    31: [(0, 17_001, 20_001, "3000=3000S"), (2048, 1, 3_001, "3000S3000=")],
    # The same molecule reported reverse — same contig bases, so the same interval.
    32: [(16, 17_001, 20_001, "3000=3000S"), (2064, 1, 3_001, "3000S3000=")],
    # Two loci of one contig, same strand, reported DESCENDING: a tandem repeat or a
    # collapsed IS element. Ordering the fragments along the read gives
    # `feature_start > feature_stop` here, which would read as a wrap that never
    # happened. Neither fragment touches an end.
    33: [(0, 15_001, 18_001, "3000=3000S"), (2048, 2_001, 5_001, "3000S3000=")],
    # A read LONGER than the contig: it laps, so several fragments reach both ends and
    # the covered set is the whole contig, which no (start, stop) pair describes.
    34: [
        (0, 17_001, 20_001, "3000=17000S"),
        (2048, 1, 20_001, "3000S20000="),
        (2048, 1, 3_001, "23000S3000="),
    ],
}


def _stage_groups(conn, groups: dict[int, list[tuple[int, int, int, str]]]) -> None:
    """Put `groups` where `_origin_spanning_sql` reads them: the streamed slice, the
    cleared pooled rows, and the per-feature lengths."""
    from qiita_common.analytic import FEATURE_LENGTHS_TABLE, STREAMED_ALIGNMENT_TABLE

    rows = ", ".join(
        f"({_ALIGNMENT_IDX}::BIGINT, {_PREP_SAMPLE_IDX}::BIGINT, {read}::BIGINT, "
        f"{_CIRCULAR}::BIGINT, {flags}::USMALLINT, {pos}::BIGINT, {stop}::BIGINT, "
        f"'{cigar}')"
        for read, frags in groups.items()
        for flags, pos, stop, cigar in frags
    )
    conn.execute(
        f"CREATE TABLE {STREAMED_ALIGNMENT_TABLE} AS SELECT * FROM (VALUES {rows}) AS t"
        "(alignment_idx, prep_sample_idx, sequence_idx, feature_idx, flags, position, "
        "stop_position, cigar)"
    )
    cleared = ", ".join(
        f"({read}::BIGINT, false, {_CIRCULAR}::BIGINT, 1.0, 1.0, {len(frags)}::BIGINT)"
        for read, frags in groups.items()
    )
    conn.execute(
        f"CREATE TABLE {align_denovo._CLEARED} AS SELECT * FROM (VALUES {cleared}) AS t"
        "(read_id, is_read1, reference, coverage, identity, n_fragments)"
    )
    conn.execute(
        f"CREATE TABLE {FEATURE_LENGTHS_TABLE} AS SELECT {_CIRCULAR}::BIGINT AS "
        f"feature_idx, {_CONTIG_LENGTH}::BIGINT AS sequence_length_bp"
    )


def test_the_aligner_really_splits_a_two_locus_read_the_criterion_refuses():
    """The other half of the test above, against REAL miint: the shape the criterion
    refuses is one the aligner actually produces.

    That test stages hand-built SAM rows so the SQL is what is under test, which means
    it cannot fail if minimap2 stopped splitting such a read — the refusal would go
    untested rather than unreachable. This runs the job's own aligner call and asserts
    the split happens, and that the second record is SUPPLEMENTARY (0x800): a secondary
    would be suppressed by `max_secondary := 0` and the group would never reach the
    gate at all.

    Measured on mirror `9fc4d12`. The origin-crossing read is the control — it must
    still split, or the comparison says nothing about the two-locus shape.
    """
    rng = random.Random(20260824)
    contig = _rand_seq(rng, _CONTIG_LENGTH)
    reads = {
        # Two loci of one contig, same strand, reported descending: never touches an end.
        41: contig[15_000:18_000] + contig[2_000:5_000],
        # The control: the same construction taken ACROSS the origin.
        42: contig[_CONTIG_LENGTH - 3_000 :] + contig[:3_000],
        43: contig[5_000 : 5_000 + _READ_LENGTH],
    }
    with open_miint_conn() as conn:
        conn.execute(f"CREATE TABLE {align_denovo._SUBJECT} (read_id BIGINT, sequence1 VARCHAR)")
        conn.execute(f"INSERT INTO {align_denovo._SUBJECT} VALUES (?, ?)", [_CIRCULAR, contig])
        conn.execute(f"CREATE TABLE {align_denovo._QUERY} (read_id BIGINT, sequence1 VARCHAR)")
        conn.executemany(f"INSERT INTO {align_denovo._QUERY} VALUES (?, ?)", list(reads.items()))
        conn.execute(
            align_denovo._streamed_alignment_sql(_ALIGNMENT_IDX, _PREP_SAMPLE_IDX),
            [align_denovo._QUERY, align_denovo._SUBJECT, "map-hifi"],
        )
        from qiita_common.analytic import STREAMED_ALIGNMENT_TABLE

        by_read: dict[int, list[int]] = {}
        for sequence_idx, flags in conn.execute(
            f"SELECT sequence_idx, flags FROM {STREAMED_ALIGNMENT_TABLE} "
            "ORDER BY sequence_idx, position"
        ).fetchall():
            by_read.setdefault(sequence_idx, []).append(flags)

    assert sorted(by_read[41]) == [0, 2048], "the two-locus read must split, supplementary"
    assert sorted(by_read[42]) == [0, 2048], "control: the origin-crossing read still splits"
    assert by_read[43] == [0]


def test_only_a_group_that_actually_crosses_the_origin_is_recorded():
    """The criterion is the coordinates, not the fragment count — and each rejected
    shape is rejected for its own reason.

    A tandem-repeat read (33) is split across two loci that never touch an end, and is
    reported descending, so a read-order derivation gives it `feature_start >
    feature_stop`: a wrap that never happened. A read longer than the contig (34) laps
    it, covering the whole contig, which no `(start, stop)` pair describes. Both clear
    the gate, so nothing upstream separates them from the real crossing (31/32).
    """
    with open_miint_conn() as conn:
        _stage_groups(conn, _GROUPS)
        rows = conn.execute(
            align_denovo._origin_spanning_sql() + " ORDER BY sequence_idx"
        ).fetchall()

    assert [row[2] for row in rows] == [31, 32]
    # Both strands of one molecule give the SAME forward-axis interval; `is_reverse` is
    # the only difference. A derivation that ordered fragments along the READ gives the
    # reverse copy `(1, 20001)` — an ordinary interval — instead.
    for row, is_reverse in zip(rows, (False, True), strict=True):
        assert (row[6], row[7]) == (17_001, 3_001)
        assert row[6] > row[7], "an origin crossing must wrap"
        assert row[8] is is_reverse
    assert [row[11] for row in rows] == [2, 2]


def test_the_rejected_groups_keep_their_alignment_rows(monkeypatch, genome, tmp_path):
    """Rejecting a group from the SIDE table drops nothing from `alignment` — the gate
    decides what is persisted, and this SQL decides only what carries a wrap
    description. `alignment` is where a consumer sees the fragments either way."""
    _, out = _run(monkeypatch, genome, tmp_path)
    recorded = {row[2] for row in out["alignment_origin_spanning"]}
    aligned = {row[2] for row in out["alignment"]}
    assert recorded < aligned


def test_the_outputs_match_the_ducklake_table_schemas(monkeypatch, genome, tmp_path):
    """Column names, order and types for both Parquets. `register-files` schema-matches
    on the full column list, so a projection change that reorders or retypes a column
    fails at registration, on the cluster, after the alignment has been paid for."""
    outputs, _ = _run(monkeypatch, genome, tmp_path)
    assert _columns(outputs["alignment"]) == [
        ("alignment_idx", "BIGINT"),
        ("prep_sample_idx", "BIGINT"),
        ("sequence_idx", "BIGINT"),
        ("feature_idx", "BIGINT"),
        ("mate_feature_idx", "BIGINT"),
        ("flags", "USMALLINT"),
        ("position", "BIGINT"),
        ("stop_position", "BIGINT"),
        ("mapq", "UTINYINT"),
        ("cigar", "VARCHAR"),
        ("mate_position", "BIGINT"),
        ("template_length", "BIGINT"),
        ("tag_as", "BIGINT"),
        ("tag_xs", "BIGINT"),
        ("tag_ys", "BIGINT"),
        ("tag_xn", "BIGINT"),
        ("tag_xm", "BIGINT"),
        ("tag_xo", "BIGINT"),
        ("tag_xg", "BIGINT"),
        ("tag_nm", "BIGINT"),
        ("tag_yt", "VARCHAR"),
        ("tag_md", "VARCHAR"),
        ("tag_sa", "VARCHAR"),
    ]
    assert _columns(outputs["alignment_origin_spanning"]) == [
        ("alignment_idx", "BIGINT"),
        ("prep_sample_idx", "BIGINT"),
        ("sequence_idx", "BIGINT"),
        ("feature_idx", "BIGINT"),
        ("query_start", "BIGINT"),
        ("query_stop", "BIGINT"),
        ("feature_start", "BIGINT"),
        ("feature_stop", "BIGINT"),
        ("is_reverse", "BOOLEAN"),
        ("pooled_identity", "DOUBLE"),
        ("pooled_coverage", "DOUBLE"),
        ("fragment_count", "BIGINT"),
    ]


def test_a_read_set_that_aligns_nowhere_still_writes_both_schema_correct_outputs(
    monkeypatch, genome, tmp_path
):
    """A completed mask can carry reads that align to none of the sample's own contigs.
    register-files then registers 0 rows and the gate flips over an empty footprint,
    which is a legitimate outcome — but only if the files exist and carry the schema."""
    rng = random.Random(7)
    outputs, out = _run(monkeypatch, genome, tmp_path, reads={21: _rand_seq(rng, _READ_LENGTH)})
    assert out == {"alignment": [], "alignment_origin_spanning": []}
    assert _columns(outputs["alignment"])[0] == ("alignment_idx", "BIGINT")
    assert _columns(outputs["alignment_origin_spanning"])[0] == ("alignment_idx", "BIGINT")


def test_an_empty_contig_roster_is_refused_and_leaves_no_partial_output(
    monkeypatch, genome, tmp_path
):
    """The submit path admits the ticket only over an `assembly_sample` gate reading
    'completed', so no contigs means the gate and the lake disagree. Failing loudly is
    the difference between that and an empty alignment reported as success."""
    workspace = tmp_path / "ws"
    _install_streams(monkeypatch, genome, contigs={})
    with pytest.raises(RuntimeError, match="has no contigs"):
        asyncio.run(execute(_inputs(), workspace))
    assert not (workspace / "alignment.parquet").exists()
    assert not (workspace / "alignment_origin_spanning.parquet").exists()


def test_a_query_that_does_not_align_produces_no_row_at_all():
    """What the absent unmapped filter rests on. `include_unmapped` defaults false, so
    `align_minimap2` emits no row for a query that found no alignment, rather than one
    flagged 0x4 (<https://the-miint.github.io/duckdb-miint/alignment_reference/>). Were
    that to change, unmapped records would reach the gate — `circular_query_coverage`
    excludes them, so `check_gate_diagnostics` would refuse the whole slice.

    The aligning control is what makes the zero meaningful: without it, a fixture that
    produces no rows either way would assert nothing.
    """
    rng = random.Random(20260825)
    subject = _rand_seq(rng, 20_000)
    with open_miint_conn() as conn:
        conn.execute("CREATE TABLE s AS SELECT 901::BIGINT AS read_id, ? AS sequence1", [subject])
        conn.execute(
            "CREATE TABLE q AS SELECT 1::BIGINT AS read_id, ? AS sequence1 "
            "UNION ALL SELECT 2::BIGINT, ?",
            [subject[4_000:10_000], _rand_seq(rng, 6_000)],
        )
        aligned = conn.execute(
            "SELECT DISTINCT read_id FROM align_minimap2("
            "'q', subject_table := 's', preset := 'map-hifi', eqx := true, "
            "max_secondary := 0) ORDER BY read_id"
        ).fetchall()
    # read 1 is a substring of the subject and aligns; read 2 is unrelated and is
    # absent entirely rather than present-and-unmapped.
    assert aligned == [(1,)]


def test_the_aligner_call_keeps_secondaries_and_asks_for_eqx_cigars(monkeypatch, genome, tmp_path):
    """The cap matches `align_sharded`'s, and `eqx` is what makes both axes scorable:
    pooled identity is NULL on a legacy-`M` CIGAR, and so is the per-record identity the
    secondary arm applies. A change to either constant surfaces here rather than as a
    quietly different output."""
    sql = align_denovo._streamed_alignment_sql(_ALIGNMENT_IDX, _PREP_SAMPLE_IDX)
    assert "max_secondary := 100" in sql
    assert "eqx := true" in sql
    # No unmapped filter, and none requested: see the contract test below.
    assert "include_unmapped" not in sql

    # From the contract layer, not a literal here: the control plane hashes the same
    # constant into `alignment_idx`, and a second copy could drift from the value the
    # identity was built on.
    from qiita_common.analytic import MAX_SECONDARY

    assert f"max_secondary := {MAX_SECONDARY}" in sql


def test_a_slice_no_axis_can_score_is_still_refused():
    """Keeping secondaries relaxed exactly one of the two classes the circular gate
    could not pool. An unmapped or coordinate-less row still has no pooled group AND no
    CIGAR span, so it would leave the slice without failing a threshold."""
    from qiita_common.analytic import AlignmentGate, check_gate_diagnostics

    with pytest.raises(ValueError, match="neither axis"):
        check_gate_diagnostics(
            AlignmentGate(circular=True),
            total_rows=10,
            scorable_rows=None,
            unpoolable_partitions=0,
            unpoolable_rows=1,
            secondary_rows=0,
            unscorable_groups=0,
            paired_rows=0,
        )
    # ... and the secondaries the aligner is now asked for are not that class.
    assert check_gate_diagnostics(
        AlignmentGate(circular=True),
        total_rows=10,
        scorable_rows=None,
        unpoolable_partitions=0,
        unpoolable_rows=0,
        secondary_rows=4,
        unscorable_groups=0,
        paired_rows=0,
    ).gate.circular


def test_no_arrow_materialization_in_the_feeding_path():
    """`con.execute(...).arrow()` returns a lazy `RecordBatchReader` in DuckDB 1.5.4,
    and a miint table function over one backed by a query on the SAME connection
    deadlocks (duckdb-miint#230, open). A Flight-sourced reader is not affected, so the
    constraint is only that this job never builds one itself."""
    import inspect

    assert ".arrow()" not in inspect.getsource(align_denovo)
