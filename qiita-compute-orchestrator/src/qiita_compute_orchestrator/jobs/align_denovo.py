"""Native job: align a sample's masked reads back to its OWN assembled contigs.

The de novo arm of alignment. `align_sharded` aligns a block of reads against a
sharded REFERENCE; this aligns one sample's reads against the contigs that sample's
own `long-read-assembly` run produced, and lands the result in the same DuckLake
`alignment` table under a de novo `alignment_idx`.

Both inputs are STREAMED from the data plane at runtime, so nothing is materialized
onto shared scratch at submit time:

  * the SUBJECT — this run's contigs (`open_assembly_chunk_stream`), through
    `stage_subject` into the same `(read_id, sequence1)` TABLE the index builders
    use. ONE DoGet: the aligner needs its subject as a table, and the per-feature
    lengths the circular gate needs are then read off that staged table rather than
    streamed again. A lengths-only DoGet would not need the reassembly at all —
    `sum(length(chunk_data))` per feature equals the reassembled length without
    ordering or concatenation.
  * the QUERY — this sample's masked reads (`open_read_masked_stream`; the
    mask-lifecycle refusals that ride that mint are on
    `fetch_read_masked_doget_ticket`). Referenced EXACTLY ONCE, through a projecting
    VIEW the aligner drains — see `data_plane_client.open_doget_stream` for what a
    second scan of a registered reader costs.

Which contigs and which reads each stream carries follows from the signed pair, not
from anything this job names.

`sequence_idx` — the globally-unique read identity — rides as the aligner's
`read_id`, so the output maps straight onto the lake's `alignment.sequence_idx`
without a join. The subject's `read_id` is the contig's `feature_idx`, so the
aligner's `reference` column IS the feature, and it arrives BIGINT because
`align_minimap2` preserves the subject table's types.

The gate is the circular-pooled one, and that choice fixes two call parameters.

An assembler emits a circular contig as a linearised sequence, so a read crossing its
origin is reported as one SAM record per side, each covering only its own share of the
read, and a per-record query-coverage floor drops both.
`qiita_common.analytic.gate`'s circular mode pools a read's records against one contig
through miint's `circular_query_coverage`
(<https://the-miint.github.io/duckdb-miint/alignment_analysis/#circular-query-coverage>)
and judges the read there.

The two thresholds themselves — `min_identity` and `min_query_coverage`, defaulting to
0.95 / 0.90 from the action's `context_schema` — reach the rows through
`circular_predicate_sql`, bound by `gate_parameters(gate)` in the `WHERE` of the
`_CLEARED` table below. That is the only place a record is dropped for scoring too low.

Two consequences of the circular choice for the aligner call:

  * `eqx := true`. `cigar_pooled_identity` — the aggregate the macro reports as its
    `identity` column — is NULL on a CIGAR carrying no `=`/`X` ops, and
    `check_gate_diagnostics` refuses a whole slice for one such read.
  * `max_secondary := 0`. SECONDARY (FLAG 0x100), not supplementary (0x800): the two
    sides of an origin-spanning read arrive as SUPPLEMENTARY records, which this does
    not touch and which the macro keeps — "a read split across a supplementary is one
    molecule whatever the reference's topology" (see the link above). So requesting no
    secondaries does not cost the crossing this gate exists to catch.
    `circular_query_coverage` excludes secondary records, a documented precondition,
    which costs a slice containing them two ways: a secondary alongside a primary rides
    into the gated output unscored, and a group whose ONLY record is secondary produces
    no macro row at all and vanishes from the slice entirely. `check_gate_diagnostics`
    refuses either. Requesting none is what puts the slice inside what the gate can
    judge. `align_sharded` keeps them (`max_secondary := 100`) because its per-record
    CIGAR gate scores them; a read that is genuinely high-identity to two contigs — a
    recent HGT into a shared operon — is therefore not placed twice here. That is the
    assay call for this workflow, not a limitation of the aligner.

Unmapped records are dropped on the way into the slice, for the reason the macro
excludes them.

This job is the first producer for the DuckLake `alignment_origin_spanning` side
table; its DDL (`qiita-data-plane/src/ducklake.rs`) carries the contract a producer
owes and what a consumer must do with it, and `_origin_spanning_sql` says which groups
this one records.

The two output basenames are the DuckLake table names `register-files` maps by stem.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field
from qiita_common.analytic import (
    CIRCULAR_ALIGNMENTS_VIEW,
    FEATURE_LENGTHS_TABLE,
    FEATURE_TOPOLOGY_VIEW,
    STREAMED_ALIGNMENT_TABLE,
    AlignmentGate,
    check_gate_diagnostics,
    circular_alignments_view_sql,
    circular_cleared_join,
    circular_predicate_sql,
    feature_lengths_table_sql,
    feature_topology_view_sql,
    gate_diagnostics_sql,
    gate_parameters,
)
from qiita_common.parquet import validate_parquet_path

from ..data_plane_client import open_assembly_chunk_stream, open_read_masked_stream
from ..miint import (
    PARQUET_OPTS,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..subject import stage_subject

# YAML step name this module implements.
YAML_STEP_NAME = "align_denovo"

# Output basenames — the DuckLake table each registers into, by stem.
_ALIGNMENT_NAME = "alignment.parquet"
_ORIGIN_SPANNING_NAME = "alignment_origin_spanning.parquet"

# Memory split, and it is the one `assembly_coverage` documents: DuckDB holds the
# alignment table and the sorts, and CAN spill, so it is capped; minimap2's index runs
# inside the miint call, outside `memory_limit`, and cannot. The unspillable side here
# is the index over ONE sample's contigs — this job has no `SEQUENCE_DATA` lookup, so
# it is not also holding the read bytes.
_DUCKDB_THREADS = 8  # keep equal to the workflow's `cpu:` — see that entry
_DUCKDB_CAP_GB = 16
# Carves the cgroup out for the in-process co-consumer that shares the box with DuckDB:
# minimap2's index over this sample's contigs, which is unspillable. `align_sharded`
# reserves for the same reason; this is the only carve-out ON TOP of
# `duckdb_headroom_gb`, which already covers DuckDB's own above-limit RSS overshoot.
# One index over one sample's contigs, against that job's up-to-`_DUCKDB_THREADS`
# concurrent shard indexes.
_DUCKDB_RESERVE_GB = 1
_DUCKDB_FALLBACK_GB = 4

# In-DuckDB relation names this job owns. The gate's own four are not re-spelled
# here: `feature_lengths_table_sql` creates one, `circular_alignments_view_sql` and
# `feature_topology_view_sql` create the two views that read it and
# `STREAMED_ALIGNMENT_TABLE`.
_SUBJECT = "denovo_contig"
_QUERY = "denovo_query"
_CLEARED = "denovo_cleared"


class Inputs(BaseModel):
    """Typed input contract for align_denovo.

    Every field is a scalar: nothing is staged onto shared scratch for this job, so
    there are no paths to bind. `prep_sample_idx` and `work_ticket_idx` are the
    framework-injected scope scalars; the rest ride the workflow's `params:`.

    `assembly_processing_idx` names the assembly run whose contigs are the subject,
    `align_mask_idx` the completed host-depletion mask whose pass-set is the query, and
    `alignment_idx` the CP-minted de novo alignment identity stamped as every output
    row's leading column. The first two carry prefixed names rather than the bare
    `processing_idx` / `mask_idx`; the reason is on those constants in
    `qiita_control_plane.runner._alignment`.

    `preset`, `min_identity` and `min_query_coverage` are the caller-settable knobs;
    the control plane resolves them and hashes them into `alignment_idx`, so a
    run at a different threshold is a different alignment rather than a silent
    overwrite of the first. Both thresholds bind the CIRCULAR axis — they are what a
    READ must score against one contig with its records pooled, not what one record
    must score on its own.
    """

    prep_sample_idx: int
    work_ticket_idx: int
    assembly_processing_idx: int
    align_mask_idx: int
    alignment_idx: int
    preset: str
    min_identity: float = Field(ge=0.0, le=1.0)
    min_query_coverage: float = Field(ge=0.0, le=1.0)


def _streamed_alignment_sql(alignment_idx: int, prep_sample_idx: int) -> str:
    """The ungated slice: the aligner's output under the lake `alignment` table's
    column names and order, minus unmapped records.

    `alignment_idx` and `prep_sample_idx` are constants for this run — unlike the block
    path, one ticket is one sample — so they are stamped rather than joined. Both are
    validated ints (pydantic `Inputs`), safe to inline.

    `mate_feature_idx` is NULL throughout: the query is single-end (see the module
    docstring), so no record has a mate. The column is written anyway because the lake
    table declares it and `register-files` schema-matches on the full column list.

    The remaining CASTs are on values the ALIGNER did not produce, which is why they
    are not redundant with its output types. `align_minimap2` preserves the input
    types, so `read_id` and `reference` arrive BIGINT from the subject and query tables
    and are passed through uncast. A bare integer literal is INTEGER and a bare NULL is
    INTEGER, so the two stamped constants and `mate_feature_idx` are cast to match what
    the lake declares.

    No unmapped filter: `include_unmapped` defaults false, so `align_minimap2` emits no
    row at all for a query that produced no alignment
    (<https://the-miint.github.io/duckdb-miint/alignment_reference/>). An empty result
    therefore means no read aligned, not that the filter removed everything.
    """
    return (
        f"CREATE TABLE {STREAMED_ALIGNMENT_TABLE} AS "
        f"SELECT CAST({alignment_idx} AS BIGINT) AS alignment_idx, "
        f"CAST({prep_sample_idx} AS BIGINT) AS prep_sample_idx, "
        "read_id AS sequence_idx, "
        "reference AS feature_idx, "
        "CAST(NULL AS BIGINT) AS mate_feature_idx, "
        "* EXCLUDE (read_id, reference, mate_reference) "
        "FROM align_minimap2(?, subject_table := ?, preset := ?, "
        "eqx := true, max_secondary := 0)"
    )


def _origin_spanning_sql() -> str:
    """One row per `(read, contig)` where the fragments the gate cleared ARE a single
    crossing of the contig's origin, in the DuckLake `alignment_origin_spanning` column
    order.

    **What the row carries that its inputs do not.** Every column is derivable from the
    `alignment` fragments plus the per-feature lengths, so what is persisted is the
    DERIVATION, not new measurement: which fragment is first along the contig's forward
    axis depends on the read's strand, and the paragraph below is the rule for it. A
    consumer that re-derives from the fragments has to reproduce that rule — and the
    failure mode of getting it wrong is silent, since ordering by query start alone
    yields a well-formed interval for the reverse copy of the same read. Writing it
    where the gate's pooled result is already in hand keeps one implementation.

    **A multi-fragment group is not evidence of a crossing, and the criterion is the
    coordinates, not the fragment count.** The gate clears any read whose records pool
    to the thresholds, which admits two shapes that never touched the origin: a read
    split across two loci of one contig (a tandem repeat, a collapsed IS element, an
    rRNA operon — what a metagenomic assembly produces), and a read split co-linearly
    across a deletion. Both would get an interval, and on the strand where the loci
    happen to be reported descending it would come out `feature_start > feature_stop`,
    i.e. a wrap that never happened. A crossing is identifiable outright: one fragment
    must reach the contig's END (`stop_position = length + 1`, from the per-feature
    lengths the gate already staged) and another must start at its BEGINNING
    (`position = 1`). Requiring exactly one of each also excludes a read LONGER than
    the contig, which laps it — that read covers the whole contig, which no
    `(start, stop)` pair describes.

    `feature_start` is then the position of the fragment reaching the end and
    `feature_stop` the stop_position of the fragment at the beginning. That is the
    interval on the contig's FORWARD axis and it does not depend on the read's strand,
    which is what lets `is_reverse` carry the orientation on its own — the DDL's
    reading. `feature_start > feature_stop` holds by construction here.

    `query_start`/`query_stop` are the span of the READ the fragments explain, over
    `cigar_query_intervals`, which places every record on the read's own axis (it
    mirrors a reverse-strand record). A read that wraps the origin covers the read
    contiguously, so on the groups this emits, the span IS the union.

    `mixed_strand` groups never reach here — the gate's predicate excludes them — so
    every fragment of a group shares a strand and `is_reverse` is well-defined.

    The `any_value` columns are constant within a group rather than arbitrary picks:
    `pooled_identity`, `pooled_coverage` and `fragment_count` are the macro's PER-GROUP
    answer, joined onto every fragment through `circular_cleared_join`'s full key, and
    `is_reverse` is constant because mixed-strand groups are excluded above. The join
    key carries `is_read1` while this GROUP BY does not, which agrees only because the
    query is single-end (see the module docstring); paired input would merge R1 and R2
    here while the join separated them.
    """
    # `cigar_query_intervals` walks the CIGAR, and the query_start/query_stop below are
    # two reads of ONE such list — so it is computed once per record here and carried as
    # a column, rather than spelled twice and left to the optimizer to common up.
    scored = (
        "SELECT a.alignment_idx, a.prep_sample_idx, a.sequence_idx, a.feature_idx, "
        "a.position, a.stop_position, alignment_is_reverse(a.flags) AS is_reverse, "
        "c.identity AS pooled_identity, c.coverage AS pooled_coverage, "
        "c.n_fragments AS fragment_count, l.sequence_length_bp, "
        "cigar_query_intervals(a.cigar, a.flags) AS query_interval "
        f"FROM {STREAMED_ALIGNMENT_TABLE} a JOIN {_CLEARED} c "
        f"ON {circular_cleared_join('a', 'c')} "
        f"JOIN {FEATURE_LENGTHS_TABLE} l ON l.feature_idx = a.feature_idx"
    )
    # `list_min`/`list_max` over the record's own intervals rather than an UNNEST: one
    # row per fragment throughout, so the aggregate below groups fragments to reads
    # without a second grouping to rebuild the fragment first.
    fragment = (
        "SELECT * EXCLUDE (query_interval), "
        "list_min(list_transform(query_interval, x -> x.start)) AS query_start, "
        "list_max(list_transform(query_interval, x -> x.stop)) AS query_stop "
        "FROM scored"
    )
    # `position` is 1-based inclusive and `stop_position` exclusive (the aligner's own
    # convention, which the lake stores unchanged), so the contig's last base is
    # covered by a fragment whose stop_position is length + 1.
    at_end = "stop_position = sequence_length_bp + 1 AND position <> 1"
    at_start = "position = 1 AND stop_position <> sequence_length_bp + 1"
    # A fragment covering the contig end to end is a full lap. It counts as neither
    # extreme above (each excludes the other's coordinate), so it has to be refused on
    # its own or a read long enough to lap comes out looking like a plain crossing.
    whole_contig = "position = 1 AND stop_position = sequence_length_bp + 1"
    return (
        f"WITH scored AS ({scored}), fragment AS ({fragment}) "
        "SELECT alignment_idx, prep_sample_idx, sequence_idx, feature_idx, "
        "min(query_start) AS query_start, max(query_stop) AS query_stop, "
        f"max(position) FILTER (WHERE {at_end}) AS feature_start, "
        f"min(stop_position) FILTER (WHERE {at_start}) AS feature_stop, "
        "any_value(is_reverse) AS is_reverse, "
        "any_value(pooled_identity) AS pooled_identity, "
        "any_value(pooled_coverage) AS pooled_coverage, "
        "any_value(fragment_count) AS fragment_count "
        "FROM fragment "
        "GROUP BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx "
        f"HAVING count(*) FILTER (WHERE {at_end}) = 1 "
        f"AND count(*) FILTER (WHERE {at_start}) = 1 "
        f"AND count(*) FILTER (WHERE {whole_contig}) = 0"
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    workspace.mkdir(parents=True, exist_ok=True)
    alignment = workspace / _ALIGNMENT_NAME
    origin_spanning = workspace / _ORIGIN_SPANNING_NAME
    alignment_sql = validate_parquet_path(alignment)
    origin_spanning_sql = validate_parquet_path(origin_spanning)

    # Both thresholds bind the circular axis; the dataclass is what refuses a
    # combination that would score two axes at once, and its defaults are where
    # `CIRCULAR_MIN_COVERAGE` / `CIRCULAR_MIN_IDENTITY` live.
    gate = AlignmentGate(
        circular=True,
        circular_min_coverage=inputs.min_query_coverage,
        circular_min_identity=inputs.min_identity,
    )

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(
                    _DUCKDB_FALLBACK_GB,
                    threads=_DUCKDB_THREADS,
                    reserve_gb=_DUCKDB_RESERVE_GB,
                    cap_gb=_DUCKDB_CAP_GB,
                ),
                threads=_DUCKDB_THREADS,
            )

            # The subject, through the same seam the index builders use — one
            # reassembly expression, one `(read_id, sequence1)` shape. The chunk
            # stream is closed before the read stream opens.
            async with open_assembly_chunk_stream(
                conn,
                prep_sample_idx=inputs.prep_sample_idx,
                processing_idx=inputs.assembly_processing_idx,
            ) as chunks:
                n_contigs = stage_subject(conn, chunks, subject_table=_SUBJECT)
            if n_contigs == 0:
                # The submit path admits this ticket only over an `assembly_sample`
                # gate reading 'completed', which means that run assembled contigs.
                # An empty roster therefore means the gate and the lake disagree, not
                # that the sample had nothing to align against.
                raise RuntimeError(
                    f"assembly run {inputs.assembly_processing_idx} has no contigs for "
                    f"prep_sample {inputs.prep_sample_idx}, but its assembly_sample "
                    "gate reads 'completed'; there is nothing to align against"
                )

            # The circular gate's length argument, per feature. `length()` over the
            # reassembled contig rather than a second lake read: the subject is already
            # here, and the gate needs the length of exactly what the aligner saw.
            conn.execute(
                feature_lengths_table_sql(
                    "(SELECT read_id AS feature_idx, "
                    f"length(sequence1) AS sequence_length_bp FROM {_SUBJECT})"
                )
            )

            # The query, and the ONE reference to the read stream. A VIEW carrying
            # exactly what `align_minimap2` reads: `sequence_idx` as the read identity,
            # and no `sequence2` — its mere presence would put the aligner in
            # paired-end mode. The CREATE TABLE below drains it.
            #
            async with open_read_masked_stream(
                conn,
                prep_sample_idx=inputs.prep_sample_idx,
                mask_idx=inputs.align_mask_idx,
            ) as reads:
                conn.execute(
                    f"CREATE VIEW {_QUERY} AS "
                    f"SELECT sequence_idx AS read_id, sequence1 FROM {reads}"
                )
                conn.execute(
                    _streamed_alignment_sql(inputs.alignment_idx, inputs.prep_sample_idx),
                    [_QUERY, _SUBJECT, inputs.preset],
                )

            # Diagnose, then gate: `circular_predicate_sql` takes the clearance
            # `check_gate_diagnostics` returns, so the refusals cannot be skipped.
            diagnostics = conn.sql(gate_diagnostics_sql(gate))
            # By NAME, not position: `gate_diagnostics_sql` emits three different SELECT
            # shapes, and a reorder would hand one count to another's parameter — both
            # ints, both plausible. The client-side consumer binds the same way.
            clearance = check_gate_diagnostics(
                gate, **dict(zip(diagnostics.columns, diagnostics.fetchone(), strict=True))
            )

            conn.execute(circular_alignments_view_sql())
            conn.execute(feature_topology_view_sql())
            # The cleared groups, materialized once: the COPY's SEMI JOIN and the
            # origin-spanning side table both read them, and re-running the macro per
            # reader would pool every read twice.
            conn.execute(
                f"CREATE TABLE {_CLEARED} AS SELECT * FROM circular_query_coverage("
                f"{CIRCULAR_ALIGNMENTS_VIEW}, {FEATURE_TOPOLOGY_VIEW}) "
                f"WHERE {circular_predicate_sql(clearance=clearance)}",
                gate_parameters(gate),
            )

            # Every record of a group that cleared. Sorted by the identifier order the
            # DuckLake `alignment` table is written in, with position/flags as
            # tiebreakers so an origin-spanning read's fragments land deterministically.
            conn.execute(
                f"COPY (SELECT a.* FROM {STREAMED_ALIGNMENT_TABLE} a "
                f"SEMI JOIN {_CLEARED} c ON {circular_cleared_join('a', 'c')} "
                "ORDER BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx, "
                f"position, flags) TO '{alignment_sql}' ({PARQUET_OPTS})"
            )
            conn.execute(
                f"COPY ({_origin_spanning_sql()} "
                "ORDER BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx) "
                f"TO '{origin_spanning_sql}' ({PARQUET_OPTS})"
            )
        # OUTSIDE the connection contexts, matching align_sharded: a context __exit__
        # that raises after the COPY must still take the cleanup below.
        success = True
    finally:
        if not success:
            alignment.unlink(missing_ok=True)
            origin_spanning.unlink(missing_ok=True)

    # `alignment_staging_dir` is the workspace a register-files step loads, and it
    # holds exactly these two Parquets — the DuckDB spill dir is torn down before this
    # returns, so anything written there cannot be picked up as a third part file.
    return {
        "alignment": alignment,
        "alignment_origin_spanning": origin_spanning,
        "alignment_staging_dir": workspace,
    }
