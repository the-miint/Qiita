"""Native job: align reads against a reference's PER-SHARD aligner indexes (C1).

The consuming side of reference sharding. Track B builds per-shard minimap2/bowtie2
indexes + a whole-reference rype router (`build_routing_index`); this job uses them
to align a block of reads against only the shard(s) each read minimises into,
rather than the whole backbone.

Pipeline (modelled on `host_filter`, same miint-connection rules):
  1. A query VIEW `(read_id = sequence_idx BIGINT, sequence1, sequence2)` over the
     staged reads Parquet (`export_read_block`'s
     `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)`).
     `sequence_idx` is the globally-unique BIGINT read identity; exposing it AS
     `read_id` lets classify + align round-trip it and the output map straight back.
     Exactly like `host_filter`, a read pair rides as ONE row
     `(read_id, sequence1, sequence2)` and is aligned natively as a pair —
     `sequence2 IS NULL` marks a single-end read. There is NO SE/PE branching: the
     sharded aligners handle a uniformly-single-end (all-NULL `sequence2`) or
     uniformly-paired-end (all-non-null) batch natively, and a read set is uniformly
     one or the other by construction (a prep/run is SE or PE, never a mix). A mixed
     batch is invalid input — bowtie2 rejects it at bind, minimap2 tolerates it — and
     we neither split around that nor paper over it. A `_READ_META` VIEW carries
     `(sequence_idx -> prep_sample_idx)` so each output row is stamped with its true
     owner (a block spans many prep_samples).
  2. `read_to_shard` — one `rype_classify` pass against the whole-reference ROUTER
     emits `(read_id, bucket_name)` = `(sequence_idx, str(shard_id))`, ≥0 rows per
     read (a read whose minimisers span K shards yields K rows). Materialised into
     a non-temp TABLE `(read_id BIGINT, shard_name VARCHAR)` — the exact shape
     `align_*_sharded` binds. Factored so a future multi-router just UNIONs more
     classify results into the same table.
  3. ONE `align_{minimap2,bowtie2}_sharded(query, shard_directory:=,
     read_to_shard:=)` call aligns each read against ONLY its routed shard(s). Its
     output is passed through VERBATIM — all standard alignment columns, INCLUDING
     the mate columns (`mate_reference`, `mate_position`, `template_length`) and the
     SAM `flags` that make a paired-end read's two mate rows an explicit pair, not
     two unrelated rows. We DROP nothing and ADD three typed identity columns:
     `prep_sample_idx` (the per-row owner, joined from `_READ_META`), `feature_idx`
     (the aligner's `reference` subject id cast to BIGINT — our builders store
     `feature_idx` there), and `mate_feature_idx` (the mate's feature, cast from
     `mate_reference`, decoding SAM's RNEXT `'='`/`'*'` encoding). The raw VARCHAR
     `reference`/`mate_reference` are kept too. `max_secondary := 0` keeps the
     primary alignment per read per shard.
  4. Stream a sorted `COPY` to `alignment.parquet`. **Every alignment row is
     emitted.** A read produces multiple rows two legitimate ways: (a) CROSS-shard —
     a read routed to K shards aligns to a DISTINCT `feature_idx` per shard (a
     feature is in exactly one shard, so these never collide); and (b) a PAIRED-END
     read's two mate rows, which together are ONE read's alignment to a feature (the
     pairing carried by `flags` + the mate columns), NOT two independent alignments.
     So `(sequence_idx, feature_idx)` is NOT unique in the output — a consumer reads
     the mate columns / flags to relate a pair's rows, and reasons multiplicity per
     read (or read-pair), never per mate. The only collapse applied is the aligner's
     own within-shard secondary drop (`max_secondary := 0`).

**Output is `alignment_idx` + `prep_sample_idx` + `feature_idx` +
`mate_feature_idx` + the FULL aligner output.** Nothing is dropped from what miint
emits. The leading `alignment_idx` (from `Inputs`, the align run's CP-minted
config identity) keys the DuckLake `alignment` table (the mask-style identity — no
processing_idx yet). Sorted by `(alignment_idx, prep_sample_idx, sequence_idx,
feature_idx, position, flags)` — the column order + sort match the DuckLake
`alignment` table (`qiita-data-plane/src/ducklake.rs::ensure_alignment_tables`) so
the `register-files` step schema-matches. The output carries `feature_idx` but NOT
`reference_idx` — reference scoping is a query-time join against
`reference_membership` (see the identifier-ownership note in CLAUDE.md).

**Wired by the C2b `align` workflow.** `workflows/align/1.0.0.yaml`
(`target_kind: block`) drives `align_sharded` → `delete-alignment-block` →
`register-files` → `reconcile-alignment-block`. The runner resolves the
router/shard paths from action_context (the C2a `_resolve_sharded_align_indexes`)
and stages the block's MASKED reads (`export_read_masked_block`); the align
planner fans out one block ticket per ~10M-read block. The integration smoke
(`tests/integration/test_sharded_alignment.py`) still drives `execute()` directly
against real miint.

miint contracts — qiita-verified against the team-mirror build via the C1 smoke
(see docs/duckdb-miint.md; C1 replaces the prior "needs a full read" note):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1])` -> `(read_id, bucket_id, bucket_name, score)`, ≥0 rows per
    read (one per bucket above threshold — multi-bucket, so a read routes to every
    shard it overlaps). Reads `sequence1` and, when present, `sequence2`.
  - `align_minimap2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [preset, max_secondary, include_shard_name, …])` and
    `align_bowtie2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [max_secondary, include_shard_name, …])`. `query_table` + `read_to_shard` are
    table NAMEs resolved on a SEPARATE connection, so both are non-temp VIEW/TABLE.
    `read_to_shard.read_id` type must EXACTLY equal `query.read_id` (BIGINT here).
    Output = the standard SAM columns; `reference`/`mate_reference` are VARCHAR
    subject ids (our `feature_idx`), and a PE read emits one row per mate. Both
    accept a uniformly-SE (all-NULL `sequence2`) or uniformly-PE batch; a MIXED
    batch is rejected by bowtie2 (`gpl_boundary`) and tolerated by minimap2.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..job_resource_plan import count_read_pairs, linear_walltime
from ..miint import PARQUET_OPTS, apply_duckdb_settings, duckdb_tmp_dir, open_miint_conn
from . import JobPlan, JobResourcePlan

YAML_STEP_NAME = "align_sharded"

# DuckDB stages the query VIEW, the small read_to_shard table, and the final
# sorted COPY; the minimap2/bowtie2 shard indexes are held OUT of DuckDB's heap by
# their runtimes (grown into the cgroup remainder a `--mem-gb` raise provides).
# Same rationale as host_filter — making DuckDB allocation-aware here would let it
# starve the out-of-heap indexes, so DuckDB stays modest and the cgroup is the
# lever for a genome-scale align.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# Routing threshold for the read_to_shard classify. Deliberately LOW: over-routing
# is safe (a read routed to a shard it does not actually align to simply produces
# no alignment row), while under-routing would LOSE alignments. 0.1 is rype's own
# default — a read routes to a shard when >=10% of its minimisers hit it. C3 (the
# whole-reference baseline oracle) pins this by test (the D5 threshold decision).
_ROUTING_THRESHOLD = 0.1

# minimap2 short-read preset for the sharded align. Matches the preset the per-shard
# `.mmi` was built with (build_minimap2_index). bowtie2 is preset-independent.
_MINIMAP2_PRESET = "sr"

# plan() walltime model — like qc, alignment STREAMS (per-read classify + align +
# a spill-to-disk sort), so runtime tracks read count while peak RAM is roughly
# flat (the out-of-heap indexes dominate and don't grow with the read block).
# Alignment is heavier per read than qc's scalar transform, so a larger per-million
# coefficient. Conservative INITIAL estimates to refine against telemetry — the CP
# only LOWERS walltime to this (never above baseline) and TIMEOUT escalation is the
# backstop, so a low coefficient costs at most a retry.
_PLAN_BASE_WALLTIME_SECONDS = 600  # 10 min: process + DuckDB init + index load + fixed I/O
_PLAN_WALLTIME_SECONDS_PER_MILLION_PAIRS = 600.0

# In-DuckDB relation names. The query + read-meta are VIEWs; read_to_shard is a
# TABLE resolved by align's separate connection; the alignments TABLE is CTAS'd by
# the aligner seam from the align function's full output, then joined + sorted into
# the COPY.
_QUERY = "align_sharded_query"
_READ_META = "align_sharded_read_meta"
_READ_TO_SHARD = "align_sharded_read_to_shard"
_ALIGNMENTS = "align_sharded_alignments"


class Inputs(BaseModel):
    """Typed input contract for align_sharded.

    `reads` is the staged read-block Parquet in the `export_read_block` column
    shape `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2,
    qual2)` — the block of reads to align. Under the C2b `align` workflow this is
    the block's HOST-DEPLETED, QC-passed reads (the runner stages the `read_masked`
    view via `export_read_masked_block`); the job treats `reads` as an opaque
    export-shaped file either way, so this is a source change, not a job change.
    `aligner` selects the sharded aligner (`minimap2` or `bowtie2`);
    `router_index_path` is the whole-reference rype ROUTER `.ryxdi`
    (`build_routing_index`) — a SINGLE path (the C2a resolver returns a LIST for
    the growable-reference case; the CP passes `router_paths[0]`, one router
    today); `shard_directory` is the per-aligner shard-root the aligner scans
    (`{ref}/minimap2-shards` of flat `{shard}.mmi`, or `{ref}/bowtie2-shards` of
    `{shard}/index.*` subdirs — see `derived_store`).

    `alignment_idx` is the CP-minted alignment-config identity (the align run this
    block belongs to); it is stamped as the leading column of EVERY output row so
    the DuckLake `alignment` table is keyed by it (the mask-style identity — no
    processing_idx yet). Provided via the workflow `params:` (the field name
    `alignment_idx` is NOT a reserved input key).

    `reference_idx` is provenance-only and OPTIONAL (`None`): it is NOT written into
    the output — the alignment carries `feature_idx`, and reference scoping is a
    query-time join against `reference_membership`. Under BLOCK scope the framework
    injects no scope scalar and `reference_idx` is a RESERVED input key that cannot
    be passed via `params:`, so the CP resolves the router/shard paths from
    action_context (the `align_reference_idx` context key) instead. `work_ticket_idx`
    is the framework-injected scope scalar. `prep_sample_idx` is OPTIONAL and unused:
    like host_filter, each output row's owner is stamped PER ROW from the reads
    Parquet, so a multi-sample block needs no scalar."""

    reads: Path
    reference_idx: int | None = None
    aligner: Literal["minimap2", "bowtie2"]
    router_index_path: Path
    shard_directory: Path
    alignment_idx: int
    prep_sample_idx: int | None = None
    work_ticket_idx: int


def _validate_router_index(path: Path) -> None:
    """The router is a `.ryxdi` DIRECTORY; reject a missing one (fail fast) and an
    empty one (no index content -> a silent no-op classify)."""
    if not path.exists():
        raise FileNotFoundError(f"router_index_path not found: {path}")
    if not path.is_dir() or not any(path.iterdir()):
        raise ValueError(f"router_index_path is not a populated .ryxdi directory: {path}")


def _validate_shard_directory(path: Path) -> None:
    """The shard directory holds the per-shard aligner indexes miint scans; reject
    a missing or empty one. miint's bind/InitGlobal does the precise per-shard
    check (a flat `{shard}.mmi` for minimap2, a `{shard}/index.*` subdir for
    bowtie2); this is the fail-fast for an absent or empty root."""
    if not path.exists():
        raise FileNotFoundError(f"shard_directory not found: {path}")
    if not path.is_dir() or not any(path.iterdir()):
        raise ValueError(f"shard_directory is not a populated directory: {path}")


def _build_read_to_shard(
    conn: duckdb.DuckDBPyConnection,
    router_index_path: Path,
    query_table: str,
    dest_table: str,
    *,
    threshold: float,
) -> None:
    """Populate the `read_to_shard` table via one `rype_classify` pass against the
    router. Appends DISTINCT `(read_id BIGINT, shard_name VARCHAR)` pairs — one per
    (read, shard) the read routes to (multi-bucket: a read spanning K shards yields
    K rows). DISTINCT because the table-function interface does not guarantee a
    single row per (read, bucket).

    Isolated so unit tests stub the real classify. Factored around `dest_table` so
    a future multi-router build just calls this once per router (each appending its
    shards), UNIONing into one `read_to_shard`. Positional args (index path,
    sequence-table NAME) + `threshold` are bound as `?` (INSERT...SELECT is DML, so
    prepared params are accepted). `read_id` is CAST to BIGINT to match the query's
    `read_id` type exactly (the type align binds `read_to_shard.read_id` against)."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT CAST(read_id AS BIGINT) AS read_id, bucket_name AS shard_name "
        "FROM rype_classify(?, ?, id_column := 'read_id', threshold := ?)",
        [str(router_index_path), query_table, threshold],
    )


def _run_align_minimap2_sharded(
    conn: duckdb.DuckDBPyConnection,
    query_table: str,
    shard_directory: Path,
    read_to_shard_table: str,
    dest_table: str,
    *,
    preset: str,
) -> None:
    """Seam around miint's `align_minimap2_sharded`. Materialises the aligner's
    FULL output VERBATIM into a fresh `dest_table` via CTAS (nothing dropped) —
    `execute()` adds `prep_sample_idx` + `feature_idx` at COPY time. Isolated so
    unit tests stub the real align.

    `query_table` (positional) + `shard_directory` + `read_to_shard` (the table
    NAME) are all bound as `?` — a table-function call in a CTAS still takes
    prepared params for its VARCHAR table-name / path args (verified against the
    real function; no string interpolation, so no injection surface).
    `max_secondary := 0` keeps the primary alignment per read per shard (the
    aligner's own within-shard collapse); cross-shard multiplicity (a distinct
    feature per shard) and a PE read's two mate rows are both preserved."""
    conn.execute(
        f"CREATE TABLE {dest_table} AS "
        "SELECT * FROM align_minimap2_sharded(?, shard_directory := ?, "
        "read_to_shard := ?, preset := ?, max_secondary := 0)",
        [query_table, str(shard_directory), read_to_shard_table, preset],
    )


def _run_align_bowtie2_sharded(
    conn: duckdb.DuckDBPyConnection,
    query_table: str,
    shard_directory: Path,
    read_to_shard_table: str,
    dest_table: str,
) -> None:
    """Seam around miint's `align_bowtie2_sharded` — the bowtie2 twin of
    `_run_align_minimap2_sharded`. No `preset` (a bowtie2 index is
    preset-independent; presets are an align-time knob left at default here).
    Same VERBATIM full-output CTAS and within-shard `max_secondary := 0`; the
    three table-name / path args are bound as `?` like the minimap2 seam."""
    conn.execute(
        f"CREATE TABLE {dest_table} AS "
        "SELECT * FROM align_bowtie2_sharded(?, shard_directory := ?, "
        "read_to_shard := ?, max_secondary := 0)",
        [query_table, str(shard_directory), read_to_shard_table],
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    _validate_router_index(inputs.router_index_path)
    _validate_shard_directory(inputs.shard_directory)

    workspace.mkdir(parents=True, exist_ok=True)
    # Output basename is the DuckLake-facing table name the register-files step
    # maps: `alignment.parquet` -> the `alignment` table.
    alignment = workspace / "alignment.parquet"
    reads_sql = validate_parquet_path(inputs.reads)
    out_sql = validate_parquet_path(alignment)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )

            # Per-read (sequence_idx -> prep_sample_idx) map, projected to the two
            # key columns so the final COPY stamps each alignment row's owner PER
            # ROW (a block spans many prep_samples). sequence_idx is unique, 1:1.
            conn.execute(
                f"CREATE VIEW {_READ_META} AS "
                f"SELECT sequence_idx, prep_sample_idx FROM read_parquet('{reads_sql}')"
            )
            # The align query: the WHOLE read set, keyed by sequence_idx AS read_id,
            # carrying sequence1 + sequence2. ONE query, no SE/PE split — the sharded
            # aligners handle the mode natively (the host_filter pattern);
            # `sequence2 IS NULL` marks single-end. A non-temp VIEW so miint's
            # separate connection can resolve it by name.
            conn.execute(
                f"CREATE VIEW {_QUERY} AS "
                "SELECT sequence_idx AS read_id, sequence1, sequence2 "
                f"FROM read_parquet('{reads_sql}')"
            )
            # read_to_shard (non-temp — align resolves it by name on its own
            # connection). One rype_classify pass fills it; multi-bucket, so a read
            # spanning K shards gets K rows and aligns against all K.
            conn.execute(f"CREATE TABLE {_READ_TO_SHARD} (read_id BIGINT, shard_name VARCHAR)")
            _build_read_to_shard(
                conn,
                inputs.router_index_path,
                _QUERY,
                _READ_TO_SHARD,
                threshold=_ROUTING_THRESHOLD,
            )

            # ONE sharded-align call. Its FULL output (all SAM columns, verbatim) is
            # materialised into _ALIGNMENTS by the seam. Empty is VALID — a block
            # whose reads align nowhere in this reference is legitimate, not a
            # fail-fast (the tools tolerate an empty query/routing, per host_filter).
            if inputs.aligner == "minimap2":
                _run_align_minimap2_sharded(
                    conn,
                    _QUERY,
                    inputs.shard_directory,
                    _READ_TO_SHARD,
                    _ALIGNMENTS,
                    preset=_MINIMAP2_PRESET,
                )
            else:
                _run_align_bowtie2_sharded(
                    conn, _QUERY, inputs.shard_directory, _READ_TO_SHARD, _ALIGNMENTS
                )

            # Stream a sorted COPY. Prepend the CP-minted `alignment_idx` as the
            # LEADING column (a constant for this align run — the block ticket
            # carries one), so the DuckLake `alignment` table is keyed by it. Then
            # pass the aligner output through VERBATIM (`a.* EXCLUDE (read_id)`,
            # which we rename to `sequence_idx`), and ADD the typed identity
            # columns: `prep_sample_idx` (per-row owner via the _READ_META join,
            # 1:many onto the alignments), `feature_idx` (`CAST(reference)`), and
            # `mate_feature_idx` (the mate's feature, cast from `mate_reference`).
            # NOTHING is dropped — the raw VARCHAR `reference`/`mate_reference` stay
            # too, and the mate columns + flags keep a PE read's two mate rows an
            # explicit pair. `mate_reference` uses SAM's RNEXT encoding, so decode
            # it: `'='` means the same feature as this row, `'*'`/`''`/NULL means no
            # mapped mate, else it's the mate's own feature id. `(sequence_idx,
            # feature_idx)` is NOT a key: cross-shard rows carry distinct feature_idx
            # (a feature is in one shard), and a PE read's two mate rows share it.
            # `alignment_idx` is a validated int (pydantic Inputs), safe to inline.
            # Sorted by the identifier order (alignment_idx leads to match the
            # register-side sort), with position/flags as tiebreakers so a PE read's
            # mate rows land in a deterministic order — the column order + this sort
            # match the DuckLake `alignment` table so register-files schema-matches.
            conn.execute(
                f"COPY (SELECT CAST({inputs.alignment_idx} AS BIGINT) AS alignment_idx, "
                "rm.prep_sample_idx, a.read_id AS sequence_idx, "
                "CAST(a.reference AS BIGINT) AS feature_idx, "
                "CASE WHEN a.mate_reference = '=' THEN CAST(a.reference AS BIGINT) "
                "WHEN a.mate_reference IS NULL OR a.mate_reference IN ('*', '') THEN NULL "
                "ELSE CAST(a.mate_reference AS BIGINT) END AS mate_feature_idx, "
                "a.* EXCLUDE (read_id) "
                f"FROM {_ALIGNMENTS} a "
                f"JOIN {_READ_META} rm ON rm.sequence_idx = a.read_id "
                "ORDER BY alignment_idx, rm.prep_sample_idx, a.read_id, feature_idx, "
                "a.position, a.flags) "
                f"TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        # On failure remove a partial output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            alignment.unlink(missing_ok=True)

    return {"alignment": alignment}


def plan(inputs: Inputs) -> JobPlan:
    """Size the step's WALLTIME down from the YAML baseline by the read-block
    cardinality (memory/cpu left to baseline — the out-of-heap shard indexes, not
    row count, dominate RAM; see the module note). Mirrors qc.plan(): a footer-only
    read-pair count + a linear `base + per-million` estimate. Advisory and
    down-only — the CP lowers walltime to this when below baseline, and TIMEOUT
    escalation is the backstop for an under-estimate. Runs at submit time in the
    orchestrator process (a Parquet footer read, not a data scan)."""
    walltime = linear_walltime(
        count_read_pairs(inputs.reads),
        base_seconds=_PLAN_BASE_WALLTIME_SECONDS,
        seconds_per_million_pairs=_PLAN_WALLTIME_SECONDS_PER_MILLION_PAIRS,
    )
    return JobPlan(resources=JobResourcePlan(walltime=walltime))
