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
     A `_READ_META` VIEW carries `(sequence_idx -> prep_sample_idx)` so each output
     row is stamped with its true owner (a block spans many prep_samples).
  2. `read_to_shard` — one `rype_classify` pass against the whole-reference ROUTER
     emits `(read_id, bucket_name)` = `(sequence_idx, str(shard_id))`, ≥0 rows per
     read (a read whose minimisers span K shards yields K rows). Materialised into
     a non-temp TABLE `(read_id BIGINT, shard_name VARCHAR)` — the exact shape
     `align_*_sharded` binds. Factored so a future multi-router just UNIONs more
     classify results into the same table.
  3. `align_{minimap2,bowtie2}_sharded(query, shard_directory:=, read_to_shard:=)`
     aligns each read against ONLY its routed shard(s). Output = the 21 standard
     alignment columns; `reference` is VARCHAR = the subject's stored id, which our
     builders set to `feature_idx`, so the aligned `feature_idx` is
     `CAST(reference AS BIGINT)` (there is no `feature_idx`/`shard_id` column in
     miint's output). `max_secondary := 0` keeps the primary alignment per read
     per shard. Steps 2-3 run TWICE — once for the single-end reads
     (`sequence2 IS NULL`) and once for the paired-end reads
     (`sequence2 IS NOT NULL`) — because `align_bowtie2_sharded` rejects a query
     that mixes null and non-null `sequence2` (a batch is single- OR paired-end,
     not both; `align_minimap2_sharded` tolerates the mix, but one split path
     serves both). Each read is in exactly one sub-batch, so no double-counting.
  4. Map `reference -> feature_idx`, join `prep_sample_idx`, and stream a sorted
     `COPY` to `alignment.parquet`. **Every alignment row is emitted — NO dedup.** A
     read produces multiple rows two intentional ways: (a) CROSS-shard — a read
     routed to K shards aligns to a DISTINCT `feature_idx` per shard (a feature is
     in exactly one shard, so these never collide); and (b) PER-MATE — a paired-end
     read aligning within ONE shard emits one SAM row per mate, i.e. two rows
     sharing the SAME `(sequence_idx, feature_idx)` but differing in
     `flags`/`position`/`cigar`. So `(sequence_idx, feature_idx)` is NOT unique in
     the output — a downstream consumer must not treat it as a key. The only
     collapse applied is the aligner's own within-shard secondary drop
     (`max_secondary := 0`).

**Output shape is a stable C1 contract, not the final sink.** C1 emits
`(prep_sample_idx, sequence_idx, feature_idx, flags, position, stop_position,
mapq, cigar)` sorted by `(prep_sample_idx, sequence_idx, feature_idx)`. The
alignment output carries `feature_idx` but NOT `reference_idx` — reference scoping
is a query-time join against `reference_membership` (see the identifier-ownership
note in CLAUDE.md). The DuckLake alignment-detail sink (columns, order, the
`register-files` path) is finalised by C2/D6; this job produces a documented,
stable intermediate.

**Not wired into a workflow yet.** C1 is native-job-only: the smoke drives
`execute()` directly with the router + shard-directory paths. The runner staging
(read-block + shard-index resolution) and the block × shard fan-out are C2.

miint contracts — qiita-verified against the team-mirror build via the C1 smoke
(see docs/duckdb-miint.md; C1 replaces the prior "needs a full read" note):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1])` -> `(read_id, bucket_id, bucket_name, score)`, ≥0 rows per
    read (one per bucket above threshold — multi-bucket, so a read routes to every
    shard it overlaps).
  - `align_minimap2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [preset, max_secondary, include_shard_name, …])` and
    `align_bowtie2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [max_secondary, include_shard_name, …])`. `query_table` + `read_to_shard` are
    table NAMEs resolved on a SEPARATE connection, so both are non-temp VIEW/TABLE.
    `read_to_shard.read_id` type must EXACTLY equal `query.read_id` (BIGINT here).
    Output `reference`/`mate_reference` are VARCHAR subject ids (our `feature_idx`).
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

# In-DuckDB relation names. The query + read-meta are VIEWs; read_to_shard and the
# alignment accumulator are TABLEs (read_to_shard is resolved by align's separate
# connection; the accumulator collects the mapped align output for the sorted COPY).
_QUERY = "align_sharded_query"
_READ_META = "align_sharded_read_meta"
_READ_TO_SHARD = "align_sharded_read_to_shard"
_ALIGNMENTS = "align_sharded_alignments"


class Inputs(BaseModel):
    """Typed input contract for align_sharded.

    `reads` is the staged read-block Parquet (`export_read_block`'s
    `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2,
    qual2)`) — the block of reads to align. `aligner` selects the sharded aligner
    (`minimap2` or `bowtie2`); `router_index_path` is the whole-reference rype
    ROUTER `.ryxdi` (`build_routing_index`); `shard_directory` is the per-aligner
    shard-root the aligner scans (`{ref}/minimap2-shards` of flat `{shard}.mmi`, or
    `{ref}/bowtie2-shards` of `{shard}/index.*` subdirs — see `derived_store`).

    `reference_idx` identifies which reference is being aligned against (provenance
    / the eventual scope scalar); it is NOT written into the output — the alignment
    carries `feature_idx`, and reference scoping is a query-time join against
    `reference_membership`. `work_ticket_idx` is the framework-injected scope
    scalar. `prep_sample_idx` is OPTIONAL and unused: like host_filter, each output
    row's owner is stamped PER ROW from the reads Parquet, so a multi-sample block
    needs no scalar (a single-sample ticket still has it injected, but the per-row
    value is authoritative)."""

    reads: Path
    reference_idx: int
    aligner: Literal["minimap2", "bowtie2"]
    router_index_path: Path
    shard_directory: Path
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
    """Seam around miint's `align_minimap2_sharded`. Appends the mapped alignment
    rows into `dest_table`. Isolated so unit tests stub the real align.

    `query_table` (positional) + `shard_directory` + `read_to_shard` (the table
    NAME) are all bound as `?` — INSERT...SELECT is DML, so the table-function's
    VARCHAR table-name / path args take prepared params (verified against the real
    function; no string interpolation, so no injection surface). `reference` is the
    VARCHAR subject id our builder stored (`feature_idx`), so
    `CAST(reference AS BIGINT)` recovers the aligned feature. `max_secondary := 0`
    keeps the primary alignment per read per shard (the aligner's own within-shard
    collapse); cross-shard multiplicity (distinct feature per shard) and per-mate PE
    rows (same feature, one row per mate) are both preserved."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT read_id AS sequence_idx, CAST(reference AS BIGINT) AS feature_idx, "
        "flags, position, stop_position, mapq, cigar "
        "FROM align_minimap2_sharded(?, shard_directory := ?, "
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
    Same `reference -> feature_idx` map and within-shard `max_secondary := 0`; the
    three table-name / path args are bound as `?` like the minimap2 seam."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT read_id AS sequence_idx, CAST(reference AS BIGINT) AS feature_idx, "
        "flags, position, stop_position, mapq, cigar "
        "FROM align_bowtie2_sharded(?, shard_directory := ?, "
        "read_to_shard := ?, max_secondary := 0)",
        [query_table, str(shard_directory), read_to_shard_table],
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    _validate_router_index(inputs.router_index_path)
    _validate_shard_directory(inputs.shard_directory)

    workspace.mkdir(parents=True, exist_ok=True)
    # Output basename is the DuckLake-facing table name a future register-files
    # (C2) step would map: `alignment.parquet` -> the alignment-detail table.
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
            # Alignment accumulator (mapped to feature_idx). Populated by the
            # aligner seam, once per non-empty sub-batch below. Empty is VALID — a
            # block whose reads align nowhere in this reference is legitimate, not a
            # fail-fast.
            # Column types match the real align_*_sharded output (probe-verified):
            # position/stop_position are BIGINT (a shard's concatenated contigs can
            # exceed INT32) — never narrow them to INTEGER; flags (USMALLINT) and
            # mapq (UTINYINT) widen losslessly into INTEGER.
            conn.execute(
                f"CREATE TABLE {_ALIGNMENTS} ("
                "sequence_idx BIGINT, feature_idx BIGINT, flags INTEGER, "
                "position BIGINT, stop_position BIGINT, mapq INTEGER, cigar VARCHAR)"
            )

            # Align SINGLE-END and PAIRED-END reads as SEPARATE uniform sub-batches.
            # `align_bowtie2_sharded` rejects a query that MIXES null and non-null
            # `sequence2` ("all must be non-null for paired-end") — a batch is single-
            # OR paired-end, never both. `align_minimap2_sharded` tolerates the mix,
            # but splitting keeps ONE correct code path for both aligners. Each read
            # falls in exactly one sub-batch (by `sequence2` nullness), so there is no
            # double-counting; `read_to_shard` is rebuilt per sub-batch to match the
            # query the aligner binds it against (its `read_id` type must equal the
            # query's). The SE sub-query omits `sequence2` entirely (a pure-SE batch);
            # the PE sub-query carries it (all non-null by the predicate).
            for projection, predicate in (
                ("SELECT sequence_idx AS read_id, sequence1", "sequence2 IS NULL"),
                ("SELECT sequence_idx AS read_id, sequence1, sequence2", "sequence2 IS NOT NULL"),
            ):
                conn.execute(
                    f"CREATE OR REPLACE VIEW {_QUERY} AS {projection} "
                    f"FROM read_parquet('{reads_sql}') WHERE {predicate}"
                )
                if conn.execute(f"SELECT count(*) FROM {_QUERY}").fetchone()[0] == 0:
                    continue  # no reads in this mode — skip the classify + align
                # read_to_shard (non-temp — align resolves it by name on its own
                # connection). One rype_classify pass fills it; multi-bucket, so a
                # read spanning K shards gets K rows and aligns against all K.
                conn.execute(
                    f"CREATE OR REPLACE TABLE {_READ_TO_SHARD} (read_id BIGINT, shard_name VARCHAR)"
                )
                _build_read_to_shard(
                    conn,
                    inputs.router_index_path,
                    _QUERY,
                    _READ_TO_SHARD,
                    threshold=_ROUTING_THRESHOLD,
                )
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

            # Stream a sorted COPY. prep_sample_idx is stamped PER ROW from the
            # reads (the _READ_META join, 1:many onto the alignments). NO dedup —
            # every alignment row is kept. `(sequence_idx, feature_idx)` is NOT
            # unique: cross-shard rows carry distinct feature_idx (a feature is in
            # one shard), and a PE read's two mate rows share (sequence_idx,
            # feature_idx) but differ in flags/position — a downstream consumer must
            # not assume that pair is a key. Sorted by the identifier order, with
            # position/flags as tiebreakers so a PE read's mate rows land in a
            # deterministic order.
            conn.execute(
                "COPY (SELECT rm.prep_sample_idx, a.sequence_idx, a.feature_idx, "
                "a.flags, a.position, a.stop_position, a.mapq, a.cigar "
                f"FROM {_ALIGNMENTS} a "
                f"JOIN {_READ_META} rm ON rm.sequence_idx = a.sequence_idx "
                "ORDER BY rm.prep_sample_idx, a.sequence_idx, a.feature_idx, "
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
