"""Native job: align reads against a reference's PER-SHARD aligner indexes.

The consuming side of reference sharding. The reference build produces per-shard
minimap2/bowtie2 indexes + a whole-reference rype router (`build_routing_index`);
this job uses them to align a block of reads against only the shard(s) each read
minimises into, rather than the whole backbone.

The aligner is chosen by the read platform (Illumina short reads → bowtie2, PacBio
HiFi / Nanopore long reads → minimap2); the control plane resolves it from
`sequencing_run.platform` at align-plan time and passes it in `Inputs`.

Pipeline (modelled on `host_filter`, same miint-connection rules):
  1. A query VIEW `(read_id = sequence_idx BIGINT, sequence1, sequence2)` over the
     block's reads, streamed from the data plane at runtime and bound by
     `read_source.bind_step_reads` in the shape
     `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)`.
     `sequence_idx` is the globally-unique BIGINT read identity; exposing it AS
     `read_id` lets classify + align round-trip it and the output map straight back.
     Exactly like `host_filter`, a read pair rides as ONE row
     `(read_id, sequence1, sequence2)` and is aligned natively as a pair —
     `sequence2 IS NULL` marks a single-end read. The read set is never SPLIT by
     mode: ONE align call handles a uniformly-single-end (all-NULL `sequence2`) or
     uniformly-paired-end (all-non-null) batch natively, and a read set is uniformly
     one or the other by construction (a prep/run is SE or PE, never a mix). The
     batch's mode IS read once, but only to pick the FILTER form in step 4 (pooled
     across a pair vs per record) — never to split or re-run the aligner. A mixed
     batch is invalid input and is REJECTED here, naming the counts, rather than
     left to surface as bowtie2's opaque bind-time `gpl_boundary` error or — on
     minimap2, which tolerates a mix — as a silently mis-pooled filter. That
     rejection sits HERE, ahead of step 2, so it is unconditional: the routing pass
     can legitimately come up empty (step 2), and validating after that would let
     invalid input exit 0 with an empty output. A `_READ_META` TABLE carries
     `(sequence_idx -> prep_sample_idx)` so each output row is stamped with its true
     owner (a block spans many prep_samples).
  2. `read_to_shard` — one `rype_classify` pass against the whole-reference ROUTER
     emits `(read_id, bucket_name)` = `(sequence_idx, str(shard_id))`, ≥0 rows per
     read (a read whose minimisers span K shards yields K rows). Materialised into
     a non-temp TABLE `(read_id BIGINT, shard_name VARCHAR)` — the exact shape
     `align_*_sharded` binds. Factored so a future multi-router just UNIONs more
     classify results into the same table. The classify reads a `sequence1`-only
     VIEW when the batch is single-end: rype sizes its batch from the column LIST,
     so an all-NULL `sequence2` would halve the batch and double the number of
     full router-index reloads it pays (see the note at that CREATE).
  3. ONE `align_{minimap2,bowtie2}_sharded(query, shard_directory:=,
     read_to_shard:=, <params>)` call aligns each read against ONLY its routed
     shard(s), reporting ALL placements (bowtie2 `report_all`, the "modified SHOGUN"
     set in `_BOWTIE2_ALIGN_PARAMS`; minimap2 `max_secondary := 100`, its analogue —
     the historical `-k 16` / `max_secondary := 0` primary-only collapse is gone, and
     dropping the arg entirely would silently fall back to a finite default). The
     aligner is handed a MATERIALIZED copy of the query relation rather than the view:
     both aligners re-read the query once per shard, and against a Parquet view each of
     those reads costs the block's whole sequence BYTES — see the materialization note
     in `execute()` for the measurements and the memory contingency. Its output
     carries all
     standard SAM columns, INCLUDING the mate columns (`mate_reference`,
     `mate_position`, `template_length`) and the SAM `flags` that make a paired-end
     read's two mate rows an explicit pair. We ADD three typed identity columns —
     `prep_sample_idx` (the per-row owner, joined from `_READ_META`), `feature_idx`
     (the aligner's `reference` subject id cast to BIGINT — our builders store
     `feature_idx` there), and `mate_feature_idx` (the mate's feature, cast from
     `mate_reference`, decoding SAM's RNEXT `'='`/`'*'` encoding) — and DROP the raw
     VARCHAR `reference`/`mate_reference`, whose identity `feature_idx` /
     `mate_feature_idx` already carry.
  4. TWO write phases, keeping only HIGH-IDENTITY placements
     (`cigar_sequence_identity` >= a per-aligner floor —
     `_MIN_SEQUENCE_IDENTITY_BOWTIE2` 0.99 for short reads,
     `_MIN_SEQUENCE_IDENTITY_MINIMAP2` 0.90 for long reads — scored from the =/X
     CIGAR that bowtie2 `xeq` / minimap2 `eqx` emit) so noisy off-target hits don't
     bloat storage. minimap2 placements must ALSO clear a query-coverage floor
     (`_MIN_QUERY_COVERAGE_MINIMAP2` 0.90, from `cigar_query_coverage`) — a
     soft-clipped long read can be high-identity over a short aligned span, and that
     low-coverage placement would otherwise persist and inflate a downstream
     breadth-of-coverage estimate; bowtie2 aligns end-to-end so needs no such gate.
     A PAIRED-END placement's two mates are POOLED and scored as a unit, so a pair is
     kept or dropped together and a mate is never orphaned; a SINGLE-END record is
     scored on its own CIGAR. Note that is a property of the BATCH, not of the
     aligner: the floors are per-aligner, the pooling is per batch shape, and an SE
     bowtie2 run gets the per-record form.

     Phase 1 streams the aligner + the `_READ_META` join into a transient
     `alignment_unsorted.parquet` inside the DuckDB temp dir; phase 2 sorts that into
     `alignment.parquet`. Neither phase materialises the aligner's output as a table.
     The split exists because the sort is a BLOCKING operator that would otherwise
     hold the whole alignment set while the shard indexes and GPL-boundary daemons
     are still resident, and because a POOLED (paired-end) filter cannot read
     directly from the aligner at all — DuckDB rewrites a windowed `QUALIFY` into an
     aggregate + self-join that reads its input TWICE, which over a table function
     would run the entire alignment twice. So SE filters in phase 1 (shrinking what
     gets sorted) and PE filters in phase 2, where that double read lands on the
     staging Parquet instead.

     A surviving read can still produce multiple rows two legitimate ways: (a)
     CROSS-shard — a read routed to K shards aligns to a DISTINCT `feature_idx` per
     shard (a feature is in exactly one shard, so these never collide); and (b) a
     PAIRED-END read's two mate rows, ONE read's alignment to a feature (pairing
     carried by `flags` + the mate columns), NOT two independent alignments. So
     `(sequence_idx, feature_idx)` is NOT unique in the output — a consumer reads the
     mate columns / flags to relate a pair's rows, and reasons multiplicity per read
     (or read-pair), never per mate.

**Output is `alignment_idx` + `prep_sample_idx` + `feature_idx` +
`mate_feature_idx` + the aligner's SAM columns MINUS the raw VARCHAR
`reference`/`mate_reference`.** The leading `alignment_idx` (from `Inputs`, the
align run's CP-minted config identity) keys the DuckLake `alignment` table (the
mask-style identity — no processing_idx yet). Sorted by `(alignment_idx,
prep_sample_idx, sequence_idx, feature_idx, position, flags)` — the column order +
sort match the DuckLake `alignment` table
(`qiita-data-plane/src/ducklake.rs::ensure_alignment_tables`) so the
`register-files` step schema-matches. The output carries `feature_idx` but NOT
`reference_idx` — reference scoping is a query-time join against
`reference_membership` (see the identifier-ownership note in CLAUDE.md).

**Wired by the `align` workflow.** `workflows/align/1.0.0.yaml`
(`target_kind: block`) drives `align_sharded` → `delete-alignment-block` →
`register-files` → `reconcile-alignment-block`. The runner resolves the
router/shard paths from action_context (`_resolve_sharded_align_indexes`); the
block's MASKED reads are streamed by the job itself (the control plane scopes the
ticket to the `read_masked` macro under the completed mask). The align planner
fans out one block ticket per ~10M-read block. The integration smoke
(`tests/integration/test_sharded_alignment.py`) drives `execute()` directly
against real miint.

miint contracts — qiita-verified against the team-mirror build via the
`align_sharded` smoke (see docs/duckdb-miint.md):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1])` -> `(read_id, bucket_id, bucket_name, score)`, ≥0 rows per
    read (one per bucket above threshold — multi-bucket, so a read routes to every
    shard it overlaps). Reads `sequence1` and, when present, `sequence2`.
    **It sizes its Arrow batch from the sequence table's COLUMN LIST, not its
    contents**, and reloads the WHOLE index once per batch — so a `sequence2` column
    that is entirely NULL halves the batch and doubles the index reloads
    (duckdb-miint#199). Hand it `sequence1` alone for a single-end batch.
  - `align_minimap2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [preset, max_secondary, include_shard_name, …])` and
    `align_bowtie2_sharded(query_table, shard_directory:=, read_to_shard:=,
    [preset, report_all, xeq, no_discordant, no_mixed, …])`. `query_table` +
    `read_to_shard` are table NAMEs resolved on a SEPARATE connection, so both are
    non-temp VIEW/TABLE. `read_to_shard.read_id` type must EXACTLY equal
    `query.read_id` (BIGINT here). Output = the standard SAM columns;
    `reference`/`mate_reference` are VARCHAR subject ids (our `feature_idx`), and a
    PE read emits one row per mate. Both accept a uniformly-SE (all-NULL
    `sequence2`) or uniformly-PE batch; a MIXED batch is rejected by bowtie2
    (`gpl_boundary`) and tolerated by minimap2.
  - `cigar_sequence_identity(cigar)` -> DOUBLE fraction of aligned columns that
    match, computed from a =/X CIGAR (needs bowtie2 `xeq := true`). Identity is
    additive over CIGAR ops, so a concatenated pair CIGAR (`string_agg`) scores the
    fragment-pooled identity.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel
from qiita_common.analytic import PAIRED_PLACEMENT_PARTITION
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ..read_source import bind_step_reads

YAML_STEP_NAME = "align_sharded"

# DuckDB threads. NOT merely a parallelism knob: `SET threads` IS the sharded
# aligners' CROSS-SHARD concurrency. miint derives `max_active_shards` from DuckDB's
# thread-pool size (`ceil(db_threads / max_threads_per_shard)`, clamped to the shard
# count) and IGNORES its own `threads` argument in sharded mode. We never set
# `max_threads_per_shard`, so it is miint's default of 1 and the derivation reduces to
# `max_active_shards = min(threads, shard_count)` — one worker per shard, each aligner
# single-threaded.
#
# **This must equal the align workflow's `baseline_resources.cpu`, and nothing derives
# it from the cgroup.** Unlike memory, there is no cpu-resolution helper: a literal
# above the allocation oversubscribes that many concurrent shards onto fewer cores, and
# a literal below it leaves cores idle. `test_align_cpu_pins_duckdb_threads` fails if
# the two drift. Deliberately NOT generalized to a repo-wide invariant — most native
# jobs set threads for DuckDB's own operator memory (per-thread sort/HASH_AGG state)
# and legitimately differ from their `cpu:`; here the cores are spent by the ALIGNER's
# shard concurrency, not by DuckDB, which is what makes the two the same number.
_DUCKDB_THREADS = 8

# DuckDB `memory_limit`, resolved from the REAL cgroup so a per-run `--mem-gb`
# override reaches DuckDB; `_DUCKDB_MEMORY_GB` is only the OFF-SLURM (local backend /
# tests) fallback.
#
# Deliberately NOT host_filter's "hold DuckDB modest so the out-of-heap indexes
# aren't starved" posture, which this job inherited by copy. `memory_limit` is a
# CEILING, not a reservation: raising it does not let DuckDB claim the box, it only
# lets an operator that genuinely needs the memory use what was granted instead of
# spilling. Under the old 8 GB literal against a 64 GB allocation the alignment
# output spilled gigabytes to the workspace — far more expensive than the memory it
# was protecting.
#
# `_DUCKDB_RESERVE_GB` carves the cgroup out for the in-process co-consumers that
# share the box with DuckDB: the rype router index while routing, then up to
# `_DUCKDB_THREADS` per-shard aligner indexes (hundreds of MB to a few GB each) plus
# their GPL-boundary daemons. It is small because it is the only carve-out ON TOP of
# `duckdb_headroom_gb`, which already reserves DuckDB's own above-limit RSS
# overshoot; the resulting limit stays under a configuration observed to work on
# this shard population.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_RESERVE_GB = 1

# Routing threshold for the read_to_shard classify. Deliberately LOW: over-routing
# is safe (a read routed to a shard it does not actually align to simply produces
# no alignment row), while under-routing would LOSE alignments. 0.1 is rype's own
# default — a read routes to a shard when >=10% of its minimisers hit it. A
# whole-reference baseline oracle can later pin this threshold by test.
_ROUTING_THRESHOLD = 0.1

# minimap2 preset for the sharded align. `map-hifi` is the long-read (PacBio HiFi)
# preset: the CP routes long-read platforms to minimap2 and short-read (Illumina)
# platforms to bowtie2 (chosen at align-plan from sequencing_run.platform), so a
# minimap2 align is always long-read here. (Oxford Nanopore also maps to minimap2 in
# the platform table but is not used today.)
#
# ACCURACY: this is NOT enforced to match the per-shard `.mmi`'s BUILD preset. The
# build preset is pinned to map-hifi only in the CLI (cli/reference_load.py); the
# workflow YAMLs still expose the full preset enum and a direct POST could build a
# shard `.mmi` under a different preset (build_minimap2_index defaults to `sr`).
# That mismatch is not a correctness hazard — verified against real miint, minimap2
# takes k/w from the `.mmi` and scoring from this align-time preset, so a preset
# mismatch changes only index density, not the alignments produced — so we do NOT
# assert equality here; the align-time preset is simply always map-hifi.
_MINIMAP2_PRESET = "map-hifi"

# Secondary-alignment cap for the minimap2 sharded align. Set arbitrarily HIGH
# (not left at the miint default) so multi-mapping placements are captured rather
# than silently truncated — dropping `max_secondary` does NOT mean "return all", it
# falls back to a finite default. This is minimap2's analogue of bowtie2's
# `report_all`: emit every placement and let the identity filter below prune the
# noise. Cross-shard multiplicity (a distinct feature per shard) is unaffected —
# that comes from routing, not from this cap.
_MINIMAP2_MAX_SECONDARY = 100

# Minimum sequence identity a surviving alignment must clear, PER ALIGNER. The
# aligners emit ALL concordant placements (bowtie2 `report_all`, minimap2
# `max_secondary := 100`); this filter keeps only high-identity, specific hits,
# dropping noisy off-target alignments to bound stored data. Identity is computed
# from the =/X CIGAR (bowtie2 `xeq := true` / minimap2 `eqx := true`) via miint's
# `cigar_sequence_identity`. The floor is aligner-specific because the read
# populations are:
#   * bowtie2 (short-read Illumina) → 0.99. A short read that is a true hit matches
#     nearly end-to-end, so a tight floor is the right specificity target. The two
#     mates of a concordant PE placement are POOLED and judged as a unit (see the
#     COPY), so a pair is kept or dropped together and a mate is never orphaned.
#   * minimap2 (long-read, PacBio HiFi) → 0.90. Long reads carry more per-read
#     divergence (indels + chemistry error over a much longer template), so the
#     short-read 0.99 floor would discard legitimate long-read hits. Judged per
#     record (single-end long reads, no mate to pool).
# We route only Illumina→bowtie2 and PacBio HiFi→minimap2 today; ONT is declared
# alignable (routes to minimap2) but is NOT used, and the 0.90 minimap2 floor keeps
# an ONT block from silently dropping every read were it ever enabled.
_MIN_SEQUENCE_IDENTITY_BOWTIE2 = 0.99
_MIN_SEQUENCE_IDENTITY_MINIMAP2 = 0.90

# Minimum query coverage (the aligned fraction of the read, scored from the CIGAR
# via miint's `cigar_query_coverage`) a surviving minimap2 placement must clear.
# Identity alone is over the ALIGNED columns only, so a long read that soft-clips
# most of itself can be high-identity over a short aligned span; this floor drops
# those low-coverage placements before they are persisted (and before they inflate
# a downstream breadth-of-coverage estimate). minimap2-only: bowtie2 aligns
# END-TO-END (`score_min 'L,0,-0.05'`, no local mode) so query coverage is ~1.0 by
# construction and a separate gate would be a no-op.
#
# Scored PER SAM RECORD, which drops a long read that crosses the ORIGIN of a
# circular reference contig. An aligner treats a circular contig as a linear one,
# so such a read is reported as one record per side of the origin, each covering
# only its own share of the query; neither side clears this floor, so the read is
# absent from `alignment` altogether rather than partially represented. Complete
# circular references — closed chromosomes, plasmids, phages — are where that
# applies. Measured against a same-length mid-contig control by
# `test_origin_spanning_read_splits_into_one_record_per_side`, which asserts both
# the per-record and the concatenated pooled score against this constant; the data
# plane's `alignment_origin_spanning` DDL says what a producer that kept such a
# read would have to gate on instead.
_MIN_QUERY_COVERAGE_MINIMAP2 = 0.90

# The bowtie2 align-time parameter set (the "modified SHOGUN" configuration): collect
# ALL concordant paired-end placements (`report_all`, replacing the historical `-k 16`
# / `max_secondary := 0`) and let the identity filter below keep only specific hits.
# `xeq` emits =/X CIGARs so identity is CIGAR-derivable; `no_discordant`/`no_mixed`
# keep only proper concordant pairs; `deterministic_seeds` + fixed `seed` make a run
# reproducible. These are fixed config constants (not caller input), inlined into the
# call; only the table-name / path args are bound as `?`. NOTE: `preset` here is an
# ALIGN-time bowtie2 preset (sensitivity), distinct from the index-build preset — a
# bowtie2 INDEX is preset-independent, but the aligner still takes one.
#
# `ignore_quals` is EXPLICIT, not incidental. bowtie2's only use of base quality is
# interpolating the mismatch penalty between `mismatch_penalty` (max) and
# `mismatch_penalty_min` (min); SHOGUN sets both to 1, so the penalty is a constant
# regardless of Q and quality cannot affect scoring. miint forwards per-base quality
# only when the query relation exposes `qual1`/`qual2`, and the align query here
# projects sequences alone — so quality was already unused, as a side effect of a
# projection rather than as a decision. Stating it means a later change to that
# projection cannot silently start scoring reads differently.
_BOWTIE2_ALIGN_PARAMS = (
    "preset := 'very-sensitive', seed := 42, n_penalty := 1, "
    "mismatch_penalty := 1, mismatch_penalty_min := 1, ignore_quals := true, "
    "read_gap_open := 0, read_gap_extend := 1, "
    "ref_gap_open := 0, ref_gap_extend := 1, "
    "score_min := 'L,0,-0.05', report_all := true, quiet := true, "
    "xeq := true, deterministic_seeds := true, lowseeds := '4%', "
    "no_1mm_upfront := true, no_exact_upfront := true, "
    "no_discordant := true, no_mixed := true"
)

# No plan(): this job's reads STREAM from the data plane, and the walltime model
# it used to carry was driven by a Parquet-footer read-count that a stream cannot
# supply. Sizing therefore falls back to the workflow YAML baseline, with TIMEOUT
# escalation as the backstop — the same posture estimate_feature_table takes, and
# for the same reason (input cardinality is not knowable at submit time).
#
# This is a deliberate trade, not an oversight: the block's exact read count IS
# derivable control-plane-side from the block members (sum of the per-member
# sequence_idx spans), so walltime sizing can come back by threading that count
# through the workflow `params:`. Left for a follow-up.

# In-DuckDB relation names. The query is a VIEW; read-meta and read_to_shard are
# TABLEs (read_to_shard has to be one — align's separate connection resolves it by
# name). There is deliberately NO relation for the aligner's output: it is a SELECT
# streamed straight into the phase-1 staging COPY, never a materialised table.
#
# `_QUERY_MATERIALIZED` is a TABLE copy of `_QUERY`, and is the relation handed to
# the ALIGNER (either aligner — both re-read the query once per shard; see the
# materialization note in execute()). Every other consumer of the query keeps reading
# a VIEW: the SE/PE probe, which is answered from Parquet row-group statistics and
# would gain nothing, and the rype routing pass, which copies the corpus internally
# anyway and runs BEFORE this table exists.
#
# `_ROUTING_QUERY` is the routing pass's own VIEW over `_QUERY` — identical for a
# paired-end batch, narrowed to `sequence1` alone for a single-end one. That
# narrowing is a rype BATCH-SIZING fix, not a tidy-up; see the note at its CREATE.
_QUERY = "align_sharded_query"
_ROUTING_QUERY = "align_sharded_routing_query"
_QUERY_MATERIALIZED = "align_sharded_query_materialized"
_READ_META = "align_sharded_read_meta"
_READ_TO_SHARD = "align_sharded_read_to_shard"

# The empty-output projection: the DuckLake `alignment` table's columns as typed
# NULLs, in the exact column order + types of
# `qiita-data-plane/src/ducklake.rs::ensure_alignment_tables` (5 CP identity
# columns + the verbatim miint aligner columns). Used only for the no-routed-reads
# path (see execute()), where miint's `align_*_sharded` cannot be called at all
# (it rejects an empty `read_to_shard`), so there is no aligner output to pass
# through and the schema must be written explicitly. `WHERE false` yields zero
# rows. MUST stay in lockstep with ensure_alignment_tables so register-files
# schema-matches an empty block exactly as it does a non-empty one.
_EMPTY_ALIGNMENT_SELECT = (
    "SELECT "
    "CAST(NULL AS BIGINT) AS alignment_idx, "
    "CAST(NULL AS BIGINT) AS prep_sample_idx, "
    "CAST(NULL AS BIGINT) AS sequence_idx, "
    "CAST(NULL AS BIGINT) AS feature_idx, "
    "CAST(NULL AS BIGINT) AS mate_feature_idx, "
    "CAST(NULL AS USMALLINT) AS flags, "
    "CAST(NULL AS BIGINT) AS position, "
    "CAST(NULL AS BIGINT) AS stop_position, "
    "CAST(NULL AS UTINYINT) AS mapq, "
    "CAST(NULL AS VARCHAR) AS cigar, "
    "CAST(NULL AS BIGINT) AS mate_position, "
    "CAST(NULL AS BIGINT) AS template_length, "
    "CAST(NULL AS BIGINT) AS tag_as, "
    "CAST(NULL AS BIGINT) AS tag_xs, "
    "CAST(NULL AS BIGINT) AS tag_ys, "
    "CAST(NULL AS BIGINT) AS tag_xn, "
    "CAST(NULL AS BIGINT) AS tag_xm, "
    "CAST(NULL AS BIGINT) AS tag_xo, "
    "CAST(NULL AS BIGINT) AS tag_xg, "
    "CAST(NULL AS BIGINT) AS tag_nm, "
    "CAST(NULL AS VARCHAR) AS tag_yt, "
    "CAST(NULL AS VARCHAR) AS tag_md, "
    "CAST(NULL AS VARCHAR) AS tag_sa "
    "WHERE false"
)


class Inputs(BaseModel):
    """Typed input contract for align_sharded.

    `reads` is left UNBOUND by the `align` workflow, which is the signal to stream
    the block's reads from the data plane at runtime (`bind_step_reads`, keyed on
    `work_ticket_idx`). The control plane decides which reads that means — for an
    align ticket, the block's HOST-DEPLETED, QC-passed `read_masked` rows scoped to
    the completed mask — so this job cannot request raw reads by mistake. The bound
    relation carries the same `(prep_sample_idx, sequence_idx, read_id, sequence1,
    qual1, sequence2, qual2)` shape the staged Parquet used to, so the alignment
    body is unchanged. The field remains an optional Path so the shared seam has
    one signature across jobs; binding one is not used by any current workflow.
    `aligner` selects the sharded aligner (`minimap2` or `bowtie2`), which the CP
    picks from the read platform at align-plan time (not a free caller choice);
    `router_index_path` is the whole-reference rype ROUTER `.ryxdi`
    (`build_routing_index`) — a SINGLE path (the resolver returns a LIST for the
    growable-reference case; the CP passes `router_paths[0]`, one router today);
    `shard_directory` is the per-aligner shard-root the aligner scans
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
    is the framework-injected scope scalar (and the key the read stream is minted
    by). `prep_sample_idx` is OPTIONAL and unused: like host_filter, each output
    row's owner is stamped PER ROW from the streamed reads, so a multi-sample block
    needs no scalar."""

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
    router. Appends `(read_id BIGINT, shard_name VARCHAR)` pairs — one per
    (read, shard) the read routes to (multi-bucket: a read spanning K shards yields
    K rows).

    **No dedup, and none is needed.** `(read_id, shard_name)` is unique by
    construction on both axes: every query row is a distinct `sequence_idx` (the
    globally-unique read identity, minted once per read), and rype emits at most one
    row per (read, bucket). Note the cost if that ever stopped holding: miint does
    NOT dedup on its side — it reads a shard's ids straight out of this table and
    joins the query against it unfiltered — so a duplicated pair would align the read
    twice against the same shard and emit duplicate output rows, which no consumer
    could detect because `(sequence_idx, feature_idx)` is legitimately non-unique
    (cross-shard and paired-end multiplicity). Keep the uniqueness invariant here.

    Isolated so unit tests stub the real classify. Factored around `dest_table` so
    a future multi-router build just calls this once per router (each appending its
    shards), UNIONing into one `read_to_shard`. Positional args (index path,
    sequence-table NAME) + `threshold` are bound as `?` (INSERT...SELECT is DML, so
    prepared params are accepted). `read_id` is CAST to BIGINT to match the query's
    `read_id` type exactly (the type align binds `read_to_shard.read_id` against) —
    current miint builds mirror the input id type, but the output type has been
    build-dependent, so the cast stays."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT CAST(read_id AS BIGINT) AS read_id, bucket_name AS shard_name "
        "FROM rype_classify(?, ?, id_column := 'read_id', threshold := ?)",
        [str(router_index_path), query_table, threshold],
    )


def _align_minimap2_sharded_sql(
    query_table: str,
    shard_directory: Path,
    read_to_shard_table: str,
    *,
    preset: str,
) -> tuple[str, list[object]]:
    """Seam around miint's `align_minimap2_sharded` (the long-read / `map-hifi`
    aligner). Returns the `(sql, params)` for a SELECT over the aligner's FULL
    output — it does NOT execute anything. `execute()` embeds the SQL as a
    subquery inside its staging COPY, so the aligner STREAMS into that write
    instead of being materialised first (see the two-phase note in `execute()`).
    Isolated so unit tests stub the real align.

    `query_table` (positional) + `shard_directory` + `read_to_shard` (the table
    NAME) are returned as bound `?` params, not interpolated: `COPY (...) TO` takes
    prepared parameters for a table function's VARCHAR table-name / path arguments
    (verified against the real function), so streaming costs no injection surface.

    `eqx := true` is REQUIRED, not optional: it makes minimap2 emit =/X CIGARs (the
    minimap2 twin of bowtie2's `xeq`), which the `execute()` identity filter needs —
    `cigar_sequence_identity` returns NULL for a plain `M` CIGAR, so without `eqx`
    every minimap2 alignment would be silently dropped by the filter.

    `max_secondary := _MINIMAP2_MAX_SECONDARY` (100) is minimap2's analogue of
    bowtie2's `report_all`: dropping `max_secondary` does NOT return all placements,
    it falls back to a finite miint default that would silently truncate multi-mapping
    reads, so we set it arbitrarily high and let the identity filter prune the noise.
    The rest of the minimap2 (long-read) parameter set beyond preset/eqx/max_secondary
    is not yet pinned the way bowtie2's is and stays at miint defaults."""
    return (
        "SELECT * FROM align_minimap2_sharded(?, shard_directory := ?, "
        "read_to_shard := ?, preset := ?, eqx := true, max_secondary := ?)",
        [query_table, str(shard_directory), read_to_shard_table, preset, _MINIMAP2_MAX_SECONDARY],
    )


def _align_bowtie2_sharded_sql(
    query_table: str,
    shard_directory: Path,
    read_to_shard_table: str,
) -> tuple[str, list[object]]:
    """Seam around miint's `align_bowtie2_sharded` — the short-read (Illumina)
    aligner. Returns the `(sql, params)` for a SELECT over the aligner's FULL
    output; it does NOT execute anything (same contract as the minimap2 seam —
    `execute()` streams it into the staging COPY). Isolated so unit tests stub the
    real align.

    Passes the fixed `_BOWTIE2_ALIGN_PARAMS` (the modified-SHOGUN set): `report_all`
    emits ALL concordant paired-end placements (replacing the old within-shard
    `max_secondary := 0` collapse), `xeq` emits =/X CIGARs so the identity filter
    can score from the CIGAR, and `no_discordant`/`no_mixed` keep only proper
    concordant pairs. The three table-name / path args are bound as `?`; the param
    set is fixed config, inlined."""
    return (
        "SELECT * FROM align_bowtie2_sharded(?, shard_directory := ?, "
        f"read_to_shard := ?, {_BOWTIE2_ALIGN_PARAMS})",
        [query_table, str(shard_directory), read_to_shard_table],
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    _validate_router_index(inputs.router_index_path)
    _validate_shard_directory(inputs.shard_directory)

    workspace.mkdir(parents=True, exist_ok=True)
    # Output basename is the DuckLake-facing table name the register-files step
    # maps: `alignment.parquet` -> the `alignment` table.
    alignment = workspace / "alignment.parquet"
    out_sql = validate_parquet_path(alignment)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(
                    _DUCKDB_MEMORY_GB,
                    threads=_DUCKDB_THREADS,
                    reserve_gb=_DUCKDB_RESERVE_GB,
                ),
                threads=_DUCKDB_THREADS,
            )

            # Bind the block's reads. Under `align` this streams from the data
            # plane (the workflow binds no `reads` path), which the seam drains
            # once into a Parquet in the DuckDB temp dir and binds as a lazy
            # `read_parquet` VIEW. The view (not a table) is what this job needs
            # from the seam: a Flight reader is single-consumption and this job
            # builds several relations over the reads below, and the drain's peak
            # memory is flat in row count rather than in block size.
            #
            # It being a VIEW over a Parquet is also load-bearing for the
            # minimap2 materialization further down — a Parquet scan cannot yield
            # a row without decompressing the whole column chunk it sits in,
            # which is exactly the per-shard cost that materialization removes.
            async with bind_step_reads(
                conn,
                reads=None,
                work_ticket_idx=inputs.work_ticket_idx,
                workspace=duckdb_tmp,
            ) as reads_rel:
                # Per-read (sequence_idx -> prep_sample_idx) map, projected to the two
                # key columns so phase 1 stamps each alignment row's owner PER ROW (a
                # block spans many prep_samples). sequence_idx is unique, 1:1.
                # A TABLE, not a VIEW over the reads relation: two narrow BIGINT
                # columns (~16 B/read) is small enough to hold outright, and
                # materializing it keeps phase 1's join off a Parquet scan (the bound
                # reads relation is a lazy `read_parquet` view). NOT for the planner's
                # benefit — a Parquet footer carries exact row counts, so a view's
                # cardinality is exact too and the join plan is identical either way.
                conn.execute(
                    f"CREATE TABLE {_READ_META} AS "
                    f"SELECT sequence_idx, prep_sample_idx FROM {reads_rel}"
                )
                # The align query: the WHOLE read set, keyed by sequence_idx AS read_id,
                # carrying sequence1 + sequence2. ONE query, no SE/PE split — the sharded
                # aligners handle the mode natively (the host_filter pattern);
                # `sequence2 IS NULL` marks single-end. A non-temp VIEW so miint's
                # separate connection can resolve it by name.
                conn.execute(
                    f"CREATE VIEW {_QUERY} AS "
                    "SELECT sequence_idx AS read_id, sequence1, sequence2 "
                    f"FROM {reads_rel}"
                )
                # Is this batch paired-end? Decides the FILTER SHAPE below and the
                # ROUTING PROJECTION handed to rype — never the aligner, which the CP
                # picks from the platform. (The routing use is the expensive one: it
                # sets rype's batch size, hence how many times the router index is
                # reloaded. See its CREATE.) The read set is
                # uniformly SE or PE by construction, so this counts rather than
                # samples: a MIXED batch is invalid input and fails HERE, naming the
                # counts, instead of surfacing as bowtie2's opaque `gpl_boundary`
                # rejection or — worse, on minimap2, which tolerates a mix — as a
                # silently mis-pooled identity filter.
                #
                # Probed BEFORE the routing pass, and before the no-routed-reads
                # early return below, so the rejection is UNCONDITIONAL: a mixed batch
                # whose reads happen to route nowhere is still invalid input, and
                # validating after that return would let it exit 0 with an empty
                # output. It also means a mixed batch fails before paying for
                # rype_classify. Cheap here, though NOT by the mechanism this comment
                # used to claim: DuckDB never sums row-group null counts. `count(*)`
                # does come from the row counts, but `count(sequence2)` is only
                # metadata-served when statistics PROVE zero NULLs — then
                # `statistics_propagation` rewrites it to `count(*)` and constant-folds
                # it (measured 0.0003 s on a 499 MB zstd column). Otherwise it SCANS:
                # an all-NULL mate column scans but costs ~nothing because it encodes to
                # ~1 KB, and a MIXED column pays a real scan (0.147 s at 3M rows, 50/50).
                # So the single-end case this projection exists for is precisely the one
                # that does not get the shortcut — it is cheap by encoding, not by
                # pruning. A `LIMIT 1` probe would also avoid scanning (row-group stats
                # prune it) and is in fact faster in most shapes, but it cannot produce
                # `total`, and the mixed-batch rejection below needs both numbers to
                # name the counts in its error (`paired not in (0, total)`). That
                # correctness requirement — not performance — is why the count probe is
                # used here.
                #
                # `total > 0` is load-bearing now that this runs ahead of the empty
                # check: an empty read batch makes `paired == total == 0`, which is
                # neither mixed (no raise) nor paired.
                paired, total = conn.execute(
                    f"SELECT count(sequence2), count(*) FROM {_QUERY}"
                ).fetchone()
                if paired not in (0, total):
                    raise ValueError(
                        "read batch mixes single- and paired-end reads "
                        f"({paired} of {total} rows carry sequence2); a prep/run is "
                        "uniformly one or the other by construction, so this batch is "
                        "invalid input"
                    )
                is_paired = paired == total and total > 0

                # The relation the ROUTING pass classifies: `_QUERY` for a paired-end
                # batch, narrowed to `sequence1` alone for a single-end one.
                #
                # **That narrowing is a rype BATCH-SIZING fix, not a projection
                # tidy-up.** miint derives rype's `is_paired` from the mere PRESENCE of a
                # `sequence2` column — `ValidateSequenceTable` inspects the column list
                # and never the values — and rype then assumes a query twice as long,
                # which doubles both terms of its per-read memory estimate and so HALVES
                # its batch size. Every extra batch costs a FULL reload of the router
                # index, because `rype_classify_arrow` runs one shard loop per Arrow
                # RecordBatch. So handing a single-end block an all-NULL `sequence2`
                # silently doubles the number of index loads. Measured on a 750k-read
                # HiFi block against the 193 GB w=20 WoL3 router: 400k reads/batch ->
                # 200k, so 2 index loads -> 4, at ~54 min each. See duckdb-miint#199.
                #
                # Sizing is ALL it changes, which is what makes this safe: `is_paired`
                # reaches rype only through the batch-size estimate
                # (`rype_classify_arrow` takes no such argument), and miint projects
                # `NULL::BLOB AS sequence2` into its own temp table when the column is
                # absent — so rype still receives a pair column and classifies exactly
                # the same reads. The ALIGNER keeps the full `_QUERY` (via
                # `_QUERY_MATERIALIZED`): both aligners need `sequence2` to align a pair
                # natively.
                #
                # A VIEW over a VIEW, so it costs nothing and the routing pass still
                # reads the lazy Parquet scan rather than a second materialization.
                conn.execute(
                    f"CREATE VIEW {_ROUTING_QUERY} AS SELECT read_id, sequence1"
                    + (", sequence2" if is_paired else "")
                    + f" FROM {_QUERY}"
                )

                # read_to_shard (non-temp — align resolves it by name on its own
                # connection). One rype_classify pass fills it; multi-bucket, so a read
                # spanning K shards gets K rows and aligns against all K.
                conn.execute(f"CREATE TABLE {_READ_TO_SHARD} (read_id BIGINT, shard_name VARCHAR)")
                _build_read_to_shard(
                    conn,
                    inputs.router_index_path,
                    _ROUTING_QUERY,
                    _READ_TO_SHARD,
                    threshold=_ROUTING_THRESHOLD,
                )

                # If NO read routed to any shard, `read_to_shard` is empty — and miint's
                # `align_*_sharded` REJECTS an empty `read_to_shard` at bind
                # ("empty or has no valid shard names"), so it cannot be called at all.
                # This is a LEGITIMATE no-op, not a failure: a block's reads can route
                # nowhere because the block is genuinely empty (a completed
                # host-depletion mask can carry 0 passing reads — a blank/control or
                # fully host/QC-filtered sample the align planner still tiles) OR because
                # none of its reads minimise into THIS reference. Either way, emit a
                # valid empty (schema-correct) alignment.parquet and skip the aligner —
                # register-files then registers 0 rows and reconcile flips the per-sample
                # gate with no rows (it has no count-assertion, by design). Verified
                # against real miint by the empty-batch case in
                # tests/integration/test_sharded_alignment.py.
                routed = conn.execute(f"SELECT count(*) FROM {_READ_TO_SHARD}").fetchone()[0]
                if routed == 0:
                    conn.execute(
                        f"COPY ({_EMPTY_ALIGNMENT_SELECT}) TO '{out_sql}' ({PARQUET_OPTS})"
                    )
                    success = True
                    return {"alignment": alignment, "alignment_staging_dir": workspace}

                # ONE sharded-align call, as a SELECT the seam hands back rather than a
                # relation it materialises. An empty aligner OUTPUT is still valid here
                # (every routed read failed to align) — only an empty read_to_shard
                # INPUT is the case handled above.
                # MATERIALIZE the query relation the aligner will read.
                #
                # Both sharded aligners read the query relation ONCE PER SHARD (a
                # per-shard id fetch; qiita-verified against real miint, see
                # docs/duckdb-miint.md), so the block's sequences are re-read
                # `n_shards` times — 1000 times at the current shard count. Against the
                # Parquet-backed VIEW each of those reads pays for ~100% of the block's
                # sequence BYTES, because a Parquet scan must decompress a whole column
                # chunk to yield any row from it, while a shard wants ~0.1% of the reads.
                # Against a materialized TABLE, DuckDB scans the narrow `read_id` column
                # and fetches `sequence1`/`sequence2` only for the rows the shard
                # actually asked for.
                #
                # **The two costs scale on DIFFERENT axes, which is the whole point and
                # is easy to get wrong.** The view's per-shard cost tracks the block's
                # total BYTES (so it grows with read count AND read length); the table's
                # tracks reads-per-shard only, and is flat in read length. Measured per
                # shard (1000 shards, scattered membership, threads=8, warm cache,
                # PARQUET_OPTS; see docs/duckdb-miint.md for the full table):
                #
                #   1M x 160 bp   ( 49 MB):  view  20.0 ms  table  3.4 ms
                #   1M x 320 bp   ( 99 MB):  view  34.9 ms  table  3.5 ms  <- view +75%,
                #                                                             table flat
                #   10M x 160 bp  (493 MB):  view 169.0 ms  table 27.2 ms
                #
                # Over 1000 shards that is 169 s -> 27 s for a 10M-read single-end short
                # block, and 331 s -> 40 s for the paired-end one (10M x 2x160 bp). A
                # long-read block is 1M reads at ~15 kb, ~15 GB — scaling the view by
                # bytes puts it near 26 min, against seconds for the table. The copy
                # itself costs ONE full scan of the reads Parquet: measured 22 ms at 1M
                # reads, 241 ms at 10M. It buys out `n_shards` of them, so it pays for
                # itself on BOTH aligners — which is why this is not conditional on the
                # aligner. (An earlier revision materialized only for minimap2, on the
                # strength of a per-1000-bp/read slope that had been measured at 1M reads
                # and was then applied to the 10M-read short-read block — understating
                # bowtie2's re-read by 10x. The bytes reading is the correct one.)
                #
                # It is created AFTER the routing pass on purpose: rype_classify
                # materializes its OWN copy of the corpus into a TEMP table that stays
                # resident for the whole classify (source-verified upstream — see the
                # rype_classify entry in docs/duckdb-miint.md), so building ours first
                # would hold two copies of the block's sequences at once, for no
                # benefit — rype copies the corpus whether we hand it a view or a table.
                #
                # **This is only a win while the copy FITS IN MEMORY, and what keeps it
                # fitting is the block target, not anything enforced here.** At the
                # current targets the copy is ~15 GB for a long-read block (1M reads) and
                # under ~1 GB for a short-read one (10M reads x 160 bp, both mates),
                # against a resolved `memory_limit` of ~57 GB under the 64 GB baseline
                # allocation. `memory_limit` is a ceiling rather than a reservation, so a
                # copy that did NOT fit degrades instead of dying (verified: an
                # over-limit CTAS spills to `temp_directory` and succeeds) — but degrades
                # badly, and quietly: `temp_directory` is the workspace on Lustre, so the
                # per-shard fetches would hit shared-filesystem spill files 1000 times
                # over. That is plausibly WORSE than the Parquet view this replaces (the
                # Parquet is at least zstd-compressed), and there is no error to notice
                # it by. Anything that raises a block's sequence bytes much above the
                # current targets — an explicit `target_reads` override, or a platform
                # with far longer reads (ONT ultra-long at ~100 kb would be ~100 GB) —
                # needs this reconsidered, not just the target.
                conn.execute(f"CREATE TABLE {_QUERY_MATERIALIZED} AS SELECT * FROM {_QUERY}")

                if inputs.aligner == "minimap2":
                    align_sql, align_params = _align_minimap2_sharded_sql(
                        _QUERY_MATERIALIZED,
                        inputs.shard_directory,
                        _READ_TO_SHARD,
                        preset=_MINIMAP2_PRESET,
                    )
                else:
                    align_sql, align_params = _align_bowtie2_sharded_sql(
                        _QUERY_MATERIALIZED, inputs.shard_directory, _READ_TO_SHARD
                    )

                # The high-identity filter. Two INDEPENDENT dimensions, previously
                # conflated under one aligner test:
                #   * the FLOORS are per-aligner — identity 0.99 for bowtie2 (a true
                #     short-read hit matches nearly end-to-end) vs 0.90 for minimap2
                #     (long reads carry more per-read divergence), and the
                #     query-coverage floor is minimap2-only because bowtie2 aligns
                #     END-TO-END so qcov is ~1.0 by construction and the gate would be
                #     a no-op.
                #   * the GROUPING is per-batch-shape. A PE placement's two mates are
                #     POOLED and judged as a unit so a pair is kept or dropped together
                #     and a mate is never orphaned; the mates store their own and their
                #     partner's coordinates in SWAPPED order, so
                #     LEAST/GREATEST(position, mate_position) gives both the same key,
                #     and including feature_idx keeps a read's distinct placements
                #     (report_all emits each as its own 2-record pair) judged
                #     separately. An SE record has no mate to pool, so the pooled
                #     window is a partition of ONE row — `string_agg` over it returns
                #     that row's own CIGAR, which makes the window equivalent to a
                #     per-row predicate at the cost of a full blocking sort of every
                #     alignment. SE therefore filters with a plain WHERE.
                #
                #     That equivalence rests on one premise worth stating rather than
                #     glossing: the OLD partition key (`read_id`, `reference`,
                #     `position` — `LEAST`/`GREATEST` collapse to `position` once
                #     `mate_position` is NULL, since both ignore NULLs) has to be
                #     UNIQUE per row for the partition to be a single row. Two SE
                #     placements of one read on one feature at the same start position
                #     would previously have been scored on their two CIGARs
                #     CONCATENATED, and are now scored individually. Neither aligner is
                #     documented to guarantee that can't happen (`report_all` /
                #     `max_secondary := 100` both emit multiple placements per read),
                #     but distinct placements carry distinct start positions in
                #     practice — and scoring the concatenation of two unrelated
                #     placements was never the intended semantics anyway, so where the
                #     two forms could differ, the per-row one is the defensible answer.
                #
                # The two dimensions are independent, so all four combinations are
                # well-defined — but only three are reachable through the control
                # plane, whose `_ALIGNER_BY_PLATFORM` maps `pacbio_smrt` and
                # `oxford_nanopore` to minimap2 (long reads,
                # single-end) and bowtie2 for short reads (either shape). PE+minimap2 is
                # therefore dead today; it is kept coherent (pooled, at the minimap2
                # floor, with the coverage gate over the pooled CIGAR) rather than
                # special-cased, so a future paired long-read platform needs no change
                # here. `test_align_sharded_minimap2_pe_pair_pooled_at_minimap2_floor`
                # pins it so the quadrant can't rot unnoticed.
                min_identity = (
                    _MIN_SEQUENCE_IDENTITY_BOWTIE2
                    if inputs.aligner == "bowtie2"
                    else _MIN_SEQUENCE_IDENTITY_MINIMAP2
                )
                needs_coverage = inputs.aligner == "minimap2"

                def _coverage_and(cigar_expr: str) -> str:
                    """The minimap2 query-coverage conjunct over `cigar_expr` (empty for
                    bowtie2). Takes the CIGAR expression because the two phases score
                    different ones — a single row's `cigar` when filtering per row, the
                    pooled `string_agg` when judging a pair as a unit."""
                    if not needs_coverage:
                        return ""
                    return (
                        f" AND cigar_query_coverage({cigar_expr}) >= {_MIN_QUERY_COVERAGE_MINIMAP2}"
                    )

                # PHASE 1 — stream the aligner into a staging Parquet, adding the typed
                # identity columns. Prepend the CP-minted `alignment_idx` as the LEADING
                # column (a constant for this align run — the block ticket carries one),
                # so the DuckLake `alignment` table is keyed by it, then `prep_sample_idx`
                # (per-row owner via the _READ_META join, 1:many onto the alignments),
                # `feature_idx` (`CAST(reference)`), and `mate_feature_idx` (the mate's
                # feature, cast from `mate_reference`, decoding SAM's RNEXT encoding:
                # `'='` = the same feature as this row, `'*'`/`''`/NULL = no mapped mate,
                # else the mate's own feature id). The rest of the aligner output passes
                # through, MINUS the raw VARCHAR `reference`/`mate_reference` (`EXCLUDE`)
                # whose identity `feature_idx`/`mate_feature_idx` already carry.
                # `(sequence_idx, feature_idx)` is NOT a key: cross-shard rows carry
                # distinct feature_idx (a feature is in one shard), and a PE read's two
                # mate rows share it. `alignment_idx` is a validated int (pydantic
                # Inputs), safe to inline.
                #
                # Why TWO phases rather than one sorted COPY over the aligner:
                #   * The sort is a BLOCKING operator. Sorting in the same statement as
                #     the aligner would hold the whole alignment set while the shard
                #     indexes and GPL-boundary daemons are still resident; splitting
                #     lets the aligner's out-of-heap footprint be released first, and
                #     the aligner streams through a write whose memory is flat in row
                #     count instead of being materialised.
                #   * The _READ_META join MUST happen here, not in phase 2: it lives in
                #     this connection's in-memory database, so the staging file has to
                #     carry `prep_sample_idx` forward itself.
                #   * SE filters here, shrinking what phase 2 sorts. PE CANNOT: DuckDB
                #     rewrites a windowed QUALIFY into an aggregate + self-join, which
                #     reads its input TWICE — over a table function that means running
                #     the entire alignment twice. So the pooled filter waits for phase 2,
                #     where the double read lands on this Parquet instead. (Verified by
                #     EXPLAIN; three formulations, including an OFFSET 0 fence, all
                #     rewrite the same way.)
                staged = duckdb_tmp / "alignment_unsorted.parquet"
                staged_sql = validate_parquet_path(staged)
                conn.execute(
                    f"COPY (SELECT CAST({inputs.alignment_idx} AS BIGINT) AS alignment_idx, "
                    "rm.prep_sample_idx, a.read_id AS sequence_idx, "
                    "CAST(a.reference AS BIGINT) AS feature_idx, "
                    "CASE WHEN a.mate_reference = '=' THEN CAST(a.reference AS BIGINT) "
                    "WHEN a.mate_reference IS NULL OR a.mate_reference IN ('*', '') THEN NULL "
                    "ELSE CAST(a.mate_reference AS BIGINT) END AS mate_feature_idx, "
                    "a.* EXCLUDE (read_id, reference, mate_reference) "
                    f"FROM ({align_sql}) a "
                    f"JOIN {_READ_META} rm ON rm.sequence_idx = a.read_id "
                    + (
                        ""
                        if is_paired
                        else f"WHERE cigar_sequence_identity(a.cigar) >= {min_identity}"
                        f"{_coverage_and('a.cigar')} "
                    )
                    + f") TO '{staged_sql}' ({PARQUET_OPTS_INTERMEDIATE})",
                    align_params,
                )

                # The materialized query relation has no reader left the moment phase 1's
                # COPY returns — the aligner is done with it and phase 2 reads only the
                # staging Parquet — so holding it through the sort is pure occupancy.
                # Drop it. This is hygiene, NOT a measured speedup: the sort's input is
                # SAM rows with no sequence columns, so it may well never have wanted
                # those bytes. What IS verified is the mechanism — dropping an in-memory
                # table returns its bytes to the buffer manager immediately rather than
                # deferring the release to connection close, pinned by
                # `test_dropping_an_in_memory_table_releases_its_bytes`. `IF EXISTS`
                # although the CREATE above is now unconditional: it keeps this correct
                # if a future change makes the copy conditional again, which is exactly
                # how a DROP silently stops matching its CREATE.
                conn.execute(f"DROP TABLE IF EXISTS {_QUERY_MATERIALIZED}")

                # PHASE 2 — sort the staged rows into the final output, applying the
                # POOLED filter first when the batch is paired-end. Sorted by the
                # identifier order (alignment_idx leads to match the register-side
                # sort), with position/flags as tiebreakers so a PE read's mate rows
                # land in a deterministic order — the column order + this sort match the
                # DuckLake `alignment` table so register-files schema-matches. The
                # pooled key uses `feature_idx` rather than the raw `reference` string
                # it was cast from (same identity, narrower sort key); both mates of a
                # concordant placement share it.
                #
                # The `string_agg` deliberately carries NO `ORDER BY`, which rests on a
                # miint contract upstream does not document: both CIGAR scorers are
                # PERMUTATION-INVARIANT, so it does not matter which mate concatenates
                # first. Verified over all 120 permutations of a 5-fragment CIGAR
                # including an indel and a soft clip, and pinned by
                # `test_pooled_cigar_scoring_is_permutation_invariant` — because if a
                # mirror build ever made either scorer position-dependent, this gate
                # would go nondeterministic (the same pair kept on one run and dropped
                # on the next) with nothing to signal it. The fix then is to add the
                # `ORDER BY`, at the cost of a sort inside every partition. See
                # docs/duckdb-miint.md.
                pooled_qualify = ""
                if is_paired:
                    # The placement key is shared with the client-side feature-table
                    # gate, which scores the same CIGARs the same way; one definition,
                    # because a change reaching only one copy would silently rescore
                    # every paired placement in the other.
                    partition = PAIRED_PLACEMENT_PARTITION
                    pooled_cigar = f"string_agg(cigar, '') OVER (PARTITION BY {partition})"
                    pooled_qualify = (
                        f"QUALIFY cigar_sequence_identity({pooled_cigar}) >= {min_identity}"
                        f"{_coverage_and(pooled_cigar)} "
                    )
                conn.execute(
                    f"COPY (SELECT * FROM read_parquet('{staged_sql}') "
                    f"{pooled_qualify}"
                    "ORDER BY alignment_idx, prep_sample_idx, sequence_idx, feature_idx, "
                    "position, flags) "
                    f"TO '{out_sql}' ({PARQUET_OPTS})"
                )
        # OUTSIDE the connection contexts, matching qc and host_filter: a context
        # __exit__ that raises after the COPY must still take the partial-output
        # cleanup below, not skip it.
        success = True
    finally:
        # On failure remove a partial output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            alignment.unlink(missing_ok=True)

    # `alignment` is the final output path; `alignment_staging_dir` is the
    # workspace a register-files step loads into the DuckLake `alignment` table.
    # Only alignment.parquet matches its `*.parquet` convention: phase 1's
    # `alignment_unsorted.parquet` is deliberately written INSIDE the DuckDB temp
    # dir, which `duckdb_tmp_dir` tears down before this returns — so it can never
    # be picked up as a second part file to register (nor promoted by the SLURM
    # launcher's manifest walker). Keep any future intermediate there for the same
    # reason. A distinct staging-dir binding (not the generic `staging_dir`),
    # mirroring how host_filter exposes `read_mask_staging_dir` for the read-mask
    # register-files step.
    return {"alignment": alignment, "alignment_staging_dir": workspace}
