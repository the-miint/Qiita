"""The analytic's relation names, and the rules about what each one is.

**Relation names here are part of the contract, not a caller's choice.**
`woltka_ogu` takes its source relation as a *quoted string literal* and resolves it
on a SEPARATE connection during bind/execute, so `OGU_INPUT_TABLE` is embedded in
the generated SQL: a caller that staged a differently-named table gets a bind error.
That separate connection also cannot see TEMP tables, registered stream relations,
or CTEs — hence every staging statement in this package creates a regular non-temp
TABLE. **Two relations are exceptions, both read only on the caller's own connection
and both VIEWs for the same reason** — materializing would duplicate a large relation
in RAM for one reader: `COVERAGE_ALIGNMENTS_VIEW` (read by the `genome_coverage`
macro) and `LABELLED_RELATION` (read by one COPY).

The releases at the bottom are the other half of the same subject: which relations
are dead at which point, and what a caller runs to let go of them. On a client
machine over a large cohort the ones held past their last reader are several hundred
MB, and DuckDB's spill directory is wherever the user happened to run the CLI.
`ogu_input_statements` sequences the mid-pipeline releases inline, since the caller
iterates that sequence rather than calling each drop by hand.
"""

from __future__ import annotations

ALIGNMENT_TABLE = "alignment_slice"
# The ungated slice, staged only when a gate needs to read `cigar`. It exists
# because the gate's fail-loud checks have to see the rows the gate would DROP, and
# the alignment arrives as a one-shot Flight stream that cannot be scanned twice.
STREAMED_ALIGNMENT_TABLE = "alignment_streamed"
MAP_TABLE = "contig_to_genome"

# The reference's per-feature lengths, as streamed. Staged only for a circular gate,
# which needs the per-feature form `circular_query_coverage` takes; the ungated and
# per-row-gated paths roll the same stream straight up to `GENOME_LENGTHS_TABLE`,
# which is all the coverage denominator needs.
FEATURE_LENGTHS_TABLE = "feature_lengths"
GENOME_LENGTHS_TABLE = "genome_lengths"

COVERAGE_ALIGNMENTS_VIEW = "cov_alignments"

# The circular gate's two arguments, both VIEWs renaming what we already hold into the
# column names `circular_query_coverage` reads. It resolves relation names on the
# caller's own connection, where a VIEW is as visible as a TABLE, so neither is
# materialized.
CIRCULAR_ALIGNMENTS_VIEW = "circ_alignments"
FEATURE_TOPOLOGY_VIEW = "feature_topology"

OGU_INPUT_TABLE = "ogu_input"

# The counts, materialized. Both the populated and the empty path land here, so the
# relabel — and every writer after it — reads one relation whose name and shape do
# not depend on whether the cohort had any alignments. Client-side only: the
# server-side job COPYs `woltka_ogu_select_sql()` straight out to Parquet and
# relabels nothing, because its output stays inside the system.
OGU_OUTPUT_TABLE = "ogu_output"

# The two label relations, each `internal key -> public handle`.
#
# **The row axis is a genome only because this table flavour is OGU.** A feature is not
# always a genome — an amplicon sequence variant, a full-length 16S observed in a sample,
# an assembled contig are all features with no genome to roll up to — so a row-keyed
# flavour is a genuine second shape, not a variant spelling of this one. The identifier
# layer is already ready for it: the mint behind `export_feature_id` publishes a genome
# OR a `(reference, feature)` pair. What is genome-only is the roll-up, which counts what
# `woltka_ogu` counts. This name says `genome` to be honest about that, rather than
# implying generality the computation does not have.
GENOME_LABEL_TABLE = "genome_label"
SAMPLE_LABEL_TABLE = "sample_label"

# The published shape — a VIEW, not a table, and the second exception to the
# non-temp-TABLE rule above (`COVERAGE_ALIGNMENTS_VIEW` is the first, for the same
# reason): only the caller's own connection reads it, and its one reader is a COPY.
# Materializing would hold a second full copy of the output — two VARCHARs and a
# DOUBLE per row, tens of millions of rows at cohort scale — alive at exactly the
# moment the BIOM writer is building its sparse matrix.
LABELLED_RELATION = "feature_table_labelled"

# The reference's per-feature taxonomy, as streamed from the data plane's
# exclusion-aware view. A real TABLE: the reduction reads it twice (once to pick
# each genome's representative, once to take that member's ranks), and a Flight stream
# cannot be scanned twice.
TAXONOMY_TABLE = "reference_taxonomy"

# The published sidecar — a VIEW, for the reason `LABELLED_RELATION` is one: nothing
# reads it except the checks and the COPY, and materializing would hold a second copy of
# the reduction's output for that. It IS evaluated twice, once per reader, which is the
# price of not holding it; the reduction runs over one row per published genome, so that
# trade is the opposite of the tree's, where the relation is the whole reference.
TAXONOMY_SIDECAR_RELATION = "feature_taxonomy"

# The reference's phylogeny as streamed from the lake. A real TABLE for two reasons:
# `shear_tree` resolves its arguments on its own connection, which cannot see a
# registered stream; and this is the largest relation in the recipe — GG2's backbone is
# ~660k nodes — so the shear's clearance drops it the moment the shear is done.
#
# Not named for the lake table it comes from, unlike `TAXONOMY_TABLE`: the
# lake's is `reference_phylogeny` exactly, and one name meaning both a caller's ticket
# argument and a local relation is a trap.
PHYLOGENY_TABLE = "phylogeny_nodes"

# The shear's two arguments, both VIEWs: `shear_tree` takes relation NAMES and resolves
# them on its own connection, where a VIEW is as visible as a TABLE, so materializing
# either would copy the biggest relation here for nothing.
SHEAR_INPUT_RELATION = "phylogeny_published_names"
SHEAR_KEEP_SET_RELATION = "phylogeny_keep_set"

# The reference's curated blocklist, resolved to feature_idx — the exclusion set the
# data plane's own contract names for a tree consumer, `tips WHERE feature_idx NOT IN
# reference_exclusion` (`qiita-data-plane/src/ducklake.rs`, `ensure_exclusion_tables`).
#
# **The tree is the one artifact here that has to apply it itself.** The alignment and
# the taxonomy arrive through exclusion-aware views, so a blocked feature is already gone
# from the table and the sidecar; the phylogeny stream has no such view,
# because anti-joining a tip's row would orphan its internal parents. So a blocked contig
# reaches this recipe with its tip intact, and nothing else would drop it.
BLOCKED_FEATURE_TABLE = "blocked_feature"

# The sheared tree, materialized — see `sheared_tree_table_sql`.
TREE_TABLE = "sheared_tree"


def drop_streamed_alignment_table_sql() -> str:
    """Release the streamed copy once the gate has been applied. Nothing reads it
    again, and it holds `cigar` — so on a client machine over a large cohort, keeping
    it roughly doubles the analytic's peak memory for no benefit."""
    return f"DROP TABLE {STREAMED_ALIGNMENT_TABLE}"


def drop_circular_inputs_statements() -> tuple[str, ...]:
    """Release the circular gate's two rename views and the per-feature lengths one of
    them reads, once the gate has been applied. The views go first because each reads a
    table below it, and the lengths are staged for this gate alone."""
    return (
        f"DROP VIEW {CIRCULAR_ALIGNMENTS_VIEW}",
        f"DROP VIEW {FEATURE_TOPOLOGY_VIEW}",
        f"DROP TABLE {FEATURE_LENGTHS_TABLE}",
    )


def drop_ogu_input_table_sql() -> str:
    """Release woltka's input once the counts exist. Dead from that point on, and
    the largest relation still standing."""
    return f"DROP TABLE {OGU_INPUT_TABLE}"


def drop_phylogeny_statements() -> tuple[str, ...]:
    """Release the whole-reference tree and the two views over it, once the shear has
    materialized its result. The views go first because they read the table."""
    return (
        f"DROP VIEW {SHEAR_INPUT_RELATION}",
        f"DROP VIEW {SHEAR_KEEP_SET_RELATION}",
        f"DROP TABLE {PHYLOGENY_TABLE}",
        f"DROP TABLE {BLOCKED_FEATURE_TABLE}",
    )
