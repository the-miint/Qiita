"""The analytic export surface, as SQL text.

Two consumers run this same analytic and must not disagree about it: the
compute-orchestrator native job `estimate_feature_table` (server-side, reached
through a work ticket) and the client-side feature-table recipe (a user's machine,
composing the analytic-export routes). They differ in everything *around* the
analytic — where the inputs come from, how the result is written — and in nothing
about the analytic itself, so the SQL lives here and the streaming and I/O stay with
each caller.

**Plain SQL text, so nothing here needs a connection of its own.** Callers execute
these statements on a connection that has miint loaded. (Same shape as `chunking.py`'s
expression builders and `parquet.py`'s option strings.) That is a choice about where
the I/O lives, not a prohibition: miint is core to all of qiita, and this package's
tests execute against it.

**The `source` argument of every staging builder is interpolated VERBATIM, and
that is the caller's obligation to make safe.** A FROM-clause relation cannot be a
bound parameter, so there is no version of this that binds instead. Pass only a
relation name you control (a registered stream relation, an internal table) or an
expression built from an already-validated path — `parquet.validate_parquet_path`
is what both current callers use. Never build one from unvalidated input: the
client-side consumer runs on a user's machine with the user's own credentials, so
a `source` assembled from user input executes as that user against their own
catalog. Everything else in this package is either a fixed literal or a bound `?`.

The modules, in the order a recipe reaches them:

| Module | What it owns |
|---|---|
| `relations` | the relation names, what each one is (TABLE vs VIEW), and its release |
| `stage` | the three input streams → named relations |
| `coverage` | the scope, the survivor set, and what the roll-up leaves behind |
| `gate` | the alignment gate — CIGAR or circular-pooled — and its clearance |
| `ogu` | `woltka_ogu`'s input and output — today's one table flavour |
| `label` | the relabel to public handles |
| `sidecar` | the taxonomy sidecar and the sheared tree |
| `write` | the Parquet / BIOM / tree writers |
"""

from __future__ import annotations

from .coverage import (
    CoverageScope,
    RollupCoverage,
    coverage_alignments_view_sql,
    coverage_filter_applies,
    rollup_coverage_diagnostics_sql,
    rollup_coverage_warning,
    survivor_table_name,
    survivor_table_sql,
)
from .gate import (
    CIRCULAR_MIN_COVERAGE,
    CIRCULAR_MIN_IDENTITY,
    PAIRED_PLACEMENT_PARTITION,
    AlignmentGate,
    GateClearance,
    check_gate_diagnostics,
    circular_alignments_view_sql,
    circular_cleared_join,
    circular_predicate_sql,
    feature_topology_view_sql,
    gate_alignment_columns,
    gate_diagnostics_sql,
    gate_parameters,
    gated_alignment_table_sql,
    streamed_alignment_table_sql,
)
from .label import (
    LABELLED_COLUMNS,
    LABELLED_SCHEMA,
    LabelClearance,
    check_relabel_diagnostics,
    genome_label_table_sql,
    labelled_relation_sql,
    published_membership_sql,
    relabel_diagnostics_sql,
    sample_label_table_sql,
)
from .ogu import (
    OUTPUT_COLUMNS,
    OUTPUT_SCHEMA,
    empty_ogu_select_sql,
    ogu_input_count_sql,
    ogu_input_statements,
    ogu_input_table_sql,
    ogu_output_table_sql,
    woltka_ogu_select_sql,
)
from .relations import (
    ALIGNMENT_TABLE,
    BLOCKED_FEATURE_TABLE,
    CIRCULAR_ALIGNMENTS_VIEW,
    COVERAGE_ALIGNMENTS_VIEW,
    FEATURE_LENGTHS_TABLE,
    FEATURE_TOPOLOGY_VIEW,
    GENOME_LABEL_TABLE,
    GENOME_LENGTHS_TABLE,
    LABELLED_RELATION,
    MAP_TABLE,
    OGU_INPUT_TABLE,
    OGU_OUTPUT_TABLE,
    PHYLOGENY_TABLE,
    SAMPLE_LABEL_TABLE,
    SHEAR_INPUT_RELATION,
    SHEAR_KEEP_SET_RELATION,
    STREAMED_ALIGNMENT_TABLE,
    TAXONOMY_SIDECAR_RELATION,
    TAXONOMY_TABLE,
    TREE_TABLE,
    drop_circular_inputs_statements,
    drop_ogu_input_table_sql,
    drop_phylogeny_statements,
    drop_streamed_alignment_table_sql,
)
from .sidecar import (
    PHYLOGENY_COLUMNS,
    TAXONOMY_SIDECAR_COLUMNS,
    TREE_COLUMNS,
    TREE_SCHEMA,
    TaxonomyClearance,
    TreeClearance,
    blocked_feature_table_sql,
    check_taxonomy_diagnostics,
    check_tree_diagnostics,
    phylogeny_table_sql,
    shear_input_statements,
    sheared_tree_table_sql,
    taxonomy_diagnostics_sql,
    taxonomy_sidecar_sql,
    taxonomy_table_sql,
    tree_diagnostics_sql,
)
from .stage import (
    ALIGNMENT_COLUMNS,
    alignment_table_sql,
    feature_lengths_table_sql,
    genome_lengths_table_sql,
    map_table_sql,
)
from .write import (
    BIOM_GENERATED_BY,
    biom_copy_sql,
    parquet_copy_sql,
    taxonomy_copy_sql,
    tree_copy_sql,
)

__all__ = [
    "ALIGNMENT_COLUMNS",
    "ALIGNMENT_TABLE",
    "AlignmentGate",
    "BIOM_GENERATED_BY",
    "BLOCKED_FEATURE_TABLE",
    "CIRCULAR_ALIGNMENTS_VIEW",
    "CIRCULAR_MIN_COVERAGE",
    "CIRCULAR_MIN_IDENTITY",
    "COVERAGE_ALIGNMENTS_VIEW",
    "CoverageScope",
    "FEATURE_LENGTHS_TABLE",
    "FEATURE_TOPOLOGY_VIEW",
    "GENOME_LABEL_TABLE",
    "GENOME_LENGTHS_TABLE",
    "GateClearance",
    "LABELLED_COLUMNS",
    "LABELLED_RELATION",
    "LABELLED_SCHEMA",
    "LabelClearance",
    "MAP_TABLE",
    "OGU_INPUT_TABLE",
    "OGU_OUTPUT_TABLE",
    "OUTPUT_COLUMNS",
    "OUTPUT_SCHEMA",
    "PAIRED_PLACEMENT_PARTITION",
    "PHYLOGENY_COLUMNS",
    "PHYLOGENY_TABLE",
    "RollupCoverage",
    "SAMPLE_LABEL_TABLE",
    "SHEAR_INPUT_RELATION",
    "SHEAR_KEEP_SET_RELATION",
    "STREAMED_ALIGNMENT_TABLE",
    "TAXONOMY_SIDECAR_COLUMNS",
    "TAXONOMY_SIDECAR_RELATION",
    "TAXONOMY_TABLE",
    "TREE_COLUMNS",
    "TREE_SCHEMA",
    "TREE_TABLE",
    "TaxonomyClearance",
    "TreeClearance",
    "alignment_table_sql",
    "biom_copy_sql",
    "blocked_feature_table_sql",
    "check_gate_diagnostics",
    "check_relabel_diagnostics",
    "check_taxonomy_diagnostics",
    "check_tree_diagnostics",
    "circular_alignments_view_sql",
    "circular_cleared_join",
    "circular_predicate_sql",
    "coverage_alignments_view_sql",
    "coverage_filter_applies",
    "drop_circular_inputs_statements",
    "drop_ogu_input_table_sql",
    "drop_phylogeny_statements",
    "drop_streamed_alignment_table_sql",
    "empty_ogu_select_sql",
    "feature_lengths_table_sql",
    "feature_topology_view_sql",
    "gate_alignment_columns",
    "gate_diagnostics_sql",
    "gate_parameters",
    "gated_alignment_table_sql",
    "genome_label_table_sql",
    "genome_lengths_table_sql",
    "labelled_relation_sql",
    "map_table_sql",
    "ogu_input_count_sql",
    "ogu_input_statements",
    "ogu_input_table_sql",
    "ogu_output_table_sql",
    "parquet_copy_sql",
    "phylogeny_table_sql",
    "published_membership_sql",
    "relabel_diagnostics_sql",
    "rollup_coverage_diagnostics_sql",
    "rollup_coverage_warning",
    "sample_label_table_sql",
    "shear_input_statements",
    "sheared_tree_table_sql",
    "streamed_alignment_table_sql",
    "survivor_table_name",
    "survivor_table_sql",
    "taxonomy_copy_sql",
    "taxonomy_diagnostics_sql",
    "taxonomy_sidecar_sql",
    "taxonomy_table_sql",
    "tree_copy_sql",
    "tree_diagnostics_sql",
    "woltka_ogu_select_sql",
]
