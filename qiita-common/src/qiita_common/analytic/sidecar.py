"""The two optional companions to a published table: the taxonomy sidecar and the
sheared tree.

Both are named from the same `exported_feature` mint the table's rows are, so
`feature_id` is the join key across the bundle — which is what makes them joinable to
the table rather than merely shipped beside it.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..taxonomy import (
    RANK_COLUMNS,
    genome_representative_taxonomy_select_sql,
    prefixed_rank_columns_sql,
    rank_columns_sql,
)
from .label import published_membership_sql
from .relations import (
    BLOCKED_FEATURE_TABLE,
    GENOME_LABEL_TABLE,
    PHYLOGENY_TABLE,
    SHEAR_INPUT_RELATION,
    SHEAR_KEEP_SET_RELATION,
    TAXONOMY_SIDECAR_RELATION,
    TAXONOMY_TABLE,
    TREE_TABLE,
    drop_phylogeny_statements,
)

# What the sidecar carries: the SAME `feature_id` the table's rows are named with, and
# the eight ranks. One file joinable to the other on one column, which is the whole
# reason both are labelled from one mint.
#
# No `genome_idx`, and no lineage string. The string form would be lossy in a way the
# columns are not — see `qiita_common.taxonomy.genome_lineage_select_sql`.
# Names only, unlike `TREE_SCHEMA` and `LABELLED_SCHEMA`: those carry types because a
# builder writes `CAST(NULL AS …)` from them for an empty result. The sidecar has no such
# path — it is as wide as the table, and an empty table's sidecar is an empty COPY of a
# relation that already has the right types.
TAXONOMY_SIDECAR_COLUMNS = ("feature_id", *RANK_COLUMNS)

# The columns of the phylogeny stream we read. `name` matters for INTERNAL nodes, which
# keep theirs; `feature_idx` is what resolves a tip to the genome whose row the table
# published. `reference_idx` is not read — the ticket is already scoped to one reference,
# so it is a whole tree's worth of a constant.
PHYLOGENY_COLUMNS = (
    "node_index",
    "parent_index",
    "name",
    "branch_length",
    "edge_id",
    "is_tip",
    "feature_idx",
)

# What the published tree carries: `shear_tree`'s own output columns, in its order and
# with the types it returns (measured — see `docs/duckdb-miint.md`).
# `node_index`/`parent_index` are the SHEAR's 0-based reindexing rather than anything of
# ours, and they are how a tree expresses its shape; `edge_id` is the reference's own
# jplace edge id, the only handle back to its placements. No `feature_idx` — a tip is
# named with the handle its row in the table carries.
#
# The types are written out only because the empty path below has to produce them without
# the shear; the populated path takes them from `shear_tree`.
TREE_SCHEMA = {
    "node_index": "BIGINT",
    "name": "VARCHAR",
    "branch_length": "DOUBLE",
    "edge_id": "BIGINT",
    "parent_index": "BIGINT",
    "is_tip": "BOOLEAN",
}
TREE_COLUMNS = tuple(TREE_SCHEMA)


def taxonomy_table_sql(source: str) -> str:
    """Materialize the streamed per-feature taxonomy into `TAXONOMY_TABLE`.

    `source` is the caller's stream relation. Projected to `feature_idx` plus the eight
    ranks: the stream also carries `reference_idx` and `ncbi_taxon_id` (always NULL
    today), and holding a whole reference's worth of columns nothing reads is the kind
    of cost that only shows up at GG2 scale.
    """
    return (
        f"CREATE TABLE {TAXONOMY_TABLE} AS SELECT feature_idx, {rank_columns_sql()} FROM {source}"
    )


def taxonomy_sidecar_sql() -> str:
    """Define `TAXONOMY_SIDECAR_RELATION`: one row per PUBLISHED row of the table, named
    the same way, carrying its genome's ranks with the source prefixes restored.

    Scoped to `GENOME_LABEL_TABLE` on both sides, which is what makes the sidecar exactly
    as wide as the table — see `published_membership_sql` for the member set and why the
    restriction matters.
    """
    reduction = genome_representative_taxonomy_select_sql(
        member_genome=f"({published_membership_sql()})", taxonomy=TAXONOMY_TABLE
    )
    return (
        f"CREATE VIEW {TAXONOMY_SIDECAR_RELATION} AS "
        f"SELECT g.feature_id, {prefixed_rank_columns_sql(alias='r')} "
        f"FROM ({reduction}) r "
        f"JOIN {GENOME_LABEL_TABLE} g ON g.genome_idx = r.genome_idx"
    )


@dataclass(frozen=True)
class TaxonomyClearance:
    """Evidence that `check_taxonomy_diagnostics` ran and passed, plus the sidecar's row
    count so a caller can report the size without counting it again.

    `taxonomy_copy_sql` takes one for the same reason `labelled_relation_sql` takes a
    `LabelClearance`: a sidecar that does not line up with the table it accompanies is
    a file people will join anyway.
    """

    rows: int


def taxonomy_diagnostics_sql() -> str:
    """One row for `check_taxonomy_diagnostics`, over the sidecar and the label set it
    must match."""
    return (
        f"SELECT (SELECT count(*) FROM {GENOME_LABEL_TABLE}) AS published_rows, "
        f"(SELECT count(*) - count(DISTINCT feature_idx) FROM {TAXONOMY_TABLE}) "
        f"    AS repeated_features, "
        f"count(*) AS taxonomy_rows, "
        f"count(DISTINCT feature_id) AS taxonomy_feature_ids, "
        f"count(*) FILTER (WHERE feature_id IS NULL) AS unnamed_rows "
        f"FROM {TAXONOMY_SIDECAR_RELATION}"
    )


def check_taxonomy_diagnostics(
    *,
    published_rows: int,
    repeated_features: int,
    taxonomy_rows: int,
    taxonomy_feature_ids: int,
    unnamed_rows: int,
) -> TaxonomyClearance:
    """Refuse a sidecar that does not describe the table beside it, and return the
    clearance `taxonomy_copy_sql` requires.

    Every fault here is silent in the file: a short sidecar reads as "those rows are
    unclassified", a duplicated one double-counts under any join, and a NULL name joins
    to nothing. An unclassified genome is NOT one of these — it is present with NULL
    ranks, which is a different statement and a legitimate one.
    """
    if repeated_features:
        # Checked on the STREAMED taxonomy rather than on the sidecar, because the
        # reduction resolves a repeat to one row and so cannot show it downstream — see
        # `genome_representative_taxonomy_select_sql`. Two rows for one feature need not
        # agree, and arbitrating between two lineages is not ours to do quietly.
        raise ValueError(
            f"this reference's taxonomy holds more than one row for {repeated_features} "
            f"feature(s), which ingest writes one-to-one, so the reference is malformed. "
            f"Two rows for one feature can disagree, and choosing between them silently "
            f"would publish an arbitrary lineage."
        )
    if unnamed_rows:
        raise ValueError(
            f"{unnamed_rows} taxonomy rows carry no feature_id, so nothing could join "
            f"them to the table. The sidecar is named from the same label relation the "
            f"table is, so this means that relation gained a NULL handle."
        )
    if taxonomy_rows != published_rows:
        raise ValueError(
            f"the taxonomy sidecar describes {taxonomy_rows} rows but the table "
            f"publishes {published_rows}. A sidecar shorter than its table reads as "
            f"though the missing rows were unclassified — an unclassified genome is "
            f"present here with NULL ranks — and a longer one describes rows nobody "
            f"can find."
        )
    if taxonomy_feature_ids != taxonomy_rows:
        raise ValueError(
            f"{taxonomy_rows} taxonomy rows carry only {taxonomy_feature_ids} distinct "
            f"feature_ids, so joining the sidecar to the table would multiply the rows "
            f"it duplicates. Each published row has exactly one representative member, "
            f"so a repeat means the reference's taxonomy holds more than one row for "
            f"one feature."
        )
    return TaxonomyClearance(rows=taxonomy_rows)


def phylogeny_table_sql(source: str) -> str:
    """Materialize the streamed phylogeny into `PHYLOGENY_TABLE`.

    **A tip's own name is dropped on the way in.** Nothing downstream reads it — the
    shear renames every tip from the mint, and an unpublished one is left nameless — and
    on a reference the size of GG2 the tip labels ARE the tree: half its ~660k nodes, and
    the bulk of 407 MB of Newick, held for the length of the run for nothing. It also
    makes the promise that no reference-internal FASTA header can reach a published file
    structural rather than a property of one CASE expression downstream.
    """
    projection = ", ".join(
        "CASE WHEN is_tip THEN NULL ELSE name END AS name" if column == "name" else column
        for column in PHYLOGENY_COLUMNS
    )
    return f"CREATE TABLE {PHYLOGENY_TABLE} AS SELECT {projection} FROM {source}"


def blocked_feature_table_sql(source: str) -> str:
    """Stage the reference's blocked features into `BLOCKED_FEATURE_TABLE` — see that
    constant for why the tree needs them and nothing else here does.

    DISTINCT because this relation is joined to, and a repeat would fan a tip out. The
    route answers one row per blocked feature today; a lookup table that quietly
    multiplies its caller's rows is not a property worth depending on.
    """
    return f"CREATE TABLE {BLOCKED_FEATURE_TABLE} AS SELECT DISTINCT feature_idx FROM {source}"


def shear_input_statements() -> tuple[str, ...]:
    """Define the shear's two arguments: the tree with its tips renamed to the handles
    the table publishes, and the keep-set of those handles.

    **The tree is renamed rather than the shear's output translated afterwards**, which
    is what makes one vocabulary structural: the shear matches by name, so a keep-set of
    published handles and a tree that speaks them cannot disagree about which tip is
    which, and the sheared output needs no second pass to name.

    A tip whose feature has no published genome gets a NULL name — measured: `shear_tree`
    ignores NULL-named tips, so it is sheared away like any tip outside the keep-set.
    Keeping its own name instead would put a reference-internal FASTA header into a
    published file, and could collide with a handle.

    **A tip whose feature is BLOCKED gets a NULL name for the same reason**, which is how
    this recipe honours the exclusion contract the phylogeny defers to — see
    `BLOCKED_FEATURE_TABLE`. Naming it, not dropping its row: the count has to stay equal
    to the tree's, since a difference is what reports a tip belonging to two genomes. It
    also means a genome with a second, unblocked tip publishes THAT one rather than being
    refused as ambiguous; a genome left with none is caught by `check_tree_diagnostics`,
    which is the honest outcome — the tree has no position for it that a curator accepts.

    **The membership it renames from is the PUBLISHED one** (`published_membership_sql`),
    not the whole reference's map: joining the map unrestricted fans a tip out once per
    genome the feature belongs to, published or not, so a tip would be refused as ambiguous
    on the strength of a genome this table never mentions.

    The keep-set is `GENOME_LABEL_TABLE` itself, so the tips asked for are exactly the
    rows the table publishes.
    """
    published_names = (
        f"CREATE VIEW {SHEAR_INPUT_RELATION} AS "
        f"SELECT p.node_index, p.parent_index, p.branch_length, p.edge_id, p.is_tip, "
        f"CASE WHEN NOT p.is_tip THEN p.name "
        f"     WHEN b.feature_idx IS NULL THEN pub.feature_id END AS name "
        f"FROM {PHYLOGENY_TABLE} p "
        f"LEFT JOIN ({published_membership_sql()}) pub ON pub.feature_idx = p.feature_idx "
        f"LEFT JOIN {BLOCKED_FEATURE_TABLE} b ON b.feature_idx = p.feature_idx"
    )
    keep_set = (
        f"CREATE VIEW {SHEAR_KEEP_SET_RELATION} AS "
        f"SELECT feature_id AS name FROM {GENOME_LABEL_TABLE}"
    )
    return (published_names, keep_set)


@dataclass(frozen=True)
class TreeClearance:
    """Evidence that `check_tree_diagnostics` ran and passed, plus the number of tips the
    sheared tree will carry — which is the published row count, so a caller can report
    the size without counting it again.

    Takes the same role `GateClearance` does: every fault the checks catch produces a
    tree somebody would join to the table anyway.
    """

    tips: int

    @property
    def statements(self) -> tuple[str, ...]:
        """The rest of the protocol in order: shear, then release the whole-reference
        tree and the two views over it.

        Iterating this is what keeps the release from being forgotten — the staged tree
        is the largest relation in the recipe and nothing reads it after the shear.
        """
        return (sheared_tree_table_sql(clearance=self), *drop_phylogeny_statements())


def sheared_tree_table_sql(*, clearance: TreeClearance) -> str:
    """Shear the tree down to the published keep-set, into `TREE_TABLE`.

    `collapse := true` is miint's default and is passed explicitly rather than relied
    on: it removes the single-child ancestors pruning leaves behind and **sums
    their branch lengths onto the surviving edge**, so a tip-to-tip distance in the
    sheared tree is that distance in the whole one.

    `ignore_missing := false` is a backstop, not the guard — `check_tree_diagnostics` has
    already refused a published row the tree has no tip for, and with a count where the
    shear's own error lists every missing name. Left false so a disagreement between what
    was checked and what is sheared fails loudly instead of quietly shearing to a subset.

    **A cleared keep-set of zero tips short-circuits**, the way `ogu_output_table_sql`
    does for an empty analytic and for the same reason: publishing no rows is a legitimate
    result here — every genome dropped by the coverage threshold — and the table written
    beside this is a real, empty file rather than an error. `shear_tree` cannot express it
    (a tree cannot be sheared to nothing, so it raises), so the empty tree is built
    directly. One relation name either way, so the writer cannot tell the difference.

    Materialized, so the whole-reference tree can be dropped before the write. Takes a
    `TreeClearance` for the reason `gated_alignment_table_sql` takes a `GateClearance`.
    """
    if not clearance.tips:
        casts = ", ".join(
            f"CAST(NULL AS {sql_type}) AS {name}" for name, sql_type in TREE_SCHEMA.items()
        )
        return f"CREATE TABLE {TREE_TABLE} AS SELECT {casts} WHERE false"
    return (
        f"CREATE TABLE {TREE_TABLE} AS SELECT {', '.join(TREE_COLUMNS)} "
        f"FROM shear_tree('{SHEAR_INPUT_RELATION}', '{SHEAR_KEEP_SET_RELATION}', "
        f"collapse := true, ignore_missing := false)"
    )


def tree_diagnostics_sql() -> str:
    """One row for `check_tree_diagnostics`, measured over the two relations the shear
    itself reads rather than over a second copy of their join — so what was checked is
    what gets sheared."""
    return (
        f"WITH tip AS ("
        f"SELECT node_index, name FROM {SHEAR_INPUT_RELATION} WHERE is_tip), "
        # The handles whose tip the rename dropped as blocked. Read off the STAGED tree
        # rather than the view, because the view is where the name was taken away — by
        # the time a row reaches `tip` there is nothing left to say it was ever blocked.
        f"blocked_handle AS ("
        f"SELECT DISTINCT pub.feature_id AS name FROM {PHYLOGENY_TABLE} p "
        f"JOIN ({published_membership_sql()}) pub ON pub.feature_idx = p.feature_idx "
        f"JOIN {BLOCKED_FEATURE_TABLE} b ON b.feature_idx = p.feature_idx "
        f"WHERE p.is_tip), "
        f"per_handle AS ("
        f"SELECT k.name, count(t.node_index) AS tips, "
        f"k.name IN (SELECT name FROM blocked_handle) AS had_blocked_tip "
        f"FROM {SHEAR_KEEP_SET_RELATION} k LEFT JOIN tip t ON t.name = k.name "
        f"GROUP BY k.name) "
        f"SELECT (SELECT count(*) FROM {PHYLOGENY_TABLE}) AS tree_nodes, "
        f"(SELECT count(*) FROM {SHEAR_INPUT_RELATION}) AS shear_nodes, "
        f"count(*) AS published_rows, "
        f"count(*) FILTER (WHERE tips = 0) AS rows_with_no_tip, "
        f"count(*) FILTER (WHERE tips > 1) AS rows_with_many_tips, "
        f"count(*) FILTER (WHERE tips = 0 AND had_blocked_tip) AS rows_with_blocked_tip, "
        f"min(name) FILTER (WHERE tips = 0) AS untreed_example, "
        f"min(name) FILTER (WHERE tips > 1) AS multi_tip_example, "
        f"min(name) FILTER (WHERE tips = 0 AND had_blocked_tip) AS blocked_tip_example "
        f"FROM per_handle"
    )


def check_tree_diagnostics(
    *,
    tree_nodes: int,
    shear_nodes: int,
    published_rows: int,
    rows_with_no_tip: int,
    rows_with_many_tips: int,
    rows_with_blocked_tip: int,
    untreed_example: str | None,
    multi_tip_example: str | None,
    blocked_tip_example: str | None,
) -> TreeClearance:
    """Refuse a tree that cannot honestly be published beside this table, and return the
    clearance `sheared_tree_table_sql` requires.

    The shear catches two of these itself, and it is the *message* that differs: its
    errors name our staged relation and, for missing tips, list every name — unusable at
    the size of a real published set. The third it does not catch at all.
    """
    if not tree_nodes:
        raise ValueError(
            "this reference has no phylogeny, so there is no tree to shear. The table "
            "and the taxonomy sidecar do not depend on one — re-run without --tree."
        )
    if shear_nodes != tree_nodes:
        # Renaming tips by genome multiplies a node whose feature belongs to more than
        # one genome, which `feature_genome` allows (identical bytes share one
        # feature_idx, so a plasmid two organisms carry resolves to one feature under
        # both). The shear rejects the result — `Duplicate node_id: N` — so this refusal
        # buys the reason rather than the failure.
        raise ValueError(
            f"the reference's tree has {tree_nodes} nodes but naming its tips by genome "
            f"produces {shear_nodes}, so {shear_nodes - tree_nodes} tip(s) belong to more "
            f"than one genome — a feature two genomes share cannot be one genome-named "
            f"tip. A genome-keyed tree is not publishable for this reference; the table "
            f"and the taxonomy sidecar are unaffected, so re-run without --tree."
        )
    if rows_with_many_tips:
        raise ValueError(
            f"{rows_with_many_tips} published row(s) own more than one tip in this "
            f"reference's tree, {multi_tip_example} among them, so the tree is not "
            f"genome-level for this reference. The shear would keep BOTH tips under the "
            f"one handle, giving a tree with duplicate tip names."
        )
    if rows_with_blocked_tip:
        # Before the untreed refusal below, which counts these rows too: both describe
        # the same genome, and only this one says why the tip went away.
        raise ValueError(
            f"{rows_with_blocked_tip} published row(s) have no tip in this reference's "
            f"tree that a curator accepts, {blocked_tip_example} among them: every tip "
            f"they own is on a blocked feature. The genome still publishes because "
            f"another of its contigs aligned, but its only position in the tree comes "
            f"from sequence the blocklist rejects, so the tree cannot honestly carry it. "
            f"The table and the taxonomy sidecar are unaffected — re-run without --tree."
        )
    if rows_with_no_tip:
        raise ValueError(
            f"{rows_with_no_tip} of {published_rows} published row(s) have no tip in this "
            f"reference's tree, {untreed_example} among them. A tree missing tips the "
            f"table publishes reads as though those rows were left out of the analysis."
        )
    return TreeClearance(tips=published_rows)
