"""String-level tests for `qiita_common.analytic.sidecar` — the taxonomy sidecar and
the sheared tree, and the refusals that keep each honest beside the table."""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft
from qiita_common.taxonomy import RANK_COLUMNS


def test_the_sidecar_carries_the_tables_own_join_key_and_no_lineage_string():
    """One published vocabulary across both files, so they join on one column — and
    eight rank columns rather than a joined string, which `concat_ws` would make lossy
    at exactly the rank a reader cares about."""
    assert ft.TAXONOMY_SIDECAR_COLUMNS[0] == "feature_id"
    assert set(ft.TAXONOMY_SIDECAR_COLUMNS[1:]) == set(RANK_COLUMNS)
    assert "lineage" not in ft.TAXONOMY_SIDECAR_COLUMNS
    assert not [name for name in ft.TAXONOMY_SIDECAR_COLUMNS if name.endswith("_idx")]


def test_the_sidecar_is_scoped_to_the_published_genomes_on_both_sides():
    """Both the row set and the cost: reducing a whole reference's taxonomy when only
    the survivors are published would be wrong AND expensive."""
    sql = ft.taxonomy_sidecar_sql()
    assert sql.count(ft.GENOME_LABEL_TABLE) == 2
    assert ft.MAP_TABLE in sql
    assert ft.TAXONOMY_TABLE in sql


def test_the_sidecar_restores_the_prefixes():
    sql = ft.taxonomy_sidecar_sql()
    assert "'d__' ||" in sql
    assert "'t__' ||" in sql


def test_the_sidecar_projection_quotes_the_keyword_ranks(tmp_path):
    """`class` and `order` are SQL keywords, and the sidecar names its columns for the
    ranks — so the COPY has to quote them while the Parquet column names stay bare."""
    sql = ft.taxonomy_copy_sql(tmp_path / "t.parquet", clearance=ft.TaxonomyClearance(rows=3))
    assert '"class"' in sql
    assert '"order"' in sql


def _taxonomy_check(**overrides):
    row = {
        "published_rows": 10,
        "repeated_features": 0,
        "taxonomy_rows": 10,
        "taxonomy_feature_ids": 10,
        "unnamed_rows": 0,
    } | overrides
    return ft.check_taxonomy_diagnostics(**row)


def test_a_clean_sidecar_clears_with_its_row_count():
    assert _taxonomy_check().rows == 10


def test_check_refuses_a_sidecar_shorter_than_its_table():
    """Which reads as though the missing rows were unclassified — and an unclassified
    genome is present here with NULL ranks, so the two would be indistinguishable."""
    with pytest.raises(ValueError, match="publishes 10"):
        _taxonomy_check(taxonomy_rows=9)


def test_check_refuses_a_duplicated_sidecar_row():
    with pytest.raises(ValueError, match="distinct"):
        _taxonomy_check(taxonomy_feature_ids=9)


def test_check_refuses_an_unnamed_sidecar_row():
    with pytest.raises(ValueError, match="no feature_id"):
        _taxonomy_check(unnamed_rows=1)


def test_the_staged_tree_keeps_the_shear_shape_plus_the_join_key():
    """`name` is staged because the shear matches tips BY NAME; `feature_idx` because
    that is what resolves a tip to the genome the table published."""
    sql = ft.phylogeny_table_sql("phylo_stream")
    assert sql.startswith(f"CREATE TABLE {ft.PHYLOGENY_TABLE} AS SELECT ")
    assert "FROM phylo_stream" in sql
    for column in ("node_index", "parent_index", "name", "branch_length", "edge_id", "feature_idx"):
        assert column in sql
    # Not `reference_idx`: the ticket is already scoped to one reference, so a whole
    # tree's worth of a constant column is pure cost.
    assert "reference_idx" not in sql


def test_the_staged_tree_drops_a_tips_own_name():
    """Nothing downstream reads it — every tip is renamed from the mint, and an
    unpublished one is left nameless — and at GG2 the tip labels are the bulk of a 407 MB
    Newick. Dropping them at the door also makes "no reference-internal name reaches a
    published file" structural rather than one CASE downstream."""
    sql = ft.phylogeny_table_sql("phylo_stream")
    assert "CASE WHEN is_tip THEN NULL ELSE name END AS name" in sql


def test_the_shear_input_names_tips_from_the_published_labels():
    """The tree the shear reads speaks the table's vocabulary, so a keep-set of
    published handles and the tree cannot disagree about which tip is which."""
    published, keep_set = ft.shear_input_statements()
    assert f"CREATE VIEW {ft.SHEAR_INPUT_RELATION} AS" in published
    assert "WHEN NOT p.is_tip THEN p.name" in published
    assert "WHEN b.feature_idx IS NULL THEN pub.feature_id" in published
    assert keep_set == (
        f"CREATE VIEW {ft.SHEAR_KEEP_SET_RELATION} AS "
        f"SELECT feature_id AS name FROM {ft.GENOME_LABEL_TABLE}"
    )


def test_the_tip_rename_reads_PUBLISHED_membership_and_not_the_whole_map():
    """The whole reference's map fans a tip out once per genome its feature belongs to —
    published or not — and the node-count check then refuses the build over a genome the
    table never mentions. Both consumers of the restriction build it the same way, which is
    why it is one function.
    """
    published, _ = ft.shear_input_statements()
    assert f"({ft.published_membership_sql()}) pub ON pub.feature_idx = p.feature_idx" in published
    # `MAP_TABLE` reaches the view only THROUGH that restriction, never on its own.
    assert f"JOIN {ft.MAP_TABLE} m ON m.contig_id" not in published
    assert ft.published_membership_sql() in ft.taxonomy_sidecar_sql()


def test_published_membership_is_an_inner_join_so_an_unpublished_genome_drops_out():
    sql = ft.published_membership_sql()
    assert f"FROM {ft.MAP_TABLE} m JOIN {ft.GENOME_LABEL_TABLE} l" in sql
    assert "LEFT JOIN" not in sql


def test_an_unpublished_tip_is_left_nameless_rather_than_keeping_its_own():
    """A LEFT JOIN onto the tree, so a tip with no published genome gets a NULL name —
    which never matches the keep-set (so it is sheared away) and cannot collide with a
    published handle. Its original name is a reference-internal FASTA header, and a
    published artifact should not carry one by accident."""
    published, _ = ft.shear_input_statements()
    assert f"FROM {ft.PHYLOGENY_TABLE} p LEFT JOIN" in published


def test_the_shear_collapses_and_refuses_a_missing_tip():
    sql = ft.sheared_tree_table_sql(clearance=ft.TreeClearance(tips=4))
    assert f"CREATE TABLE {ft.TREE_TABLE} AS SELECT " in sql
    assert f"shear_tree('{ft.SHEAR_INPUT_RELATION}', '{ft.SHEAR_KEEP_SET_RELATION}'" in sql
    assert "collapse := true" in sql
    assert "ignore_missing := false" in sql


def test_the_published_tree_carries_no_identifier_of_ours():
    """`node_index`/`parent_index` are the SHEAR's own 0-based reindexing and are how a
    tree expresses its shape; `edge_id` is the reference's jplace edge id. What must not
    appear is `feature_idx` — a tip is named by the handle its table row carries."""
    assert ft.TREE_COLUMNS == (
        "node_index",
        "name",
        "branch_length",
        "edge_id",
        "parent_index",
        "is_tip",
    )
    assert "feature_idx" not in ft.sheared_tree_table_sql(clearance=ft.TreeClearance(tips=2))


def test_the_clearance_shears_then_releases_the_whole_reference_tree():
    """In that order, and the views before the table they read: the staged tree is the
    largest relation in this recipe, and nothing reads it after the shear."""
    statements = ft.TreeClearance(tips=3).statements
    assert statements[0] == ft.sheared_tree_table_sql(clearance=ft.TreeClearance(tips=3))
    assert statements[1:] == (
        f"DROP VIEW {ft.SHEAR_INPUT_RELATION}",
        f"DROP VIEW {ft.SHEAR_KEEP_SET_RELATION}",
        f"DROP TABLE {ft.PHYLOGENY_TABLE}",
        f"DROP TABLE {ft.BLOCKED_FEATURE_TABLE}",
    )


def test_the_tree_diagnostics_measure_the_relations_the_shear_reads():
    """Not a second copy of the join: what was checked has to be what gets sheared."""
    sql = ft.tree_diagnostics_sql()
    assert ft.SHEAR_INPUT_RELATION in sql
    assert ft.SHEAR_KEEP_SET_RELATION in sql
    assert ft.PHYLOGENY_TABLE in sql


def _tree_check(**overrides):
    row = {
        "tree_nodes": 9,
        "shear_nodes": 9,
        "published_rows": 4,
        "rows_with_no_tip": 0,
        "rows_with_many_tips": 0,
        "rows_with_blocked_tip": 0,
        "untreed_example": None,
        "multi_tip_example": None,
        "blocked_tip_example": None,
    } | overrides
    return ft.check_tree_diagnostics(**row)


def test_a_clean_tree_clears_with_its_tip_count():
    assert _tree_check().tips == 4


def test_check_refuses_a_reference_with_no_phylogeny():
    """Detected rather than left to the shear, whose own message names our staged
    relation instead of saying the reference has no tree."""
    with pytest.raises(ValueError, match="no phylogeny"):
        _tree_check(tree_nodes=0, shear_nodes=0, rows_with_no_tip=4, untreed_example="GCF_1")


def test_check_refuses_a_tip_shared_by_more_than_one_genome():
    """The shape a shared plasmid produces: one feature under two genomes, so naming
    tips by genome duplicates the node."""
    with pytest.raises(ValueError, match="more than one genome"):
        _tree_check(shear_nodes=11)


def test_check_refuses_a_published_row_the_tree_has_no_tip_for():
    with pytest.raises(ValueError, match="GCF_ABSENT"):
        _tree_check(rows_with_no_tip=2, untreed_example="GCF_ABSENT")


def test_check_refuses_a_genome_owning_more_than_one_tip():
    """Which the shear would accept, keeping both tips under one published handle."""
    with pytest.raises(ValueError, match="GCF_MULTI"):
        _tree_check(rows_with_many_tips=1, multi_tip_example="GCF_MULTI")


def test_check_refuses_a_published_row_whose_only_tip_is_blocked():
    """A genome-level tree carries one tip per genome, so a curator who blocks the contig
    that tip is wired to leaves the genome with no honest position — and the genome still
    publishes, on the strength of a sibling contig nothing blocked."""
    with pytest.raises(ValueError, match="GCF_BLOCKED"):
        _tree_check(rows_with_blocked_tip=1, blocked_tip_example="GCF_BLOCKED")


def test_a_blocked_tip_is_refused_before_a_missing_one():
    """Both counts describe the same genome when its only tip is blocked and therefore
    unnamed. The blocked message is the one that says WHY, so it has to win."""
    with pytest.raises(ValueError, match="blocked"):
        _tree_check(
            rows_with_no_tip=1,
            untreed_example="GCF_BLOCKED",
            rows_with_blocked_tip=1,
            blocked_tip_example="GCF_BLOCKED",
        )


def test_check_refuses_a_reference_whose_taxonomy_repeats_a_feature():
    """Measured on the STREAMED taxonomy, not the sidecar: the reduction resolves a repeat
    to one row, so by the time the sidecar exists the repeat is invisible — and two rows
    for one feature can carry different lineages."""
    with pytest.raises(ValueError, match="more than one row"):
        _taxonomy_check(repeated_features=1)


def test_a_cleared_keep_set_of_no_tips_builds_the_tree_without_the_shear():
    """Publishing no rows is a legitimate result — every genome dropped by the threshold —
    and the table written beside this one is a real, empty file. `shear_tree` cannot say
    it: a tree sheared to nothing raises. Same short-circuit, and same one-relation-name
    discipline, as `ogu_output_table_sql`'s empty path."""
    sql = ft.sheared_tree_table_sql(clearance=ft.TreeClearance(tips=0))
    assert "shear_tree" not in sql
    assert sql.endswith("WHERE false")
    for name, sql_type in ft.TREE_SCHEMA.items():
        assert f"CAST(NULL AS {sql_type}) AS {name}" in sql
