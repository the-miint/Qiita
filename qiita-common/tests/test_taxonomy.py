"""Contract tests for the taxonomy rank shape and the SQL builders over it.

Text-level, because the module is text — the same split `test_feature_table.py`
keeps. What the reduction actually *computes* is asserted against a real DuckDB in
the control plane (`tests/test_taxonomy_rollup.py`, and the shard planner's own
`tests/test_plan_shards.py`, which pins the representative rule end to end).

So what is worth pinning here is the shape three things agree on by index — the
column order, the prefix sequence, and the quoting — plus the two properties of the
generated SQL that a reader cannot see by inspection: that a caller's relation
expression is used verbatim, and that prefix restoration distinguishes NULL from
empty.
"""

import pytest

from qiita_common.taxonomy import (
    QUOTED_RANK_COLUMNS,
    RANK_COLUMNS,
    RANK_PREFIXES,
    genome_lineage_select_sql,
    genome_representative_taxonomy_select_sql,
    prefixed_rank_columns_sql,
    quote_rank,
    rank_columns_sql,
)


def test_the_ranks_run_coarsest_to_finest():
    """The order is load-bearing three times over — it is the DuckLake column order,
    the order a lineage concatenates in, and the order the prefixes pair with."""
    assert RANK_COLUMNS == (
        "domain",
        "phylum",
        "class",
        "order",
        "family",
        "genus",
        "species",
        "strain",
    )


def test_every_rank_has_exactly_one_prefix():
    assert len(RANK_PREFIXES) == len(RANK_COLUMNS)
    assert RANK_PREFIXES == ("d__", "p__", "c__", "o__", "f__", "g__", "s__", "t__")
    # The prefix letters are not derivable from the column names — `class` is `c__`
    # but `species` is `s__` while `strain` is `t__` — so the pairing is data, and a
    # test that recomputed it from the names would encode the same guess twice.
    assert RANK_PREFIXES[RANK_COLUMNS.index("strain")] == "t__"
    assert RANK_PREFIXES[RANK_COLUMNS.index("species")] == "s__"


def test_only_the_keyword_ranks_are_quoted():
    """Quoting all eight would be uniform and would make them case-sensitive against
    a schema that spells them lowercase."""
    assert quote_rank("class") == '"class"'
    assert quote_rank("order") == '"order"'
    assert quote_rank("domain") == "domain"
    assert QUOTED_RANK_COLUMNS[RANK_COLUMNS.index("class")] == '"class"'


@pytest.mark.parametrize("alias", ["", "t"])
def test_rank_columns_sql_qualifies_every_column_or_none(alias):
    sql = rank_columns_sql(alias=alias)
    for quoted in QUOTED_RANK_COLUMNS:
        assert f"{alias}.{quoted}" in sql if alias else quoted in sql


def test_prefix_restoration_keeps_null_distinct_from_empty():
    """An absent rank must not become the bare prefix: `NULL` means nothing was
    reported, `''` means the source lineage carried `p__` with nothing after it, and
    a published sidecar has to keep those apart."""
    sql = prefixed_rank_columns_sql()
    assert "IS NULL THEN NULL" in sql
    for quoted, prefix in zip(QUOTED_RANK_COLUMNS, RANK_PREFIXES, strict=True):
        assert f"'{prefix}' || {quoted} END AS {quoted}" in sql


def test_the_reduction_uses_the_callers_relation_expressions_verbatim():
    """Relation *expressions*, not table names, so a caller whose map is keyed
    differently renames at the call site."""
    sql = genome_representative_taxonomy_select_sql(
        member_genome="(SELECT contig_id AS feature_idx, genome_id AS genome_idx FROM m)",
        taxonomy="read_parquet('t.parquet')",
    )
    assert "(SELECT contig_id AS feature_idx, genome_id AS genome_idx FROM m) mg" in sql
    assert "read_parquet('t.parquet') t" in sql


def test_the_representative_is_the_lowest_classified_feature():
    """Both halves in one place: `min(feature_idx)` for determinism and the
    `FILTER` so an unclassified lowest member does not speak for the genome."""
    sql = genome_representative_taxonomy_select_sql(member_genome="mg", taxonomy="t")
    assert "min(feature_idx) FILTER (WHERE lineage <> '')" in sql
    assert "LEFT JOIN t t ON t.feature_idx = mg.feature_idx" in sql


def test_the_lineage_projection_is_the_reduction_plus_a_join():
    """One reduction, two projections — so the genome that tiles under a lineage is
    the genome that publishes those ranks."""
    reduction = genome_representative_taxonomy_select_sql(member_genome="mg", taxonomy="t")
    lineage = genome_lineage_select_sql(member_genome="mg", taxonomy="t")
    assert reduction in lineage
    assert "concat_ws(';'" in lineage
    assert "AS lineage" in lineage
