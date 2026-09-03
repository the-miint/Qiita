"""String-level tests for `qiita_common.analytic.relations` — what each relation
must BE, and what a builder is not allowed to make it.

The rules here are cross-cutting: they sweep every builder that emits a CREATE,
because a new one is covered by default rather than by somebody remembering to
extend a list.
"""

from __future__ import annotations

from qiita_common import analytic as ft

# Every builder that emits a CREATE, with a representative call. Used by the
# sweeps below so a newly-added builder is covered by default rather than by
# somebody remembering to extend a list.
_CREATE_BUILDERS = {
    "alignment_table_sql": lambda: ft.alignment_table_sql("some_stream"),
    "map_table_sql": lambda: ft.map_table_sql("read_parquet('m.parquet')"),
    "genome_lengths_table_sql": lambda: ft.genome_lengths_table_sql("lengths_stream"),
    "coverage_alignments_view_sql": ft.coverage_alignments_view_sql,
    "survivor_table_sql(pooled)": lambda: ft.survivor_table_sql(ft.CoverageScope.POOLED),
    "survivor_table_sql(per-sample)": lambda: ft.survivor_table_sql(ft.CoverageScope.PER_SAMPLE),
    "ogu_input_table_sql": lambda: ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED),
    "ogu_output_table_sql": lambda: ft.ogu_output_table_sql(populated=True),
    "genome_label_table_sql": lambda: ft.genome_label_table_sql("mint_src"),
    "sample_label_table_sql": lambda: ft.sample_label_table_sql("mint_src"),
    "labelled_relation_sql": lambda: ft.labelled_relation_sql(clearance=ft.LabelClearance(rows=10)),
    "taxonomy_table_sql": lambda: ft.taxonomy_table_sql("taxonomy_stream"),
    "taxonomy_sidecar_sql": ft.taxonomy_sidecar_sql,
}


def test_no_builder_creates_a_temp_relation():
    """`woltka_ogu` resolves its source on a separate connection, which sees
    regular tables and views but NOT TEMP tables, registered stream relations, or
    CTEs. A TEMP anywhere in this chain fails at bind time on the real thing."""
    for name, build in _CREATE_BUILDERS.items():
        sql = build().upper()
        assert "TEMP" not in sql, name
        assert "TEMPORARY" not in sql, name


def test_coverage_input_is_a_view_and_woltka_input_is_a_table():
    """Two different rules, one per relation. `cov_alignments` is read only by the
    `genome_coverage` macro on the caller's own connection, so materializing it
    would just duplicate the alignment slice in RAM; `ogu_input` is read by
    woltka's separate connection, so it must be a real table."""
    assert "CREATE VIEW" in ft.coverage_alignments_view_sql()
    assert "CREATE TABLE" in ft.ogu_input_table_sql(survivor_scope=ft.CoverageScope.POOLED)
    assert "CREATE TABLE" in ft.alignment_table_sql("s")


def test_the_labelled_relation_is_a_view():
    """Its one reader is a COPY on the caller's own connection, so materializing would
    hold a second full copy of the output alive exactly while the writer builds its
    own — the same reason `cov_alignments` is a view."""
    sql = ft.labelled_relation_sql(clearance=ft.LabelClearance(rows=3))
    assert sql.startswith(f"CREATE VIEW {ft.LABELLED_RELATION} ")
