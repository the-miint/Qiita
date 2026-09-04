"""String-level tests for `qiita_common.analytic.stage` — the three input streams
and the projection each one is narrowed to."""

from __future__ import annotations

from qiita_common import analytic as ft


def test_coverage_denominator_does_not_depend_on_the_alignment():
    """The denominator is the genome's FULL length, including contigs nothing
    aligned to. Joining the alignment slice in here would silently narrow it to
    aligned-length-only — every genome's breadth would rise and low-coverage
    genomes would survive a threshold they should fail.
    """
    sql = ft.genome_lengths_table_sql("lengths_stream")
    assert ft.ALIGNMENT_TABLE not in sql
    assert ft.MAP_TABLE in sql  # rolls contig lengths up to the genome
    assert "SUM(" in sql.upper()
    assert "GROUP BY" in sql.upper()


def test_alignment_columns_exclude_cigar():
    """`cigar` is the wide column the signed projection exists to leave out — this
    analytic derives breadth from `position`/`stop_position` and never reads it.
    Its absence is most of what the projection buys.
    """
    assert "cigar" not in ft.ALIGNMENT_COLUMNS


def test_alignment_table_binds_exactly_the_declared_columns():
    """One list, used for both the DoGet request and the SELECT, so the projection
    a caller signs and the columns it binds cannot drift."""
    sql = ft.alignment_table_sql("some_stream")
    for column in ft.ALIGNMENT_COLUMNS:
        assert column in sql, column
    assert "some_stream" in sql


def test_staging_builders_read_from_the_source_they_are_given():
    """Each staging builder reads from the caller's relation — a registered stream, a
    `read_parquet(...)` expression — because where the input comes from differs per
    consumer while the projection does not. Asserted as `FROM <source>` so the source
    has to land in the FROM clause rather than merely appear somewhere.
    """
    assert "FROM my_relation" in ft.alignment_table_sql("my_relation")
    assert "FROM read_parquet('m.parquet')" in ft.map_table_sql("read_parquet('m.parquet')")
    assert "FROM my_lengths l" in ft.genome_lengths_table_sql("my_lengths")


def test_denovo_quality_renames_genome_idx_to_the_shared_key():
    """`genome_idx` in, `genome_id` out — the spelling every other de novo relation
    keys on (`denovo_map_table_sql` makes the same rename), so a consumer joins the
    quality to the map without a second name for one column."""
    sql = ft.denovo_genome_quality_table_sql("read_parquet('q.parquet')")
    assert f"CREATE TABLE {ft.DENOVO_GENOME_QUALITY_TABLE} AS " in sql
    assert "genome_idx AS genome_id" in sql
    assert "read_parquet('q.parquet')" in sql


def test_denovo_quality_stages_the_scores_without_a_default():
    """No COALESCE, no NOT NULL, no filter: the scores pass through as they arrive.

    A genome CheckM did not score reaches this staging with NULLs, and defaulting
    them here would turn "nobody measured this" into "this measured zero" — a value
    a completeness predicate would then act on as if it were evidence."""
    sql = ft.denovo_genome_quality_table_sql("src").upper()
    assert "COALESCE" not in sql
    assert "WHERE" not in sql


def test_map_table_renames_each_column_to_the_right_one():
    """`genome_coverage`'s `subject_genome_id` relation is `(contig_id, genome_id)`.
    Both consumers stage the map from a source keyed `(feature_idx, genome_idx)`, so
    the rename lives here rather than in each of them.

    Asserted as whole `X AS Y` fragments, not as four independent substrings: a
    SWAPPED rename (`feature_idx AS genome_id, genome_idx AS contig_id`) contains
    all four names and would satisfy a looser test, while silently corrupting every
    join keyed on them — the length roll-up, the survivor join, and the OGU key.
    """
    sql = ft.map_table_sql("src")
    assert "feature_idx AS contig_id" in sql
    assert "genome_idx AS genome_id" in sql
