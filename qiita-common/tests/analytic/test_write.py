"""String-level tests for `qiita_common.analytic.write` — which relation each writer
copies, and the options it copies it with."""

from __future__ import annotations

import pytest

from qiita_common import analytic as ft
from qiita_common.parquet import PARQUET_OPTS


def test_the_public_schema_is_exactly_the_biom_writers_required_set():
    """miint's BIOM writer looks its three columns up BY NAME and **ignores every
    other column** (probed), so it would happily write a relation still carrying
    `genome_idx` — it is our projection, not the writer, that keeps our identifiers
    out of the file. It does insist on the types: `value` must be DOUBLE exactly, not
    FLOAT, DECIMAL or BIGINT.
    """
    assert ft.LABELLED_SCHEMA == {
        "sample_id": "VARCHAR",
        "feature_id": "VARCHAR",
        "value": "DOUBLE",
    }


@pytest.mark.parametrize("build", [ft.parquet_copy_sql, ft.biom_copy_sql])
def test_both_writers_copy_the_RELABELLED_relation(build, tmp_path):
    """Not `OGU_OUTPUT_TABLE`, which is the same numbers keyed by our own identifiers
    — and only one of these two relations may be written to a file a user
    publishes."""
    sql = build(tmp_path / "out")
    assert f"COPY {ft.LABELLED_RELATION} TO " in sql
    assert ft.OGU_OUTPUT_TABLE not in sql


def test_the_parquet_writer_uses_the_canonical_options(tmp_path):
    """One definition of the Parquet shape for every artifact qiita writes, so a
    version or compression bump moves in one place."""
    assert PARQUET_OPTS in ft.parquet_copy_sql(tmp_path / "t.parquet")


def test_the_biom_writer_names_qiita_as_the_generator(tmp_path):
    """The writer's own default is `miint`, which names the library rather than the
    system that produced the file. Compression is passed explicitly even though gzip
    is also the default — a published artifact's encoding should not change under us
    if an upstream default does.
    """
    sql = ft.biom_copy_sql(tmp_path / "t.biom")
    assert "FORMAT BIOM" in sql
    assert "COMPRESSION 'gzip'" in sql
    assert f"GENERATED_BY '{ft.BIOM_GENERATED_BY}'" in sql
    assert ft.BIOM_GENERATED_BY == "qiita-miint"


def test_the_biom_writer_sets_no_table_id(tmp_path):
    """`ID` is left at the writer's default: the only distinctive handle
    for this table today is an internal identifier, which must not ride a published
    file, and a constant would identify nothing."""
    assert "ID '" not in ft.biom_copy_sql(tmp_path / "t.biom")


@pytest.mark.parametrize("build", [ft.parquet_copy_sql, ft.biom_copy_sql])
def test_a_copy_target_that_cannot_be_interpolated_safely_is_refused(build, tmp_path):
    """The COPY target is a SQL string literal, so it cannot be bound. Both writers
    validate rather than escape — and they do it themselves, since they are the site
    that interpolates."""
    with pytest.raises(ValueError):
        build(tmp_path / "the user's table.biom")


def test_the_tree_copy_takes_a_clearance_and_the_shared_parquet_options(tmp_path):
    sql = ft.tree_copy_sql(tmp_path / "t.tree.parquet", clearance=ft.TreeClearance(tips=4))
    assert PARQUET_OPTS in sql
    assert ft.TREE_TABLE in sql
