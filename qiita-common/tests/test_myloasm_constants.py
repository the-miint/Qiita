"""The myloasm provenance tables' column contract against the lake DDL."""

from __future__ import annotations

import pytest

from qiita_common.myloasm_constants import (
    MYLOASM_RUN_KEY,
    MYLOASM_RUN_TABLE,
    MYLOASM_TABLES,
    myloasm_projection,
)


def test_the_six_tables_are_named_singular_and_cover_every_reader():
    """One lake table per miint `read_myloasm_*` reader, named for one row of it
    (CLAUDE.md: table names are singular)."""
    assert set(MYLOASM_TABLES) == {
        "myloasm_contig",
        "myloasm_layout",
        "myloasm_mapping",
        "myloasm_link",
        "myloasm_split",
        "myloasm_run",
    }
    assert {t.reader for t in MYLOASM_TABLES.values()} == {
        "read_myloasm_contigs",
        "read_myloasm_layout",
        "read_myloasm_mappings",
        "read_myloasm_links",
        "read_myloasm_splits",
        "read_myloasm_run",
    }


@pytest.mark.parametrize("table", sorted(MYLOASM_TABLES))
def test_myloasm_columns_match_the_lake_ddl_exactly(table, lake_table_ddl):
    """The staging Parquet the transform writes and the table register-files loads
    it into must agree on name, type and order; a mismatch is a registration
    failure at the end of an assembly, not a rename."""
    assert list(MYLOASM_TABLES[table].columns) == lake_table_ddl(table)


@pytest.mark.parametrize("table", sorted(MYLOASM_TABLES))
def test_every_table_leads_with_the_run_key(table):
    """Every table is replaced per run on `MYLOASM_RUN_KEY`, so every table
    carries it, first and as BIGINT."""
    columns = MYLOASM_TABLES[table].columns
    assert columns[: len(MYLOASM_RUN_KEY)] == tuple((c, "BIGINT") for c in MYLOASM_RUN_KEY)


def test_run_is_the_replace_key_source():
    assert MYLOASM_RUN_TABLE in MYLOASM_TABLES
    assert MYLOASM_RUN_KEY == ("prep_sample_idx", "processing_idx")


def test_no_table_carries_a_read_name_or_a_contig_name():
    """Reads are named by `sequence_idx` and contigs by `contig_num` (and
    `feature_idx` where one was minted); the producer's text names stop at the
    transform."""
    for table in MYLOASM_TABLES.values():
        names = {c for c, _ in table.columns}
        assert not names & {"read_id", "from_read_id", "to_read_id", "contig_id", "fasta_id"}


def test_projection_casts_every_column_in_lake_order():
    sources = {c: f"src_{c}" for c in MYLOASM_TABLES[MYLOASM_RUN_TABLE].column_names}
    projection = myloasm_projection(MYLOASM_RUN_TABLE, sources)
    assert projection.startswith("CAST(src_prep_sample_idx AS BIGINT) AS prep_sample_idx, ")
    assert projection.endswith("CAST(src_hifi AS BOOLEAN) AS hifi")


def test_projection_refuses_a_missing_or_stray_column():
    columns = MYLOASM_TABLES[MYLOASM_RUN_TABLE].column_names
    with pytest.raises(KeyError, match="missing \\['hifi'\\]"):
        myloasm_projection(MYLOASM_RUN_TABLE, {c: "NULL" for c in columns if c != "hifi"})
    with pytest.raises(KeyError, match="unexpected \\['extra'\\]"):
        myloasm_projection(MYLOASM_RUN_TABLE, {**{c: "NULL" for c in columns}, "extra": "1"})
