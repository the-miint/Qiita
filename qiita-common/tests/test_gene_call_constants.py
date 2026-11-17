"""The gene-call tables' column contract against the lake DDL."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from qiita_common.gene_call_constants import (
    GENE_CALL_ATTRIBUTE_KEYS,
    GENE_CALL_CONTIG_TABLE,
    GENE_CALL_RUN_KEY,
    GENE_CALL_STEP_IDENTIFIERS,
    GENE_CALL_SUBJECT_KEY,
    GENE_CALL_TABLE,
    GENE_CALL_TABLES,
    gene_call_step_columns,
)


def test_the_data_plane_creates_and_lists_exactly_these_tables():
    """A table the DDL creates but the data plane's own list (which the
    replace-on-re-run registry test walks) or this contract lacks would pile up a
    copy of its rows on every re-run, and fail no other test."""
    src = (
        Path(__file__).resolve().parents[2] / "qiita-data-plane" / "src" / "ducklake.rs"
    ).read_text()
    created = set(re.findall(r"CREATE TABLE IF NOT EXISTS qiita_lake\.(gene_call\w*)", src))
    (listed_src,) = re.findall(r"GENE_CALL_TABLES: &\[&str\] = &\[(.*?)\];", src, re.S)
    listed = set(re.findall(r'"(gene_call\w*)"', listed_src))
    assert created == listed == set(GENE_CALL_TABLES)


@pytest.mark.parametrize("table", sorted(GENE_CALL_TABLES))
def test_gene_call_columns_match_the_lake_ddl_exactly(table, lake_table_ddl):
    """The staging Parquet the writer produces and the table register-files loads
    it into must agree on name, type and order; a mismatch is a registration
    failure at the end of an assembly, not a rename."""
    assert list(GENE_CALL_TABLES[table]) == lake_table_ddl(table)


@pytest.mark.parametrize("table", sorted(GENE_CALL_TABLES))
def test_every_table_leads_with_the_subject_key(table):
    """Each row carries the run, the subject's genome and the call set; the run key
    leads, as the replace reads it."""
    names = tuple(name for name, _ in GENE_CALL_TABLES[table])
    assert names[: len(GENE_CALL_SUBJECT_KEY)] == GENE_CALL_SUBJECT_KEY
    assert GENE_CALL_SUBJECT_KEY[: len(GENE_CALL_RUN_KEY)] == GENE_CALL_RUN_KEY


def test_no_attribute_key_duplicates_a_column():
    """A value is stored once: in its column or in `attributes`, not both."""
    for table, columns in GENE_CALL_TABLES.items():
        names = {name for name, _ in columns}
        assert not names & set(GENE_CALL_ATTRIBUTE_KEYS), table


def test_no_table_carries_a_contig_name():
    """A contig is named by its `feature_idx`; the FASTA header stops at the
    writer."""
    for columns in GENE_CALL_TABLES.values():
        names = {name for name, _ in columns}
        assert not names & {"read_id", "contig_id", "seqid", "genome_id"}


def test_step_identifiers_replace_lake_columns_outside_the_run_key():
    assert set(GENE_CALL_STEP_IDENTIFIERS) == set(GENE_CALL_TABLES)
    for table, identifiers in GENE_CALL_STEP_IDENTIFIERS.items():
        names = {name for name, _ in GENE_CALL_TABLES[table]} - set(GENE_CALL_RUN_KEY)
        assert set(identifiers) <= names, table


def test_step_columns_swap_identifiers_in_place():
    assert [name for name, _ in gene_call_step_columns(GENE_CALL_CONTIG_TABLE)][:4] == [
        "kind",
        "bin_id",
        "call_set",
        "sequence_hash",
    ]
    gene = dict(gene_call_step_columns(GENE_CALL_TABLE))
    assert gene["parent_sequence_hash"] == "UUID"
    assert not {"genome_idx", "feature_idx", "parent_feature_idx"} & set(gene)
    assert not set(GENE_CALL_RUN_KEY) & set(gene)
