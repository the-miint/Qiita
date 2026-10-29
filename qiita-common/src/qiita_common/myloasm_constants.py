"""The myloasm provenance contract: what a myloasm assembly run persists beside its
contigs, and the lake schema it lands in.

myloasm decides, per read, which contig the read belongs to, with a strain-level
test (SNPmers: k-mers spanning a variable site). duckdb-miint reads one myloasm
output directory as six tables (`read_myloasm_*`; see
https://the-miint.github.io/duckdb-miint/reading/#myloasm). Each becomes one
lake table here, keyed on the assembly run and re-keyed onto our identifiers:

- A read is named by its `sequence_idx` (the CLAUDE.md read-identity rule).
- A contig is named by `contig_num`, the `N` of myloasm's `u<N>ctg`, and by its
  `feature_idx` where the run minted one (primary contigs only: alternates,
  duplicates and contigs filtered after the GFA was written are not hashed, so
  they have none).

**Why `contig_num` identifies a contig within one run.** myloasm writes `N` as the
index of the contig's first read in its internal read numbering (myloasm v0.6.0
`src/unitig.rs`: `node_id = read_indices_ori[0].0`, the same value the GFA `S`
line and the polished FASTA header print). Contigs never share a read: the first
unitigs are non-branching paths of the read graph, which partition the reads;
merges join whole unitigs; and the progressive-coverage merge in `src/main.rs`
refuses a contig that reuses an already-claimed read and drops base contigs that
share one. So two contigs cannot have the same first read. This is a consequence
of how myloasm builds contigs, not a documented contract, and `N` means nothing
across runs. The transform therefore checks uniqueness per run rather than
assuming it, and every key that uses `contig_num` also carries the run.

Neither component can import the other, so a test pins each table's column list
here against `qiita-data-plane/src/ducklake.rs`.
"""

from __future__ import annotations

from dataclasses import dataclass

# The directory, inside the assemble step's `genomes_dir`, that holds one raw
# Parquet per reader, verbatim (`<table>.parquet`). Raw means miint's own columns:
# read and contig names are still myloasm's text, and the run key is absent.
MYLOASM_RAW_DIR = "myloasm"

# Every table is replaced per assembly run at registration. `myloasm_run` is the
# key source for all six: every myloasm run writes at least one log, which
# `read_myloasm_run` requires, so its file always names the run even when another
# table is empty. The transform refuses a dump without a run row.
MYLOASM_RUN_TABLE = "myloasm_run"
MYLOASM_RUN_KEY = ("prep_sample_idx", "processing_idx")

_RUN_KEY_COLUMNS: tuple[tuple[str, str], ...] = tuple((c, "BIGINT") for c in MYLOASM_RUN_KEY)


@dataclass(frozen=True)
class MyloasmTable:
    """One persisted table: the miint reader it comes from, and its lake columns
    in DDL order with DDL types."""

    reader: str
    columns: tuple[tuple[str, str], ...]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(name for name, _ in self.columns)


# Column meanings are miint's (the reading docs linked above); the only renames are
# the identity columns: `contig_id` -> `contig_num`, `read_id` -> `sequence_idx`.
# One row is miint's row: a contig (contig), a read piece in a contig's layout
# (layout), a read piece's mapping to a contig (mapping), a graph edge (link), a
# split breakpoint (split), and a myloasm log in the run's output tree (run). The
# run key is therefore not unique in `myloasm_run` by construction: myloasm writes
# one log per invocation, and the assemble step invokes it once per attempt into a
# fresh directory, so a run has one row in practice.
MYLOASM_TABLES: dict[str, MyloasmTable] = {
    "myloasm_contig": MyloasmTable(
        reader="read_myloasm_contigs",
        columns=(
            *_RUN_KEY_COLUMNS,
            ("contig_num", "BIGINT"),
            ("feature_idx", "BIGINT"),
            ("dereplication_status", "VARCHAR"),
            ("polished_length", "BIGINT"),
            ("unpolished_length", "BIGINT"),
            ("circularity", "VARCHAR"),
            ("depth_identity_99", "DOUBLE"),
            ("depth_identity_99_75", "DOUBLE"),
            ("depth_identity_100", "DOUBLE"),
            ("median_depth_identity_100", "DOUBLE"),
            ("kmer_multiplicity", "DOUBLE"),
            ("kmer_multiplicity_flag", "VARCHAR"),
            ("secondary_to_contig_num", "BIGINT"),
            ("secondary_aligned_percent", "DOUBLE"),
            ("secondary_ani_percent", "DOUBLE"),
        ),
    ),
    "myloasm_layout": MyloasmTable(
        reader="read_myloasm_layout",
        columns=(
            *_RUN_KEY_COLUMNS,
            ("contig_num", "BIGINT"),
            ("layout_order", "BIGINT"),
            ("position", "BIGINT"),
            ("stop_position", "BIGINT"),
            ("sequence_idx", "BIGINT"),
            ("split_suffix", "VARCHAR"),
            ("piece_offset", "BIGINT"),
            ("piece_length", "BIGINT"),
            ("query_start", "BIGINT"),
            ("query_stop", "BIGINT"),
            ("strand", "VARCHAR"),
            ("read_depth_identity_99", "DOUBLE"),
            ("read_depth_identity_99_75", "DOUBLE"),
            ("read_depth_identity_100", "DOUBLE"),
            ("next_overlap_length", "BIGINT"),
            ("next_shared_snpmers", "BIGINT"),
            ("next_differing_snpmers", "BIGINT"),
        ),
    ),
    "myloasm_mapping": MyloasmTable(
        reader="read_myloasm_mappings",
        columns=(
            *_RUN_KEY_COLUMNS,
            ("sequence_idx", "BIGINT"),
            ("split_suffix", "VARCHAR"),
            ("piece_offset", "BIGINT"),
            ("piece_length", "BIGINT"),
            ("query_start", "BIGINT"),
            ("query_stop", "BIGINT"),
            ("strand", "VARCHAR"),
            ("contig_num", "BIGINT"),
            ("position", "BIGINT"),
            ("stop_position", "BIGINT"),
            ("query_anchor_span", "BIGINT"),
            ("contig_anchor_span", "BIGINT"),
            ("shared_minimizers", "BIGINT"),
            ("shared_snpmers", "BIGINT"),
            ("differing_snpmers", "BIGINT"),
        ),
    ),
    "myloasm_link": MyloasmTable(
        reader="read_myloasm_links",
        columns=(
            *_RUN_KEY_COLUMNS,
            ("from_contig_num", "BIGINT"),
            ("from_orientation", "VARCHAR"),
            ("to_contig_num", "BIGINT"),
            ("to_orientation", "VARCHAR"),
            ("overlap_length", "BIGINT"),
            ("shared_snpmers", "BIGINT"),
            ("differing_snpmers", "BIGINT"),
            ("shared_minimizers", "BIGINT"),
            ("snpmer_identity_percent", "DOUBLE"),
            ("from_sequence_idx", "BIGINT"),
            ("from_split_suffix", "VARCHAR"),
            ("to_sequence_idx", "BIGINT"),
            ("to_split_suffix", "VARCHAR"),
        ),
    ),
    "myloasm_split": MyloasmTable(
        reader="read_myloasm_splits",
        columns=(
            *_RUN_KEY_COLUMNS,
            ("sequence_idx", "BIGINT"),
            ("parent_split_suffix", "VARCHAR"),
            ("cleaning_round", "INTEGER"),
            ("breakpoint_order", "BIGINT"),
            ("query_start", "BIGINT"),
            ("query_stop", "BIGINT"),
            ("split_condition", "INTEGER"),
        ),
    ),
    MYLOASM_RUN_TABLE: MyloasmTable(
        reader="read_myloasm_run",
        columns=(
            *_RUN_KEY_COLUMNS,
            # The log's file name only: miint returns the path it read, which is a
            # job workspace path.
            ("log_name", "VARCHAR"),
            ("started_at", "TIMESTAMP"),
            ("myloasm_version", "VARCHAR"),
            ("command", "VARCHAR"),
            ("kmer_size", "INTEGER"),
            ("compression", "INTEGER"),
            ("hifi", "BOOLEAN"),
        ),
    ),
}


def myloasm_projection(table: str, sources: dict[str, str]) -> str:
    """The SELECT list for `table`'s staging Parquet: every lake column, in order, as
    `CAST(<sources[column]> AS <type>) AS <column>`.

    `sources` maps each column to the SQL that fills it and must name exactly the
    table's columns: a missing one would write a narrower Parquet than the lake table
    and a stray one would be dropped in silence, and both would surface only as a
    register-files rejection at the end of an assembly. Every column is CAST so an
    empty table's Parquet still carries the lake types.

    Each source is SQL pasted in as is, so it must be built from constants and
    integers, never from text a caller or a file supplied.
    """
    spec = MYLOASM_TABLES[table]
    if set(sources) != set(spec.column_names):
        raise KeyError(
            f"{table} needs exactly {sorted(spec.column_names)}; "
            f"missing {sorted(set(spec.column_names) - set(sources))}, "
            f"unexpected {sorted(set(sources) - set(spec.column_names))}"
        )
    return ", ".join(
        f"CAST({sources[name]} AS {sqltype}) AS {name}" for name, sqltype in spec.columns
    )
