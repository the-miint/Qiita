"""The gene-call contract: Prodigal's genes for each assembly subject, and the lake
schema they land in.

miint's `predict_genes` runs Prodigal through gpl-boundary
(https://the-miint.github.io/duckdb-miint/gene_prediction/). Each subject that
CheckM scores (a refined bin, a circular contig, an unbinned contig above the
residue cut; `assembly_constants`) is called once per call set, and each call set
is one way of running Prodigal:

- `prodigal_checkm`: CheckM 1.2.5's own rules, so CheckM can reuse the genes
  instead of calling its own. Single-genome mode for a subject of 100,000 bp or
  more and metagenomic mode below; in single mode, tables 4 and 11 with table 4
  kept only when its coding density beats 11's by more than 0.05 and exceeds 0.7;
  regions of N masked; a single-mode call that fails or finds no genes rerun in
  metagenomic mode.
- `prodigal_meta_closed`: metagenomic mode with closed ends (`prodigal -p meta
  -c`): no gene runs off a contig's edge.

Two tables. `gene_call` has one row per gene. `gene_call_contig` has one row per
contig of each called subject, genes or not: it carries the settings, the outcome
and Prodigal's model, which in metagenomic mode is chosen per contig, and it is
what numbers the contigs when Prodigal's own GFF is written back out
(`COPY ... (FORMAT GFF, DIALECT 'prodigal')`).

**Coordinates are on the contig's canonical strand.** A contig is stored once per
`feature_idx`, whose hash folds a sequence and its reverse complement together
(`chunking.canonical_sequence_hash_expr`), and a later registration's strand
replaces the stored bytes (`flight_service::registration`). So Prodigal's
coordinates, which are on the strand the gene-calling step read, are moved onto
the canonical strand (the one whose md5 is the hash) before they are stored:
`position`, `stop_position`, `strand`, and `partial_left` / `partial_right` (which
swap) all describe the gene on that strand. Whichever strand the lake holds, a
reader orients the contig canonically and cuts the gene from it.

Coordinates are 1-based half-open `[position, stop_position)`, as `predict_genes`
returns them and as `reference_annotation` stores them.

Neither component can import the other, so a test pins each column list here
against `qiita-data-plane/src/ducklake.rs`.
"""

from __future__ import annotations

GENE_CALL_TABLE = "gene_call"
GENE_CALL_CONTIG_TABLE = "gene_call_contig"

CALL_SET_CHECKM = "prodigal_checkm"
CALL_SET_META_CLOSED = "prodigal_meta_closed"
CALL_SETS = (CALL_SET_CHECKM, CALL_SET_META_CLOSED)

# Both tables are replaced per assembly run at registration, on the run keys that
# registration's `assembly_membership` files name, as `bin_quality` is. A run with no
# subject to call (no refined bin, nothing circular, no unbinned contig over the
# residue cut) writes both tables empty, and an empty file names no run, so its own
# keys would leave the previous run's rows in place. A load of these tables must
# register both files, empty or not, in the same registration as
# `assembly_membership`; registration refuses them without it.
GENE_CALL_RUN_KEY = ("prep_sample_idx", "processing_idx")

# The subject a row belongs to, as `bin_quality` names it, then the call set.
GENE_CALL_SUBJECT_KEY = ("prep_sample_idx", "processing_idx", "kind", "bin_id", "call_set")

_SUBJECT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("prep_sample_idx", "BIGINT"),
    ("processing_idx", "BIGINT"),
    ("kind", "VARCHAR"),
    ("bin_id", "VARCHAR"),
    ("call_set", "VARCHAR"),
)

# Prodigal's per-gene values kept in `gene_call.attributes`, under the names Prodigal's
# GFF gives them, as text. The values a reader filters on have columns of their own.
# Prodigal's `ID`, `partial`, `start_type`, `conf` and `score` are not here: `ID` is the
# contig's number and the gene's within it, both derived at export, and the others are
# columns.
GENE_CALL_ATTRIBUTE_KEYS = (
    "rbs_motif",
    "rbs_spacer",
    "gc_cont",
    "cscore",
    "sscore",
    "rscore",
    "uscore",
    "tscore",
)

# `predict_genes`' `status` values (the gene prediction docs linked above), as
# `gene_call_contig.status` records them. A subject whose training failed gives each
# of its contigs `training_failed`.
STATUS_OK = "ok"
GENE_CALL_STATUSES = (STATUS_OK, "no_genes", "training_failed")

# Both lists in DDL ORDER with DDL TYPES: the staging Parquet is registered into the
# table as it is, and one ordered or typed differently is a load error.
GENE_CALL_COLUMNS: tuple[tuple[str, str], ...] = (
    *_SUBJECT_COLUMNS,
    ("parent_feature_idx", "BIGINT"),
    ("feature_idx", "BIGINT"),
    ("position", "BIGINT"),
    ("stop_position", "BIGINT"),
    ("strand", "VARCHAR"),
    ("score", "DOUBLE"),
    ("partial_left", "BOOLEAN"),
    ("partial_right", "BOOLEAN"),
    ("start_type", "VARCHAR"),
    ("confidence", "DOUBLE"),
    ("attributes", "MAP(VARCHAR, VARCHAR)"),
    ("protein", "VARCHAR"),
)

GENE_CALL_CONTIG_COLUMNS: tuple[tuple[str, str], ...] = (
    *_SUBJECT_COLUMNS,
    ("parent_feature_idx", "BIGINT"),
    ("contig_order", "INTEGER"),
    ("meta_mode", "BOOLEAN"),
    ("closed_ends", "BOOLEAN"),
    ("mask_regions", "BOOLEAN"),
    ("status", "VARCHAR"),
    ("translation_table", "INTEGER"),
    ("uses_sd", "BOOLEAN"),
    ("meta_bin", "INTEGER"),
    ("model_desc", "VARCHAR"),
    ("model_gc_content", "DOUBLE"),
)

GENE_CALL_TABLES: dict[str, tuple[tuple[str, str], ...]] = {
    GENE_CALL_TABLE: GENE_CALL_COLUMNS,
    GENE_CALL_CONTIG_TABLE: GENE_CALL_CONTIG_COLUMNS,
}
