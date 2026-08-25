"""Writing the published artifacts out: the table, and the two sidecars.

Every COPY target is a SQL string literal, which cannot be bound, so each writer
validates the path it interpolates (`validate_parquet_path`).
"""

from __future__ import annotations

from pathlib import Path

# `validate_parquet_path` is named for its first caller but checks a COPY *target* —
# a path safe to interpolate into a SQL string literal, which cannot be bound — so it
# is what every writer below uses, BIOM included.
from ..parquet import PARQUET_OPTS, validate_parquet_path
from ..taxonomy import QUOTED_RANK_COLUMNS
from .relations import LABELLED_RELATION, TAXONOMY_SIDECAR_RELATION, TREE_TABLE
from .sidecar import TaxonomyClearance, TreeClearance

# BIOM's `generated-by` attribute. The writer's own default is `miint`, which names
# the library rather than the system that produced the file. The system and no
# version: a version here would have to be kept honest against four
# components and a pinned extension build, and the bundle's manifest is where the
# provenance that reproduces a table actually lives — the reference, the cohort, the
# coverage scope and threshold, the gate, and the tool versions including the miint
# build. This attribute only says which system wrote the file.
BIOM_GENERATED_BY = "qiita-miint"


def parquet_copy_sql(path: Path) -> str:
    """COPY the relabelled table to Parquet, with the canonical options every qiita
    Parquet artifact shares.

    `PARQUET_OPTS`' `ROW_GROUP_SIZE_BYTES` requires `SET preserve_insertion_order =
    false` on the writing connection — DuckDB errors at bind time otherwise — which is
    the caller's to set (see `parquet.py`).
    """
    return f"COPY {LABELLED_RELATION} TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"


def biom_copy_sql(path: Path) -> str:
    """COPY the relabelled table to a BIOM 2.1 (HDF5) file.

    The writer requires exactly `LABELLED_SCHEMA` — `feature_id`/`sample_id` VARCHAR
    and `value` DOUBLE, looked up BY NAME — and **silently ignores any other column**,
    so it is `labelled_relation_sql`'s projection, not this writer, that keeps our
    identifiers out of the file. Behaviours it does enforce, and two it applies
    without asking, are recorded in `docs/duckdb-miint.md` and pinned by the
    control-plane's BIOM contract test; the one that shapes callers most is that it
    **refuses to overwrite an existing file**, unlike the Parquet COPY.

    `COMPRESSION` is passed explicitly even though gzip is also the writer's default,
    so a published artifact's encoding does not change under us if that default does.
    `ID` is left alone: the only distinctive handle for this table today is an
    internal identifier, which must not ride a published file.
    """
    return (
        f"COPY {LABELLED_RELATION} TO '{validate_parquet_path(path)}' "
        f"(FORMAT BIOM, COMPRESSION 'gzip', GENERATED_BY '{BIOM_GENERATED_BY}')"
    )


def taxonomy_copy_sql(path: Path, *, clearance: TaxonomyClearance) -> str:
    """COPY the sidecar to Parquet, with the options every qiita Parquet artifact
    shares. Parquet and not TSV: the ranks are eight nullable strings, and a TSV cannot
    tell an empty rank from an absent one without a convention every reader has to be
    told about.

    **Takes a `TaxonomyClearance`**, so it cannot be reached without having run
    `check_taxonomy_diagnostics`; see that dataclass.
    """
    _ = clearance
    # Quoted, because two of the eight ranks are SQL keywords. The Parquet column names
    # are the unquoted ones — `TAXONOMY_SIDECAR_COLUMNS` — since quoting is how the
    # identifier is written, not part of it.
    projection = ", ".join(("feature_id", *QUOTED_RANK_COLUMNS))
    return (
        f"COPY (SELECT {projection} FROM {TAXONOMY_SIDECAR_RELATION}) "
        f"TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"
    )


def tree_copy_sql(path: Path, *, clearance: TreeClearance) -> str:
    """COPY the sheared tree to Parquet, with the options every qiita Parquet artifact
    shares.

    **Parquet, not Newick.** We ship the node table and let a consumer that wants Newick
    convert it — which also keeps `COPY … (FORMAT NEWICK)`'s edge-id default from being
    ours to dodge (it annotates every branch whenever an `edge_id` column is present; see
    `docs/duckdb-miint.md`). `edge_id` is worth carrying: the shear preserves the
    surviving edge's original id, which is the handle back to the reference's placements.

    **A consumer joining this to the table must filter `is_tip`.** Only tips are named from
    the mint; a surviving internal node keeps the reference's own Newick label, and nothing
    makes those labels disjoint from published handles — so an unfiltered
    `name = feature_id` join can match an internal node. (The opposite direction is closed:
    an unpublished tip is nameless, see `shear_input_statements`.)

    **Takes a `TreeClearance`**, so it cannot be reached without the check; see that
    dataclass.
    """
    _ = clearance
    return f"COPY {TREE_TABLE} TO '{validate_parquet_path(path)}' ({PARQUET_OPTS})"
