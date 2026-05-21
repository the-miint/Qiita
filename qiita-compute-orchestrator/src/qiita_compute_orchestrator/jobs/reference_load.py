"""Native job: re-key hash_sequences' outputs to feature_idx, write the
six DuckLake-shape staging Parquets the data plane registers.

Reads the upstream Parquets (manifest from hash_sequences, feature_map
from mint-features) and emits the files `register-files` then hands to
the data plane's DoAction. The six staging files are:

  - `reference_sequences.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `reference_sequence_chunks.parquet`  (feature_idx, chunk_index, chunk_data)
  - `reference_membership.parquet`       (reference_idx, feature_idx)
  - `reference_taxonomy.parquet`         (if taxonomy_path is set)
  - `reference_phylogeny.parquet`        (if tree_path is set)
  - `reference_placements.parquet`       (if jplace_path is set)

Naming matches the DuckLake table names verbatim — `register-files`
derives `staging_dir/<table>.parquet` from this convention, so a rename
here is a cross-component contract break.

**Architectural call.** `hash_sequences` writes its intermediates keyed
on `sequence_hash` (it has no feature_idx yet). DuckLake's
`reference_sequences` and `reference_sequence_chunks` carry
`feature_idx`. This module performs the hash→feature_idx re-key by
joining hash_sequences' outputs with mint-features' `feature_map.parquet`
(sequence_hash → feature_idx). The alternative — pinning DuckLake to
hash-keyed sequence tables — would force every query-time consumer to
JOIN through `reference_membership` for `feature_idx`, which is the
lake-wide identifier.

**Optional inputs.** Taxonomy / tree / jplace each fan out into its own
write only when the corresponding `*_path` flows through `bound` from
the work_ticket's `action_context`. The runner injects them under
`taxonomy_path` / `tree_path` / `jplace_path` after upload-handle
resolution; absent uploads → absent paths → absent outputs.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.parquet import validate_parquet_path

from ..miint import (
    PARQUET_OPTS,
    PARQUET_OPTS_CHUNKED,
    ensure_miint_installed,
    open_conn,
)

YAML_STEP_NAME = "load"

_DUCKDB_MAX_MEMORY_GB = 7
_DUCKDB_MAX_THREADS = 2


class Inputs(BaseModel):
    """Typed input contract for reference_load.

    The first four fields are required outputs of the upstream pipeline
    (hash_sequences → mint-features) and carry bare names matching what
    those steps emit and what the YAML's `inputs:` list declares.

    `taxonomy_path` / `tree_path` / `jplace_path` carry the `_path`
    suffix because the runner injects them under that form when the
    work_ticket's `action_context` carries the matching `*_upload_idx`.

    `reference_idx` is framework-injected (REFERENCE-scoped ticket) and
    load-bearing: it's the `reference_membership` row's left-hand idx
    and the per-reference scoping column on every other staging file.
    `work_ticket_idx` flows through for parity; this step doesn't read
    it.
    """

    manifest: Path
    feature_map: Path
    reference_sequence: Path
    reference_sequence_chunks: Path
    taxonomy_path: Path | None = None
    tree_path: Path | None = None
    jplace_path: Path | None = None
    reference_idx: int
    work_ticket_idx: int


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    for label, path in [
        ("manifest", inputs.manifest),
        ("feature_map", inputs.feature_map),
        ("reference_sequence", inputs.reference_sequence),
        ("reference_sequence_chunks", inputs.reference_sequence_chunks),
    ]:
        if not path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")
    for label, opt in [
        ("taxonomy", inputs.taxonomy_path),
        ("tree", inputs.tree_path),
        ("jplace", inputs.jplace_path),
    ]:
        if opt is not None and not opt.exists():
            raise FileNotFoundError(f"{label} not found: {opt}")

    workspace.mkdir(parents=True, exist_ok=True)
    sequences_path = workspace / "reference_sequences.parquet"
    chunks_path = workspace / "reference_sequence_chunks.parquet"
    membership_path = workspace / "reference_membership.parquet"
    taxonomy_out_path = workspace / "reference_taxonomy.parquet"
    phylogeny_out_path = workspace / "reference_phylogeny.parquet"
    placements_out_path = workspace / "reference_placements.parquet"

    sequences_out = validate_parquet_path(sequences_path)
    chunks_out = validate_parquet_path(chunks_path)
    membership_out = validate_parquet_path(membership_path)
    taxonomy_out = validate_parquet_path(taxonomy_out_path)
    phylogeny_out = validate_parquet_path(phylogeny_out_path)
    placements_out = validate_parquet_path(placements_out_path)
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    # miint is needed for `read_newick` and `read_jplace` when the
    # optional tree / jplace inputs are present. Install eagerly even
    # when those inputs are absent — the install is a no-op after the
    # first call per process and keeps the connection setup uniform.
    await ensure_miint_installed()

    written: list[Path] = []
    success = False
    try:
        with open_conn() as conn:
            conn.execute("LOAD miint;")
            conn.execute(f"SET memory_limit='{_DUCKDB_MAX_MEMORY_GB}GB'")
            conn.execute(f"SET threads={_DUCKDB_MAX_THREADS}")
            conn.execute("SET preserve_insertion_order=false")
            conn.execute(f"SET temp_directory='{duckdb_tmp}'")

            # Pull feature_map into a TEMP TABLE once — every downstream
            # write JOINs against it (sequences, chunks, membership) and
            # _build_id_map needs it too. Without this each helper would
            # re-scan the file.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )

            # id_map: read_id → feature_idx (via sequence_hash). The
            # taxonomy / phylogeny / placements writes all key off
            # read_id, so this single JOIN is the bridge to feature_idx.
            # Counts also drive the unmapped-hash check below.
            _build_id_map(conn, inputs.manifest)

            _write_reference_sequences(
                conn,
                inputs.reference_sequence,
                sequences_out,
            )
            written.append(sequences_path)

            _write_reference_sequence_chunks(
                conn,
                inputs.reference_sequence_chunks,
                chunks_out,
            )
            written.append(chunks_path)

            _write_reference_membership(conn, inputs.reference_idx, membership_out)
            written.append(membership_path)

            if inputs.taxonomy_path is not None:
                _write_taxonomy(conn, inputs.taxonomy_path, inputs.reference_idx, taxonomy_out)
                written.append(taxonomy_out_path)

            if inputs.tree_path is not None:
                # The CLI's DoPut writes Newick input as a single-row
                # Parquet `(newick_bytes BLOB)` so the data plane stays
                # schema-agnostic (Parquet-in, Parquet-out). miint's
                # `read_newick` parses a Newick TEXT file, not a Parquet,
                # so unwrap the BLOB to a temp `.nwk` here before reading.
                newick_path = _unwrap_blob_to_temp_file(
                    conn,
                    parquet_path=inputs.tree_path,
                    column_name="newick_bytes",
                    out_path=duckdb_tmp / "tree.nwk",
                )
                _write_phylogeny(conn, newick_path, inputs.reference_idx, phylogeny_out)
                written.append(phylogeny_out_path)

            if inputs.jplace_path is not None:
                # Same shape as the Newick branch — DoPut wrapped the
                # jplace JSON as a single-row Parquet `(jplace_bytes
                # BLOB)`; miint's `read_jplace` needs a JSON file.
                jplace_path = _unwrap_blob_to_temp_file(
                    conn,
                    parquet_path=inputs.jplace_path,
                    column_name="jplace_bytes",
                    out_path=duckdb_tmp / "placement.jplace",
                )
                _write_placements(conn, jplace_path, inputs.reference_idx, placements_out)
                written.append(placements_out_path)

            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        if not success:
            for partial in (
                sequences_path,
                chunks_path,
                membership_path,
                taxonomy_out_path,
                phylogeny_out_path,
                placements_out_path,
            ):
                partial.unlink(missing_ok=True)

    # Return a single binding pointing at the staging dir; the YAML's
    # `outputs: [staging_dir]` declaration matches this. register-files
    # globs *.parquet inside.
    return {"staging_dir": workspace}


def _unwrap_blob_to_temp_file(
    conn: duckdb.DuckDBPyConnection,
    *,
    parquet_path: Path,
    column_name: str,
    out_path: Path,
) -> Path:
    """Extract a single-row BLOB column from `parquet_path` and write its
    bytes to `out_path`. Bridges the CLI's DoPut wire shape (Newick /
    jplace wrapped as `(<name>_bytes BLOB)` so the data plane stays
    schema-agnostic) to miint's `read_newick` / `read_jplace`, which
    parse text/JSON files on disk.

    Reads through DuckDB so the schema check (column exists, exactly one
    row) surfaces as a clean error rather than a pyarrow `KeyError`.
    Writes the bytes verbatim — no decoding, no newline normalization."""
    rows = conn.execute(
        f"SELECT {column_name} FROM read_parquet(?)",
        [str(parquet_path)],
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"expected exactly one row in {parquet_path} carrying {column_name!r}, got {len(rows)}"
        )
    payload = rows[0][0]
    if payload is None:
        raise ValueError(f"{parquet_path} row 0 has NULL {column_name!r} — upload was malformed")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(bytes(payload))
    return out_path


def _build_id_map(
    conn: duckdb.DuckDBPyConnection,
    manifest_path: Path,
) -> None:
    """Join manifest + feature_map (TEMP TABLE pre-loaded by execute) on
    sequence_hash. Raises ValueError if any manifest row lacks a matching
    feature_map row — mint-features is supposed to mint a feature_idx for
    every distinct hash, so a gap means upstream produced inconsistent
    inputs (permanent error)."""
    manifest_count = conn.execute(
        "SELECT count(*) FROM read_parquet(?)",
        [str(manifest_path)],
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TABLE id_map AS "
        "SELECT m.read_id, fm.feature_idx,"
        "  m.sequence_hash,"
        "  m.sequence_length_bp "
        "FROM read_parquet(?) m "
        "JOIN feature_map fm "
        "  ON m.sequence_hash = fm.sequence_hash",
        [str(manifest_path)],
    )

    id_map_count = conn.execute("SELECT count(*) FROM id_map").fetchone()[0]
    if id_map_count != manifest_count:
        n_unmapped = manifest_count - id_map_count
        unmapped = conn.execute(
            "SELECT m.sequence_hash FROM read_parquet(?) m "
            "ANTI JOIN id_map x ON m.sequence_hash = x.sequence_hash "
            "LIMIT 10",
            [str(manifest_path)],
        ).fetchall()
        hashes = [str(r[0]) for r in unmapped]
        raise ValueError(f"{n_unmapped} unmapped sequence hash(es) in feature_map: {hashes}")


def _write_reference_sequences(
    conn: duckdb.DuckDBPyConnection,
    reference_sequence_path: Path,
    out: str,
) -> None:
    """Re-key hash_sequences' `reference_sequence.parquet` (hash-keyed)
    to DuckLake's `reference_sequences` schema (feature_idx-keyed) by
    JOINing on sequence_hash. Sorted by feature_idx for row-group pruning."""
    conn.execute(
        "COPY ("
        "  SELECT "
        "    fm.feature_idx,"
        "    rs.sequence_hash,"
        "    rs.sequence_length_bp"
        "  FROM read_parquet(?) rs"
        "  JOIN feature_map fm ON rs.sequence_hash = fm.sequence_hash"
        "  ORDER BY fm.feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(reference_sequence_path)],
    )


def _write_reference_sequence_chunks(
    conn: duckdb.DuckDBPyConnection,
    reference_sequence_chunks_path: Path,
    out: str,
) -> None:
    """Re-key hash_sequences' chunks (hash-keyed) to DuckLake's
    `reference_sequence_chunks` schema (feature_idx-keyed). Same
    ROW_GROUP_SIZE tuning as hash_sequences — the chunked write was
    sized to keep peak RSS under control on a 11 GB GG2 backbone."""
    conn.execute(
        "COPY ("
        "  SELECT "
        "    fm.feature_idx,"
        "    rc.chunk_index,"
        "    rc.chunk_data"
        "  FROM read_parquet(?) rc"
        "  JOIN feature_map fm ON rc.sequence_hash = fm.sequence_hash"
        "  ORDER BY fm.feature_idx, rc.chunk_index"
        f") TO '{out}' ({PARQUET_OPTS_CHUNKED})",
        [str(reference_sequence_chunks_path)],
    )


def _write_reference_membership(
    conn: duckdb.DuckDBPyConnection,
    reference_idx: int,
    out: str,
) -> None:
    """One row per (reference_idx, feature_idx) — the DuckLake-side
    membership table. Postgres has its own `qiita.reference_membership`
    populated by the `write-membership` LIBRARY primitive; both tables
    hold the same rows, the Postgres one for transactional queries and
    this one for the lake."""
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx, feature_idx"
        "  FROM feature_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )


def _write_taxonomy(
    conn: duckdb.DuckDBPyConnection,
    taxonomy_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse semicolon-delimited rank string from the input Parquet's
    `(feature_id, taxonomy)` rows, JOIN against id_map on read_id, and
    emit one DuckLake row per matched feature. Validation: ≤8 ranks, no
    blank fields, prefix order."""
    conn.execute(
        "CREATE TEMP TABLE parsed_taxonomy AS "
        "SELECT "
        "  m.feature_idx,"
        "  string_split_regex(t.taxonomy, ';\\s*') AS ranks,"
        "  len(string_split_regex(t.taxonomy, ';\\s*')) AS nranks "
        "FROM read_parquet(?) t "
        "INNER JOIN id_map m ON t.feature_id = m.read_id",
        [str(taxonomy_path)],
    )

    bad = conn.execute(
        "SELECT feature_idx, nranks FROM parsed_taxonomy WHERE nranks > 8 LIMIT 5"
    ).fetchall()
    if bad:
        raise ValueError(f"Taxonomy has >8 semicolon-delimited fields: {bad}")

    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE list_contains(ranks, '') LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy contains blank fields for feature_idx: {ids}")

    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE "
        "(nranks >= 1 AND NOT starts_with(ranks[1], 'd__')) OR "
        "(nranks >= 2 AND NOT starts_with(ranks[2], 'p__')) OR "
        "(nranks >= 3 AND NOT starts_with(ranks[3], 'c__')) OR "
        "(nranks >= 4 AND NOT starts_with(ranks[4], 'o__')) OR "
        "(nranks >= 5 AND NOT starts_with(ranks[5], 'f__')) OR "
        "(nranks >= 6 AND NOT starts_with(ranks[6], 'g__')) OR "
        "(nranks >= 7 AND NOT starts_with(ranks[7], 's__')) OR "
        "(nranks >= 8 AND NOT starts_with(ranks[8], 't__')) "
        "LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy has wrong rank prefix order for feature_idx: {ids}")

    conn.execute(
        "COPY ("
        "  SELECT "
        f"    CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    feature_idx,"
        "    NULLIF(substr(ranks[1], 4), '') AS domain,"
        "    NULLIF(substr(ranks[2], 4), '') AS phylum,"
        "    NULLIF(substr(ranks[3], 4), '') AS class,"
        "    NULLIF(substr(ranks[4], 4), '') AS \"order\","
        "    NULLIF(substr(ranks[5], 4), '') AS family,"
        "    NULLIF(substr(ranks[6], 4), '') AS genus,"
        "    NULLIF(substr(ranks[7], 4), '') AS species,"
        "    NULLIF(substr(ranks[8], 4), '') AS strain,"
        "    NULL::BIGINT AS ncbi_taxon_id"
        "  FROM parsed_taxonomy"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({PARQUET_OPTS})"
    )
    conn.execute("DROP TABLE parsed_taxonomy")


def _write_phylogeny(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse a Newick tree and emit one DuckLake row per node, with
    feature_idx populated on tips that match a known read_id."""
    conn.execute(
        "CREATE TEMP TABLE tree_nodes AS SELECT * FROM read_newick(?)",
        [str(tree_path)],
    )

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    t.node_index, t.name, t.branch_length, t.edge_id,"
        "    t.parent_index, t.is_tip, m.feature_idx"
        "  FROM tree_nodes t"
        "  LEFT JOIN id_map m ON t.is_tip AND t.name = m.read_id"
        "  ORDER BY t.node_index"
        f") TO '{out}' ({PARQUET_OPTS})",
    )
    conn.execute("DROP TABLE tree_nodes")


def _write_placements(
    conn: duckdb.DuckDBPyConnection,
    jplace_path: Path,
    reference_idx: int,
    out: str,
) -> None:
    """Parse a jplace file and emit one row per (fragment, edge_num).
    Fragments not in id_map are dropped (jplace may carry rows for
    fragments outside this reference's mint scope)."""
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    m.feature_idx, j.edge_num,"
        "    j.likelihood, j.like_weight_ratio,"
        "    j.distal_length, j.pendant_length"
        "  FROM read_jplace(?) j"
        "  INNER JOIN id_map m ON j.fragment = m.read_id"
        "  ORDER BY m.feature_idx, j.edge_num"
        f") TO '{out}' ({PARQUET_OPTS})",
        [str(jplace_path)],
    )
