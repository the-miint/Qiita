"""Native job: re-key hash_sequences' outputs to feature_idx, write the
six DuckLake-shape staging Parquets the data plane registers.

Reads the upstream Parquets (manifest from hash_sequences, feature_map
from mint-features) and emits the files `register-files` then hands to
the data plane's DoAction. The six staging outputs are:

  - `reference_sequences.parquet`        (feature_idx, sequence_hash, sequence_length_bp)
  - `reference_sequence_chunks/part_*.parquet` (feature_idx, chunk_index, chunk_data)
  - `reference_membership.parquet`       (reference_idx, feature_idx)
  - `reference_taxonomy.parquet`         (if taxonomy_path is set)
  - `reference_phylogeny.parquet`        (if tree_path is set)
  - `reference_placements.parquet`       (if jplace_path is set)

`reference_sequence_chunks` is a DIRECTORY of `part_*.parquet` files
rather than a single file — the chunks output is bin-pack-batched by
chunk count (same pattern as hash_sequences) so the per-batch sort
stays well under the DuckDB cap on GG2-scale inputs. The runner's
register-files convention treats a top-level subdir as a multi-file
DuckLake table whose name matches the directory.

Single-file output names match the DuckLake table names verbatim;
multi-file outputs use the directory name as the table name. Renames
on either side are cross-component contract breaks.

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
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_miint_conn,
    resolve_duckdb_memory_gb,
)
from ._feature_load import (
    build_feature_id_map,
    write_feature_sequence_chunks,
    write_feature_sequences,
)

YAML_STEP_NAME = "load"

# DuckDB resource caps for this step. `_DUCKDB_MEMORY_GB` is the OFF-SLURM
# fallback (local backend / tests), sized to the YAML baseline
# (workflows/reference-add/1.0.0.yaml: mem_gb=32 minus ~1 GB Python/miint/OS
# headroom). Under SLURM the limit instead tracks the real cgroup via
# `resolve_duckdb_memory_gb()` (SLURM_MEM_PER_NODE), so a `--mem-gb` override
# reaches DuckDB. DuckDB owns the whole box in this step (no in-process
# co-consumer), so it gets the allocation minus headroom.
_DUCKDB_MEMORY_GB = 31
_DUCKDB_THREADS = 8


class Inputs(BaseModel):
    """Typed input contract for reference_load.

    The first three fields are required outputs of the upstream pipeline
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
    # `reference_sequence_chunks` is a DIRECTORY of `part_*.parquet`
    # files. The runner's register-files convention treats top-level
    # subdirs as multi-file DuckLake tables (table name = subdir name).
    chunks_dir = workspace / "reference_sequence_chunks"
    membership_path = workspace / "reference_membership.parquet"
    taxonomy_out_path = workspace / "reference_taxonomy.parquet"
    phylogeny_out_path = workspace / "reference_phylogeny.parquet"
    placements_out_path = workspace / "reference_placements.parquet"

    sequences_out = validate_parquet_path(sequences_path)
    membership_out = validate_parquet_path(membership_path)
    taxonomy_out = validate_parquet_path(taxonomy_out_path)
    phylogeny_out = validate_parquet_path(phylogeny_out_path)
    placements_out = validate_parquet_path(placements_out_path)

    # miint is needed for `read_newick` and `read_jplace` when the
    # optional tree / jplace inputs are present. LOAD unconditionally even
    # when those inputs are absent — the extension is pre-staged, LOAD is
    # cheap, and it keeps the connection setup uniform.
    written: list[Path] = []
    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn,
                duckdb_tmp,
                memory_gb=resolve_duckdb_memory_gb(_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS),
                threads=_DUCKDB_THREADS,
            )

            # Pull feature_map into a TEMP TABLE once — every downstream
            # write JOINs against it (sequences, chunks, membership) and
            # build_feature_id_map needs it too. Without this each helper would
            # re-scan the file.
            conn.execute(
                "CREATE TEMP TABLE feature_map AS SELECT * FROM read_parquet(?)",
                [str(inputs.feature_map)],
            )

            # id_map: read_id → feature_idx (via sequence_hash). The
            # taxonomy / phylogeny / placements writes all key off
            # read_id, so this single JOIN is the bridge to feature_idx.
            # Counts also drive the unmapped-hash check below.
            build_feature_id_map(conn, inputs.manifest)

            write_feature_sequences(conn, sequences_out)
            written.append(sequences_path)

            write_feature_sequence_chunks(
                conn,
                inputs.reference_sequence_chunks,
                chunks_dir,
            )
            written.append(chunks_dir)

            _write_reference_membership(conn, inputs.reference_idx, membership_out)
            written.append(membership_path)

            if inputs.taxonomy_path is not None:
                _write_taxonomy(conn, inputs.taxonomy_path, inputs.reference_idx, taxonomy_out)
                written.append(taxonomy_out_path)

            if inputs.tree_path is not None:
                # The CLI's DoPut writes Newick / jplace as a chunked
                # `(chunk_index, chunk_data BLOB)` Parquet so the data
                # plane stays schema-agnostic and large blobs stream
                # under bounded memory. miint's `read_newick` /
                # `read_jplace` parse on-disk text/JSON files, so we
                # stitch chunks back into a temp file here.
                newick_path = _unwrap_chunks_to_temp_file(
                    conn,
                    parquet_path=inputs.tree_path,
                    out_path=duckdb_tmp / "tree.nwk",
                )
                _write_phylogeny(conn, newick_path, inputs.reference_idx, phylogeny_out)
                written.append(phylogeny_out_path)

            if inputs.jplace_path is not None:
                jplace_path = _unwrap_chunks_to_temp_file(
                    conn,
                    parquet_path=inputs.jplace_path,
                    out_path=duckdb_tmp / "placement.jplace",
                )
                _write_placements(conn, jplace_path, inputs.reference_idx, placements_out)
                written.append(placements_out_path)

            conn.execute("DROP TABLE id_map")
            conn.execute("DROP TABLE feature_map")
        success = True
    finally:
        if not success:
            for partial in (
                sequences_path,
                membership_path,
                taxonomy_out_path,
                phylogeny_out_path,
                placements_out_path,
            ):
                partial.unlink(missing_ok=True)
            shutil.rmtree(chunks_dir, ignore_errors=True)

    # `staging_dir` is the binding register-files consumes; the YAML's
    # `outputs: [staging_dir]` declaration matches it. The runner's
    # register-files convention picks up flat `*.parquet` files and
    # top-level subdirs of `part_*.parquet` (multi-file tables).
    #
    # `reference_sequence_chunks` re-exposes the feature-keyed chunks dir
    # (written by `write_feature_sequence_chunks` above) as its own binding.
    # The hash_sequences step's same-named binding is sequence_hash-keyed
    # (pre-minting); this is the feature_idx-keyed re-key, which is what rype
    # needs. host-reference-add's build_rype_index step consumes it — and must
    # run BEFORE register-files, which MOVES these part files into permanent
    # DuckLake storage (data-plane `move_file`). reference-add declares only
    # `staging_dir` as an output, so this extra key is inert there (the runner
    # binds only a step's declared outputs).
    return {"staging_dir": workspace, "reference_sequence_chunks": chunks_dir}


def _unwrap_chunks_to_temp_file(
    conn: duckdb.DuckDBPyConnection,
    *,
    parquet_path: Path,
    out_path: Path,
) -> Path:
    """Stitch a chunked-BLOB upload Parquet back into a temp file.

    Upload shape: `(chunk_index INTEGER, chunk_data BLOB)`. Writes
    `chunk_data` to `out_path` in `chunk_index` order, fetching rows in
    batches so we never materialise the whole BLOB in memory — important
    for jplace inputs that can run into the GB range."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cursor = conn.execute(
        "SELECT chunk_data FROM read_parquet(?) ORDER BY chunk_index",
        [str(parquet_path)],
    )
    with out_path.open("wb") as f:
        while True:
            rows = cursor.fetchmany(1024)
            if not rows:
                break
            for (chunk_data,) in rows:
                if chunk_data is None:
                    raise ValueError(f"{parquet_path} contains a NULL chunk_data")
                f.write(bytes(chunk_data))
    if out_path.stat().st_size == 0:
        raise ValueError(f"{parquet_path} produced an empty file — upload was malformed")
    return out_path


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
