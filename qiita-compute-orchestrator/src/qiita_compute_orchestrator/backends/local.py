"""Local compute backend — runs DuckDB+miint in-process for dev/test."""

import asyncio
import json
import os
import uuid
from pathlib import Path

import duckdb

from ..backend import ComputeBackend

# miint is installed once per process to avoid a network call on every hash job.
_miint_install_lock = asyncio.Lock()
_miint_installed = False

# MIINT_EXTENSION_PATH overrides the community extension with a local build.
# Required until the community release includes max_batch_bytes for read_fastx
# (streaming fix for large genomes). Unset to use the community extension.
_MIINT_EXT_PATH = os.environ.get("MIINT_EXTENSION_PATH")
_MIINT_USE_LOCAL = _MIINT_EXT_PATH is not None


async def _ensure_miint_installed() -> None:
    """Install miint once per process, concurrency-safe."""
    global _miint_installed
    if _miint_installed:
        return
    async with _miint_install_lock:
        if _miint_installed:
            return
        with _open_conn() as conn:
            if _MIINT_USE_LOCAL:
                conn.execute(f"FORCE INSTALL '{_MIINT_EXT_PATH}';")
            else:
                conn.execute("INSTALL miint FROM community;")
        _miint_installed = True


def _open_conn() -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection with unsigned extensions allowed if needed."""
    if _MIINT_USE_LOCAL:
        return duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    return duckdb.connect(":memory:")


def _md5_hex_to_uuid(hex_str: str) -> str:
    """Convert a 32-char hex MD5 to UUID string format."""
    return str(uuid.UUID(hex_str))


class LocalBackend(ComputeBackend):
    """Runs compute jobs in-process using DuckDB+miint. For dev/test only."""

    async def run_hash_job(self, fasta_path: Path, output_dir: Path, reference_idx: int) -> Path:
        if not fasta_path.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        await _ensure_miint_installed()

        with _open_conn() as conn:
            conn.execute("LOAD miint;")

            # Materialize FASTA into a temp table — single read of the file.
            # DuckDB raises on empty files — treat as zero sequences.
            try:
                conn.execute(
                    "CREATE TEMP TABLE raw_seqs AS "
                    "SELECT read_id, md5(sequence1) AS hash, length(sequence1) AS len "
                    "FROM read_fastx(?)",
                    [str(fasta_path)],
                )
            except duckdb.Error as exc:
                if "Empty file" in str(exc):
                    conn.execute(
                        "CREATE TEMP TABLE raw_seqs (read_id VARCHAR, hash VARCHAR, len BIGINT)"
                    )
                else:
                    raise

            # Reject duplicate read_ids — pure DuckDB, no second FASTA scan.
            dup_result = conn.execute(
                "SELECT read_id FROM raw_seqs GROUP BY read_id HAVING count(*) > 1 LIMIT 10"
            ).fetchall()
            if dup_result:
                dup_ids = sorted(row[0] for row in dup_result)
                raise ValueError(f"FASTA contains duplicate read_id(s): {dup_ids}")

            rows = conn.execute("SELECT read_id, hash, len FROM raw_seqs").fetchall()
            entry_count = len(rows)
            conn.execute("DROP TABLE raw_seqs")

        entries = [
            {
                "read_id": row[0],
                "sequence_hash": _md5_hex_to_uuid(row[1]),
                "length": row[2],
            }
            for row in rows
        ]

        manifest = {
            "reference_idx": reference_idx,
            "entry_count": entry_count,
            "entries": entries,
        }

        manifest_path = output_dir / "hash_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        return manifest_path

    async def run_load_job(
        self,
        manifest_path: Path,
        fasta_path: Path,
        feature_map_path: Path,
        output_dir: Path,
        reference_idx: int,
        *,
        taxonomy_path: Path | None = None,
        tree_path: Path | None = None,
        jplace_path: Path | None = None,
    ) -> Path:
        if not manifest_path.exists():
            raise FileNotFoundError(f"Manifest not found: {manifest_path}")
        if not fasta_path.exists():
            raise FileNotFoundError(f"FASTA file not found: {fasta_path}")
        if not feature_map_path.exists():
            raise FileNotFoundError(f"Feature map not found: {feature_map_path}")
        if jplace_path is not None and not jplace_path.exists():
            raise FileNotFoundError(f"jplace file not found: {jplace_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        await _ensure_miint_installed()

        with _open_conn() as conn:
            conn.execute("LOAD miint;")
            conn.execute("SET preserve_insertion_order=false;")
            conn.execute(f"SET temp_directory='{output_dir}/.duckdb_tmp';")

            _build_id_map(conn, manifest_path, feature_map_path)
            _write_sequence_metadata(conn, output_dir)
            _write_sequence_chunks(conn, fasta_path, output_dir)
            _write_membership(conn, output_dir, reference_idx)

            if taxonomy_path is not None:
                _write_taxonomy(conn, taxonomy_path, output_dir, reference_idx)

            if tree_path is not None:
                _write_phylogeny(conn, tree_path, output_dir, reference_idx)

            if jplace_path is not None:
                _write_placements(conn, jplace_path, output_dir, reference_idx)

            conn.execute("DROP TABLE id_map")

        return output_dir


_PARQUET_OPTS = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd'"
# 16384 rows × ~64 KB chunk_data ≈ 1 GB per row group. Smaller values flush
# more frequently, preventing OOM on genome-heavy references. Empirically
# tuned against GG2 backbone (21 MB max genome, 11 GB FASTA):
#   16384 → 4.2 GB peak RSS (OK), 32768 → OOM on 30 GB machine.
_CHUNK_ROW_GROUP_SIZE = 16384
_PARQUET_OPTS_CHUNKED = f"{_PARQUET_OPTS}, ROW_GROUP_SIZE {_CHUNK_ROW_GROUP_SIZE}"
_CHUNK_SIZE = 65536  # 64 KB


def _validate_parquet_path(path: Path) -> str:
    """Validate a path is safe for SQL string interpolation in COPY TO."""
    path_str = str(path)
    if "'" in path_str or "\\" in path_str or any(ord(c) < 0x20 for c in path_str):
        raise ValueError(f"Output path contains unsafe characters: {path_str}")
    return path_str


def _build_id_map(
    conn: duckdb.DuckDBPyConnection, manifest_path: Path, feature_map_path: Path
) -> None:
    """Build the id_map temp table by joining manifest + feature_map in DuckDB.

    Both files are read directly by DuckDB — no Python-side parsing.
    - manifest_path: JSON with {entries: [{read_id, sequence_hash, length}, ...]}
    - feature_map_path: NDJSON with {sequence_hash, feature_idx} per line

    Raises ValueError if any manifest entry has no matching feature_idx.
    """
    # entry_count is written by run_hash_job — avoids a separate count query.
    manifest_count = conn.execute(
        "SELECT entry_count FROM read_json(?, maximum_object_size=536870912)",
        [str(manifest_path)],
    ).fetchone()[0]

    conn.execute(
        "CREATE TEMP TABLE id_map AS "
        "SELECT e.read_id, f.feature_idx,"
        "  CAST(e.sequence_hash AS VARCHAR) AS sequence_hash,"
        "  e.length AS sequence_length_bp "
        "FROM ("
        "  SELECT unnest(entries) AS e FROM read_json(?, maximum_object_size=536870912)"
        ") "
        "JOIN read_json(?, format='newline_delimited',"
        "  columns={'sequence_hash': 'VARCHAR', 'feature_idx': 'BIGINT'}) f "
        "  ON e.sequence_hash = f.sequence_hash",
        [str(manifest_path), str(feature_map_path)],
    )

    id_map_count = conn.execute("SELECT count(*) FROM id_map").fetchone()[0]
    if id_map_count != manifest_count:
        n_unmapped = manifest_count - id_map_count
        # Diagnostic ANTI JOIN only on mismatch — the only case requiring
        # a second manifest read.
        unmapped = conn.execute(
            "SELECT e.sequence_hash FROM ("
            "  SELECT unnest(entries) AS e FROM read_json(?, maximum_object_size=536870912)"
            ") "
            "ANTI JOIN id_map m ON e.sequence_hash = m.sequence_hash",
            [str(manifest_path)],
        ).fetchall()
        hashes = [str(r[0]) for r in unmapped[:10]]
        raise ValueError(f"{n_unmapped} unmapped sequence hash(es) in feature_map: {hashes}")


def _write_sequence_metadata(conn: duckdb.DuckDBPyConnection, output_dir: Path) -> None:
    """Write reference_sequences.parquet — metadata from id_map (no FASTA read)."""
    out = _validate_parquet_path(output_dir / "reference_sequences.parquet")
    conn.execute(
        "COPY ("
        "  SELECT feature_idx,"
        "    CAST(sequence_hash AS UUID) AS sequence_hash,"
        "    CAST(sequence_length_bp AS BIGINT) AS sequence_length_bp"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({_PARQUET_OPTS})"
    )


def _write_sequence_chunks(
    conn: duckdb.DuckDBPyConnection, fasta_path: Path, output_dir: Path
) -> None:
    """Write reference_sequence_chunks.parquet — sequences chunked at 64 KB.

    Uses the list_transform + UNNEST macro pattern with ROW_GROUP_SIZE 16384
    to force row group flushing before memory pressure builds. Without a small
    ROW_GROUP_SIZE, DuckDB buffers too many rows and OOMs on large references.

    max_batch_bytes='512MB' caps read_fastx memory per batch — prevents it
    from buffering entire genomes (up to 21 MB each) before yielding rows.
    Without this, a single batch spanning many large sequences exhausts memory
    before the first row group is written.
    """
    out = _validate_parquet_path(output_dir / "reference_sequence_chunks.parquet")
    conn.execute(
        f"CREATE OR REPLACE MACRO chunk_seq(str) AS "
        f"list_transform("
        f"  range(1, CAST(length(str) + 1 AS BIGINT), {_CHUNK_SIZE}),"
        f"  lambda idx : {{"
        f"    'chunk_index': CAST((idx - 1) / {_CHUNK_SIZE} AS INTEGER),"
        f"    'chunk_data': substring(str, CAST(idx AS BIGINT), {_CHUNK_SIZE})"
        f"  }}"
        f")"
    )
    # max_batch_bytes caps read_fastx memory per batch — prevents buffering
    # entire genomes (up to 21 MB) before yielding rows. Only available in
    # local miint builds with the streaming fix; community release omits it.
    fastx_opts = ", max_batch_bytes='512MB'" if _MIINT_USE_LOCAL else ""
    conn.execute(
        "COPY ("
        "  WITH unnested AS ("
        "    SELECT m.feature_idx,"
        "      UNNEST(chunk_seq(f.sequence1)) AS chunk"
        f"    FROM read_fastx(?{fastx_opts}) f"
        "    JOIN id_map m ON f.read_id = m.read_id"
        "  )"
        "  SELECT feature_idx, chunk.chunk_index, chunk.chunk_data"
        "  FROM unnested"
        f") TO '{out}' ({_PARQUET_OPTS_CHUNKED})",
        [str(fasta_path)],
    )


def _write_membership(
    conn: duckdb.DuckDBPyConnection, output_dir: Path, reference_idx: int
) -> None:
    """Write reference_membership.parquet from id_map."""
    out = _validate_parquet_path(output_dir / "reference_membership.parquet")
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx, feature_idx"
        "  FROM id_map"
        "  ORDER BY feature_idx"
        f") TO '{out}' ({_PARQUET_OPTS})"
    )


def _write_taxonomy(
    conn: duckdb.DuckDBPyConnection,
    taxonomy_path: Path,
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_taxonomy.parquet from a Parquet input (feature_id, taxonomy).

    Rank extraction and validation done entirely in DuckDB.
    Partial coverage is allowed (not all features need taxonomy).
    """
    out = _validate_parquet_path(output_dir / "reference_taxonomy.parquet")

    # Read taxonomy Parquet, join with id_map, split ranks.
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

    # Validate: field count ≤ 8.
    bad = conn.execute(
        "SELECT feature_idx, nranks FROM parsed_taxonomy WHERE nranks > 8 LIMIT 5"
    ).fetchall()
    if bad:
        raise ValueError(f"Taxonomy has >8 semicolon-delimited fields: {bad}")

    # Validate: no blank fields.
    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE list_contains(ranks, '') LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy contains blank fields for feature_idx: {ids}")

    # Validate: prefix order.
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
        f") TO '{out}' ({_PARQUET_OPTS})"
    )

    conn.execute("DROP TABLE parsed_taxonomy")


def _write_phylogeny(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_phylogeny.parquet from Newick with feature_idx on tips.

    Tips with matching sequences get feature_idx populated; tips without
    sequences (and internal nodes) get NULL. No error on unmatched tips.
    """
    out = _validate_parquet_path(output_dir / "reference_phylogeny.parquet")

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
        f") TO '{out}' ({_PARQUET_OPTS})",
    )

    conn.execute("DROP TABLE tree_nodes")


def _write_placements(
    conn: duckdb.DuckDBPyConnection,
    jplace_path: Path,
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_placements.parquet from jplace.

    Maps placed fragments to feature_idx via id_map. Fragments not in
    id_map are skipped (they weren't hashed/minted).
    """
    out = _validate_parquet_path(output_dir / "reference_placements.parquet")

    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    m.feature_idx, j.edge_num,"
        "    j.likelihood, j.like_weight_ratio,"
        "    j.distal_length, j.pendant_length"
        "  FROM read_jplace(?) j"
        "  INNER JOIN id_map m ON j.fragment = m.read_id"
        "  ORDER BY m.feature_idx, j.edge_num"
        f") TO '{out}' ({_PARQUET_OPTS})",
        [str(jplace_path)],
    )
