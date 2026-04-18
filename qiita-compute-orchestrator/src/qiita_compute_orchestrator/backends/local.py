"""Local compute backend — runs DuckDB+miint in-process for dev/test."""

import asyncio
import json
import uuid
from pathlib import Path

import duckdb

from ..backend import ComputeBackend, FeatureMap

# miint is installed once per process to avoid a network call on every hash job.
_miint_install_lock = asyncio.Lock()
_miint_installed = False


async def _ensure_miint_installed() -> None:
    """Install miint from the community registry once per process, concurrency-safe."""
    global _miint_installed
    if _miint_installed:
        return
    async with _miint_install_lock:
        if _miint_installed:
            return
        with duckdb.connect(":memory:") as conn:
            conn.execute("INSTALL miint FROM community;")
        _miint_installed = True


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

        with duckdb.connect(":memory:") as conn:
            conn.execute("LOAD miint;")
            # Parameterized query to avoid SQL injection via fasta_path.
            # DuckDB raises on empty files — treat as zero sequences.
            try:
                rows = conn.execute(
                    "SELECT read_id, md5(sequence1) AS hash, length(sequence1) AS len"
                    " FROM read_fastx(?)",
                    [str(fasta_path)],
                ).fetchall()
            except duckdb.Error as exc:
                if "Empty file" in str(exc):
                    rows = []
                else:
                    raise
            # TODO(phase-9): for large references (millions of sequences), replace
            # fetchall() with chunked iteration or DuckDB COPY TO JSON to avoid
            # loading the full result set into Python memory.

            # Reject duplicate read_ids — use DuckDB for O(n) efficiency.
            # The read_ids are already in the result set, so we query directly
            # from the same FASTA file to leverage DuckDB's grouping.
            if rows:
                try:
                    dup_result = conn.execute(
                        "SELECT read_id, count(*) AS cnt FROM read_fastx(?)"
                        " GROUP BY read_id HAVING count(*) > 1",
                        [str(fasta_path)],
                    ).fetchall()
                except duckdb.Error:
                    dup_result = []
                if dup_result:
                    dup_ids = sorted(row[0] for row in dup_result)
                    raise ValueError(
                        f"FASTA contains {len(dup_ids)} duplicate read_id(s): {dup_ids[:10]}"
                    )

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
            "entries": entries,
        }

        manifest_path = output_dir / "hash_manifest.json"
        manifest_path.write_text(json.dumps(manifest, indent=2))
        return manifest_path

    async def run_load_job(
        self,
        manifest_path: Path,
        fasta_path: Path,
        feature_map: FeatureMap,
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
        if jplace_path is not None and not jplace_path.exists():
            raise FileNotFoundError(f"jplace file not found: {jplace_path}")

        output_dir.mkdir(parents=True, exist_ok=True)
        await _ensure_miint_installed()

        # Build read_id → (feature_idx, sequence_hash) from manifest + feature_map.
        manifest = json.loads(manifest_path.read_text())
        unmapped: list[str] = []
        read_id_map: dict[str, tuple[int, str]] = {}
        for entry in manifest["entries"]:
            hash_uuid = uuid.UUID(entry["sequence_hash"])
            if hash_uuid not in feature_map:
                unmapped.append(str(hash_uuid))
            else:
                read_id_map[entry["read_id"]] = (
                    feature_map[hash_uuid],
                    entry["sequence_hash"],
                )
        if unmapped:
            raise ValueError(
                f"{len(unmapped)} unmapped sequence hash(es) in feature_map: {unmapped[:10]}"
            )

        with duckdb.connect(":memory:") as conn:
            conn.execute("LOAD miint;")
            # Required for COPY ... ORDER BY to honour ROW_GROUP_SIZE.
            conn.execute("SET preserve_insertion_order=false;")

            _load_id_map(conn, read_id_map)
            _write_sequence_metadata(conn, fasta_path, output_dir)
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
_CHUNK_SIZE = 65536  # 64 KB


def _validate_parquet_path(path: Path) -> str:
    """Validate a path is safe for SQL string interpolation in COPY TO."""
    path_str = str(path)
    if "'" in path_str or "\\" in path_str or any(ord(c) < 0x20 for c in path_str):
        raise ValueError(f"Output path contains unsafe characters: {path_str}")
    return path_str


def _load_id_map(conn: duckdb.DuckDBPyConnection, read_id_map: dict[str, tuple[int, str]]) -> None:
    """Load the read_id → (feature_idx, sequence_hash) mapping into a temp table.

    Shared across all write functions — created once, dropped at the end.
    """
    conn.execute(
        "CREATE TEMP TABLE id_map (  read_id VARCHAR, feature_idx BIGINT, sequence_hash VARCHAR)"
    )
    conn.executemany(
        "INSERT INTO id_map VALUES (?, ?, ?)",
        [(rid, fidx, shash) for rid, (fidx, shash) in read_id_map.items()],
    )


def _write_sequence_metadata(
    conn: duckdb.DuckDBPyConnection, fasta_path: Path, output_dir: Path
) -> None:
    """Write reference_sequences.parquet — metadata only (hash + length)."""
    out = _validate_parquet_path(output_dir / "reference_sequences.parquet")
    conn.execute(
        "COPY ("
        "  SELECT m.feature_idx,"
        "    CAST(m.sequence_hash AS UUID) AS sequence_hash,"
        "    CAST(length(f.sequence1) AS BIGINT) AS sequence_length_bp"
        "  FROM read_fastx(?) f"
        "  JOIN id_map m ON f.read_id = m.read_id"
        "  ORDER BY m.feature_idx"
        f") TO '{out}' ({_PARQUET_OPTS})",
        [str(fasta_path)],
    )


def _write_sequence_chunks(
    conn: duckdb.DuckDBPyConnection, fasta_path: Path, output_dir: Path
) -> None:
    """Write reference_sequence_chunks.parquet — sequences chunked at 64 KB."""
    out = _validate_parquet_path(output_dir / "reference_sequence_chunks.parquet")
    conn.execute(
        "COPY ("
        "  WITH chunked AS ("
        "    SELECT m.feature_idx, f.sequence1"
        "    FROM read_fastx(?) f"
        "    JOIN id_map m ON f.read_id = m.read_id"
        "  )"
        "  SELECT feature_idx,"
        f"    CAST((idx - 1) / {_CHUNK_SIZE} AS INTEGER) AS chunk_index,"
        f"    substring(sequence1, CAST(idx AS BIGINT), {_CHUNK_SIZE}) AS chunk_data"
        "  FROM chunked,"
        f"    range(1, CAST(length(sequence1) + 1 AS BIGINT), {_CHUNK_SIZE}) AS t(idx)"
        "  ORDER BY feature_idx, chunk_index"
        f") TO '{out}' ({_PARQUET_OPTS})",
        [str(fasta_path)],
    )


def _write_membership(
    conn: duckdb.DuckDBPyConnection, output_dir: Path, reference_idx: int
) -> None:
    """Write reference_membership.parquet from the id_map."""
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
