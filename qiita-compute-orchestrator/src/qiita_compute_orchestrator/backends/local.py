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
        """Load sequences, taxonomy, phylogeny into Parquet files.

        jplace_path is not consumed directly by this method, but its presence
        signals that phylogenetic placement tips exist. When set, unmatched
        tree tips (not in manifest) are expected and excluded from
        tip_features.json rather than raising an error. The path is validated
        for existence to catch typos early.
        """
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

        # Single connection for all write operations; miint loaded once.
        with duckdb.connect(":memory:") as conn:
            conn.execute("LOAD miint;")

            _write_sequences_parquet(conn, fasta_path, read_id_map, output_dir)

            if taxonomy_path is not None:
                _write_taxonomy_parquet(conn, taxonomy_path, read_id_map, output_dir, reference_idx)

            if tree_path is not None:
                _write_phylogeny_parquet(
                    conn,
                    tree_path,
                    read_id_map,
                    output_dir,
                    reference_idx,
                    has_placements=jplace_path is not None,
                )

        return output_dir


# GG2 rank prefixes in positional order. Includes t__ (strain) used by some
# references (e.g., genome-resolved databases).
_RANK_PREFIXES = ("d__", "p__", "c__", "o__", "f__", "g__", "s__", "t__")


def _validate_parquet_path(path: Path) -> str:
    """Validate a Parquet output path is safe for SQL interpolation.

    DuckDB COPY TO does not support parameterized destination paths,
    so we must validate before interpolation. Rejects single quotes,
    backslashes, and control characters (which are legal in Linux
    filenames but break or confuse SQL string literals).
    """
    path_str = str(path)
    if "'" in path_str or "\\" in path_str or any(ord(c) < 0x20 for c in path_str):
        raise ValueError(f"Output path contains unsafe characters: {path_str}")
    return path_str


def _parse_taxonomy(taxon_string: str) -> list[str | None]:
    """Parse a semicolon-separated taxonomy into 8 rank values (d__ through t__).

    Empty values after the prefix (e.g., "f__") become None. Fewer than 8
    fields is acceptable (trailing ranks become None). Raises ValueError on
    wrong prefix order, blank fields without a prefix, or more than 8 fields.
    """
    parts = [p.strip() for p in taxon_string.split(";")]
    if len(parts) > len(_RANK_PREFIXES):
        raise ValueError(
            f"Taxonomy string has {len(parts)} fields, expected at most "
            f"{len(_RANK_PREFIXES)}: {taxon_string!r}"
        )
    result: list[str | None] = []
    for i, prefix in enumerate(_RANK_PREFIXES):
        if i < len(parts):
            val = parts[i]
            if not val:
                raise ValueError(
                    f"Rank {i} is blank (expected prefix {prefix!r}) "
                    f"in taxonomy string: {taxon_string!r}"
                )
            if val.startswith(prefix):
                val = val[len(prefix) :]
            else:
                raise ValueError(
                    f"Rank {i} expected prefix {prefix!r}, got: {parts[i]!r}"
                    f" in taxonomy string: {taxon_string!r}"
                )
            result.append(val if val else None)
        else:
            result.append(None)
    return result


_TAXONOMY_HEADER_PREFIX = "Feature ID"
_PARQUET_OPTS = "FORMAT PARQUET, PARQUET_VERSION 'v2', COMPRESSION 'zstd'"


def _write_sequences_parquet(
    conn: duckdb.DuckDBPyConnection,
    fasta_path: Path,
    read_id_map: dict[str, tuple[int, str]],
    output_dir: Path,
) -> None:
    """Write reference_sequences.parquet from FASTA + feature map."""
    out_str = _validate_parquet_path(output_dir / "reference_sequences.parquet")

    conn.execute(
        "CREATE TEMP TABLE seq_id_map ("
        "  read_id VARCHAR, feature_idx BIGINT, sequence_hash VARCHAR"
        ")"
    )
    conn.executemany(
        "INSERT INTO seq_id_map VALUES (?, ?, ?)",
        [(rid, fidx, shash) for rid, (fidx, shash) in read_id_map.items()],
    )
    conn.execute(
        "COPY ("
        "  SELECT"
        "    m.feature_idx,"
        "    f.sequence1 AS sequence,"
        "    CAST(m.sequence_hash AS UUID) AS sequence_hash,"
        "    CAST(length(f.sequence1) AS BIGINT) AS sequence_length_bp"
        "  FROM read_fastx(?) f"
        "  JOIN seq_id_map m ON f.read_id = m.read_id"
        "  ORDER BY m.feature_idx"
        f") TO '{out_str}' ({_PARQUET_OPTS})",
        [str(fasta_path)],
    )

    # Verify row count: FASTA must match manifest exactly.
    written = conn.execute(f"SELECT count(*) FROM '{out_str}'").fetchone()[0]
    if written != len(read_id_map):
        raise ValueError(
            f"Sequences Parquet has {written} rows but manifest has "
            f"{len(read_id_map)} entries — FASTA may have been modified after hashing"
        )

    conn.execute("DROP TABLE seq_id_map")


def _write_taxonomy_parquet(
    conn: duckdb.DuckDBPyConnection,
    taxonomy_path: Path,
    read_id_map: dict[str, tuple[int, str]],
    output_dir: Path,
    reference_idx: int,
) -> None:
    """Write reference_taxonomy.parquet from a GG2-style two-column TSV.

    All transformation (TSV read, string split, rank extraction) is done in
    DuckDB. Validates header, unknown IDs, field count, blank fields, and
    prefix order before writing. Partial coverage is allowed.
    """
    out_str = _validate_parquet_path(output_dir / "reference_taxonomy.parquet")

    # Validate header — one-line Python check for a clear error message.
    with open(taxonomy_path) as f:
        header = next(f).strip()
        if not header.startswith(_TAXONOMY_HEADER_PREFIX):
            raise ValueError(
                f"Unexpected taxonomy header (expected '{_TAXONOMY_HEADER_PREFIX}...'): {header!r}"
            )

    # Load id_map for joins.
    conn.execute("CREATE TEMP TABLE tax_id_map (read_id VARCHAR, feature_idx BIGINT)")
    conn.executemany(
        "INSERT INTO tax_id_map VALUES (?, ?)",
        [(rid, fidx) for rid, (fidx, _) in read_id_map.items()],
    )

    # Read TSV into DuckDB — columns param gives clean names and skips header.
    conn.execute(
        "CREATE TEMP TABLE raw_taxonomy AS "
        "SELECT * FROM read_csv(?, delim='\\t', header=true, "
        "columns={'read_id': 'VARCHAR', 'taxon': 'VARCHAR'})",
        [str(taxonomy_path)],
    )

    # Validate: unknown IDs (taxonomy entries not in manifest).
    unknown = conn.execute(
        "SELECT read_id FROM raw_taxonomy WHERE read_id NOT IN (SELECT read_id FROM tax_id_map)"
    ).fetchall()
    if unknown:
        ids = [r[0] for r in unknown[:10]]
        raise ValueError(f"Taxonomy file contains {len(unknown)} entry(ies) not in manifest: {ids}")

    # Parse: inner join (only known IDs), split ranks in DuckDB.
    conn.execute(
        "CREATE TEMP TABLE parsed_taxonomy AS "
        "SELECT "
        "  m.feature_idx, "
        "  string_split_regex(r.taxon, ';\\s*') AS ranks, "
        "  len(string_split_regex(r.taxon, ';\\s*')) AS nranks "
        "FROM raw_taxonomy r "
        "INNER JOIN tax_id_map m ON r.read_id = m.read_id"
    )

    # Validate: field count ≤ 8.
    bad = conn.execute(
        "SELECT feature_idx, nranks FROM parsed_taxonomy WHERE nranks > 8 LIMIT 5"
    ).fetchall()
    if bad:
        raise ValueError(f"Taxonomy has >8 semicolon-delimited fields: {bad}")

    # Validate: no blank fields (empty string after split = missing prefix).
    bad = conn.execute(
        "SELECT feature_idx FROM parsed_taxonomy WHERE list_contains(ranks, '') LIMIT 5"
    ).fetchall()
    if bad:
        ids = [r[0] for r in bad]
        raise ValueError(f"Taxonomy contains blank fields for feature_idx: {ids}")

    # Validate: prefix order (d__, p__, c__, o__, f__, g__, s__, t__).
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

    # Extract ranks and write — substr(ranks[i], 4) strips the 3-char prefix,
    # NULLIF converts empty string (e.g., "o__" → "") to NULL.
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
        f") TO '{out_str}' ({_PARQUET_OPTS})"
    )

    conn.execute("DROP TABLE parsed_taxonomy")
    conn.execute("DROP TABLE raw_taxonomy")
    conn.execute("DROP TABLE tax_id_map")


def _write_phylogeny_parquet(
    conn: duckdb.DuckDBPyConnection,
    tree_path: Path,
    read_id_map: dict[str, tuple[int, str]],
    output_dir: Path,
    reference_idx: int,
    *,
    has_placements: bool = False,
) -> None:
    """Write reference_phylogeny.parquet and tip_features.json from a Newick tree.

    If has_placements is False, all tips must match a sequence in read_id_map
    (mismatches raise ValueError). If True, unmatched tips are expected
    (placement tips from jplace) and excluded from tip_features.json.
    """
    out_str = _validate_parquet_path(output_dir / "reference_phylogeny.parquet")
    tip_path = output_dir / "tip_features.json"

    # Parse tree once into a temp table — avoids double-parsing for the
    # Parquet write and the tip extraction.
    conn.execute(
        "CREATE TEMP TABLE tree_nodes AS SELECT * FROM read_newick(?)",
        [str(tree_path)],
    )

    # reference_idx is a Python int — int.__format__ always produces a decimal
    # string, so this interpolation is safe from injection.
    conn.execute(
        "COPY ("
        f"  SELECT CAST({reference_idx} AS BIGINT) AS reference_idx,"
        "    node_index, name, branch_length, edge_id, parent_index, is_tip"
        "  FROM tree_nodes"
        f") TO '{out_str}' ({_PARQUET_OPTS})",
    )

    tips = conn.execute(
        "SELECT node_index, name FROM tree_nodes WHERE is_tip = true",
    ).fetchall()

    conn.execute("DROP TABLE tree_nodes")

    tip_features = []
    unmatched_tips: list[str] = []
    for node_index, name in tips:
        if name in read_id_map:
            tip_features.append(
                {
                    "reference_idx": reference_idx,
                    "node_index": int(node_index),
                    "feature_idx": read_id_map[name][0],
                }
            )
        else:
            unmatched_tips.append(name or f"<unnamed node {node_index}>")

    if unmatched_tips and not has_placements:
        raise ValueError(
            f"{len(unmatched_tips)} phylogeny tip(s) have no matching sequence "
            f"in manifest: {unmatched_tips[:10]}"
        )

    tip_path.write_text(json.dumps(tip_features, indent=2))
