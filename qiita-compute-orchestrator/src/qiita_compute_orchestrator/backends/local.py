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
        feature_map: FeatureMap,
        output_dir: Path,
        reference_idx: int,
        *,
        taxonomy_path: Path | None = None,
        tree_path: Path | None = None,
        jplace_path: Path | None = None,
    ) -> Path:
        raise NotImplementedError("run_load_job will be implemented in Phase 9")
