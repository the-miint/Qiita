"""Native job: host-deplete short reads (rype → minimap2) on `reads.parquet`.

A pure `reads.parquet -> filtered_reads.parquet` transform keyed by the
already-minted `sequence_idx`. Minting happened upstream in `fastq_to_parquet`;
this step is an additive downstream filter that only DROPS rows, so the surviving
`sequence_idx` are a subset of the minted range (benign gaps — `sequence_idx`
stays a unique sorted join key).

Two-stage host filter, locked with the design:
  1. rype `rype_classify` against the host's POSITIVE index — host = any emitted
     row (a low explicit threshold, not rype's `-N` negative mode);
  2. minimap2 `align_minimap2` (preset 'sr') on rype's SURVIVORS only — host =
     any alignment hit.
The drop set is the union; minimap2 runs on the reads rype didn't already flag,
so the two indexes never re-examine the same read.

**Paired-end: drop the whole pair if either mate is flagged by either tool.**
This falls out of the keying: both mates of a pair carry one `sequence_idx`, so
we unroll R1/R2 into a `mates` relation keyed by `sequence_idx` (passed to the
tools AS `read_id` — miint accepts a BIGINT id), classify/align each mate
independently, and DISTINCT the flagged `sequence_idx`. Either mate hitting puts
the shared `sequence_idx` in the drop set.

Gating: when neither index path is bound (host filtering disabled) this is a
pass-through copy. A fully host-contaminated sample is valid — the output is an
empty (0-row) but well-formed Parquet, not an error.

miint contracts (qiita-verified against the team-mirror build; see
docs/duckdb-miint.md):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1], [negative_index])` → one row per HOST read with columns
    `(read_id, bucket_id, bucket_name, score)`. rype's id codec returns `read_id`
    as VARCHAR even for a BIGINT input, so we CAST back to BIGINT.
  - `align_minimap2(query_table, [index_path], [preset], ...)` → SAM-like rows
    (`read_id, flags, reference, ...`); `read_id` stays BIGINT. Any row = a hit.
Both read a `sequence1` column off the query/sequence table and resolve the
table by NAME on a SEPARATE connection during bind/execute — so `mates` /
`survivors` are non-temp VIEWs and the host-id accumulators are non-temp TABLEs
(TEMP tables / CTEs are not visible to that connection; see docs/duckdb-miint.md).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel

from ..miint import PARQUET_OPTS, apply_duckdb_settings, ensure_miint_installed, open_conn

YAML_STEP_NAME = "host_filter"

# DuckDB stages the (streamed) mate VIEWs, the small host-id accumulators, and
# the final sorted COPY; the rype / minimap2 runtimes hold the indexes
# out-of-heap. Literals mirror the fastq-to-parquet/1.1.0 YAML's host_filter
# baseline_resources (a mismatch is visible at review). Genome-scale host-index
# sizing is a deferred follow-up — bump the YAML mem_gb and this cap together.
_DUCKDB_MEMORY_GB = 8
_DUCKDB_THREADS = 4

# rype "host = any emitted row": a low threshold so a nonzero score (any host
# minimizer match) flags the read. Explicit, NOT rype's 0.1 default — host
# depletion is deliberately aggressive (drop a borderline read rather than retain
# host contamination). Pinned by the smoke test against the real function.
_RYPE_THRESHOLD = 0.0

# minimap2 short-read preset — the host-filter alignment mode, matching the
# preset the `.mmi` was built with (build_minimap2_index).
_MINIMAP2_PRESET = "sr"

# In-DuckDB relation names. mates/survivors are VIEWs read by miint's separate
# connection; the *_host accumulators are TABLEs (set algebra + always-present
# union, even when a tool is skipped).
_MATES = "host_filter_mates"
_SURVIVORS = "host_filter_survivors"
_RYPE_HOST = "host_filter_rype_hits"
_MM2_HOST = "host_filter_minimap2_hits"


class Inputs(BaseModel):
    """Typed input contract for host_filter.

    `reads` is fastq_to_parquet's `reads.parquet` (binding name `reads`):
    `(sequence_idx BIGINT, read_id, sequence1, qual1, sequence2, qual2)`.
    `host_rype_path` (a `.ryxdi` DIRECTORY) and `host_minimap2_path` (a `.mmi`
    FILE) are the host indexes — both bound when host filtering is enabled,
    neither when disabled (the runner resolves them as optional inputs). When a
    path is None its stage is skipped. `prep_sample_idx` / `work_ticket_idx` are
    the framework-injected scope scalars (PREP_SAMPLE kind + the always-on
    work_ticket_idx).
    """

    reads: Path
    host_rype_path: Path | None = None
    host_minimap2_path: Path | None = None
    prep_sample_idx: int
    work_ticket_idx: int


def _run_rype_classify(
    conn: duckdb.DuckDBPyConnection,
    index_path: Path,
    sequence_table: str,
    dest_table: str,
    *,
    threshold: float,
) -> None:
    """Seam around miint's `rype_classify`. Appends the DISTINCT host
    `sequence_idx` set (reads that matched the positive index) into the
    pre-created `dest_table`. Isolated so unit tests stub the real classify.

    Positional args (index path, sequence-table NAME) + `threshold` are bound as
    `?` (INSERT...SELECT is DML, so prepared params are accepted here, unlike a
    CREATE VIEW). rype returns `read_id` as VARCHAR — CAST back to BIGINT."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT CAST(read_id AS BIGINT) AS sequence_idx "
        "FROM rype_classify(?, ?, id_column := 'read_id', threshold := ?)",
        [str(index_path), sequence_table, threshold],
    )


def _run_align_minimap2(
    conn: duckdb.DuckDBPyConnection,
    index_path: Path,
    query_table: str,
    dest_table: str,
    *,
    preset: str,
) -> None:
    """Seam around miint's `align_minimap2`. Appends the DISTINCT host
    `sequence_idx` set (reads with any alignment to the host index) into the
    pre-created `dest_table`. `align_minimap2` keeps `read_id` as BIGINT, so no
    cast is needed."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx "
        "FROM align_minimap2(?, index_path := ?, preset := ?)",
        [query_table, str(index_path), preset],
    )


def _validate_rype_index(path: Path) -> None:
    """A rype index is a `.ryxdi` DIRECTORY; reject a missing one (fail fast)
    and an empty one (no index content → a silent no-op classify)."""
    if not path.exists():
        raise FileNotFoundError(f"host_rype_path not found: {path}")
    if not path.is_dir() or not any(path.iterdir()):
        raise ValueError(f"host_rype_path is not a populated .ryxdi directory: {path}")


def _validate_minimap2_index(path: Path) -> None:
    """A minimap2 index is a single `.mmi` FILE; reject a missing or zero-byte
    one (a broken/partial build)."""
    if not path.exists():
        raise FileNotFoundError(f"host_minimap2_path not found: {path}")
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError(f"host_minimap2_path is not a non-empty .mmi file: {path}")


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    if not inputs.reads.exists():
        raise FileNotFoundError(f"reads parquet not found: {inputs.reads}")
    if inputs.host_rype_path is not None:
        _validate_rype_index(inputs.host_rype_path)
    if inputs.host_minimap2_path is not None:
        _validate_minimap2_index(inputs.host_minimap2_path)

    workspace.mkdir(parents=True, exist_ok=True)
    filtered = workspace / "filtered_reads.parquet"
    duckdb_tmp = workspace / ".duckdb_tmp"
    duckdb_tmp.mkdir(parents=True, exist_ok=True)

    reads_sql = str(inputs.reads).replace("'", "''")
    out_sql = str(filtered).replace("'", "''")

    await ensure_miint_installed()
    success = False
    try:
        with open_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            conn.execute("LOAD miint;")

            # One row per mate, keyed by the shared sequence_idx (passed AS the
            # tools' read_id). R2 only when present (unpaired → sequence2 NULL).
            # CREATE VIEW can't take prepared params, so the path is inlined
            # (quote-escaped — a filesystem path, no other injection surface).
            conn.execute(
                f"CREATE VIEW {_MATES} AS "
                f"SELECT sequence_idx AS read_id, sequence1 FROM read_parquet('{reads_sql}') "
                "UNION ALL "
                f"SELECT sequence_idx AS read_id, sequence2 AS sequence1 "
                f"FROM read_parquet('{reads_sql}') "
                "WHERE sequence2 IS NOT NULL AND length(sequence2) > 0"
            )
            # Always-present accumulators (empty when a stage is skipped) so the
            # survivors view and the final anti-joins reference them
            # unconditionally.
            conn.execute(f"CREATE TABLE {_RYPE_HOST} (sequence_idx BIGINT)")
            conn.execute(f"CREATE TABLE {_MM2_HOST} (sequence_idx BIGINT)")

            if inputs.host_rype_path is not None:
                _run_rype_classify(
                    conn, inputs.host_rype_path, _MATES, _RYPE_HOST, threshold=_RYPE_THRESHOLD
                )

            if inputs.host_minimap2_path is not None:
                # Stage 2 sees only the mates rype didn't flag (empty rype set →
                # all mates). NOT EXISTS, not NOT IN: an anti-join is NULL-safe,
                # whereas `NOT IN` over a set containing a NULL collapses to
                # UNKNOWN for every row and would silently drop ALL reads.
                conn.execute(
                    f"CREATE VIEW {_SURVIVORS} AS SELECT m.* FROM {_MATES} m "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {_RYPE_HOST} h "
                    "WHERE h.sequence_idx = m.read_id)"
                )
                _run_align_minimap2(
                    conn,
                    inputs.host_minimap2_path,
                    _SURVIVORS,
                    _MM2_HOST,
                    preset=_MINIMAP2_PRESET,
                )

            # Drop = rype ∪ minimap2 (both empty → pass-through). A read survives
            # only if NEITHER accumulator holds its sequence_idx — two NULL-safe
            # anti-joins (see the NOT EXISTS note above). ORDER BY keeps the
            # lake-friendly sorted `sequence_idx` layout fastq_to_parquet wrote
            # and makes the output deterministic across runs.
            conn.execute(
                "COPY (SELECT sequence_idx, read_id, sequence1, qual1, sequence2, qual2 "
                f"FROM read_parquet('{reads_sql}') r "
                f"WHERE NOT EXISTS (SELECT 1 FROM {_RYPE_HOST} h "
                "WHERE h.sequence_idx = r.sequence_idx) "
                f"  AND NOT EXISTS (SELECT 1 FROM {_MM2_HOST} h "
                "WHERE h.sequence_idx = r.sequence_idx) "
                f"ORDER BY sequence_idx) TO '{out_sql}' ({PARQUET_OPTS})"
            )
        success = True
    finally:
        # Drop the spill dir before returning so the SLURM launcher's manifest
        # walker (which runs after execute()) sees only filtered_reads.parquet;
        # on failure remove a partial output so it can't be promoted.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        if not success:
            filtered.unlink(missing_ok=True)

    return {"filtered_reads": filtered}
