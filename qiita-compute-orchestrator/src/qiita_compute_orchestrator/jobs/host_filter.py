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

**Paired-end is handled natively, not by flattening.** A row of `reads.parquet`
is one read pair: `sequence1`/`sequence2` are R1/R2 under a single minted
`sequence_idx`. We pass that pair straight to the tools as
`(read_id := sequence_idx, sequence1, sequence2)` — `rype_classify` reads BOTH
mates' k-mers and `align_minimap2` aligns the pair in proper PE mode (it sets the
mate/template-length SAM fields). Either mate matching the host flags the read's
single `sequence_idx`, so "drop the whole pair if either mate hits" falls out
without ever moving R2 into an R1 slot. Single-end reads simply have
`sequence2 IS NULL`, which both tools tolerate.

Gating: when neither index path is bound (host filtering disabled) this is a
pass-through copy. A fully host-contaminated sample is valid — the output is an
empty (0-row) but well-formed Parquet, not an error.

miint contracts (qiita-verified against the team-mirror build via the smoke; see
docs/duckdb-miint.md):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1], [negative_index])` → host-matching reads with columns
    `(read_id, bucket_id, bucket_name, score)`. It reads `sequence1` and (when
    present) `sequence2`. We DISTINCT the `read_id` — the table-function
    interface does not guarantee one best-hit row per read (that is a CLI
    behavior) — and append into a BIGINT accumulator column, which coerces
    rype's `read_id` to BIGINT on insert whether the build returns it as BIGINT
    or VARCHAR (so the typed column is the contract; no explicit cast).
  - `align_minimap2(query_table, [index_path], [preset], [max_secondary], ...)` →
    SAM-like rows (`read_id, flags, reference, ...`); `read_id` round-trips as
    BIGINT (no cast). It reads `sequence1`/`sequence2` and emits one row per mate
    (plus secondaries), so we pass `max_secondary := 0` and DISTINCT the
    `read_id` to collapse a pair's rows to its single `sequence_idx`. Any
    surviving row = a hit.
Both resolve the query/sequence table by NAME on a SEPARATE connection during
bind/execute — so the query / survivors relations are non-temp VIEWs and the
host-id accumulators are non-temp TABLEs (TEMP tables / CTEs are not visible to
that connection; see docs/duckdb-miint.md).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
from pydantic import BaseModel

from ..miint import PARQUET_OPTS, apply_duckdb_settings, open_miint_conn
from ..read_count import write_read_count

YAML_STEP_NAME = "host_filter"

# DuckDB stages the (streamed) query VIEW, the small host-id accumulators, and
# the final sorted COPY; the rype / minimap2 runtimes hold the indexes
# out-of-heap. Literals mirror the fastq-to-parquet/1.1.0 YAML's host_filter
# baseline_resources (a mismatch is visible at review).
#
# NOT converted to the allocation-aware `resolve_duckdb_memory_gb` the
# reference-add build steps use, and deliberately so: at filter time the
# genome-scale memory is the loaded rype `.ryxdi` + minimap2 `.mmi`, which the
# runtimes hold OUT of DuckDB's heap and which already grow into the cgroup
# remainder a `--mem-gb` raise provides — DuckDB's cap doesn't gate them. Making
# DuckDB allocation-aware here would be wrong: it would let DuckDB claim the box
# and STARVE those out-of-heap indexes. The right lever for a genome-scale host
# filter is the cgroup (YAML mem_gb / `--mem-gb`), which already reaches the
# indexes with DuckDB held modest. Bump the YAML mem_gb (and this cap, if the
# DuckDB-side staging itself ever needs it) when sized against a real host filter.
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

# In-DuckDB relation names. query/survivors are VIEWs read by miint's separate
# connection; the *_host accumulators are TABLEs (set algebra + always-present
# union, even when a tool is skipped).
_QUERY = "host_filter_query"
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
    CREATE VIEW). DISTINCT because the table-function interface does not
    guarantee one best-hit row per read (that is a CLI behavior). `dest_table` is
    declared BIGINT, so rype's `read_id` coerces to it on insert — no explicit
    cast, and the typed column is the contract regardless of whether the build
    returns the id as BIGINT or VARCHAR."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx "
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
    pre-created `dest_table`. `align_minimap2` emits one row per mate (plus
    secondaries) in PE mode, so `max_secondary := 0` drops secondaries and
    DISTINCT collapses a pair's per-mate rows to its single `sequence_idx`.
    `read_id` round-trips as BIGINT, so no cast is needed."""
    conn.execute(
        f"INSERT INTO {dest_table} "
        "SELECT DISTINCT read_id AS sequence_idx "
        "FROM align_minimap2(?, index_path := ?, preset := ?, max_secondary := 0)",
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

    success = False
    try:
        with open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )

            # One row per read PAIR, keyed by the shared sequence_idx (passed AS
            # the tools' read_id), carrying both mates as sequence1/sequence2.
            # The tools handle PE natively (rype reads both mates; minimap2
            # aligns the pair). CREATE VIEW can't take prepared params, so the
            # path is inlined (quote-escaped — a filesystem path, no other
            # injection surface).
            conn.execute(
                f"CREATE VIEW {_QUERY} AS "
                "SELECT sequence_idx AS read_id, sequence1, sequence2 "
                f"FROM read_parquet('{reads_sql}')"
            )
            # Always-present accumulators (empty when a stage is skipped) so the
            # survivors view and the final anti-join reference them
            # unconditionally.
            conn.execute(f"CREATE TABLE {_RYPE_HOST} (sequence_idx BIGINT)")
            conn.execute(f"CREATE TABLE {_MM2_HOST} (sequence_idx BIGINT)")

            if inputs.host_rype_path is not None:
                _run_rype_classify(
                    conn, inputs.host_rype_path, _QUERY, _RYPE_HOST, threshold=_RYPE_THRESHOLD
                )

            if inputs.host_minimap2_path is not None:
                # Stage 2 sees only the pairs rype didn't flag (empty rype set →
                # all pairs). An ANTI JOIN is NULL-safe by construction — unlike
                # `NOT IN`, a stray NULL can't collapse the result to empty.
                # Carries sequence1/sequence2 so minimap2 still aligns in PE.
                conn.execute(
                    f"CREATE VIEW {_SURVIVORS} AS "
                    f"SELECT q.read_id, q.sequence1, q.sequence2 FROM {_QUERY} q "
                    f"ANTI JOIN {_RYPE_HOST} h ON h.sequence_idx = q.read_id"
                )
                _run_align_minimap2(
                    conn,
                    inputs.host_minimap2_path,
                    _SURVIVORS,
                    _MM2_HOST,
                    preset=_MINIMAP2_PRESET,
                )

            # Drop = rype ∪ minimap2 (both empty → pass-through). A read survives
            # only if its sequence_idx is in NEITHER accumulator — one ANTI JOIN
            # against the unioned drop set (NULL-safe; see the note above). ORDER
            # BY keeps the lake-friendly sorted `sequence_idx` layout
            # fastq_to_parquet wrote and makes the output deterministic.
            conn.execute(
                "COPY (SELECT sequence_idx, read_id, sequence1, qual1, sequence2, qual2 "
                f"FROM read_parquet('{reads_sql}') r "
                f"ANTI JOIN (SELECT sequence_idx FROM {_RYPE_HOST} "
                f"           UNION ALL SELECT sequence_idx FROM {_MM2_HOST}) drop_set "
                "  ON drop_set.sequence_idx = r.sequence_idx "
                f"ORDER BY sequence_idx) TO '{out_sql}' ({PARQUET_OPTS})"
            )
            # Emit the quality-filtered read count (#141): reads surviving host
            # depletion (== biological count on a pass-through). Reuse this
            # connection — write_read_count only does a footer-level count.
            quality_filtered_read_count = write_read_count(conn, filtered, workspace)
        success = True
    finally:
        # Drop the spill dir before returning so the SLURM launcher's manifest
        # walker (which runs after execute()) sees only filtered_reads.parquet;
        # on failure remove a partial output so it can't be promoted.
        shutil.rmtree(duckdb_tmp, ignore_errors=True)
        if not success:
            filtered.unlink(missing_ok=True)

    return {"filtered_reads": filtered, "quality_filtered_read_count": quality_filtered_read_count}
