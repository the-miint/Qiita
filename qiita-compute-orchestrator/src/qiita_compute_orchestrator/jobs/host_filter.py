"""Native job: host-deplete reads (rype -> minimap2) by MASKING, not dropping.

Merges host-filter hits into the partial `qc_mask` from the `qc` step and emits
the final DuckLake `read_mask` parquet — one row per read, recording the read's
mask `reason` and trims. NO read is dropped: the full reads live once in the
DuckLake `read` table; this step only writes mask state keyed by the already-minted
`sequence_idx`.

Two-stage host filter, run on the QC-PASS subset only (the reads `read_masked`
would actually surface):
  1. rype `rype_classify` against the host's POSITIVE index — host = any emitted
     row (a low explicit threshold, not rype's `-N` negative mode);
  2. minimap2 `align_minimap2` (preset 'sr') on rype's SURVIVORS only — host =
     any alignment hit.
The hit set is the union; minimap2 runs on the reads rype didn't already flag,
so the two indexes never re-examine the same read. Host classification runs on
the TRIMMED QC-pass sequences (the same trims the `read_masked` view applies), so
a hit reflects the read as it would be served.

**Reason precedence (privacy-critical).** The final reason is, per read:
`host_minimap2` if minimap2 hit, else `host_rype` if rype hit, else the qc_mask's
own reason (`pass` or a `qc_*` failure). Host wins over `pass`; a read that
already failed QC keeps its `qc_*` reason (it's excluded from `read_masked`
either way, and host classify never ran on it). So `host_*` only ever overrides
`pass` — the privacy-sensitive host/human rows can never leak through a code path
that only inspects `qc_*`.

**Paired-end is handled natively, not by flattening.** A read row is one pair:
`sequence1`/`sequence2` are R1/R2 under one minted `sequence_idx`. We pass the
trimmed pair to the tools as `(read_id := sequence_idx, sequence1, sequence2)` —
`rype_classify` reads BOTH mates' k-mers and `align_minimap2` aligns the pair in
PE mode. Either mate matching the host flags the read's single `sequence_idx`.
Single-end reads have `sequence2 IS NULL`, which both tools tolerate.

Gating: when neither index path is bound (host filtering disabled) the mask is
the qc_mask unchanged (no host stage runs). A fully host-contaminated sample is
valid — every QC-pass read becomes `host_*`, which is correct, not an error.

miint contracts (qiita-verified against the team-mirror build via the smoke; see
docs/duckdb-miint.md):
  - `rype_classify(index_path, sequence_table, [id_column='read_id'],
    [threshold=0.1], [negative_index])` -> host-matching reads with columns
    `(read_id, bucket_id, bucket_name, score)`. It reads `sequence1` and (when
    present) `sequence2`. We DISTINCT the `read_id` — the table-function interface
    does not guarantee one best-hit row per read — and append into a BIGINT
    accumulator column, which coerces rype's `read_id` to BIGINT on insert.
  - `align_minimap2(query_table, [index_path], [preset], [max_secondary], ...)` ->
    SAM-like rows (`read_id, flags, reference, ...`); `read_id` round-trips as
    BIGINT (no cast). It reads `sequence1`/`sequence2` and emits one row per mate
    (plus secondaries), so we pass `max_secondary := 0` and DISTINCT the `read_id`
    to collapse a pair's rows to its single `sequence_idx`. Any surviving row = a hit.
Both resolve the query/sequence table by NAME on a SEPARATE connection during
bind/execute — so the query / survivors relations are non-temp VIEWs and the
host-id accumulators are non-temp TABLEs (TEMP tables / CTEs are not visible to
that connection; see docs/duckdb-miint.md).

`mask_idx` is a per-run constant stamped onto every `read_mask` row at emit
time — the CP-minted filtering-config identity (the runner mints it before this
step and threads it in via `params`); multiple masks coexist over the same
reads. `prep_sample_idx`, by contrast, is stamped PER ROW from the bound reads
relation (a staged Parquet on the per-sample path, a data-plane stream on the
block path): a block spans many prep_samples, so there is no single owner. A
single-sample ticket has one prep_sample_idx on every read, so this is a strict
generalization — identical output for the per-sample read-mask path.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.models import ReadMaskReason
from qiita_common.parquet import validate_parquet_path

from ..miint import PARQUET_OPTS, apply_duckdb_settings, duckdb_tmp_dir, open_miint_conn
from ..read_source import bind_step_reads

YAML_STEP_NAME = "host_filter"

# DuckDB stages the (streamed) query VIEW, the small host-id accumulators, and
# the final sorted COPY; the rype / minimap2 runtimes hold the indexes
# out-of-heap.
#
# NOT converted to the allocation-aware `resolve_duckdb_memory_gb` the
# reference-add build steps use, and deliberately so: at filter time the
# genome-scale memory is the loaded rype `.ryxdi` + minimap2 `.mmi`, which the
# runtimes hold OUT of DuckDB's heap and which already grow into the cgroup
# remainder a `--mem-gb` raise provides — DuckDB's cap doesn't gate them. Making
# DuckDB allocation-aware here would be wrong: it would let DuckDB claim the box
# and STARVE those out-of-heap indexes. The right lever for a genome-scale host
# filter is the cgroup (YAML mem_gb / `--mem-gb`), which already reaches the
# indexes with DuckDB held modest.
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

# In-DuckDB relation names. qc_mask + the trimmed-QC-pass query/survivors are
# VIEWs; the *_host accumulators are TABLEs (set algebra + always-present union,
# even when a tool is skipped). qc_mask is read on the SAME connection but is a
# VIEW (not a CTE) so the COPY and the query view can both reference it.
_QC_MASK = "host_filter_qc_mask"
_QUERY = "host_filter_query"
_SURVIVORS = "host_filter_survivors"
_RYPE_HOST = "host_filter_rype_hits"
_MM2_HOST = "host_filter_minimap2_hits"
# The per-read (sequence_idx -> prep_sample_idx) map, projected from the bound
# reads relation so the final mask can stamp prep_sample_idx PER ROW rather than as a
# per-run constant — a block spans many prep_samples (see the COPY below).
_READ_META = "host_filter_read_meta"

# A QC-pass read's trimmed sequence/qual: the same substr / list-slice math the
# read_masked view applies (1-based start, length arg for substr; 1-based
# inclusive slice for the qual array). Built from the read.parquet columns joined
# to the qc_mask trims. `r` is the read alias, `q` the qc_mask alias.
_TRIM_SEQ1 = (
    "substr(r.sequence1, q.left_trim1 + 1, length(r.sequence1) - q.left_trim1 - q.right_trim1)"
)
_TRIM_SEQ2 = (
    "CASE WHEN r.sequence2 IS NULL THEN NULL ELSE "
    "substr(r.sequence2, q.left_trim2 + 1, "
    "length(r.sequence2) - q.left_trim2 - q.right_trim2) END"
)


class Inputs(BaseModel):
    """Typed input contract for host_filter.

    `reads` (OPTIONAL — see the note below on why) is a staged read Parquet:
    `(prep_sample_idx, sequence_idx, read_id, sequence1, qual1, sequence2, qual2)`
    — the FULL reads. `qc_mask` is the partial mask the `qc` step emitted
    `(sequence_idx, reason, left_trim1, right_trim1, left_trim2, right_trim2)`.
    `host_rype_path` (a `.ryxdi` DIRECTORY) and `host_minimap2_path` (a `.mmi`
    FILE) are the host indexes — bound when host filtering is enabled, neither
    when disabled (the runner resolves them as optional inputs); a None path
    skips its stage. `mask_idx` is the CP-minted filtering-config identity stamped
    onto every output row. `work_ticket_idx` is the framework-injected scope
    scalar.

    `prep_sample_idx` is OPTIONAL and no longer read: the mask stamps each row's
    owner from the reads parquet's own `prep_sample_idx` column, so a multi-sample
    block is handled without a scope scalar. A PREP_SAMPLE-scoped ticket still has
    the framework inject it (one sample), but the kernel ignores it — the per-row
    value is authoritative and identical for the single-sample case. A block
    ticket flows no prep_sample_idx scalar at all (None here).

    `reads` is OPTIONAL because its SOURCE is a property of the workflow, not of
    this job: the per-sample `read-mask` workflow stages a Parquet, while
    `read-mask-block` binds none and the block's reads STREAM from the data plane
    at runtime. `bind_step_reads` resolves whichever applies and yields one
    relation name, so the kernel below is source-agnostic — including the per-row
    `prep_sample_idx` stamping, which reads the same column either way.
    """

    reads: Path | None = None
    qc_mask: Path
    mask_idx: int
    host_rype_path: Path | None = None
    host_minimap2_path: Path | None = None
    prep_sample_idx: int | None = None
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
    `?` (INSERT...SELECT is DML, so prepared params are accepted here). DISTINCT
    because the table-function interface does not guarantee one best-hit row per
    read. `dest_table` is declared BIGINT, so rype's `read_id` coerces to it on
    insert — no explicit cast."""
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
    and an empty one (no index content -> a silent no-op classify)."""
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
    if not inputs.qc_mask.exists():
        raise FileNotFoundError(f"qc_mask parquet not found: {inputs.qc_mask}")
    if inputs.host_rype_path is not None:
        _validate_rype_index(inputs.host_rype_path)
    if inputs.host_minimap2_path is not None:
        _validate_minimap2_index(inputs.host_minimap2_path)

    workspace.mkdir(parents=True, exist_ok=True)
    # Output basename is the DuckLake table name: a downstream register-files
    # step maps `read_mask.parquet` -> the `read_mask` table.
    read_mask = workspace / "read_mask.parquet"

    # COPY / CREATE VIEW path literals can't take a bound param; route them
    # through validate_parquet_path rather than inline-escaping.
    qc_mask_sql = validate_parquet_path(inputs.qc_mask)
    out_sql = validate_parquet_path(read_mask)

    success = False
    try:
        with duckdb_tmp_dir(workspace) as duckdb_tmp, open_miint_conn() as conn:
            apply_duckdb_settings(
                conn, duckdb_tmp, memory_gb=_DUCKDB_MEMORY_GB, threads=_DUCKDB_THREADS
            )
            # Bind the block/sample reads: a staged Parquet on the per-sample
            # path, a data-plane stream on the block path. One relation either
            # way, so the per-row owner stamping below is source-agnostic.
            async with bind_step_reads(
                conn,
                reads=inputs.reads,
                work_ticket_idx=inputs.work_ticket_idx,
                workspace=duckdb_tmp,
            ) as reads_rel:
                # qc_mask as a VIEW (read on this connection by both the query view
                # and the final COPY).
                conn.execute(
                    f"CREATE VIEW {_QC_MASK} AS SELECT * FROM read_parquet('{qc_mask_sql}')"
                )

                # Per-read (sequence_idx -> prep_sample_idx) map. The final COPY joins
                # it to stamp each mask row's owner FROM THE READS rather than from a
                # per-run constant — a block's reads span many prep_samples. Projected
                # to the two key columns so DuckDB reads only them from the (wide)
                # reads parquet; sequence_idx is globally unique, so the join is 1:1.
                conn.execute(
                    f"CREATE VIEW {_READ_META} AS "
                    f"SELECT sequence_idx, prep_sample_idx FROM {reads_rel}"
                )

                # The host-classify query: the TRIMMED QC-pass reads, keyed by
                # sequence_idx AS read_id (the tools' id column), carrying the trimmed
                # R1/R2 the tools k-mer/align. Only reason='pass' rows — a QC-failed
                # read is already excluded from read_masked, and host classify must
                # never run on (and so never reclassify) a non-pass read. The tools
                # handle PE natively. A non-temp VIEW so miint's separate connection
                # can resolve it by name.
                conn.execute(
                    f"CREATE VIEW {_QUERY} AS "
                    f"SELECT r.sequence_idx AS read_id, "
                    f"{_TRIM_SEQ1} AS sequence1, {_TRIM_SEQ2} AS sequence2 "
                    f"FROM {reads_rel} r "
                    f"JOIN {_QC_MASK} q USING (sequence_idx) "
                    f"WHERE q.reason = '{ReadMaskReason.PASS.value}'"
                )
                # Always-present accumulators (empty when a stage is skipped) so the
                # merge below references them unconditionally.
                conn.execute(f"CREATE TABLE {_RYPE_HOST} (sequence_idx BIGINT)")
                conn.execute(f"CREATE TABLE {_MM2_HOST} (sequence_idx BIGINT)")

                if inputs.host_rype_path is not None:
                    _run_rype_classify(
                        conn, inputs.host_rype_path, _QUERY, _RYPE_HOST, threshold=_RYPE_THRESHOLD
                    )

                if inputs.host_minimap2_path is not None:
                    # Stage 2 sees only the QC-pass reads rype didn't flag (empty rype
                    # set -> all QC-pass reads). An ANTI JOIN is NULL-safe by
                    # construction — unlike `NOT IN`, a stray NULL can't collapse the
                    # result to empty. Carries the trimmed sequence1/sequence2 so
                    # minimap2 still aligns in PE.
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

                # Merge host hits into the qc_mask under the privacy precedence:
                # minimap2 > rype > the qc_mask's own reason. Host only ever overrides
                # 'pass' (the query view restricted classify to pass reads), so a
                # qc_* row is untouched. mask_idx is the per-run constant stamped here;
                # prep_sample_idx is stamped PER ROW from the reads (the _READ_META
                # join) so a multi-sample block records each read's true owner. The
                # inner join is 1:1 (every qc_mask row is a read qc emitted from these
                # same reads). ORDER BY (mask_idx, prep_sample_idx, sequence_idx) — the
                # read_mask table's sort key (sequence_idx last for row-group pruning).
                conn.execute(
                    "COPY (SELECT "
                    "  ?::BIGINT AS mask_idx, "
                    "  rm.prep_sample_idx, "
                    "  q.sequence_idx, "
                    "  CASE "
                    "    WHEN mm.sequence_idx IS NOT NULL THEN "
                    f"'{ReadMaskReason.HOST_MINIMAP2.value}' "
                    f"    WHEN ry.sequence_idx IS NOT NULL THEN '{ReadMaskReason.HOST_RYPE.value}' "
                    "    ELSE q.reason "
                    "  END AS reason, "
                    "  q.left_trim1, q.right_trim1, q.left_trim2, q.right_trim2 "
                    f"FROM {_QC_MASK} q "
                    f"JOIN {_READ_META} rm USING (sequence_idx) "
                    f"LEFT JOIN {_RYPE_HOST} ry USING (sequence_idx) "
                    f"LEFT JOIN {_MM2_HOST} mm USING (sequence_idx) "
                    "ORDER BY mask_idx, prep_sample_idx, sequence_idx) "
                    f"TO '{out_sql}' ({PARQUET_OPTS})",
                    [inputs.mask_idx],
                )
        success = True
    finally:
        # On failure remove a partial output so the SLURM launcher's manifest
        # walker (which runs after execute()) can't promote it as the result.
        if not success:
            read_mask.unlink(missing_ok=True)

    # `read_mask` is the final mask path; `read_mask_staging_dir` is the
    # workspace a register-files step loads into the DuckLake `read_mask` table
    # (only read_mask.parquet matches its `*.parquet` convention). A distinct
    # staging-dir binding (not the generic `staging_dir`) so it doesn't collide
    # with fastq's read staging dir.
    return {"read_mask": read_mask, "read_mask_staging_dir": workspace}
