"""Native download-and-store step of the `download-ena-study` workflow: fetch a
sequenced_pool's reads from ENA via miint's `read_ena_sequences` into the
DuckLake `read` table, once. The ENA-fetch analog of `ingest_reads`.

For every `(prep_sample_idx, ena_run_accession)` in the runner-staged
`run_map.parquet` roster, fetch that run's reads, mint a contiguous
`sequence_idx` range from the control plane, and write them as `read.parquet` —
the same mint-then-sort-and-assign pipeline as `ingest_reads` (`..read_staging`).
Two-write-target and idempotent/re-runnable semantics are identical too.

Gotchas specific to the ENA source:
- md5 verification is miint's, not this job's. `read_ena_sequences` verifies
  downloaded bytes against ENA's `fastq_md5` once the data plane bundles the miint
  build that adds `verify_md5` (on by default there). This job relies on that
  default rather than passing it, so it stays a no-op against an older bundled
  extension and activates automatically on the bump -- no code change. When active,
  a mismatch raises `duckdb.IOException` with "md5" in the message and is classified
  BAD_INPUT (permanent, re-downloading yields the same bytes); the classification
  branch is dormant until then.
- One FRESH DuckDB connection PER RUN. `miint_warnings()` accumulates across
  queries in a session (duckdb-miint/docs/utilities.md), so a reused connection
  would leak one run's warnings into the next run's fail-loud check.
- Fail loud on a silent skip. `read_ena_sequences` does not raise on a run that
  fails to open or fails mid-stream — it retries once, then skips/truncates and
  records a `miint_warnings()` entry, returning fewer/zero rows. So a "skip"
  warning (`_skip_warnings`) OR a clean 0-row result fails the run BAD_INPUT (an
  ENA run never legitimately has zero reads, unlike a demux well). A raised
  `duckdb.Error` is classified separately (`_classify_ena_fetch_error`).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
from pydantic import BaseModel
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage
from qiita_common.parquet import validate_parquet_path

from ..cp_client import make_cp_client
from ..miint import (
    PARQUET_OPTS_INTERMEDIATE,
    apply_duckdb_settings,
    duckdb_tmp_dir,
    open_conn,
    open_miint_ena_conn,
)
from ..read_staging import hardlink, per_slot_caps, write_sorted_reads
from ..sequence_range_retry import mint_or_reuse_sequence_range

# Hard-coded (not derived from the YAML) so a rename diverging from the
# `- step: ingest_ena_reads` entry fails loudly at BackendFailure attribution.
YAML_STEP_NAME = "ingest_ena_reads"

# Bounded per-run fan-out, same shape as ingest_reads' _CONCURRENCY (here
# network-bound). Per-slot DuckDB caps come from `per_slot_caps`; the two
# _DUCKDB_* values are the off-SLURM (test/local) per-slot fallback.
_CONCURRENCY = 4
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2

# Substring marking a `miint_warnings()` message where THIS run's data is
# missing/partial: both the end-of-scan skip summary and the mid-stream
# truncation warning contain "skip". A self-healed "...retrying..." message
# does not, so a once-retried successful run is never mistaken for a skip.
_SKIP_WARNING_MARKER = "skip"

# Substrings marking a raised duckdb.Error as transport/network-shaped (vs.
# format/parse) — see `_classify_ena_fetch_error`. Conservative: a non-match is
# treated as permanent, since a false "retriable" burns a retry on an error
# that fails identically every time.
_TRANSIENT_ERROR_MARKERS = (
    "connection",
    "timed out",
    "timeout",
    "network",
    "reset",
    "refused",
    "unreachable",
    "temporarily",
    "curl",
)


class Inputs(BaseModel):
    """Typed input contract for ingest_ena_reads.

    `run_map` is the runner-staged Parquet roster `(prep_sample_idx BIGINT,
    ena_run_accession VARCHAR)` from a live Postgres query. `reads_staging_root`
    is the scratch root durable per-sample `read.parquet` copies hang under (via
    `compute_reads_staging_path`). `download_method` defaults to 'http', the only
    transport this environment supports. `sequenced_pool_idx` / `sequencing_run_idx`
    / `work_ticket_idx` are framework-injected scope scalars."""

    run_map: Path
    reads_staging_root: Path
    download_method: str = "http"
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _read_run_map(path: Path) -> list[tuple[int, str]]:
    """Read the `(prep_sample_idx, ena_run_accession)` roster from the staged
    Parquet, ordered by prep_sample_idx for deterministic processing/reporting.
    Raises ValueError on an empty or unreadable roster — the CP already fails
    loud on an empty pool, so an empty file here signals a dispatch bug."""
    try:
        with open_conn() as conn:
            rows = conn.execute(
                "SELECT prep_sample_idx, ena_run_accession FROM read_parquet(?) "
                "ORDER BY prep_sample_idx",
                [str(path)],
            ).fetchall()
    except duckdb.Error as exc:
        raise ValueError(f"run_map could not be read: {path}: {exc}") from exc
    if not rows:
        raise ValueError(f"run_map is empty: {path}")
    return [(int(r[0]), str(r[1])) for r in rows]


def _skip_warnings(messages: list[str]) -> list[str]:
    """Filter `messages` down to the ones meaning this run's data is missing or
    partial. See `_SKIP_WARNING_MARKER` for why a substring match suffices."""
    return [m for m in messages if _SKIP_WARNING_MARKER in m.lower()]


def _stage_run_reads(
    run_accession: str,
    download_method: str,
    intermediate_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> tuple[int, list[str]]:
    """Fetch one ENA run's reads via `read_ena_sequences` into a transient
    intermediate Parquet at `intermediate_path`, on a FRESH per-run DuckDB
    connection (see module docstring). Returns `(row_count, warning_messages)`.

    The explicit 6-column projection drops the `comment`/`*_accession` columns
    the caller already knows from the roster/scope. `warning_messages` is read
    from `miint_warnings()` on the SAME connection right after the COPY (empty
    until then, so every message is about exactly this run); this function does
    not interpret them — the caller applies `_skip_warnings`.

    Raises `duckdb.Error` on a raised transport/format failure (caller
    classifies via `_classify_ena_fetch_error`). Does NOT raise on an internal
    skip/partial-download — that returns normally with a warning, which is why
    the caller must inspect `warning_messages`."""
    intermediate = validate_parquet_path(intermediate_path)
    with open_miint_ena_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)
        (count,) = conn.execute(
            "COPY ( SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
            "FROM read_ena_sequences(?, download_method => ?) ) "
            f"TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
            [run_accession, download_method],
        ).fetchone()
        warnings = [
            str(row[0]) for row in conn.execute("SELECT message FROM miint_warnings()").fetchall()
        ]
    return int(count), warnings


def _classify_ena_fetch_error(
    run_accession: str, exc: duckdb.Error, *, step_name: str
) -> BackendFailure:
    """Classify a raised `duckdb.Error` from `_stage_run_reads` as retriable
    (transport/network-shaped) or permanent (md5-verification, format/parse, or
    anything not confidently network-shaped). A raised exception means miint's
    internal open-retry-then-skip did NOT run, so there is no `miint_warnings()`
    entry — the exception text is all the caller has.

    The transient-marker check runs FIRST, so text mentioning both md5 and a
    transient marker classifies transient; the md5 branch only sees text already
    ruled out as network-shaped."""
    text = str(exc).lower()
    if any(marker in text for marker in _TRANSIENT_ERROR_MARKERS):
        return BackendFailure(
            kind=FailureKind.EXTERNAL_FETCH_TRANSIENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"ENA run {run_accession}: transient fetch error ({type(exc).__name__}): {exc}"
            ),
        )
    if "md5" in text:
        return BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"ENA run {run_accession}: ENA download md5 verification failed "
                f"(data corruption) ({type(exc).__name__}): {exc}"
            ),
        )
    return BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=step_name,
        reason=f"ENA run {run_accession}: fetch failed ({type(exc).__name__}): {exc}",
    )


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Download every pool run's reads, up to `_CONCURRENCY` at once. See the
    module docstring for the per-run pipeline and fail-loud checks."""
    roster = _read_run_map(inputs.run_map)

    workspace.mkdir(parents=True, exist_ok=True)
    # register-files maps the `read/` subdir's part files -> the `read` table.
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)

    memory_gb, threads = per_slot_caps(
        _CONCURRENCY, threads=_DUCKDB_THREADS, fallback_memory_gb=_DUCKDB_MEMORY_GB
    )
    sem = asyncio.Semaphore(_CONCURRENCY)

    async def _process_run(http: object, prep_sample_idx: int, run_accession: str) -> str:
        """Store one run's reads. Returns `"registered"`; raises on any failure
        — unlike `ingest_reads`, an ENA run has no legitimate "empty" outcome,
        so every run either registers or fails the whole step."""
        async with sem:
            durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
            part = register_dir / f"{prep_sample_idx}.parquet"

            # Idempotent fast path: reads already stored on a prior attempt.
            # Re-create the register hardlink (the prior workspace is gone).
            if durable.exists():
                hardlink(durable, part)
                return "registered"

            durable.parent.mkdir(parents=True, exist_ok=True)
            intermediate = durable.parent / "_intermediate_reads.parquet"
            # Per-run temp dir so concurrent slots never collide on spill.
            run_tmp = duckdb_tmp / str(prep_sample_idx)
            run_tmp.mkdir(parents=True, exist_ok=True)
            try:
                try:
                    count, warnings = await asyncio.to_thread(
                        _stage_run_reads,
                        run_accession,
                        inputs.download_method,
                        intermediate,
                        run_tmp,
                        memory_gb,
                        threads,
                    )
                except duckdb.Error as exc:
                    raise _classify_ena_fetch_error(
                        run_accession, exc, step_name=YAML_STEP_NAME
                    ) from exc

                skip_msgs = _skip_warnings(warnings)
                if skip_msgs:
                    raise BackendFailure(
                        kind=FailureKind.BAD_INPUT,
                        stage=WorkTicketFailureStage.STEP_RUN,
                        step_name=YAML_STEP_NAME,
                        reason=(
                            f"ENA run {run_accession} (prep_sample {prep_sample_idx}) "
                            "reported download warning(s) -- its data is missing or "
                            "partial; refusing to silently register an incomplete "
                            f"read set: {'; '.join(skip_msgs)}"
                        ),
                    )
                if count == 0:
                    # Zero reads with no explanatory warning is anomalous for an
                    # ENA run (unlike a legitimately empty demux well) -- fail
                    # loud rather than silently register nothing.
                    raise BackendFailure(
                        kind=FailureKind.BAD_INPUT,
                        stage=WorkTicketFailureStage.STEP_RUN,
                        step_name=YAML_STEP_NAME,
                        reason=(
                            f"ENA run {run_accession} (prep_sample {prep_sample_idx}) "
                            "produced zero reads with no explanatory "
                            "miint_warnings() entry -- refusing to silently "
                            "register an empty read set"
                        ),
                    )

                sequence_idx_start = await mint_or_reuse_sequence_range(
                    http,
                    prep_sample_idx,
                    count,
                    work_ticket_idx=inputs.work_ticket_idx,
                    step_name=YAML_STEP_NAME,
                )
                await asyncio.to_thread(
                    write_sorted_reads,
                    intermediate,
                    prep_sample_idx,
                    sequence_idx_start,
                    durable,
                    run_tmp,
                    memory_gb,
                    threads,
                )
            finally:
                intermediate.unlink(missing_ok=True)
            hardlink(durable, part)
            return "registered"

    with duckdb_tmp_dir(workspace) as duckdb_tmp:
        async with make_cp_client() as http:
            outcomes = await asyncio.gather(
                *(_process_run(http, psi, acc) for psi, acc in roster),
                return_exceptions=True,
            )

    # Surface the first hard error in roster order (matches ingest_reads'
    # abort-on-first). Runs that already completed only wrote their own durable
    # copy, which is harmless.
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome

    # register-files loads the workspace's `read/` parts into the `read` table.
    return {"read_staging_dir": workspace}
