"""Native download-and-store step of the `download-ena-study` workflow: fetch a
sequenced_pool's reads from ENA via miint's `read_ena_sequences` into the
DuckLake `read` table, once. The ENA-fetch analog of `ingest_reads`.

For every `(prep_sample_idx, ena_run_accession)` in the runner-staged
`ena_run_map.parquet` roster, fetch that run's reads, mint a contiguous
`sequence_idx` range from the control plane, and write them as `read.parquet` —
the same mint-then-sort-and-assign pipeline as `ingest_reads` (`..read_staging`).
Two-write-target and idempotent/re-runnable semantics are identical too.

Gotchas specific to the ENA source:
- md5 verification is miint's, not this job's: `read_ena_sequences`'s
  `verify_md5` defaults to true
  (https://the-miint.github.io/duckdb-miint/insdc_ena/) and a single-run scan
  raises `duckdb.IOException` naming the md5 mismatch, which
  `_classify_ena_fetch_error` classifies retriable (a truncated transfer and a
  bad digest are indistinguishable; `max_retries` bounds it).
- One FRESH DuckDB connection PER RUN. `miint_warnings()` accumulates across
  queries in a session (duckdb-miint/docs/utilities.md), so a reused connection
  would leak one run's warnings into the next run's fail-loud check.
- Fail loud on a silent skip. `read_ena_sequences` does not raise on a run that
  fails to open or fails mid-stream — it retries once, then skips/truncates and
  records a `miint_warnings()` entry, returning fewer/zero rows. So a "skip"
  warning (`_skip_warnings`) fails the run retriably and a clean 0-row result
  fails it BAD_INPUT (an ENA run never legitimately has zero reads, unlike a
  demux well). A raised
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
    open_miint_conn,
)
from ..read_staging import hardlink, per_slot_caps, read_roster_parquet, write_sorted_reads
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

# Cap on the combined failure reason, which lands in a DB column and an email.
_REASON_FULL_RUNS = 3
_REASON_LISTED_ACCESSIONS = 20

# Substring marking a `miint_warnings()` message where THIS run's data is
# missing/partial: both the end-of-scan skip summary and the mid-stream
# truncation warning contain "skip". A self-healed "...retrying..." message
# does not, so a once-retried successful run is never mistaken for a skip.
_SKIP_WARNING_MARKER = "skip"
# Also contains "skip", but reports a complete run whose md5 could not be
# checked (SFF, a non-gzip file), not missing data.
_MD5_SKIPPED_WARNING_MARKER = "md5 verification skipped"
# miint's mid-stream failure warning (https://the-miint.github.io/duckdb-miint/insdc_ena/).
_MID_STREAM_WARNING_MARKER = "mid-stream"

# Prefix of the transient-fetch reason; the live e2e test keys its skip on it.
TRANSIENT_FETCH_ERROR_TEXT = "transient fetch error"

# Substrings marking a raised duckdb.Error as transport/network-shaped (vs.
# format/parse) — see `_classify_ena_fetch_error`. A non-match is permanent
# unless it is an md5 mismatch, which miint cannot tell from a truncation.
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

    `ena_run_map` is the runner-staged Parquet roster `(prep_sample_idx BIGINT,
    ena_run_accession VARCHAR)` from a live Postgres query. `reads_staging_root`
    is the scratch root durable per-sample `read.parquet` copies hang under (via
    `compute_reads_staging_path`). `download_method` defaults to 'http', the only
    transport this environment supports. `sequenced_pool_idx` / `sequencing_run_idx`
    / `work_ticket_idx` are framework-injected scope scalars."""

    ena_run_map: Path
    reads_staging_root: Path
    download_method: str = "http"
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _skip_warnings(messages: list[str]) -> list[str]:
    """Filter `messages` down to the ones meaning this run's data is missing or
    partial. See `_SKIP_WARNING_MARKER` for why a substring match suffices."""
    return [
        m
        for m in messages
        if _SKIP_WARNING_MARKER in m.lower() and _MD5_SKIPPED_WARNING_MARKER not in m.lower()
    ]


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
    with open_miint_conn() as conn:
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
    (transport/network-shaped or md5 mismatch) or permanent (format/parse, or
    anything not confidently network-shaped). The exception text is all the
    caller has: a raise means miint's open-retry-then-skip did not run."""
    text = str(exc).lower()
    if any(marker in text for marker in _TRANSIENT_ERROR_MARKERS):
        return BackendFailure(
            kind=FailureKind.EXTERNAL_FETCH_TRANSIENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"ENA run {run_accession}: {TRANSIENT_FETCH_ERROR_TEXT} "
                f"({type(exc).__name__}): {exc}"
            ),
        )
    # miint raises the same error for a truncated transfer and for bytes that
    # genuinely disagree with ENA's digest
    # (https://the-miint.github.io/duckdb-miint/insdc_ena/): retry, bounded.
    if "md5" in text:
        return BackendFailure(
            kind=FailureKind.EXTERNAL_FETCH_TRANSIENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"ENA run {run_accession}: downloaded bytes don't match ENA's "
                f"declared fastq_md5; miint cannot tell a truncated transfer from a "
                f"bad digest. If it keeps failing, "
                f"compare the run's fastq_md5 in the ENA Portal API against the "
                f"value reported here ({type(exc).__name__}): {exc}"
            ),
        )
    return BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name=step_name,
        reason=f"ENA run {run_accession}: fetch failed ({type(exc).__name__}): {exc}",
    )


def _combine_failures(failed: list[tuple[str, BackendFailure]]) -> BackendFailure:
    """One failure for the step from `(run_accession, failure)` pairs: a permanent
    outcome wins over a transient one so it is not retried. The reason gives the
    first few runs in full (permanent ones first) and only lists the rest."""
    if len(failed) == 1:
        return failed[0][1]
    ordered = sorted(failed, key=lambda pair: pair[1].transient)
    lead = ordered[0][1]
    reason = f"{len(failed)} runs failed: " + " | ".join(
        f.reason for _, f in ordered[:_REASON_FULL_RUNS]
    )
    rest = [acc for acc, _ in ordered[_REASON_FULL_RUNS:]]
    if rest:
        listed = ", ".join(rest[:_REASON_LISTED_ACCESSIONS])
        ellipsis = "…" if len(rest) > _REASON_LISTED_ACCESSIONS else ""
        reason += f" | and {len(rest)} more failed runs: {listed}{ellipsis}"
    return BackendFailure(kind=lead.kind, stage=lead.stage, step_name=lead.step_name, reason=reason)


async def execute(inputs: Inputs, workspace: Path) -> dict[str, Path]:
    """Download every pool run's reads, up to `_CONCURRENCY` at once. See the
    module docstring for the per-run pipeline and fail-loud checks."""
    roster = read_roster_parquet(
        inputs.ena_run_map, value_column="ena_run_accession", roster_name="ena_run_map"
    )

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
                    mid_stream = any(_MID_STREAM_WARNING_MARKER in m.lower() for m in skip_msgs)
                    raise BackendFailure(
                        kind=FailureKind.EXTERNAL_FETCH_TRANSIENT,
                        stage=WorkTicketFailureStage.STEP_RUN,
                        step_name=YAML_STEP_NAME,
                        reason=(
                            f"ENA run {run_accession} (prep_sample {prep_sample_idx}) "
                            "was skipped by miint -- its data is missing or partial; "
                            "refusing to register an incomplete read set"
                            + (
                                "; miint's docs say a re-run recovers a mid-stream failure"
                                if mid_stream
                                else ""
                            )
                            + f": {'; '.join(skip_msgs)}"
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

    # Runs that already completed only wrote their own durable copy, which is
    # harmless.
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(outcome, BackendFailure):
            raise outcome
    # Retriable and permanent runs can mix; collect all so a permanent one
    # decides the kind.
    failed = [(acc, o) for (_, acc), o in zip(roster, outcomes) if isinstance(o, BackendFailure)]
    if failed:
        raise _combine_failures(failed)

    # register-files loads the workspace's `read/` parts into the `read` table.
    return {"read_staging_dir": workspace}
