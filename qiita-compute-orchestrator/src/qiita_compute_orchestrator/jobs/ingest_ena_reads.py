"""Native download-and-store step of the `download-ena-study` workflow: fetch a
sequenced_pool's reads from ENA via miint's `read_ena_sequences` into the
DuckLake `read` table, once. The ENA-fetch analog of `ingest_reads`.

For every `(prep_sample_idx, ena_run_accession)` in the runner-staged
`ena_run_map.parquet` roster, fetch that run's reads, mint a contiguous
`sequence_idx` range from the control plane, and write them as `read.parquet` —
the same mint-then-sort-and-assign pipeline as `ingest_reads` (`..read_staging`).
Two-write-target and idempotent/re-runnable semantics are identical too.

A run whose fetch fails permanently (a 404, a format error, no sequence data, zero
reads) does not fail the pool when another run was stored: it is listed in the
`run_outcomes` JSON for `persist-ena-run-outcomes` and the rest land. Any
transient failure, any unrecognised error (OOM, local staging), or no run stored
fails the step.

Gotchas specific to the ENA source:
- md5 verification is miint's, not this job's: `read_ena_sequences`'s
  `verify_md5` defaults to true
  (https://the-miint.github.io/duckdb-miint/insdc_ena/) and a single-run scan
  raises `duckdb.IOException` naming the md5 mismatch, which
  `_classify_ena_fetch_error` classifies retriable (a truncated transfer and a
  bad digest are indistinguishable; `max_retries` bounds it).
- One DuckDB connection per run, each its own in-memory database, so
  `apply_duckdb_settings` gives every concurrent run its own per-slot memory limit
  and threads.
- A run miint could not read raises. A scan of one run raises on a failed open
  with the server's own error, and on a mid-stream failure as "every run failed"
  (duckdb-miint#312; https://the-miint.github.io/duckdb-miint/insdc_ena/). A scan
  of several runs skips one that fails and returns the rest, so `execute` refuses
  a roster accession that is not a run: `read_ena_sequences` would expand an
  experiment or sample accession to every run under it.
  `_classify_ena_fetch_error` classifies the raised `duckdb.Error`, and a 404 is
  permanent there. A clean 0-row result fails the run BAD_INPUT: an ENA run never
  legitimately has zero reads, unlike a demux well.
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import duckdb
from pydantic import BaseModel, Field
from qiita_common.api_paths import compute_reads_staging_path
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.ena_accession import (
    EnaAccessionKind,
    InvalidEnaAccessionError,
    detect_accession_kind,
)
from qiita_common.http_status import is_transient_error_text, is_transient_http_status
from qiita_common.models import FailureType, WorkTicketFailureStage
from qiita_common.models.ena_import import MAX_ENA_DOWNLOAD_RUN_CONCURRENCY, FailedRunOutcome
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

RUN_OUTCOMES_FILENAME = "run_outcomes.json"

# Default per-run fan-out (network-bound); `Inputs.run_concurrency` overrides it.
# Per-slot DuckDB caps come from `per_slot_caps`; the two _DUCKDB_* values are
# the off-SLURM (test/local) per-slot fallback.
_CONCURRENCY = 4
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2

# Cap on the combined failure reason, which lands in a DB column and an email.
_REASON_FULL_RUNS = 3
_REASON_LISTED_ACCESSIONS = 20

# Prefix of the transient-fetch reason; the live e2e test keys its skip on it.
TRANSIENT_FETCH_ERROR_TEXT = "transient fetch error"

_PERMANENT_ERROR_MARKERS = ("malformed", "invalid fastq", "no sequence data")
# ENA's own words when it answers a search with HTTP 200 but stops writing the
# results; with duckdb-miint#310, miint quotes them when the answer survives its
# retries.
_ENA_INCOMPLETE_ANSWER_MARKER = "not all results may have been written"
# miint's error for a scan of one run that failed mid-stream.
_EVERY_RUN_FAILED_MARKER = "every run failed"
# The reason for a run whose data is partial (`_classify_ena_fetch_error`).
_PARTIAL_RUN_TEXT = "its data is partial; refusing to register an incomplete read set"
_MID_STREAM_RERUN_HINT = "miint's docs say a re-run recovers a mid-stream failure"


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
    run_concurrency: int = Field(_CONCURRENCY, ge=1, le=MAX_ENA_DOWNLOAD_RUN_CONCURRENCY)
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


class _RunFetchFailure(Exception):
    """A run's fetch failure; `failure` is its classification."""

    def __init__(self, failure: BackendFailure) -> None:
        super().__init__(failure.reason)
        self.failure = failure


def _stage_run_reads(
    run_accession: str,
    download_method: str,
    intermediate_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> int:
    """Fetch one ENA run's reads via `read_ena_sequences` into a transient
    intermediate Parquet at `intermediate_path`, on a connection of its own (see
    module docstring). Returns the row count.

    The explicit 6-column projection drops the `comment`/`*_accession` columns
    the caller already knows from the roster/scope.

    Raises `duckdb.Error` on a transport/format failure (caller classifies via
    `_classify_ena_fetch_error`)."""
    intermediate = validate_parquet_path(intermediate_path)
    with open_miint_conn() as conn:
        apply_duckdb_settings(conn, duckdb_tmp, memory_gb=memory_gb, threads=threads)
        (count,) = conn.execute(
            "COPY ( SELECT sequence_index, read_id, sequence1, qual1, sequence2, qual2 "
            "FROM read_ena_sequences(?, download_method => ?) ) "
            f"TO '{intermediate}' ({PARQUET_OPTS_INTERMEDIATE})",
            [run_accession, download_method],
        ).fetchone()
    return int(count)


def _classify_ena_fetch_error(
    run_accession: str, exc: duckdb.Error, *, step_name: str
) -> BackendFailure:
    """Classify a raised `duckdb.Error` from `_stage_run_reads`. Retriable: a
    network-shaped error, a transient or zero HTTP status, ENA's incomplete answer,
    a mid-stream failure, an md5 mismatch. Anything else is BAD_INPUT, which
    `_is_permanent_fetch_error` then narrows to the recognised bad runs."""
    text = str(exc).lower()
    # A run that broke mid-download: its data is partial, which is not the same
    # as ENA being unreachable, so the reason does not carry
    # TRANSIENT_FETCH_ERROR_TEXT. Checked first, because miint's message ends
    # with the transfer's own error, which may carry a network word.
    if _EVERY_RUN_FAILED_MARKER in text:
        return BackendFailure(
            kind=FailureKind.EXTERNAL_FETCH_TRANSIENT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name=step_name,
            reason=(
                f"ENA run {run_accession} broke mid-download -- {_PARTIAL_RUN_TEXT}; "
                f"{_MID_STREAM_RERUN_HINT} ({type(exc).__name__}): {exc}"
            ),
        )
    # httpfs and miint's ENA client raise duckdb.HTTPException with the status;
    # status 0 means no usable HTTP status, so it retries.
    status = getattr(exc, "status_code", None)
    unavailable = (
        is_transient_error_text(text)
        or (status is not None and (status == 0 or is_transient_http_status(status)))
        or _ENA_INCOMPLETE_ANSWER_MARKER in text
    )
    if unavailable:
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


def _is_run_accession(accession: str) -> bool:
    try:
        return detect_accession_kind(accession) is EnaAccessionKind.RUN
    except InvalidEnaAccessionError:
        return False


def _is_permanent_fetch_error(exc: duckdb.Error) -> bool:
    """True for a raised error naming a bad run: a 404, a format error, no data."""
    text = str(exc).lower()
    return getattr(exc, "status_code", None) == 404 or any(
        marker in text for marker in _PERMANENT_ERROR_MARKERS
    )


def _combine_failures(failed: list[tuple[str, BackendFailure]]) -> BackendFailure:
    """One failure for the step from `(run_accession, failure)` pairs: a transient
    outcome wins so the ticket retries before landing. The reason gives the first
    few runs in full (transient ones first) and only lists the rest."""
    if len(failed) == 1:
        return failed[0][1]
    ordered = sorted(failed, key=lambda pair: not pair[1].transient)
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
    """Download every pool run's reads, up to `inputs.run_concurrency` at once. See the
    module docstring for the per-run pipeline and fail-loud checks."""
    roster = read_roster_parquet(
        inputs.ena_run_map, value_column="ena_run_accession", roster_name="ena_run_map"
    )
    not_runs = sorted({acc for _, acc in roster if not _is_run_accession(acc)})
    if not_runs:
        raise ValueError(
            f"ena_run_map holds {len(not_runs)} accession(s) that are not ENA runs, which "
            "read_ena_sequences would expand to every run under each: "
            + ", ".join(not_runs[:_REASON_LISTED_ACCESSIONS])
        )

    workspace.mkdir(parents=True, exist_ok=True)
    # register-files maps the `read/` subdir's part files -> the `read` table.
    register_dir = workspace / "read"
    register_dir.mkdir(parents=True, exist_ok=True)

    memory_gb, threads = per_slot_caps(
        inputs.run_concurrency, threads=_DUCKDB_THREADS, fallback_memory_gb=_DUCKDB_MEMORY_GB
    )
    print(
        f"{YAML_STEP_NAME}: {len(roster)} runs, run_concurrency={inputs.run_concurrency} "
        f"({memory_gb} GB, {threads} threads per slot)",
        file=sys.stderr,
    )
    sem = asyncio.Semaphore(inputs.run_concurrency)

    async def _process_run(http: object, prep_sample_idx: int, run_accession: str) -> str:
        """Store one run's reads. Returns `"registered"`; raises on any failure
        — unlike `ingest_reads`, an ENA run has no legitimate "empty" outcome."""
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
                    count = await asyncio.to_thread(
                        _stage_run_reads,
                        run_accession,
                        inputs.download_method,
                        intermediate,
                        run_tmp,
                        memory_gb,
                        threads,
                    )
                except duckdb.Error as exc:
                    failure = _classify_ena_fetch_error(
                        run_accession, exc, step_name=YAML_STEP_NAME
                    )
                    if failure.transient or _is_permanent_fetch_error(exc):
                        raise _RunFetchFailure(failure) from exc
                    raise

                if count == 0:
                    # Zero reads is anomalous for an ENA run (unlike a legitimately
                    # empty demux well). Failing here makes it this run's outcome;
                    # the mint would refuse a zero count for the whole step.
                    raise _RunFetchFailure(
                        BackendFailure(
                            kind=FailureKind.BAD_INPUT,
                            stage=WorkTicketFailureStage.STEP_RUN,
                            step_name=YAML_STEP_NAME,
                            reason=(
                                f"ENA run {run_accession} (prep_sample {prep_sample_idx}) "
                                "produced zero reads -- refusing to silently register "
                                "an empty read set"
                            ),
                        )
                    )

                sequence_idx_start = await mint_or_reuse_sequence_range(
                    http,
                    prep_sample_idx,
                    count,
                    work_ticket_idx=inputs.work_ticket_idx,
                    step_name=YAML_STEP_NAME,
                )
                print(
                    f"{YAML_STEP_NAME}: sorting {run_accession} (prep_sample {prep_sample_idx}): "
                    f"{count} reads ({memory_gb} GB, {threads} threads)",
                    file=sys.stderr,
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

    fetch_failures = [
        (psi, acc, o.failure)
        for (psi, acc), o in zip(roster, outcomes, strict=True)
        if isinstance(o, _RunFetchFailure)
    ]
    combined = (
        _combine_failures([(acc, f) for _, acc, f in fetch_failures]) if fetch_failures else None
    )
    # Runs that already completed only wrote their own durable copy, which is harmless.
    if combined is not None and combined.transient:
        raise combined
    for outcome in outcomes:
        if isinstance(outcome, BaseException) and not isinstance(outcome, _RunFetchFailure):
            raise outcome
    if combined is not None and len(fetch_failures) == len(roster):
        raise combined

    run_outcomes = workspace / RUN_OUTCOMES_FILENAME
    run_outcomes.write_text(
        json.dumps(
            [
                FailedRunOutcome(
                    prep_sample_idx=psi,
                    ena_run_accession=acc,
                    failure_reason=f.reason,
                    failure_type=FailureType.PERMANENT,
                ).model_dump(mode="json")
                for psi, acc, f in fetch_failures
            ]
        )
    )
    # register-files loads the workspace's `read/` parts into the `read` table.
    return {"read_staging_dir": workspace, "run_outcomes": run_outcomes}
