"""Native job: download a sequenced_pool's reads from ENA via miint's
`read_ena_sequences` into the DuckLake `read` table, once.

Runs as the download-and-store step of the `download-ena-study` workflow —
the ENA-fetch analog of `ingest_reads` (bcl-convert's read-storage step; see
that module's docstring for the shared shape). Instead of demuxed FASTQs on
disk, this job pulls each of the pool's runs directly from ENA: for every
`(prep_sample_idx, ena_run_accession)` in the runner-staged `run_map.parquet`
roster, it fetches that run's reads with `read_ena_sequences`, mints a
contiguous `sequence_idx` range from the control plane, and writes the FULL
reads as `read.parquet` keyed by the minted range — exactly the same
mint-then-sort-and-assign pipeline `ingest_reads` uses (`..read_staging`).

**md5 verification is miint's, not this job's.** `read_ena_sequences` verifies
every downloaded run's bytes against ENA's reported `fastq_md5` by default
(`verify_md5` defaults on) — this job does not pass `verify_md5` explicitly
and relies on that default. A mismatch surfaces as a raised `duckdb.IOException`
whose message contains "md5" and carries none of `_TRANSIENT_ERROR_MARKERS`
(by design — a corrupted download is a data-integrity failure, not a
retryable network blip); `_classify_ena_fetch_error` classifies it BAD_INPUT
(permanent), since retrying would just re-download the same corrupted bytes.

**One fresh DuckDB connection PER RUN, never reused across the roster loop.**
`miint_warnings()` is scoped to the DatabaseInstance a connection belongs to
and ACCUMULATES across queries within one session (see
`duckdb-miint/docs/utilities.md`); a connection reused across runs would mix
one run's warnings into the next run's fail-loud check. Opening
`open_miint_ena_conn()` fresh per run (`_stage_run_reads`) keeps the check
scoped to exactly the run just fetched.

**Fail-loud on a silent skip.** `read_ena_sequences` does not raise when one
of its runs fails to open or fails mid-stream — it retries once internally,
then SKIPS the run (or truncates it) and records a warning via
`miint::EmitWarning` (visible through `miint_warnings()`), returning fewer (or
zero) rows rather than throwing. A naive "0 rows ⇒ empty well" read (the
`ingest_reads` convention for an empty demux FASTQ) would silently accept a
skipped/truncated ENA run as if it legitimately had no reads. So after every
per-run COPY, this job inspects `miint_warnings()` on that SAME fresh
connection: any message that plausibly means "this run's data is missing or
partial" (a substring match on "skip" — see `_skip_warnings`) fails the run
loud as BAD_INPUT (permanent), and so does a clean 0-row result with NO such
warning (an ENA run is never expected to legitimately carry zero reads, unlike
a demux well). A raised `duckdb.Error` (e.g. ENA metadata resolution failing
outright, before any per-run reader even exists) is classified separately —
retriable for a transport/network-shaped error, permanent otherwise (see
`_classify_ena_fetch_error`).

**Two write targets per run** (one inode, hardlinked) and **idempotent /
re-runnable** semantics are identical to `ingest_reads` — see that module's
docstring for the full rationale; not repeated here.
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

# YAML step name this module implements. Hard-coded because execute() raises
# BackendFailures itself (which need a step_name); a rename here that diverges
# from the `- step: ingest_ena_reads` YAML entry fails loudly at attribution.
YAML_STEP_NAME = "ingest_ena_reads"

# Bounded-concurrent pool loop — same shape and rationale as ingest_reads'
# _CONCURRENCY (network-bound here rather than CPU/parse-bound, but the
# per-run pipeline is still independent per run, so the same bounded fan-out
# applies). Per-slot DuckDB caps come from the shared `per_slot_caps`
# (../read_staging.py); `_DUCKDB_MEMORY_GB` is only the off-SLURM (test /
# local) per-slot fallback.
_CONCURRENCY = 4
_DUCKDB_MEMORY_GB = 7
_DUCKDB_THREADS = 2

# Substring marker for a `miint_warnings()` message that means THIS run's
# data is missing or partial. Both of miint's user-facing failure messages for
# a single run contain it: the end-of-scan summary ("N run(s) skipped due to
# download errors: <accession>") and the mid-stream truncation warning
# ("failed mid-stream after emitting N read(s) ...; skipping remainder"). A
# transient "... failed to open (...), retrying..." message that self-healed
# on miint's internal retry does NOT contain it, so a fully-successful,
# once-retried run is never mistaken for a skip.
_SKIP_WARNING_MARKER = "skip"

# Keyword substrings that identify a raised duckdb.Error as a transport/
# network-shaped failure (vs. a format/parse failure) — see
# `_classify_ena_fetch_error`. Deliberately conservative (checked against the
# lower-cased exception text): anything not matching one of these is treated
# as permanent, since a false "retriable" would burn a retry on an error that
# fails identically every time.
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
    ena_run_accession VARCHAR)` — sourced from a LIVE Postgres query
    (`runner._read_ingest._stage_ena_run_roster`), unlike bcl-convert's
    action-context-embedded `sample_map`. `reads_staging_root` is the scratch
    staging root the durable per-sample `read.parquet` copies hang under (via
    `compute_reads_staging_path`). `download_method` is the ENA transport
    (`params: {download_method: download_method}` in the workflow YAML,
    optional — defaults to 'http', the only transport this compute
    environment supports; see ARCHITECTURE.md's ENA Study Import
    download-ticket-granularity decision). `sequenced_pool_idx` / `sequencing_run_idx` /
    `work_ticket_idx` are the framework-injected scope scalars for the
    sequenced_pool-scoped download-ena-study ticket."""

    run_map: Path
    reads_staging_root: Path
    download_method: str = "http"
    sequenced_pool_idx: int
    sequencing_run_idx: int
    work_ticket_idx: int


def _read_run_map(path: Path) -> list[tuple[int, str]]:
    """Read the `(prep_sample_idx, ena_run_accession)` roster from the staged
    Parquet. Ordered by prep_sample_idx for deterministic processing / error
    reporting. Raises ValueError (BAD_INPUT via the dispatcher) on an empty or
    unreadable roster — the CP's `_stage_ena_run_roster` already fails loud on
    an empty pool, so an empty file here would indicate a resolver/dispatch
    bug, not a legitimate empty ticket."""
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
    """Filter `messages` (as returned by `_stage_run_reads`) down to the ones
    that mean this run's data is missing or partial. See the
    `_SKIP_WARNING_MARKER` module constant for why a plain substring match is
    sufficient to separate a genuine skip/truncation from a self-healed
    retry."""
    return [m for m in messages if _SKIP_WARNING_MARKER in m.lower()]


def _stage_run_reads(
    run_accession: str,
    download_method: str,
    intermediate_path: Path,
    duckdb_tmp: Path,
    memory_gb: int,
    threads: int,
) -> tuple[int, list[str]]:
    """Fetch one ENA run's reads via miint's `read_ena_sequences` into a
    transient intermediate Parquet at `intermediate_path`, keyed by miint's
    1-based per-run `sequence_index`, on a FRESH per-run DuckDB connection
    (`open_miint_ena_conn` — LOAD miint + httpfs). Returns `(row_count,
    warning_messages)`.

    EXPLICIT 6-column projection: `read_ena_sequences` also returns `comment`
    / `run_accession` / `sample_accession` / `experiment_accession`, which
    this job does not need (the caller already knows all four from the
    roster / scope) — so they are dropped rather than carried into the
    intermediate.

    `warning_messages` is read from `miint_warnings()` on the SAME connection
    immediately after the COPY — a fresh connection's log is empty until this
    call populates it, so every message returned here is about EXACTLY this
    run (see the module docstring's "one connection PER RUN" rationale). This
    function does not interpret the messages — it surfaces every one it sees;
    the caller decides which (if any) mean the run's data is incomplete
    (`_skip_warnings`).

    Raises `duckdb.Error` on a raised transport/format failure (e.g. ENA
    metadata resolution failing outright before any per-run reader exists) —
    the caller classifies retriable vs permanent
    (`_classify_ena_fetch_error`). Does NOT raise for an internal
    skip/partial-download: miint's own per-run open-retry-then-skip and
    mid-stream-failure paths return normally (with fewer, or zero, rows and a
    warning), which is exactly why the caller must inspect
    `warning_messages` rather than trusting a clean return."""
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
    (a transport/network-shaped failure) or permanent (an md5-verification
    failure, a format/parse failure, or anything not confidently
    network-shaped). A raised exception here means miint's own internal
    open-retry-then-skip did NOT run — e.g. ENA Portal metadata resolution for
    `run_accession` failed before any per-run reader was even constructed, or
    miint's own md5 tap detected corruption after streaming a run to true EOF
    — so there is no `miint_warnings()` entry to inspect; the exception text
    is all the caller has.

    The transient-marker check runs FIRST, so a (hypothetical) message that
    mentions both md5 and a transient marker (e.g. a network reset that
    interrupted md5-tap streaming) still classifies transient — the explicit
    md5 branch below only ever sees text that has already been ruled out as
    network-shaped."""
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
    module docstring for the per-run pipeline, the fail-loud fetch-warning
    check, and the idempotency model."""
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
        """Store one run's reads. Returns `"registered"`; raises (→
        dispatcher) on any failure — unlike `ingest_reads`, there is no
        legitimate "empty" outcome for an ENA run (see the module docstring),
        so every run either registers or fails the whole step."""
        async with sem:
            durable = compute_reads_staging_path(inputs.reads_staging_root, prep_sample_idx)
            part = register_dir / f"{prep_sample_idx}.parquet"

            # Idempotent fast path: reads already stored on a prior attempt.
            # Re-create the register hardlink (the prior workspace is gone) so
            # the retry still registers this run.
            if durable.exists():
                hardlink(durable, part)
                return "registered"

            durable.parent.mkdir(parents=True, exist_ok=True)
            intermediate = durable.parent / "_intermediate_reads.parquet"
            # Per-run DuckDB temp dir so concurrent slots never collide on spill.
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
                    # No skip warning explains a zero-read result. Unlike an
                    # empty demux well (ingest_reads' legitimate, numerous
                    # "empty well" case), an ENA run producing zero reads with
                    # no explanatory miint_warnings() entry is anomalous --
                    # fail loud rather than silently register nothing.
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

    # Surface the first hard error in roster order — matches ingest_reads'
    # abort-on-first behavior and preserves the dispatcher's wrapping of bare
    # ValueError / duckdb.Error. Independent runs that already completed have
    # only written their own durable copy, which is harmless.
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            raise outcome

    # `read_staging_dir` is the workspace: register-files finds the `read/`
    # subdir of per-run parts and loads them all into the `read` table.
    return {"read_staging_dir": workspace}
