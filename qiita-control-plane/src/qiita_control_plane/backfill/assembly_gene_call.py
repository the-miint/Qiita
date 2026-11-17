"""Submit one `assembly-gene-call-recover` ticket per long-read-assembly run that
completed without gene calls: every run of 1.0.0 and 1.0.1, and the 1.0.2 runs that
completed before 1.0.2 gained its gene_call step.

It writes nothing itself: the recovery runs
as a work ticket, because it needs miint and the data plane. Each run is resolved to
the step and action attempts whose outputs that ticket reads: the newest completed
ticket of the run's workflow version, prep_sample, mask and assembler, then that
ticket's highest completed attempt of each entry in `SOURCE_ENTRIES`, then their
outputs on scratch. The jobs read the outputs; this only finds them.

**Done** means a completed recovery ticket naming the run in
`action_context.assembly_processing_idx` (the control plane cannot read the lake), or
a source ticket that completed a `gene_call` step itself. register-files replaces a
run's rows, so a re-submitted run is harmless.

**Residue.** A run whose ticket, step rows or outputs cannot be found, whose
`qiita.assembly_membership` rows lack a genome (export-subject-genome-map refuses
them), or whose recovery ticket ended without completing, is listed with the reason
rather than guessed at. A run whose `assembly_sample` is not `completed`, or whose
processing is deprecated, is skipped and counted.

A one-off; `workflows/assembly-gene-call-recover/1.0.0.yaml`'s header says when it is
removed.
"""

from __future__ import annotations

import stat
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

import asyncpg
import httpx
from qiita_common.actions import LONG_READ_ASSEMBLY_ACTION_ID, MINT_FEATURES_OUTPUT_BASENAME
from qiita_common.api_paths import LibraryPrimitive
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    ProcessingStatus,
    ScopeTargetKind,
    StepProgressState,
    WorkTicketState,
)

from ..repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_INVALIDATED,
    ASSEMBLY_SAMPLE_NO_DATA,
    ASSEMBLY_SAMPLE_PENDING,
)
from ..runner import ASSEMBLY_PROCESSING_IDX_BINDING
from ..runner._mask import MASK_IDX_BINDING
from ..runner._processing import ASSEMBLER_BINDING
from ..workspace import (
    STEP_MANIFEST_FILENAME,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

RECOVER_ACTION_ID = "assembly-gene-call-recover"
RECOVER_ACTION_VERSION = "1.0.0"
# The long-read-assembly versions whose runs can lack gene calls.
SOURCE_VERSIONS = ("1.0.0", "1.0.1", "1.0.2")

# The step a source ticket that called genes itself completed, as 1.0.2 names it.
_GENE_CALL_STEP = "gene_call"
# Each long-read-assembly entry the recovery reads, as every source version names it,
# and the recovery's `action_context` key (its workflow's `context_schema`) carrying
# the entry's completed attempt. The first four are steps, read through their
# manifests; mint-features is an action, read through the file it writes.
SOURCE_STEPS = ("assemble", "bin_refine", "checkm", "assembly_hash")
SOURCE_ENTRIES = (*SOURCE_STEPS, LibraryPrimitive.MINT_FEATURES.value)
_ATTEMPT_KEY = {
    "assemble": "assemble_attempt",
    "bin_refine": "bin_refine_attempt",
    "checkm": "checkm_attempt",
    "assembly_hash": "assembly_hash_attempt",
    LibraryPrimitive.MINT_FEATURES.value: "mint_features_attempt",
}
_SOURCE_TICKET_KEY = "source_work_ticket_idx"

# Every run of a source version, with its source ticket (NULL when none), that
# ticket's highest completed attempt of each entry in `SOURCE_ENTRIES` (as parallel
# arrays, empty when none), whether it completed a gene_call step, the run's
# Postgres membership counts, and its newest recovery ticket (NULL when none). An
# omitted `assembler` on a ticket is the action's context_schema default, which is
# how the runner resolved it. `_build_processing_params` (runner/_processing.py)
# writes `processing.params` under keys equal to `MASK_IDX_BINDING` and
# `ASSEMBLER_BINDING`, so the query reads it with those.
_RUNS_SQL = """
SELECT s.prep_sample_idx, s.processing_idx, s.state AS sample_state,
       p.status AS processing_status,
       t.work_ticket_idx AS source_ticket,
       COALESCE(st.entries, '{}'::text[]) AS entries,
       COALESCE(st.attempts, '{}'::int[]) AS attempts,
       COALESCE(gc.called, false) AS called_genes,
       m.total AS membership_rows, m.missing AS membership_without_genome,
       r.work_ticket_idx AS recover_ticket, r.state AS recover_state,
       r.failure_reason AS recover_failure
  FROM qiita.assembly_sample s
  JOIN qiita.processing p USING (processing_idx)
  LEFT JOIN LATERAL (
        SELECT wt.work_ticket_idx
          FROM qiita.work_ticket wt
          JOIN qiita.action a ON a.action_id = wt.action_id AND a.version = wt.action_version
         WHERE wt.action_id = p.workflow
           AND wt.action_version = p.version
           AND wt.prep_sample_idx = s.prep_sample_idx
           AND wt.mask_idx = (p.params->>$3::text)::bigint
           AND COALESCE(wt.action_context->>$4::text,
                        a.context_schema->'properties'->$4::text->>'default')
               = p.params->>$4::text
           AND wt.state = $5::qiita.work_ticket_state
         ORDER BY wt.created_at DESC, wt.work_ticket_idx DESC
         LIMIT 1
       ) t ON true
  LEFT JOIN LATERAL (
        SELECT array_agg(step_name ORDER BY step_name) AS entries,
               array_agg(attempt ORDER BY step_name) AS attempts
          FROM (SELECT wts.step_name, max(wts.attempt) AS attempt
                  FROM qiita.work_ticket_step wts
                 WHERE wts.work_ticket_idx = t.work_ticket_idx
                   AND wts.step_name = ANY($6::text[])
                   AND wts.state = $7
                 GROUP BY wts.step_name) done
       ) st ON true
  LEFT JOIN LATERAL (
        SELECT true AS called
          FROM qiita.work_ticket_step wts
         WHERE wts.work_ticket_idx = t.work_ticket_idx
           AND wts.step_name = $8
           AND wts.state = $7
         LIMIT 1
       ) gc ON true
  CROSS JOIN LATERAL (
        SELECT count(*) AS total, count(*) FILTER (WHERE am.genome_idx IS NULL) AS missing
          FROM qiita.assembly_membership am
         WHERE am.prep_sample_idx = s.prep_sample_idx
           AND am.processing_idx = s.processing_idx
       ) m
  LEFT JOIN LATERAL (
        SELECT rt.work_ticket_idx, rt.state, rt.failure_reason
          FROM qiita.work_ticket rt
         WHERE rt.action_id = $9
           AND rt.prep_sample_idx = s.prep_sample_idx
           AND rt.action_context->$10::text = to_jsonb(s.processing_idx)
         ORDER BY rt.created_at DESC, rt.work_ticket_idx DESC
         LIMIT 1
       ) r ON true
 WHERE p.workflow = $1
   AND p.version = ANY($2::text[])
 ORDER BY s.prep_sample_idx, s.processing_idx
"""


class RunStatus(StrEnum):
    WRITABLE = "writable"  # a source was found; submit a recovery ticket
    DONE = "done"  # the run has gene calls, or a recovery ticket for it completed
    IN_FLIGHT = "in_flight"  # a recovery ticket for the run is still running
    SKIPPED = "skipped"  # nothing to recover
    RESIDUE = "residue"  # needs a person: the reason says why


class ResidueKind(StrEnum):
    """The reason that a run is residue, as a fixed label that names no run. The
    dry run prints a count per kind, which can be posted where an `_idx` must not
    appear."""

    RECOVERY_FAILED = "recovery ticket failed"
    RECOVERY_CANCELLED = "recovery ticket cancelled"
    RECOVERY_NO_DATA = "recovery ticket ended no_data"
    NO_SOURCE_TICKET = "no completed source ticket"
    NO_COMPLETED_ENTRY = "a source entry with no completed attempt"
    NO_MEMBERSHIP = "no assembly_membership rows"
    MEMBERSHIP_WITHOUT_GENOME = "assembly_membership rows without a genome"
    NO_MANIFEST = "no step manifest"
    NO_FEATURE_MAP = "no mint-features map"


@dataclass(frozen=True, slots=True)
class Run:
    prep_sample_idx: int
    processing_idx: int
    status: RunStatus
    reason: str | None = None
    residue_kind: ResidueKind | None = None
    source_work_ticket_idx: int | None = None
    # The completed attempt of each entry in `SOURCE_ENTRIES`, for a writable run.
    attempts: dict[str, int] | None = None

    def __post_init__(self) -> None:
        if (self.residue_kind is not None) != (self.status is RunStatus.RESIDUE):
            raise ValueError(
                f"run {self.processing_idx}: a residue kind goes with residue status, "
                f"got {self.status} with {self.residue_kind!r}"
            )

    def submit_body(self) -> dict:
        """The `POST /work-ticket` body that recovers this run."""
        if self.status is not RunStatus.WRITABLE:
            raise ValueError(f"run {self.processing_idx} is {self.status}, not writable")
        if self.attempts is None or set(self.attempts) != set(SOURCE_ENTRIES):
            raise ValueError(
                f"run {self.processing_idx}: attempts must name exactly {SOURCE_ENTRIES}, "
                f"got {self.attempts!r}"
            )
        return {
            "action_id": RECOVER_ACTION_ID,
            "action_version": RECOVER_ACTION_VERSION,
            "scope_target": {
                "kind": ScopeTargetKind.PREP_SAMPLE.value,
                "prep_sample_idx": self.prep_sample_idx,
            },
            "action_context": {
                ASSEMBLY_PROCESSING_IDX_BINDING: self.processing_idx,
                _SOURCE_TICKET_KEY: self.source_work_ticket_idx,
                **{_ATTEMPT_KEY[e]: self.attempts[e] for e in SOURCE_ENTRIES},
            },
        }


@dataclass(frozen=True, slots=True)
class BackfillPlan:
    runs: list[Run] = field(default_factory=list)

    def by_status(self, status: RunStatus) -> list[Run]:
        return [r for r in self.runs if r.status is status]


# The residue kind of a run whose newest recovery ticket ended without completing.
_RECOVERY_ENDED = {
    WorkTicketState.FAILED: ResidueKind.RECOVERY_FAILED,
    WorkTicketState.CANCELLED: ResidueKind.RECOVERY_CANCELLED,
    WorkTicketState.NO_DATA: ResidueKind.RECOVERY_NO_DATA,
}

# The reasons a run is done, id-free so the dry run can count them.
DONE_BY_RECOVERY = "recovery ticket completed"
DONE_BY_SOURCE = "source ticket called genes"


def _is(path: Path, mode_test: Callable[[int], bool]) -> bool:
    """Whether `path` exists and passes `mode_test` (`stat.S_ISREG`, `stat.S_ISDIR`).
    Only absence reads as False: a path that cannot be looked at (no permission to
    traverse its parent) raises, where `Path.is_file()` would report it missing."""
    try:
        return mode_test(path.stat().st_mode)
    except FileNotFoundError, NotADirectoryError:
        return False


def _source_residue(ticket_dir: Path, attempts: dict[str, int]) -> tuple[ResidueKind, str] | None:
    """Why the source's outputs cannot be read, or None if they can: each step's
    manifest, and mint-features' map."""
    for step in SOURCE_STEPS:
        manifest = (
            step_output_dir(step_attempt_dir(ticket_dir, step, attempts[step]))
            / STEP_MANIFEST_FILENAME
        )
        if not _is(manifest, stat.S_ISREG):
            kind = ResidueKind.NO_MANIFEST
            return kind, f"{kind} at {manifest}"
    mint = LibraryPrimitive.MINT_FEATURES.value
    feature_map = step_attempt_dir(ticket_dir, mint, attempts[mint]) / MINT_FEATURES_OUTPUT_BASENAME
    if not _is(feature_map, stat.S_ISREG):
        kind = ResidueKind.NO_FEATURE_MAP
        return kind, f"{kind} at {feature_map}"
    return None


def _classify(row: asyncpg.Record, *, ticket_root: Path, retry_failed: bool) -> Run:
    key = {"prep_sample_idx": row["prep_sample_idx"], "processing_idx": row["processing_idx"]}
    state = row["sample_state"]
    if state in (ASSEMBLY_SAMPLE_NO_DATA, ASSEMBLY_SAMPLE_INVALIDATED):
        return Run(**key, status=RunStatus.SKIPPED, reason=f"assembly is {state}")
    if state == ASSEMBLY_SAMPLE_PENDING:
        # Not assembled yet: a ticket that completes it runs 1.0.2, which calls the genes.
        return Run(**key, status=RunStatus.SKIPPED, reason="assembly is pending")
    if state != ASSEMBLY_SAMPLE_COMPLETED:
        raise ValueError(f"assembly_sample state {state!r} is not one this backfill knows")
    if row["processing_status"] == ProcessingStatus.DEPRECATED:
        return Run(**key, status=RunStatus.SKIPPED, reason="run is deprecated")

    recover, recover_state = row["recover_ticket"], row["recover_state"]
    if recover_state == WorkTicketState.COMPLETED:
        return Run(**key, status=RunStatus.DONE, reason=DONE_BY_RECOVERY)
    if row["called_genes"]:
        return Run(**key, status=RunStatus.DONE, reason=DONE_BY_SOURCE)
    if recover_state in NON_TERMINAL_WORK_TICKET_STATES:
        return Run(
            **key,
            status=RunStatus.IN_FLIGHT,
            source_work_ticket_idx=row["source_ticket"],
            reason=f"recovery ticket {recover} is {recover_state}",
        )
    if recover is not None and not retry_failed:
        failure = row["recover_failure"]
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            # A state outside the map is one this planner does not know: KeyError.
            residue_kind=_RECOVERY_ENDED[WorkTicketState(recover_state)],
            reason=f"recovery ticket {recover} {recover_state}"
            + (f": {failure}" if failure else ""),
        )

    source = row["source_ticket"]
    if source is None:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.NO_SOURCE_TICKET,
            reason=f"no completed {LONG_READ_ASSEMBLY_ACTION_ID} ticket for the run",
        )
    attempts = dict(zip(row["entries"], row["attempts"], strict=True))
    missing = [e for e in SOURCE_ENTRIES if e not in attempts]
    if missing:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.NO_COMPLETED_ENTRY,
            reason=f"ticket {source} has no completed attempt of {', '.join(missing)}",
        )
    if row["membership_rows"] == 0:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.NO_MEMBERSHIP,
            reason=ResidueKind.NO_MEMBERSHIP.value,
        )
    if row["membership_without_genome"]:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.MEMBERSHIP_WITHOUT_GENOME,
            reason=f"{row['membership_without_genome']} of {row['membership_rows']} "
            "assembly_membership rows without a genome",
        )
    residue = _source_residue(ticket_workspace(ticket_root, source), attempts)
    if residue is not None:
        kind, reason = residue
        return Run(**key, status=RunStatus.RESIDUE, residue_kind=kind, reason=reason)
    return Run(**key, status=RunStatus.WRITABLE, source_work_ticket_idx=source, attempts=attempts)


async def plan_backfill(
    pool: asyncpg.Pool,
    *,
    ticket_root: Path,
    retry_failed: bool = False,
    source_versions: tuple[str, ...] = SOURCE_VERSIONS,
) -> BackfillPlan:
    """Read-only: every long-read-assembly run of `source_versions`, classified.

    `ticket_root` is the per-ticket workspace root (`PATH_SCRATCH/ticket`).
    `retry_failed` makes a run whose recovery ticket ended without completing
    (failed, cancelled or no_data) writable again.
    """
    rows = await pool.fetch(
        _RUNS_SQL,
        LONG_READ_ASSEMBLY_ACTION_ID,
        list(source_versions),
        MASK_IDX_BINDING,
        ASSEMBLER_BINDING,
        WorkTicketState.COMPLETED.value,
        list(SOURCE_ENTRIES),
        StepProgressState.COMPLETED.value,
        _GENE_CALL_STEP,
        RECOVER_ACTION_ID,
        ASSEMBLY_PROCESSING_IDX_BINDING,
    )
    return BackfillPlan(
        runs=[_classify(r, ticket_root=ticket_root, retry_failed=retry_failed) for r in rows]
    )


@dataclass(slots=True)
class SubmitReport:
    submitted: list[tuple[Run, int]] = field(default_factory=list)
    deferred: list[Run] = field(default_factory=list)
    failed: list[tuple[Run, str]] = field(default_factory=list)
    # Writable runs left for a later pass by `limit`.
    not_attempted: list[Run] = field(default_factory=list)


def submit_runs(
    plan: BackfillPlan,
    *,
    submit: Callable[[dict], dict],
    limit: int | None = None,
    rate_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
) -> SubmitReport:
    """Submit a recovery ticket for each writable run, through `submit` (a
    `POST /work-ticket` returning the response body).

    At most one per prep_sample per pass: the in-flight gate admits one ticket per
    action and prep_sample whatever its context, so a second run of a prep_sample,
    or one whose prep_sample has a recovery ticket in flight, is deferred to the
    next pass. A refused submission is recorded and the loop carries on. `limit`
    caps the attempts, refused ones included, and the writable runs past it are
    reported as not attempted; `rate_seconds` is slept between attempts.
    """
    if limit is not None and limit <= 0:
        raise ValueError(f"limit must be positive, got {limit}")
    busy = {r.prep_sample_idx for r in plan.by_status(RunStatus.IN_FLIGHT)}
    report = SubmitReport()
    attempts = 0
    for run in plan.by_status(RunStatus.WRITABLE):
        if run.prep_sample_idx in busy:
            report.deferred.append(run)
            continue
        if limit is not None and attempts >= limit:
            report.not_attempted.append(run)
            continue
        if attempts and rate_seconds > 0:
            sleep(rate_seconds)
        attempts += 1
        busy.add(run.prep_sample_idx)
        try:
            body = submit(run.submit_body())
        except httpx.HTTPStatusError as exc:
            report.failed.append((run, f"{exc.response.status_code}: {exc.response.text}"))
            continue
        except httpx.HTTPError as exc:
            report.failed.append((run, f"{type(exc).__name__}: {exc}"))
            continue
        report.submitted.append((run, body["work_ticket_idx"]))
    return report
