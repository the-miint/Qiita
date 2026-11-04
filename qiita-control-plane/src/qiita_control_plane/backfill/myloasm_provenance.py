"""Submit one `myloasm-provenance-recover` ticket per myloasm run that finished
before long-read-assembly wrote the `myloasm_*` provenance tables (1.0.2 writes
them itself).

Unlike the other backfills this one writes nothing itself: the recovery runs as a
work ticket, because it needs miint and the data plane. Each run is resolved to the
`assemble` attempt whose output that ticket reads: the newest completed ticket of
the run's workflow version, prep_sample, mask and assembler, then that ticket's
highest completed `assemble` attempt, then its output on scratch. The job checks
the tree against the run's lake rows before anything is written; this only finds it.

**Done means a completed recovery ticket.** The control plane cannot read the lake,
so a run counts as recovered when a recovery ticket naming it in
`action_context.assembly_processing_idx` completed. register-files replaces a run's
rows, so a re-submitted run is harmless.

**Residue.** A run whose ticket, step row or tree cannot be found, or whose
recovery ticket failed, is listed with the reason rather than guessed at. A run
whose `assembly_sample` is not `completed`, or whose processing is deprecated, is
skipped and counted.

A one-off; `workflows/myloasm-provenance-recover/1.0.0.yaml`'s header names the
issue that removes it.
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
from qiita_common.actions import LONG_READ_ASSEMBLY_ACTION_ID
from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    ProcessingStatus,
    ScopeTargetKind,
    StepProgressState,
    WorkTicketState,
)

from ..repositories.assembly import ASSEMBLY_SAMPLE_COMPLETED
from ..runner._mask import MASK_IDX_BINDING
from ..runner._processing import ASSEMBLER_BINDING
from ..workspace import (
    STEP_MANIFEST_FILENAME,
    step_attempt_dir,
    step_output_dir,
    ticket_workspace,
)

RECOVER_ACTION_ID = "myloasm-provenance-recover"
RECOVER_ACTION_VERSION = "1.0.0"
# The long-read-assembly versions that predate the provenance tables.
SOURCE_VERSIONS = ("1.0.0", "1.0.1")

# The `assembler` value of a myloasm run, and the step and subdirectory that the
# recovery reads — the names in workflows/long-read-assembly/<version>.yaml and
# assemble.sh.
_MYLOASM = "myloasm"
_ASSEMBLE_STEP = "assemble"
_ASSEMBLER_DIR = "assembler"
# The recovery's `action_context` keys (its workflow's `context_schema`).
_PROCESSING_KEY = "assembly_processing_idx"
_MASK_KEY = "assembly_mask_idx"
_SOURCE_TICKET_KEY = "source_work_ticket_idx"
_ATTEMPT_KEY = "assemble_attempt"

# Every run of a source version's myloasm processing, with its source ticket and
# attempt (NULL when either is missing) and its newest recovery ticket (NULL when
# none). An omitted `assembler` on a ticket is the action's context_schema default,
# which is how the runner resolved it. `_build_processing_params`
# (runner/_processing.py) writes `processing.params` under keys equal to
# `MASK_IDX_BINDING` and `ASSEMBLER_BINDING`, so the query reads it with those.
_RUNS_SQL = """
SELECT s.prep_sample_idx, s.processing_idx, s.state AS sample_state,
       p.status AS processing_status, (p.params->>$9::text)::bigint AS mask_idx,
       t.work_ticket_idx AS source_ticket, st.attempt,
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
           AND wt.mask_idx = (p.params->>$9::text)::bigint
           AND COALESCE(wt.action_context->>$3::text,
                        a.context_schema->'properties'->$3::text->>'default') = $4
           AND wt.state = $5::qiita.work_ticket_state
         ORDER BY wt.created_at DESC, wt.work_ticket_idx DESC
         LIMIT 1
       ) t ON true
  LEFT JOIN LATERAL (
        SELECT wts.attempt
          FROM qiita.work_ticket_step wts
         WHERE wts.work_ticket_idx = t.work_ticket_idx
           AND wts.step_name = $6
           AND wts.state = $7
         ORDER BY wts.attempt DESC
         LIMIT 1
       ) st ON true
  LEFT JOIN LATERAL (
        SELECT rt.work_ticket_idx, rt.state, rt.failure_reason
          FROM qiita.work_ticket rt
         WHERE rt.action_id = $8
           AND rt.prep_sample_idx = s.prep_sample_idx
           AND rt.action_context->$10::text = to_jsonb(s.processing_idx)
         ORDER BY rt.created_at DESC, rt.work_ticket_idx DESC
         LIMIT 1
       ) r ON true
 WHERE p.workflow = $1
   AND p.version = ANY($2::text[])
   AND p.params->>$3::text = $4
 ORDER BY s.prep_sample_idx, s.processing_idx
"""


class RunStatus(StrEnum):
    WRITABLE = "writable"  # a source was found; submit a recovery ticket
    DONE = "done"  # a recovery ticket for the run completed
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
    NO_ASSEMBLE_STEP = "no completed assemble step"
    NO_MANIFEST = "no manifest"
    NO_ASSEMBLER_TREE = "no assembler tree"


@dataclass(frozen=True, slots=True)
class Run:
    prep_sample_idx: int
    processing_idx: int
    mask_idx: int
    status: RunStatus
    reason: str | None = None
    residue_kind: ResidueKind | None = None
    source_work_ticket_idx: int | None = None
    assemble_attempt: int | None = None

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
        return {
            "action_id": RECOVER_ACTION_ID,
            "action_version": RECOVER_ACTION_VERSION,
            "scope_target": {
                "kind": ScopeTargetKind.PREP_SAMPLE.value,
                "prep_sample_idx": self.prep_sample_idx,
            },
            "action_context": {
                _PROCESSING_KEY: self.processing_idx,
                _MASK_KEY: self.mask_idx,
                _SOURCE_TICKET_KEY: self.source_work_ticket_idx,
                _ATTEMPT_KEY: self.assemble_attempt,
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


def _is(path: Path, mode_test: Callable[[int], bool]) -> bool:
    """Whether `path` exists and passes `mode_test` (`stat.S_ISREG`, `stat.S_ISDIR`).
    Only absence reads as False: a path that cannot be looked at (no permission to
    traverse its parent) raises, where `Path.is_file()` would report it missing."""
    try:
        return mode_test(path.stat().st_mode)
    except FileNotFoundError, NotADirectoryError:
        return False


def _source_tree_residue(output: Path) -> tuple[ResidueKind, str] | None:
    """Why the attempt's output cannot be read, or None if it can."""
    if not _is(output / STEP_MANIFEST_FILENAME, stat.S_ISREG):
        kind = ResidueKind.NO_MANIFEST
        return kind, f"{kind} at {output / STEP_MANIFEST_FILENAME}"
    if not _is(output / _ASSEMBLER_DIR, stat.S_ISDIR):
        kind = ResidueKind.NO_ASSEMBLER_TREE
        return kind, f"{kind} at {output / _ASSEMBLER_DIR}"
    return None


def _classify(row: asyncpg.Record, *, ticket_root: Path, retry_failed: bool) -> Run:
    key = {
        "prep_sample_idx": row["prep_sample_idx"],
        "processing_idx": row["processing_idx"],
        "mask_idx": row["mask_idx"],
    }
    if row["sample_state"] != ASSEMBLY_SAMPLE_COMPLETED:
        return Run(**key, status=RunStatus.SKIPPED, reason=f"assembly is {row['sample_state']}")
    if row["processing_status"] == ProcessingStatus.DEPRECATED:
        return Run(**key, status=RunStatus.SKIPPED, reason="run is deprecated")

    recover, recover_state = row["recover_ticket"], row["recover_state"]
    if recover_state == WorkTicketState.COMPLETED:
        return Run(**key, status=RunStatus.DONE)
    if recover_state in NON_TERMINAL_WORK_TICKET_STATES:
        return Run(
            **key,
            status=RunStatus.IN_FLIGHT,
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

    if row["source_ticket"] is None:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.NO_SOURCE_TICKET,
            reason=f"no completed {LONG_READ_ASSEMBLY_ACTION_ID} ticket for the run",
        )
    if row["attempt"] is None:
        return Run(
            **key,
            status=RunStatus.RESIDUE,
            residue_kind=ResidueKind.NO_ASSEMBLE_STEP,
            reason=f"ticket {row['source_ticket']} has no completed {_ASSEMBLE_STEP!r} step",
        )
    output = step_output_dir(
        step_attempt_dir(
            ticket_workspace(ticket_root, row["source_ticket"]), _ASSEMBLE_STEP, row["attempt"]
        )
    )
    residue = _source_tree_residue(output)
    if residue is not None:
        kind, reason = residue
        return Run(**key, status=RunStatus.RESIDUE, residue_kind=kind, reason=reason)
    return Run(
        **key,
        status=RunStatus.WRITABLE,
        source_work_ticket_idx=row["source_ticket"],
        assemble_attempt=row["attempt"],
    )


async def plan_backfill(
    pool: asyncpg.Pool,
    *,
    ticket_root: Path,
    retry_failed: bool = False,
    source_versions: tuple[str, ...] = SOURCE_VERSIONS,
) -> BackfillPlan:
    """Read-only: every myloasm run of `source_versions`, classified.

    `ticket_root` is the per-ticket workspace root (`PATH_SCRATCH/ticket`).
    `retry_failed` makes a run whose recovery ticket ended without completing
    (failed, cancelled or no_data) writable again.
    """
    rows = await pool.fetch(
        _RUNS_SQL,
        LONG_READ_ASSEMBLY_ACTION_ID,
        list(source_versions),
        ASSEMBLER_BINDING,
        _MYLOASM,
        WorkTicketState.COMPLETED.value,
        _ASSEMBLE_STEP,
        StepProgressState.COMPLETED.value,
        RECOVER_ACTION_ID,
        MASK_IDX_BINDING,
        _PROCESSING_KEY,
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
