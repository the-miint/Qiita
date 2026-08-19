"""Typed failure surface for compute backends.

A backend (LocalBackend, SlurmBackend, future cloud backends) raises
`BackendFailure` when a step fails for a reason the workflow itself
should know about — bad input, transient infra, container-contract
violations, etc. Plain Python exceptions (KeyError, TypeError, ...) are
*not* wrapped; they represent programming bugs and must surface
unchanged so they crash loudly.

The runner consumes `BackendFailure.kind`'s `.transient` property to
decide:

  - retriable + retry_count < max_retries → bump retry_count, transition
    PROCESSING → QUEUED, retry the failing step.
  - retriable + retry_count >= max_retries → transition to FAILED,
    persist failure_type='retriable' (so post-mortems can tell
    "exhausted retries" from "permanent on first attempt").
  - permanent → skip the retry loop, transition straight to FAILED with
    failure_type='permanent'.

Adding a new `FailureKind` requires only updating `_RETRIABLE` if it
should retry. The persisted `failure_type` (qiita.failure_type) is the
two-valued discriminator the DB stores; FailureKind is the finer-grained
classification surfaced in `failure_reason` for ops triage.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from pydantic import BaseModel

from .models import WorkTicketFailureStage

# Wire-format discriminator for BackendFailure round-tripping through the
# orchestrator's /step/* HTTP boundary. The orchestrator sets this
# header on responses that carry a BackendFailureBody so the client can
# reconstruct a typed BackendFailure (and the runner sees the same
# transient/permanent classification it would in-process). Without the
# header, the response is treated as a generic HTTP error and falls
# through to raise_for_status.
BACKEND_FAILURE_HEADER = "X-Qiita-Backend-Failure"

# 422 is shared with the ValueError → HTTPException path in step.py
# (route argument validation). The header above is the disambiguator;
# bodies are shaped differently (BackendFailureBody fields vs FastAPI's
# {"detail": ...}).
BACKEND_FAILURE_HTTP_STATUS = 422

# Wire-format discriminator for StepNoData round-tripping through the
# orchestrator's /step/* HTTP boundary, parallel to BACKEND_FAILURE_HEADER.
# StepNoData is a TERMINAL-SUCCESS-ISH outcome, NOT a failure: a step
# (fastq_to_parquet on an empty well) exits without minting identifiers or
# writing output, and the runner transitions the ticket to NO_DATA rather than
# FAILED. It rides its own header so the client reconstructs the distinct typed
# signal and never confuses it with a BackendFailure. Reuses the same 422 status
# as BackendFailure — the header is the only disambiguator.
STEP_NO_DATA_HEADER = "X-Qiita-Step-No-Data"
STEP_NO_DATA_HTTP_STATUS = 422


class FailureKind(StrEnum):
    """Backend-emitted failure category. Finer-grained than the DB
    `failure_type` enum (which only carries retriable vs permanent);
    used in `failure_reason` and in logs for triage.

    Adding a value: place it in the appropriate group below and, if
    retriable, add it to `_RETRIABLE`.
    """

    # ---- Retriable: transient infra / scheduling issues ------------------
    NODE_FAIL = "node_fail"
    OOM_KILLED = "oom_killed"
    TIMEOUT_BEFORE_START = "timeout_before_start"
    PREEMPTED = "preempted"
    TRANSIENT_FS_ERROR = "transient_fs_error"
    SLURMRESTD_UNREACHABLE = "slurmrestd_unreachable"
    # CP could not reach the orchestrator (httpx transport/timeout on a
    # submit/status/result call). Like SLURMRESTD_UNREACHABLE it is an
    # infra-reachability hiccup, NEVER a statement that the step failed —
    # the runner's poll loop keeps retrying the same call rather than
    # failing the ticket or resubmitting. This is the direct fix for the
    # old 600s held-connection bug: a long step no longer self-fails when
    # the CP→CO hop times out.
    ORCHESTRATOR_UNREACHABLE = "orchestrator_unreachable"
    # A native job could not reach the control plane on a callback (httpx
    # transport/timeout error, or an HTTP 5xx) — e.g. the per-sample
    # `POST /sequence-range` call the `ingest_reads` step makes back to the CP.
    # The exact mirror of ORCHESTRATOR_UNREACHABLE for the CO→CP direction: an
    # infra-reachability blip on one of a pool's N callbacks, NEVER a statement
    # that the step's work is broken. The step retries the call in-job first;
    # only if that's exhausted does it raise this so the runner re-dispatches
    # the (idempotent) step rather than discarding hours of demux over one blip.
    CONTROL_PLANE_UNREACHABLE = "control_plane_unreachable"
    PROCESS_RESTARTED = "process_restarted"  # CP drain cancelled a task
    # The data plane returned a transient, retriable error over Flight — e.g. a
    # DuckLake catalog write that lost a Postgres serialization race under
    # concurrent attaches (SQLSTATE 40001, "could not serialize access due to
    # concurrent update"). NOT a malformed request: the same call succeeds once
    # the contention clears, so a redrive self-heals.
    DATA_PLANE_TRANSIENT = "data_plane_transient"

    # ---- Permanent: workflow / input / contract issues -------------------
    BAD_INPUT = "bad_input"
    EXIT_NONZERO = "exit_nonzero"
    CONTRACT_VIOLATION = "contract_violation"
    # A step exhausted its retriable resource escalation: it OOM-killed at the
    # action's mem ceiling, or timed out at the action's walltime ceiling. The
    # triggering kind (OOM_KILLED / TIMEOUT_BEFORE_START) is itself retriable,
    # but once the escalated floor is pinned at the ceiling a re-run would fail
    # identically — so the runner reclassifies it as permanent rather than
    # burning the remaining retry budget (and a SLURM allocation) on a
    # guaranteed repeat. Operator fix: raise the action ceiling or shrink the
    # input. Raised by the runner, never emitted by a backend over the wire.
    RESOURCE_CEILING_EXHAUSTED = "resource_ceiling_exhausted"
    UNKNOWN_PERMANENT = "unknown_permanent"


_RETRIABLE: frozenset[FailureKind] = frozenset(
    {
        FailureKind.NODE_FAIL,
        FailureKind.OOM_KILLED,
        FailureKind.TIMEOUT_BEFORE_START,
        FailureKind.PREEMPTED,
        FailureKind.TRANSIENT_FS_ERROR,
        FailureKind.SLURMRESTD_UNREACHABLE,
        FailureKind.ORCHESTRATOR_UNREACHABLE,
        FailureKind.CONTROL_PLANE_UNREACHABLE,
        FailureKind.PROCESS_RESTARTED,
        FailureKind.DATA_PLANE_TRANSIENT,
    }
)


# Note: plain @dataclass (NOT frozen, NOT slots). Both flags break
# Exception's traceback machinery: slots makes the dataclass-decorated
# class a different object than the public class (super() inside
# contextlib's traceback handling breaks); frozen blocks Exception's
# `__traceback__ = ...` assignment via FrozenInstanceError. Plain
# dataclass works because Exception sets traceback via normal attribute
# assignment, which dataclass doesn't override. We lose post-construction
# immutability but exceptions are constructed once and not mutated, so
# the loss is theoretical.
@dataclass
class BackendFailure(Exception):
    """Structured workflow-step failure raised by ComputeBackend implementations.

    `kind` is the fine-grained category (logged + retry decision).
    `stage` is the coarse lifecycle stage (DB-persisted as failure_stage).
    `step_name` is the YAML entry's `.name`; required iff stage=STEP_RUN
    so DB-side `work_ticket_failure_step_name_consistent` CHECK is
    honoured. `reason` is the human-readable explanation persisted in
    failure_reason.

    Use as a normal exception:

        raise BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="hash",
            reason="FASTA contains duplicate read_id(s): ['A', 'B']",
        )
    """

    kind: FailureKind
    stage: WorkTicketFailureStage
    reason: str
    step_name: str | None = None

    def __post_init__(self) -> None:
        # Mirror the DB CHECK: step_name is required for STEP_RUN, forbidden otherwise.
        if self.stage == WorkTicketFailureStage.STEP_RUN:
            if self.step_name is None:
                raise ValueError("BackendFailure: step_name is required when stage=STEP_RUN")
        elif self.step_name is not None:
            raise ValueError(
                f"BackendFailure: step_name must be None when stage={self.stage.value!r}"
            )
        # Exception() takes positional args; pass a useful str() for tracebacks.
        Exception.__init__(self, str(self))

    @property
    def transient(self) -> bool:
        """True if this failure should be retried (per `_RETRIABLE` set)."""
        return self.kind in _RETRIABLE

    def __str__(self) -> str:
        if self.step_name is not None:
            return f"[{self.kind.value}] {self.stage.value}/{self.step_name}: {self.reason}"
        return f"[{self.kind.value}] {self.stage.value}: {self.reason}"


class BackendFailureBody(BaseModel):
    """Wire format for BackendFailure crossing the orchestrator's HTTP
    boundary.

    The orchestrator emits this in `/step/*`'s response body (with
    `BACKEND_FAILURE_HEADER` set) when a backend raises BackendFailure;
    ComputeBackendClient reconstructs and re-raises so the runner's
    retry classification sees the same typed surface it would for an
    in-process backend. Without this round-trip, a transient
    `BackendFailure(kind=NODE_FAIL, ...)` raised inside SlurmBackend
    would degrade into an HTTPStatusError at the client and be
    classified UNKNOWN_PERMANENT by the runner — auto-retry would
    never fire for SLURM-side failures.
    """

    kind: FailureKind
    stage: WorkTicketFailureStage
    reason: str
    step_name: str | None = None

    @classmethod
    def from_exception(cls, exc: BackendFailure) -> BackendFailureBody:
        return cls(
            kind=exc.kind,
            stage=exc.stage,
            reason=exc.reason,
            step_name=exc.step_name,
        )

    def to_exception(self) -> BackendFailure:
        return BackendFailure(
            kind=self.kind,
            stage=self.stage,
            reason=self.reason,
            step_name=self.step_name,
        )


# Same plain-@dataclass rationale as BackendFailure above: neither `frozen`
# nor `slots`, so Exception's traceback machinery keeps working.
@dataclass
class StepNoData(Exception):
    """Terminal no-data signal raised by a native job whose input legitimately
    carried no data — an empty FASTQ well (a blank, a no-template control, or a
    failed-yield well).

    This is NOT a failure. The step exits without minting identifiers or writing
    output, and the runner transitions the work_ticket to NO_DATA (a terminal
    state with NULL failure_* columns), distinct from the BackendFailure →
    FAILED path. It is deliberately a separate type from BackendFailure — it is
    not a FailureKind and must never ride the failure_* columns or be retried.

    `step_name` is the YAML entry's `.name`, recorded for operator-side context
    (which step produced no data). `reason` is the human-readable explanation
    (e.g. "FASTQ file contains no records: ...").

        raise StepNoData(
            step_name="fastq",
            reason="FASTQ file contains no records: .../<well>_R1.fastq.gz",
        )
    """

    reason: str
    step_name: str | None = None

    def __post_init__(self) -> None:
        # Exception() takes positional args; pass a useful str() for tracebacks.
        Exception.__init__(self, str(self))

    def __str__(self) -> str:
        if self.step_name is not None:
            return f"[no_data] {self.step_name}: {self.reason}"
        return f"[no_data] {self.reason}"


class StepNoDataBody(BaseModel):
    """Wire format for StepNoData crossing the orchestrator's HTTP boundary,
    parallel to BackendFailureBody.

    The orchestrator emits this in `/step/*`'s response body (with
    `STEP_NO_DATA_HEADER` set) when a backend raises StepNoData;
    ComputeBackendClient reconstructs and re-raises so the runner's NO_DATA
    transition fires identically for an in-process LocalBackend and a SLURM job.
    """

    reason: str
    step_name: str | None = None

    @classmethod
    def from_exception(cls, exc: StepNoData) -> StepNoDataBody:
        return cls(reason=exc.reason, step_name=exc.step_name)

    def to_exception(self) -> StepNoData:
        return StepNoData(reason=self.reason, step_name=self.step_name)
