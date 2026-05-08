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

from .models import WorkTicketFailureStage


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
    PROCESS_RESTARTED = "process_restarted"  # CP drain cancelled a task

    # ---- Permanent: workflow / input / contract issues -------------------
    BAD_INPUT = "bad_input"
    EXIT_NONZERO = "exit_nonzero"
    CONTRACT_VIOLATION = "contract_violation"
    UNKNOWN_PERMANENT = "unknown_permanent"


_RETRIABLE: frozenset[FailureKind] = frozenset(
    {
        FailureKind.NODE_FAIL,
        FailureKind.OOM_KILLED,
        FailureKind.TIMEOUT_BEFORE_START,
        FailureKind.PREEMPTED,
        FailureKind.TRANSIENT_FS_ERROR,
        FailureKind.SLURMRESTD_UNREACHABLE,
        FailureKind.PROCESS_RESTARTED,
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
