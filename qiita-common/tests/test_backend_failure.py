"""Unit tests for qiita_common.backend_failure.

Covers the typed failure surface backends (LocalBackend, SlurmBackend,
future cloud backends) raise and the runner consumes for retry decisions.
"""

from __future__ import annotations

import pytest

from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import WorkTicketFailureStage

# ---------------------------------------------------------------------------
# transient classification
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    [
        FailureKind.NODE_FAIL,
        FailureKind.OOM_KILLED,
        FailureKind.TIMEOUT_BEFORE_START,
        FailureKind.PREEMPTED,
        FailureKind.TRANSIENT_FS_ERROR,
        FailureKind.SLURMRESTD_UNREACHABLE,
        FailureKind.PROCESS_RESTARTED,
    ],
)
def test_retriable_kinds_are_transient(kind):
    f = BackendFailure(
        kind=kind,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="hash",
        reason="x",
    )
    assert f.transient is True


@pytest.mark.parametrize(
    "kind",
    [
        FailureKind.BAD_INPUT,
        FailureKind.EXIT_NONZERO,
        FailureKind.CONTRACT_VIOLATION,
        FailureKind.UNKNOWN_PERMANENT,
    ],
)
def test_permanent_kinds_are_not_transient(kind):
    f = BackendFailure(
        kind=kind,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="hash",
        reason="x",
    )
    assert f.transient is False


def test_every_failure_kind_is_classified():
    """Belt-and-suspenders: every FailureKind is either retriable or not.
    Catches the case where someone adds a new kind without remembering to
    update _RETRIABLE; the classification still resolves (False) but at
    least someone reading the test sees the ground truth set."""
    classifications = {
        kind: BackendFailure(
            kind=kind,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="x",
            reason="x",
        ).transient
        for kind in FailureKind
    }
    # 8 retriable + 4 permanent at the time of writing. If you add a
    # kind, update this count and decide which side it lands on.
    assert sum(classifications.values()) == 8
    assert len(classifications) == 12


# ---------------------------------------------------------------------------
# step_name / stage coupling
# ---------------------------------------------------------------------------


def test_step_run_requires_step_name():
    with pytest.raises(ValueError, match="step_name is required"):
        BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=WorkTicketFailureStage.STEP_RUN,
            reason="x",
        )


@pytest.mark.parametrize(
    "stage", [WorkTicketFailureStage.SUBMISSION, WorkTicketFailureStage.FINALIZE]
)
def test_non_step_run_forbids_step_name(stage):
    with pytest.raises(ValueError, match="step_name must be None"):
        BackendFailure(
            kind=FailureKind.BAD_INPUT,
            stage=stage,
            step_name="hash",
            reason="x",
        )


def test_submission_with_no_step_name_is_valid():
    # No step name is correct for submission/finalize.
    f = BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.SUBMISSION,
        reason="action not enabled",
    )
    assert f.step_name is None


# ---------------------------------------------------------------------------
# str() shape (drives logs + traceback messages)
# ---------------------------------------------------------------------------


def test_str_with_step_name():
    f = BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.STEP_RUN,
        step_name="hash",
        reason="duplicate read_id",
    )
    assert str(f) == "[bad_input] step_run/hash: duplicate read_id"


def test_str_without_step_name():
    f = BackendFailure(
        kind=FailureKind.BAD_INPUT,
        stage=WorkTicketFailureStage.SUBMISSION,
        reason="action not enabled",
    )
    assert str(f) == "[bad_input] submission: action not enabled"


# ---------------------------------------------------------------------------
# raise / catch behavior (it's still an Exception)
# ---------------------------------------------------------------------------


def test_can_be_raised_and_caught():
    with pytest.raises(BackendFailure) as ei:
        raise BackendFailure(
            kind=FailureKind.NODE_FAIL,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="hash",
            reason="node 5 down",
        )
    assert ei.value.kind == FailureKind.NODE_FAIL
    assert ei.value.transient


# ---------------------------------------------------------------------------
# StepNoData — the terminal no-data signal (NOT a failure)
# ---------------------------------------------------------------------------


def test_step_no_data_is_not_a_backend_failure():
    """StepNoData is a distinct Exception type — not a BackendFailure and not a
    FailureKind — so a runner `except BackendFailure` never swallows it."""
    from qiita_common.backend_failure import StepNoData

    exc = StepNoData(step_name="fastq", reason="no records")
    assert not isinstance(exc, BackendFailure)


def test_step_no_data_str_with_and_without_step_name():
    from qiita_common.backend_failure import StepNoData

    assert str(StepNoData(step_name="fastq", reason="no records")) == "[no_data] fastq: no records"
    assert str(StepNoData(reason="no records")) == "[no_data] no records"


def test_step_no_data_body_round_trips():
    """StepNoDataBody.from_exception → to_exception preserves the fields, so the
    /step/* HTTP boundary reconstructs the signal exactly."""
    from qiita_common.backend_failure import StepNoData, StepNoDataBody

    original = StepNoData(step_name="fastq", reason="FASTQ file contains no records: x.fastq")
    rebuilt = StepNoDataBody.from_exception(original).to_exception()
    assert rebuilt.step_name == original.step_name
    assert rebuilt.reason == original.reason


def test_step_no_data_can_be_raised_and_caught():
    from qiita_common.backend_failure import StepNoData

    with pytest.raises(StepNoData) as ei:
        raise StepNoData(step_name="fastq", reason="no records")
    assert ei.value.step_name == "fastq"
