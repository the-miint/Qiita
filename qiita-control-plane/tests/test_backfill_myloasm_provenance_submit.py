"""Unit tests for `qiita-admin backfill myloasm-provenance --execute`'s submit loop.

No database: the loop takes the plan and a `submit` callable, so these pin what it
sends, what it holds back, and that one refused submission never stops the rest.
"""

from pathlib import Path

import httpx
import pytest

from qiita_control_plane.actions.context_validator import validate_context
from qiita_control_plane.actions.loader import load_actions
from qiita_control_plane.backfill.myloasm_provenance import (
    RECOVER_ACTION_ID,
    RECOVER_ACTION_VERSION,
    BackfillPlan,
    Run,
    RunStatus,
    submit_runs,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _run(ps: int, p: int, status: RunStatus = RunStatus.WRITABLE) -> Run:
    return Run(
        prep_sample_idx=ps,
        processing_idx=p,
        mask_idx=5,
        status=status,
        reason=None if status is RunStatus.WRITABLE else "x",
        source_work_ticket_idx=100 + p if status is RunStatus.WRITABLE else None,
        assemble_attempt=1 if status is RunStatus.WRITABLE else None,
    )


class _Submit:
    """Records each body; answers with a fresh ticket, or raises for chosen runs."""

    def __init__(self, fail: dict[int, int] | None = None):
        self.bodies: list[dict] = []
        self.fail = fail or {}

    def __call__(self, body: dict) -> dict:
        self.bodies.append(body)
        p = body["action_context"]["assembly_processing_idx"]
        if p in self.fail:
            request = httpx.Request("POST", "http://cp.test/api/v1/work-ticket")
            response = httpx.Response(self.fail[p], request=request, json={"detail": "nope"})
            raise httpx.HTTPStatusError("refused", request=request, response=response)
        return {"work_ticket_idx": 9000 + p, "state": "pending"}


def test_submits_every_writable_run_and_nothing_else():
    plan = BackfillPlan(
        runs=[_run(1, 10), _run(2, 20), _run(3, 30, RunStatus.DONE), _run(4, 40, RunStatus.RESIDUE)]
    )
    submit = _Submit()
    report = submit_runs(plan, submit=submit, sleep=lambda _s: None)
    assert [b["action_context"]["assembly_processing_idx"] for b in submit.bodies] == [10, 20]
    assert [(r.processing_idx, wt) for r, wt in report.submitted] == [(10, 9010), (20, 9020)]
    assert report.deferred == [] and report.failed == []


def test_one_ticket_per_prep_sample_per_pass():
    """The in-flight gate allows one ticket per (action, prep_sample) whatever its
    context, so a second run of a prep_sample waits for the next pass, and so does a
    run whose prep_sample already has a recovery ticket in flight."""
    plan = BackfillPlan(
        runs=[_run(1, 10), _run(1, 11), _run(2, 20, RunStatus.IN_FLIGHT), _run(2, 21)]
    )
    submit = _Submit()
    report = submit_runs(plan, submit=submit, sleep=lambda _s: None)
    assert [r.processing_idx for r, _ in report.submitted] == [10]
    assert [r.processing_idx for r in report.deferred] == [11, 21]


def test_a_refused_submission_is_recorded_and_the_rest_still_go():
    plan = BackfillPlan(runs=[_run(1, 10), _run(2, 20), _run(3, 30)])
    submit = _Submit(fail={20: 409})
    report = submit_runs(plan, submit=submit, sleep=lambda _s: None)
    assert [r.processing_idx for r, _ in report.submitted] == [10, 30]
    [(failed, error)] = report.failed
    assert failed.processing_idx == 20
    assert "409" in error and "nope" in error


def test_limit_caps_attempts_and_rate_sleeps_between_them():
    plan = BackfillPlan(runs=[_run(1, 10), _run(2, 20), _run(3, 30)])
    slept: list[float] = []
    report = submit_runs(
        plan, submit=_Submit(fail={10: 500}), limit=2, rate_seconds=1.5, sleep=slept.append
    )
    # The refused first attempt counts against the limit.
    assert [r.processing_idx for r, _ in report.submitted] == [20]
    assert [r.processing_idx for r, _ in report.failed] == [10]
    assert slept == [1.5]


@pytest.mark.parametrize("limit", [0, -1])
def test_a_non_positive_limit_is_refused(limit):
    with pytest.raises(ValueError, match="limit"):
        submit_runs(BackfillPlan(runs=[]), submit=_Submit(), limit=limit)


def test_the_submit_body_satisfies_the_shipped_workflow():
    """The body's action names the shipped workflow, and its context passes that
    workflow's `context_schema`, attempt 0 included."""
    [action] = [
        a
        for a in load_actions(_REPO_ROOT / "workflows")
        if (a.action_id, a.version) == (RECOVER_ACTION_ID, RECOVER_ACTION_VERSION)
    ]
    run = Run(1, 10, 5, RunStatus.WRITABLE, source_work_ticket_idx=100, assemble_attempt=0)
    assert validate_context(action.context_schema, run.submit_body()["action_context"]) == []
