"""Wire-model tests for the /step/plan request/response contract."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from qiita_common.models import StepPlanRequest, StepPlanResponse


def test_plan_request_normalizes_scope_target():
    """scope_target is validated against the ScopeTarget union and normalized
    to JSON shape (enum kind -> plain string), like StepSubmitRequest."""
    req = StepPlanRequest(
        step_name="qc",
        inputs={"reads": "/scratch/r.parquet"},
        scope_target={"kind": "prep_sample", "prep_sample_idx": 5},
        work_ticket_idx=7,
        module="qiita_compute_orchestrator.jobs.qc",
    )
    assert req.scope_target["kind"] == "prep_sample"
    assert req.scope_target["prep_sample_idx"] == 5


def test_plan_request_module_required():
    """plan is native-only — module must be a non-empty string."""
    with pytest.raises(ValidationError):
        StepPlanRequest(
            step_name="qc",
            scope_target={"kind": "prep_sample", "prep_sample_idx": 5},
            work_ticket_idx=7,
        )


def test_plan_request_rejects_bad_scope_target():
    with pytest.raises(ValidationError):
        StepPlanRequest(
            step_name="qc",
            scope_target={"kind": "not_a_real_kind"},
            work_ticket_idx=7,
            module="qiita_compute_orchestrator.jobs.qc",
        )


def test_plan_response_all_optional_defaults_none():
    resp = StepPlanResponse()
    assert resp.cpu is None
    assert resp.mem_gb is None
    assert resp.walltime_seconds is None


def test_plan_response_accepts_partial_hint():
    resp = StepPlanResponse(walltime_seconds=600)
    assert resp.walltime_seconds == 600
    assert resp.mem_gb is None


@pytest.mark.parametrize("field", ["cpu", "mem_gb", "walltime_seconds"])
def test_plan_response_rejects_non_positive(field):
    """A set axis must be positive — a 0/negative hint is a producer bug, not a
    'size to zero' request."""
    with pytest.raises(ValidationError):
        StepPlanResponse(**{field: 0})
