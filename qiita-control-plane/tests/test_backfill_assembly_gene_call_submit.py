"""Unit tests for `qiita-admin backfill assembly-gene-call`: its submit loop, the
submit body against the shipped workflow, and the planner's source-output check.

No database: the loop takes the plan and a `submit` callable, so these pin what it
sends, what it holds back, and that one refused submission never stops the rest.
"""

import os
from pathlib import Path

import httpx
import pytest

from qiita_control_plane.actions.context_validator import validate_context
from qiita_control_plane.actions.loader import load_actions
from qiita_control_plane.backfill.assembly_gene_call import (
    RECOVER_ACTION_ID,
    RECOVER_ACTION_VERSION,
    SOURCE_ENTRIES,
    BackfillPlan,
    ResidueKind,
    Run,
    RunStatus,
    _source_residue,
    submit_runs,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_ATTEMPTS = {
    "assemble": 1,
    "bin_refine": 0,
    "checkm": 2,
    "assembly_hash": 0,
    "mint-features": 0,
}


def _run(ps: int, p: int, status: RunStatus = RunStatus.WRITABLE) -> Run:
    writable = status is RunStatus.WRITABLE
    return Run(
        prep_sample_idx=ps,
        processing_idx=p,
        status=status,
        reason=None if writable else "x",
        residue_kind=ResidueKind.NO_MANIFEST if status is RunStatus.RESIDUE else None,
        source_work_ticket_idx=100 + p if writable else None,
        attempts=dict(_ATTEMPTS) if writable else None,
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


def test_the_submit_body_names_every_attempt():
    assert _run(1, 10).submit_body() == {
        "action_id": RECOVER_ACTION_ID,
        "action_version": RECOVER_ACTION_VERSION,
        "scope_target": {"kind": "prep_sample", "prep_sample_idx": 1},
        "action_context": {
            "assembly_processing_idx": 10,
            "source_work_ticket_idx": 110,
            "assemble_attempt": 1,
            "bin_refine_attempt": 0,
            "checkm_attempt": 2,
            "assembly_hash_attempt": 0,
            "mint_features_attempt": 0,
        },
    }


def test_the_submit_body_satisfies_the_shipped_workflow():
    """The body's action names the shipped workflow, and its context passes that
    workflow's `context_schema`, attempt 0 included."""
    [action] = [
        a
        for a in load_actions(_REPO_ROOT / "workflows")
        if (a.action_id, a.version) == (RECOVER_ACTION_ID, RECOVER_ACTION_VERSION)
    ]
    run = Run(
        1,
        10,
        RunStatus.WRITABLE,
        source_work_ticket_idx=100,
        attempts=dict.fromkeys(SOURCE_ENTRIES, 0),
    )
    context = run.submit_body()["action_context"]
    assert validate_context(action.context_schema, context) == []
    # Every key the schema requires is sent, and nothing it does not declare.
    assert set(context) == set(action.context_schema["required"])
    assert set(context) == set(action.context_schema["properties"])


def test_a_body_missing_an_attempt_is_refused():
    run = Run(1, 10, RunStatus.WRITABLE, source_work_ticket_idx=100, attempts={"assemble": 0})
    with pytest.raises(ValueError, match="attempts must name exactly"):
        run.submit_body()


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
    plan = BackfillPlan(
        runs=[_run(1, 10), _run(1, 11), _run(2, 20, RunStatus.IN_FLIGHT), _run(2, 21)]
    )
    report = submit_runs(plan, submit=_Submit(), sleep=lambda _s: None)
    assert [r.processing_idx for r, _ in report.submitted] == [10]
    assert [r.processing_idx for r in report.deferred] == [11, 21]


def test_a_refused_submission_is_recorded_and_the_rest_still_go():
    plan = BackfillPlan(runs=[_run(1, 10), _run(2, 20), _run(3, 30)])
    report = submit_runs(plan, submit=_Submit(fail={20: 409}), sleep=lambda _s: None)
    assert [r.processing_idx for r, _ in report.submitted] == [10, 30]
    [(failed, error)] = report.failed
    assert failed.processing_idx == 20
    assert "409" in error and "nope" in error


def test_every_writable_run_is_accounted_for_under_a_limit():
    plan = BackfillPlan(
        runs=[
            _run(1, 10),
            _run(1, 11),
            _run(2, 20),
            _run(3, 30),
            _run(4, 40, RunStatus.IN_FLIGHT),
            _run(4, 41),
            _run(5, 50),
        ]
    )
    slept: list[float] = []
    report = submit_runs(
        plan, submit=_Submit(fail={20: 409}), limit=2, rate_seconds=1.5, sleep=slept.append
    )
    seen = sorted(
        [r.processing_idx for r, _ in report.submitted]
        + [r.processing_idx for r in report.deferred]
        + [r.processing_idx for r, _ in report.failed]
        + [r.processing_idx for r in report.not_attempted]
    )
    assert seen == sorted(r.processing_idx for r in plan.by_status(RunStatus.WRITABLE))
    assert [r.processing_idx for r in report.not_attempted] == [30, 50]
    assert slept == [1.5]


@pytest.mark.parametrize("limit", [0, -1])
def test_a_non_positive_limit_is_refused(limit):
    with pytest.raises(ValueError, match="limit"):
        submit_runs(BackfillPlan(runs=[]), submit=_Submit(), limit=limit)


@pytest.mark.parametrize(
    ("status", "kind"),
    [
        pytest.param(RunStatus.RESIDUE, None, id="residue-without-kind"),
        pytest.param(RunStatus.SKIPPED, ResidueKind.NO_MANIFEST, id="kind-without-residue"),
    ],
)
def test_a_residue_kind_goes_with_residue_status_only(status, kind):
    with pytest.raises(ValueError, match="residue kind"):
        Run(1, 10, status, reason="x", residue_kind=kind)


def _source_tree(ticket_dir: Path) -> None:
    """Each step's manifest and mint-features' map, at the paths spelled out."""
    for step in ("assemble", "bin_refine", "checkm", "assembly_hash"):
        out = ticket_dir / step / f"attempt-{_ATTEMPTS[step]}" / "output"
        out.mkdir(parents=True)
        (out / "manifest.json").write_text("{}")
    mint = ticket_dir / "mint-features" / f"attempt-{_ATTEMPTS['mint-features']}"
    mint.mkdir(parents=True)
    (mint / "feature_map.parquet").write_bytes(b"x")


def test_a_complete_source_has_no_residue(tmp_path):
    _source_tree(tmp_path)
    assert _source_residue(tmp_path, _ATTEMPTS) is None


@pytest.mark.parametrize(
    ("missing", "kind"),
    [
        ("checkm/attempt-2/output/manifest.json", ResidueKind.NO_MANIFEST),
        ("assemble/attempt-1/output/manifest.json", ResidueKind.NO_MANIFEST),
        ("mint-features/attempt-0/feature_map.parquet", ResidueKind.NO_FEATURE_MAP),
    ],
)
def test_a_missing_source_output_is_residue(tmp_path, missing, kind):
    _source_tree(tmp_path)
    (tmp_path / missing).unlink()
    got, reason = _source_residue(tmp_path, _ATTEMPTS)
    assert got is kind
    assert str(tmp_path / missing) in reason


@pytest.mark.skipif(os.geteuid() == 0, reason="root traverses a mode-000 directory")
def test_a_source_that_cannot_be_looked_at_raises_rather_than_reading_as_missing(tmp_path):
    _source_tree(tmp_path)
    locked = tmp_path / "assemble"
    locked.chmod(0)
    try:
        with pytest.raises(PermissionError):
            _source_residue(tmp_path, _ATTEMPTS)
    finally:
        locked.chmod(0o755)
