"""The CP's mirror of jobs/coverage_depth's constants must not drift from the job.

The control plane mints `coverage_idx` from a params blob that has to describe the
measurement that ACTUALLY RAN — but the CP cannot import the orchestrator, so the job's
thresholds are hand-copied into `runner/_mask`. If they drift, nothing crashes: the job
computes with one gate, the identity records another, and the rows land under a
coverage_idx whose stored params are a lie. Every downstream consumer then reads a number
it cannot interpret.

Same arrangement, and the same reasoning, as `test_syndna_pins`. The job is read by AST
rather than imported, because the control plane's venv has no orchestrator.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from qiita_control_plane.runner import _mask

# (job constant, CP mirror constant)
_PINNED = [
    ("_MIN_IDENTITY", "_COVERAGE_MIN_IDENTITY"),
    ("_MIN_ALIGNED_FRACTION", "_COVERAGE_MIN_ALIGNED_FRACTION"),
]

_JOB = (
    Path(__file__).resolve().parents[2]
    / "qiita-compute-orchestrator"
    / "src"
    / "qiita_compute_orchestrator"
    / "jobs"
    / "coverage_depth.py"
)


def _job_constants() -> dict[str, object]:
    """Module-level literal constants of jobs/coverage_depth.py, by AST.

    Not an import: the control-plane venv does not have qiita-compute-orchestrator on its
    path, and adding it just to read four numbers would couple the two packages for no
    reason.
    """
    tree = ast.parse(_JOB.read_text())
    out: dict[str, object] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                try:
                    out[target.id] = ast.literal_eval(node.value)
                except ValueError:
                    pass  # a non-literal (a call, a name) — not a pinnable constant
    return out


@pytest.mark.parametrize(("job_name", "cp_name"), _PINNED)
def test_pinned_constants_match(job_name, cp_name):
    job = _job_constants()
    assert job_name in job, f"{job_name} is gone from jobs/coverage_depth.py"
    assert getattr(_mask, cp_name) == job[job_name], (
        f"{cp_name} ({getattr(_mask, cp_name)!r}) has drifted from the job's "
        f"{job_name} ({job[job_name]!r}). The coverage_idx would then describe a "
        "measurement that did not run."
    )


def test_depth_mode_matches_the_job():
    """`_DEPTH_MODE` is assigned from an imported name, not a literal, so the AST reader
    above cannot evaluate it — assert against the value that name resolves to."""
    src = _JOB.read_text()
    assert "_DEPTH_MODE = DEPTH_MODE_INCLUDE_DELETIONS" in src, (
        "the job's depth mode changed; update _COVERAGE_DEPTH_MODE and this pin"
    )
    assert _mask._COVERAGE_DEPTH_MODE == "include_deletions"


def test_every_pinned_constant_reaches_the_identity_hash():
    """A constant can be faithfully mirrored and STILL not reach the hash. The mirror is
    only half the job — the value has to be in the params blob, or a change to it re-uses
    the old coverage_idx and the rows silently describe the wrong measurement."""
    from qiita_control_plane.repositories.coverage_definition import build_coverage_params

    params = build_coverage_params(
        reference_idx=1,
        aligner=_mask._COVERAGE_ALIGNER,
        preset=_mask._COVERAGE_PRESET,
        min_identity=_mask._COVERAGE_MIN_IDENTITY,
        min_aligned_fraction=_mask._COVERAGE_MIN_ALIGNED_FRACTION,
        depth_mode=_mask._COVERAGE_DEPTH_MODE,
        mask_idx=9,
    )
    values = set(params.values())
    for _, cp_name in _PINNED:
        assert getattr(_mask, cp_name) in values, (
            f"{cp_name} is mirrored from the job but never reaches build_coverage_params, "
            "so changing it would NOT re-mint the coverage_idx"
        )
    assert _mask._COVERAGE_DEPTH_MODE in values
    assert _mask._COVERAGE_ALIGNER in values
    assert _mask._COVERAGE_PRESET in values


def test_every_gate_knob_in_the_job_is_pinned():
    """A NEW knob added to the job must be registered here.

    Without this, a knob can be added, change the number, and leave the identity hash
    untouched — the exact failure this whole apparatus exists to prevent. (It has already
    happened once, in the syndna job, and every test still passed.)
    """
    knob_prefixes = ("_MIN_", "_MAX_")
    job = _job_constants()
    pinned = {job_name for job_name, _ in _PINNED}
    knobs = {name for name in job if name.startswith(knob_prefixes)}
    unpinned = knobs - pinned
    assert not unpinned, (
        f"gate knob(s) in jobs/coverage_depth.py are not pinned: {sorted(unpinned)}. "
        "A knob that changes the measured NUMBER must be mirrored into runner/_mask AND "
        "reach build_coverage_params, or coverage rows will land under a coverage_idx "
        "whose params describe the old measurement."
    )
