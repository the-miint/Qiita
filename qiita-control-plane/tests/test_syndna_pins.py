"""Pin the control plane's syndna constants to the orchestrator's.

`_resolved_syndna` folds the spike-in filter (aligner, preset, identity method,
min identity) into the read-mask identity hash, so the mask's stored params describe
the filter that actually ran. But the values are DUPLICATED: the control plane cannot
import `qiita_compute_orchestrator` (it does not depend on it — see CLAUDE.md), so it
mirrors the job's constants by hand.

Nothing else enforces that mirror, and the drift is silent and nasty in a specific way:
bump `_MIN_IDENTITY` (or switch `_IDENTITY_METHOD`) on the ORCHESTRATOR side alone and
the job starts applying a different filter while the control plane keeps hashing to the
OLD `resolved_syndna` — so the new filter's output is stored under a `mask_idx` whose
params describe the old cutoff, and every affected mask silently collapses onto it
instead of re-minting.

This is not hypothetical: the identity threshold is explicitly expected to move once it
is confirmed against real data with the assay owner, and `blast` vs `gap_compressed` is
an open question. So the pin exists to make that change fail loudly here, forcing both
sides to move together.

Same shape as `test_lima_pins.py`: the values live in different components, so this
reads the orchestrator's source rather than importing it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from qiita_control_plane.runner import _mask

_SYNDNA_JOB = (
    Path(__file__).resolve().parents[2]
    / "qiita-compute-orchestrator"
    / "src"
    / "qiita_compute_orchestrator"
    / "jobs"
    / "syndna.py"
)

# (orchestrator constant, control-plane constant) — every value `_resolved_syndna`
# folds into the mask identity that is really owned by the job.
_PINNED = [
    ("_MM2_PRESET", "_SYNDNA_MM2_PRESET"),
    ("_IDENTITY_METHOD", "_SYNDNA_IDENTITY_METHOD"),
    # These two live in jobs/_coverage.py (the ONE gate both syndna and coverage use),
    # not in syndna.py — pinning the CP mirror to the shared source also enforces that the
    # syndna gate and the coverage gate are the same number.
    ("MIN_IDENTITY", "_SYNDNA_MIN_IDENTITY"),
    ("MIN_ALIGNED_FRACTION", "_SYNDNA_MIN_ALIGNED_FRACTION"),
    ("_PRIMARY_ONLY", "_SYNDNA_PRIMARY_ONLY"),
]


def _job_constants() -> dict[str, object]:
    """Module-level literal assignments in the syndna job, by AST.

    Parsed rather than imported: the control-plane venv does not install
    `qiita-compute-orchestrator`, and importing it would create exactly the dependency
    the mirror exists to avoid.
    """
    # Read the shared-gate module too: MIN_IDENTITY / MIN_ALIGNED_FRACTION moved there so
    # syndna and coverage cannot diverge, so the values the mask identity pins now live in
    # _coverage.py, not syndna.py.
    coverage_job = _SYNDNA_JOB.parent / "_coverage.py"
    tree = ast.parse(_SYNDNA_JOB.read_text() + "\n" + coverage_job.read_text())
    out: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                try:
                    out[target.id] = ast.literal_eval(node.value)
                except ValueError:
                    pass  # not a literal (a dict comprehension, an f-string, ...)
    return out


def test_the_syndna_job_source_is_where_we_think_it_is():
    """Guard against a moved file making every pin below vacuously pass."""
    assert _SYNDNA_JOB.is_file(), f"expected the syndna job at {_SYNDNA_JOB}"


@pytest.mark.parametrize("job_name,cp_name", _PINNED)
def test_cp_syndna_constant_matches_the_job(job_name: str, cp_name: str):
    """The control plane hashes what the orchestrator applies — or the mask's params
    describe a filter that never ran."""
    job = _job_constants()
    assert job_name in job, (
        f"{job_name} is gone from jobs/syndna.py. The control plane folds it into the "
        f"read-mask identity as {cp_name}; if the filter changed, change BOTH."
    )
    assert job[job_name] == getattr(_mask, cp_name), (
        f"jobs/syndna.py {job_name}={job[job_name]!r} but runner/_mask.py "
        f"{cp_name}={getattr(_mask, cp_name)!r}. These MUST agree: the orchestrator "
        f"applies the filter, the control plane hashes it into the mask_idx. Drift means "
        f"the new filter's output is stored under a mask whose params describe the old one."
    )


def test_resolved_syndna_carries_every_pinned_constant():
    """A new knob added to the job's filter must reach the identity hash, not just the
    job. This fails if someone adds one to `_PINNED` and forgets `_resolved_syndna`."""
    resolved = _mask._resolved_syndna({"syndna_enabled": True, "syndna_reference_idx": 7})
    values = set(resolved.values())
    for _job_name, cp_name in _PINNED:
        assert getattr(_mask, cp_name) in values, (
            f"{cp_name} is pinned to the job but does not appear in resolved_syndna — "
            f"the mask identity would not change when it does."
        )


def test_every_filter_knob_in_the_job_is_pinned():
    """A NEW filter knob added to the job must be registered in `_PINNED`.

    `test_pinned_constants_match` only checks the constants already listed, so a knob
    added to `jobs/syndna.py` and forgotten here slips through — and the failure is the
    worst kind: the job filters differently, but the mask identity hash does not change,
    so the new mask silently collapses onto a `mask_idx` whose stored params describe the
    OLD filter. That is the same defect class the syndna mask identity was introduced to
    fix, one level up.

    Caught exactly this: `_MIN_ALIGNED_FRACTION` was added to the job and every test still
    passed. So the guard is a NAME-shaped one — anything that looks like a filter knob has
    to be pinned deliberately, or explicitly excluded here with a reason.
    """
    # Constants that describe the FILTER (what gets called a spike-in) rather than the
    # execution environment (memory, threads, table names).
    knob_prefixes = ("_MIN_", "_MAX_", "_IDENTITY_", "_PRIMARY_", "_MM2_PRESET")
    not_filter_knobs = {
        # Resource caps — they change how the job RUNS, never which reads it calls.
        "_MM2_RESERVE_GB",
    }

    job = _job_constants()
    pinned = {job_name for job_name, _ in _PINNED}
    knobs = {
        name for name in job if name.startswith(knob_prefixes) and name not in not_filter_knobs
    }
    unpinned = knobs - pinned
    assert not unpinned, (
        f"filter knob(s) in jobs/syndna.py are not in _PINNED: {sorted(unpinned)}. "
        "A knob that changes which reads are called spike-in MUST enter the mask identity "
        "hash (runner/_mask.py::_resolved_syndna), or masks built under the new setting "
        "will reuse a mask_idx describing the old one."
    )
