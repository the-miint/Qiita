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
    ("_MIN_IDENTITY", "_SYNDNA_MIN_IDENTITY"),
    ("_PRIMARY_ONLY", "_SYNDNA_PRIMARY_ONLY"),
]


def _job_constants() -> dict[str, object]:
    """Module-level literal assignments in the syndna job, by AST.

    Parsed rather than imported: the control-plane venv does not install
    `qiita-compute-orchestrator`, and importing it would create exactly the dependency
    the mirror exists to avoid.
    """
    tree = ast.parse(_SYNDNA_JOB.read_text())
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
