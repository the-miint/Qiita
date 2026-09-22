"""Pin the control plane's host-filter constants to the orchestrator's.

`_resolved_host_filter` folds rype's host-call threshold into the read-mask identity
hash, so the mask's stored params describe the depletion that actually ran. But the
value is DUPLICATED: the control plane cannot import `qiita_compute_orchestrator` (it
does not depend on it — see CLAUDE.md), so it mirrors the job's constant by hand.

Nothing else enforces that mirror, and the drift is silent and severe. Move
`_RYPE_THRESHOLD` on the ORCHESTRATOR side alone and the job depletes against a
different cutoff while the control plane keeps hashing the OLD one — so the new
filter's output lands under a `mask_idx` whose params describe the old threshold, and
every affected mask silently collapses onto it instead of re-minting. Worse than a
mislabel: the per-`(mask_idx, prep_sample)` gate would read those samples as already
masked, so the new threshold would never be applied to them at all.

That is not hypothetical — the threshold moved once already (0.0 -> 0.05), which is
what this pin exists to make loud.

Same shape as `test_syndna_pins.py` / `test_lima_pins.py`: the values live in
different components, so this reads the orchestrator's source rather than importing it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from qiita_control_plane.runner import _mask

_HOST_FILTER_JOB = (
    Path(__file__).resolve().parents[2]
    / "qiita-compute-orchestrator"
    / "src"
    / "qiita_compute_orchestrator"
    / "jobs"
    / "host_filter.py"
)

# (orchestrator constant, control-plane constant) — every value the mask identity
# folds in that is really owned by the job.
_PINNED = [("_RYPE_THRESHOLD", "_HOST_FILTER_RYPE_THRESHOLD")]


def _job_constants() -> dict[str, object]:
    """Module-level literal assignments in the host_filter job, by AST.

    Parsed rather than imported: the control-plane venv does not install
    `qiita-compute-orchestrator`, and importing it would create exactly the dependency
    the mirror exists to avoid (host_filter.py also pulls in duckdb at import).
    """
    tree = ast.parse(_HOST_FILTER_JOB.read_text())
    out: dict[str, object] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                try:
                    out[target.id] = ast.literal_eval(node.value)
                except ValueError:
                    pass  # not a literal (an f-string, a call, ...)
    return out


def test_the_host_filter_job_source_is_where_we_think_it_is():
    """Guard against a moved file making every pin below vacuously pass."""
    assert _HOST_FILTER_JOB.is_file(), f"expected the host_filter job at {_HOST_FILTER_JOB}"


@pytest.mark.parametrize("job_name,cp_name", _PINNED)
def test_cp_host_filter_constant_matches_the_job(job_name: str, cp_name: str):
    """The control plane hashes what the orchestrator applies — or the mask's params
    describe a depletion that never ran."""
    job = _job_constants()
    assert job_name in job, (
        f"{job_name} is gone from jobs/host_filter.py. The control plane folds it into "
        f"the read-mask identity as {cp_name}; if the filter changed, change BOTH."
    )
    assert job[job_name] == getattr(_mask, cp_name), (
        f"jobs/host_filter.py {job_name}={job[job_name]!r} but runner/_mask.py "
        f"{cp_name}={getattr(_mask, cp_name)!r}. These MUST agree: the orchestrator "
        f"applies the depletion, the control plane hashes it into the mask_idx. Drift "
        f"means the new filter's output is stored under a mask whose params describe "
        f"the old one — and the completion gate then skips re-masking entirely."
    )


def test_resolved_host_filter_carries_every_pinned_constant():
    """A knob pinned to the job must reach the identity hash, not just the job. This
    fails if someone adds one to `_PINNED` and forgets `_resolved_host_filter`."""
    resolved = _mask._resolved_host_filter(7)
    assert resolved is not None
    values = set(resolved.values())
    for _job_name, cp_name in _PINNED:
        assert getattr(_mask, cp_name) in values, (
            f"{cp_name} is pinned to the job but does not appear in resolved_host_filter "
            f"— the mask identity would not change when it does."
        )


def test_every_filter_knob_in_the_job_is_pinned():
    """A NEW depletion knob added to the job must be registered in `_PINNED`.

    The parity test above only checks what is already listed, so a knob added to
    `jobs/host_filter.py` and forgotten here slips through — and the failure is the bad
    kind: the job depletes differently, the identity hash does not move, and the new
    mask silently collapses onto a `mask_idx` describing the OLD depletion. Same
    defect class the host-filter mask identity exists to close, one level up.

    So the guard is NAME-shaped: anything that looks like a depletion knob has to be
    pinned deliberately, or excluded here with a reason.
    """
    # Constants that decide WHICH READS ARE CALLED HOST, as opposed to how the job
    # runs (memory, threads) or what its relations are named. Matched on both ends: a
    # prefix net alone misses the shape the pinned knob itself has (`_QCOV_THRESHOLD`,
    # `_MIN_IDENTITY_FLOOR`), which is the whole failure mode this guard is for.
    knob_prefixes = ("_RYPE_", "_MINIMAP2_", "_MIN_", "_MAX_", "_PRESET")
    knob_suffixes = ("_THRESHOLD", "_PRESET", "_IDENTITY", "_FLOOR", "_CUTOFF")
    not_filter_knobs = {
        # In-DuckDB relation names that happen to start with a knob prefix.
        "_RYPE_HOST",
        # Not a per-mask choice: the job pins it to the preset its `.mmi` was built
        # with, and the identity already names that index via
        # host_minimap2_reference_idx — so hashing it would re-mint every mask
        # fleet-wide to discriminate nothing. Pin it the moment a minimap2 param
        # becomes a per-mask choice (a caller-supplied preset, or a preset that varies
        # by platform).
        "_MINIMAP2_PRESET",
    }

    job = _job_constants()
    pinned = {job_name for job_name, _ in _PINNED}
    knobs = {
        name
        for name in job
        if (name.startswith(knob_prefixes) or name.endswith(knob_suffixes))
        and name not in not_filter_knobs
    }
    unpinned = knobs - pinned
    assert not unpinned, (
        f"depletion knob(s) in jobs/host_filter.py are not in _PINNED: {sorted(unpinned)}. "
        "A knob that changes which reads are called host MUST enter the mask identity "
        "hash (runner/_mask.py::_resolved_host_filter), or masks built under the new "
        "setting will reuse a mask_idx describing the old one."
    )
