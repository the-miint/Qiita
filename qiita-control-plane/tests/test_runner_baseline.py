"""Pure-unit tests for the runner's A4 baseline-resource resolution.

`_resolve_baseline_for_step` and `_assert_within_ceiling` are sync, pure
functions (no DB, no orchestrator round-trip) — they translate a step's
`baseline_resources` declaration into a concrete `FlatBaselineResources`
and clamp it against the action ceiling. They live in `runner.py`, but
unlike the rest of that module they touch neither asyncpg nor the
LIBRARY, so these tests carry no `db` marker and run in the pure-unit
tier alongside the model-level coverage in
`qiita-common/tests/test_actions.py`.

The model-level `BaselineResources` validator (exactly-one-population,
mixed/neither/partial) is covered there; this file covers the runner
side the validator can't reach: the bound-key lookup, the file read,
the profile-key miss, and each ceiling axis.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from qiita_common.actions import (
    ActionCeiling,
    BaselineResources,
    FlatBaselineResources,
    WorkflowStep,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import StepType, WorkTicketFailureStage

from qiita_control_plane.runner import (
    _attempt_is_unowned,
    _escalated_mem_floor_after_oom,
    _escalated_walltime_after_timeout,
    _resolve_baseline_for_step,
)

# A generous ceiling that the happy-path fixtures stay well under; the
# overage tests construct their own tight ceilings per axis.
_CEILING = ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=24), gpu=4)


def _step(baseline_resources: BaselineResources, *, name: str = "demux") -> WorkflowStep:
    """Minimal container step carrying the given baseline_resources.
    Exactly one of container/module is required; container is arbitrary
    here — only `.name` and `.baseline_resources` are read by the
    resolution path."""
    return WorkflowStep(
        kind="step",
        name=name,
        step_type=StepType.SINGLETON,
        container="bcl-convert-4.5.4.sif",
        baseline_resources=baseline_resources,
    )


# =============================================================================
# Flat population — values pass through verbatim
# =============================================================================


def test_flat_population_passes_through_verbatim():
    step = _step(BaselineResources(cpu=16, mem_gb=240, walltime=timedelta(hours=3), gpu=1))
    resolved = _resolve_baseline_for_step(entry=step, bound={}, action_ceiling=_CEILING)
    assert resolved == FlatBaselineResources(cpu=16, mem_gb=240, walltime=timedelta(hours=3), gpu=1)


def test_flat_population_defaults_gpu_to_zero():
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1)))
    resolved = _resolve_baseline_for_step(entry=step, bound={}, action_ceiling=_CEILING)
    assert resolved.gpu == 0


# =============================================================================
# Lookup population — read upstream output file, pick the matching profile
# =============================================================================


def _lookup_step(name: str = "bcl_convert") -> WorkflowStep:
    return _step(
        BaselineResources(
            from_step_output="instrument_model",
            profiles={
                "Illumina NovaSeq 6000": FlatBaselineResources(
                    cpu=16, mem_gb=480, walltime=timedelta(hours=6)
                ),
                "Illumina iSeq 100": FlatBaselineResources(
                    cpu=16, mem_gb=16, walltime=timedelta(hours=3)
                ),
            },
        ),
        name=name,
    )


def test_lookup_population_reads_file_and_selects_profile(tmp_path: Path):
    lookup_file = tmp_path / "instrument_model"
    # Trailing whitespace/newline is stripped before the profile lookup.
    lookup_file.write_text("Illumina NovaSeq 6000\n", encoding="utf-8")
    step = _lookup_step()

    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={"instrument_model": str(lookup_file)},
        action_ceiling=_CEILING,
    )
    assert resolved == FlatBaselineResources(cpu=16, mem_gb=480, walltime=timedelta(hours=6))


def test_lookup_from_step_output_not_bound():
    step = _lookup_step()
    with pytest.raises(BackendFailure) as ei:
        # `instrument_model` was never produced upstream — not in bound.
        _resolve_baseline_for_step(entry=step, bound={}, action_ceiling=_CEILING)
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.stage == WorkTicketFailureStage.STEP_RUN
    assert exc.step_name == "bcl_convert"
    assert "is not bound" in exc.reason
    assert "instrument_model" in exc.reason


def test_lookup_file_unreadable(tmp_path: Path):
    missing = tmp_path / "does-not-exist"
    step = _lookup_step()
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(
            entry=step,
            bound={"instrument_model": str(missing)},
            action_ceiling=_CEILING,
        )
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.step_name == "bcl_convert"
    assert "failed to read" in exc.reason


def test_lookup_key_not_in_profiles(tmp_path: Path):
    lookup_file = tmp_path / "instrument_model"
    lookup_file.write_text("Illumina HiSeq 4000", encoding="utf-8")
    step = _lookup_step()
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(
            entry=step,
            bound={"instrument_model": str(lookup_file)},
            action_ceiling=_CEILING,
        )
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.step_name == "bcl_convert"
    assert "no" in exc.reason and "resource profile" in exc.reason
    # The known-profiles list is surfaced so the YAML author can fix it.
    assert "Illumina NovaSeq 6000" in exc.reason


# =============================================================================
# Ceiling clamp — one rejected resolution per axis
# =============================================================================


@pytest.mark.parametrize(
    ("baseline", "tight_ceiling", "axis"),
    [
        (
            BaselineResources(cpu=64, mem_gb=8, walltime=timedelta(hours=1)),
            ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=24), gpu=4),
            "cpu",
        ),
        (
            BaselineResources(cpu=4, mem_gb=1024, walltime=timedelta(hours=1)),
            ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=24), gpu=4),
            "mem_gb",
        ),
        (
            BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=48)),
            ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=24), gpu=4),
            "walltime",
        ),
        (
            BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1), gpu=2),
            ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=24), gpu=0),
            "gpu",
        ),
    ],
)
def test_resolved_baseline_exceeding_ceiling_is_rejected(
    baseline: BaselineResources, tight_ceiling: ActionCeiling, axis: str
):
    step = _step(baseline, name="over")
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(entry=step, bound={}, action_ceiling=tight_ceiling)
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.stage == WorkTicketFailureStage.STEP_RUN
    assert exc.step_name == "over"
    # The reason names the offending axis and both sides of the comparison.
    assert axis in exc.reason
    assert "exceeds" in exc.reason


def test_lookup_resolved_profile_also_clamped(tmp_path: Path):
    """The clamp applies to the lookup population too, not just flat."""
    lookup_file = tmp_path / "instrument_model"
    lookup_file.write_text("Illumina NovaSeq 6000", encoding="utf-8")
    step = _lookup_step()
    # NovaSeq profile asks mem_gb=480; ceiling caps it at 240.
    tight = ActionCeiling(cpu=32, mem_gb=240, walltime=timedelta(hours=24), gpu=4)
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(
            entry=step,
            bound={"instrument_model": str(lookup_file)},
            action_ceiling=tight,
        )
    assert "mem_gb" in ei.value.reason


# =============================================================================
# Per-run mem_gb override — raise-only floor, ceiling-bounded
# =============================================================================


def test_mem_gb_override_raises_floor_above_baseline():
    """An override above the YAML baseline raises mem_gb; cpu/walltime/gpu
    are untouched."""
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1), gpu=1))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, mem_gb_override=48
    )
    assert resolved == FlatBaselineResources(cpu=4, mem_gb=48, walltime=timedelta(hours=1), gpu=1)


def test_mem_gb_override_below_baseline_is_noop():
    """Raise-only: an override smaller than the step's baseline never lowers
    a step the YAML sized higher."""
    step = _step(BaselineResources(cpu=8, mem_gb=32, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, mem_gb_override=16
    )
    assert resolved.mem_gb == 32


def test_mem_gb_override_none_leaves_baseline_verbatim():
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, mem_gb_override=None
    )
    assert resolved.mem_gb == 8


def test_mem_gb_override_applies_to_lookup_population(tmp_path: Path):
    """The override applies after the profile is resolved, not just to flat."""
    lookup_file = tmp_path / "instrument_model"
    lookup_file.write_text("Illumina iSeq 100", encoding="utf-8")
    step = _lookup_step()
    # iSeq profile asks mem_gb=16; floor raises it to 64.
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={"instrument_model": str(lookup_file)},
        action_ceiling=_CEILING,
        mem_gb_override=64,
    )
    assert resolved.mem_gb == 64


def test_mem_gb_override_above_ceiling_is_rejected():
    """Defense in depth: an override above the ceiling is rejected at dispatch
    (the submission route already 422s it earlier)."""
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1)), name="over")
    tight = ActionCeiling(cpu=32, mem_gb=64, walltime=timedelta(hours=24), gpu=4)
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(entry=step, bound={}, action_ceiling=tight, mem_gb_override=128)
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.step_name == "over"
    assert "mem_gb" in exc.reason and "exceeds" in exc.reason


# =============================================================================
# OOM memory escalation — grow the floor on each OOM retry, clamped to ceiling
# =============================================================================

# The reference_load shape the escalation was written for: 32 GB baseline,
# 128 GB action ceiling. Doubling reaches the ceiling in two OOM retries.
_LOAD_CEILING = ActionCeiling(cpu=16, mem_gb=128, walltime=timedelta(hours=4), gpu=0)


def _load_step() -> WorkflowStep:
    return _step(BaselineResources(cpu=8, mem_gb=32, walltime=timedelta(hours=2)), name="load")


def test_escalation_doubles_from_baseline_when_no_override():
    """First OOM with no prior override grows the resolved baseline ×2."""
    floor = _escalated_mem_floor_after_oom(
        entry=_load_step(),
        bound={},
        action_ceiling=_LOAD_CEILING,
        current_override=None,
    )
    assert floor == 64


def test_escalation_doubles_from_current_override_floor():
    """The grow is relative to what the failed attempt actually ran at —
    max(baseline, current_override) — not the raw baseline."""
    floor = _escalated_mem_floor_after_oom(
        entry=_load_step(),
        bound={},
        action_ceiling=_LOAD_CEILING,
        current_override=40,
    )
    # resolved = max(32, 40) = 40; 40 * 2 = 80, under the 128 ceiling.
    assert floor == 80


def test_escalation_clamps_to_action_ceiling():
    floor = _escalated_mem_floor_after_oom(
        entry=_load_step(),
        bound={},
        action_ceiling=_LOAD_CEILING,
        current_override=80,
    )
    # 80 * 2 = 160, clamped down to the 128 ceiling.
    assert floor == 128


def test_escalation_at_ceiling_keeps_current_override():
    """Once resolved memory is at the ceiling there is no headroom: keep the
    current floor so the retry still runs at the ceiling (not back to baseline)."""
    floor = _escalated_mem_floor_after_oom(
        entry=_load_step(),
        bound={},
        action_ceiling=_LOAD_CEILING,
        current_override=128,
    )
    assert floor == 128


def test_escalation_full_sequence_to_ceiling():
    """End-to-end floor trajectory across successive OOM retries: a 32 GB
    baseline climbs 64 → 128 and then pins at the 128 GB ceiling."""
    step, bound = _load_step(), {}
    floor = None
    trajectory = []
    for _ in range(4):
        floor = _escalated_mem_floor_after_oom(
            entry=step, bound=bound, action_ceiling=_LOAD_CEILING, current_override=floor
        )
        trajectory.append(floor)
    assert trajectory == [64, 128, 128, 128]


# =============================================================================
# Per-run walltime override — raise-only floor, ceiling-bounded
# =============================================================================


def test_walltime_override_raises_floor_above_baseline():
    """An override above the YAML baseline raises walltime; cpu/mem_gb/gpu
    are untouched."""
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1), gpu=1))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, walltime_override=timedelta(hours=4)
    )
    assert resolved == FlatBaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=4), gpu=1)


def test_walltime_override_below_baseline_is_noop():
    """Raise-only: an override smaller than the step's baseline never lowers
    a step the YAML sized higher."""
    step = _step(BaselineResources(cpu=8, mem_gb=32, walltime=timedelta(hours=6)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, walltime_override=timedelta(hours=2)
    )
    assert resolved.walltime == timedelta(hours=6)


def test_walltime_override_none_leaves_baseline_verbatim():
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, walltime_override=None
    )
    assert resolved.walltime == timedelta(hours=1)


def test_walltime_override_applies_to_lookup_population(tmp_path: Path):
    """The override applies after the profile is resolved, not just to flat."""
    lookup_file = tmp_path / "instrument_model"
    lookup_file.write_text("Illumina iSeq 100", encoding="utf-8")
    step = _lookup_step()
    # iSeq profile asks walltime=3h; floor raises it to 5h.
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={"instrument_model": str(lookup_file)},
        action_ceiling=_CEILING,
        walltime_override=timedelta(hours=5),
    )
    assert resolved.walltime == timedelta(hours=5)


def test_walltime_override_above_ceiling_is_rejected():
    """Defense in depth: an override above the ceiling is rejected at dispatch
    (the submission route already 422s it earlier)."""
    step = _step(BaselineResources(cpu=4, mem_gb=8, walltime=timedelta(hours=1)), name="over")
    tight = ActionCeiling(cpu=32, mem_gb=512, walltime=timedelta(hours=4), gpu=4)
    with pytest.raises(BackendFailure) as ei:
        _resolve_baseline_for_step(
            entry=step, bound={}, action_ceiling=tight, walltime_override=timedelta(hours=8)
        )
    exc = ei.value
    assert exc.kind == FailureKind.CONTRACT_VIOLATION
    assert exc.step_name == "over"
    assert "walltime" in exc.reason and "exceeds" in exc.reason


# =============================================================================
# TIMEOUT walltime escalation — grow the floor on each TIMEOUT retry, clamped
# =============================================================================

# The qc shape the escalation was written for: 2h baseline, 8h action ceiling.
# Doubling reaches the ceiling in two TIMEOUT retries.
_QC_CEILING = ActionCeiling(cpu=8, mem_gb=32, walltime=timedelta(hours=8), gpu=0)


def _qc_step() -> WorkflowStep:
    return _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)), name="qc")


def test_walltime_escalation_doubles_from_baseline_when_no_override():
    """First TIMEOUT with no prior override grows the resolved baseline ×2."""
    floor = _escalated_walltime_after_timeout(
        entry=_qc_step(),
        bound={},
        action_ceiling=_QC_CEILING,
        current_override=None,
    )
    assert floor == timedelta(hours=4)


def test_walltime_escalation_doubles_from_current_override_floor():
    """The grow is relative to what the failed attempt actually ran at —
    max(baseline, current_override) — not the raw baseline."""
    floor = _escalated_walltime_after_timeout(
        entry=_qc_step(),
        bound={},
        action_ceiling=_QC_CEILING,
        current_override=timedelta(hours=3),
    )
    # resolved = max(2h, 3h) = 3h; 3h * 2 = 6h, under the 8h ceiling.
    assert floor == timedelta(hours=6)


def test_walltime_escalation_clamps_to_action_ceiling():
    floor = _escalated_walltime_after_timeout(
        entry=_qc_step(),
        bound={},
        action_ceiling=_QC_CEILING,
        current_override=timedelta(hours=6),
    )
    # 6h * 2 = 12h, clamped down to the 8h ceiling.
    assert floor == timedelta(hours=8)


def test_walltime_escalation_at_ceiling_keeps_current_override():
    """Once resolved walltime is at the ceiling there is no headroom: keep the
    current floor so the retry still runs at the ceiling (not back to baseline)."""
    floor = _escalated_walltime_after_timeout(
        entry=_qc_step(),
        bound={},
        action_ceiling=_QC_CEILING,
        current_override=timedelta(hours=8),
    )
    assert floor == timedelta(hours=8)


def test_walltime_escalation_full_sequence_to_ceiling():
    """End-to-end floor trajectory across successive TIMEOUT retries: a 2h
    baseline climbs 4h → 8h and then pins at the 8h ceiling."""
    step, bound = _qc_step(), {}
    floor = None
    trajectory = []
    for _ in range(4):
        floor = _escalated_walltime_after_timeout(
            entry=step, bound=bound, action_ceiling=_QC_CEILING, current_override=floor
        )
        trajectory.append(floor)
    assert trajectory == [
        timedelta(hours=4),
        timedelta(hours=8),
        timedelta(hours=8),
        timedelta(hours=8),
    ]


def test_attempt_is_unowned():
    """Guard for the fresh-re-run attempt-dir advance. A pre-existing progress row
    for this exact (step_index, attempt) means resume-adoption is in play (the
    runner re-attaches to the live job and reuses its dir) — the attempt is owned,
    leave it. No row means the attempt is unowned: a fresh re-run (e.g. a redrive
    whose completed prep row was invalidated, or `/run` having dropped the failed
    row), so any attempt dir on disk is orphaned and the runner advances past it
    to a fresh one rather than deleting the SLURM-job-owned output."""
    rows = [SimpleNamespace(step_index=0, attempt=0)]
    # Pre-existing row for this exact (step_index, attempt) → adoption, owned.
    assert _attempt_is_unowned(rows, step_index=0, attempt=0) is False
    # No rows at all → fresh re-run, unowned.
    assert _attempt_is_unowned([], step_index=0, attempt=0) is True
    # A row for a different attempt of the same step → this attempt is fresh.
    assert _attempt_is_unowned(rows, step_index=0, attempt=1) is True
    # A row for a different step → unrelated to this dir, unowned.
    assert _attempt_is_unowned(rows, step_index=1, attempt=0) is True
