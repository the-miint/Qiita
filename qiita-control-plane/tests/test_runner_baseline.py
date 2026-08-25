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

import tempfile
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import asyncpg
import pytest
from qiita_common.actions import (
    ActionCeiling,
    BaselineResources,
    FlatBaselineResources,
    WorkflowStep,
)
from qiita_common.backend_failure import BackendFailure, FailureKind
from qiita_common.models import (
    LIVE_STEP_PROGRESS_STATES,
    TERMINAL_STEP_PROGRESS_STATES,
    StepPlanResponse,
    StepProgressState,
    StepType,
    WorkTicketFailureStage,
)

from qiita_control_plane.runner import (
    _POLL_DB_READ_MAX_ATTEMPTS,
    StepResourceFloor,
    WorkflowAborted,
    _attempt_is_terminal,
    _attempt_is_unowned,
    _bind_step_inputs,
    _escalated_mem_floor_after_oom,
    _escalated_walltime_after_timeout,
    _fetch_plan_hint,
    _is_transient_db_error,
    _parse_escalated_floor,
    _raise_if_ticket_terminal,
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
        entrypoint="/opt/qiita/entrypoint.sh",
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
    """Once resolved memory is at the ceiling there is no headroom: return the
    current floor unchanged. The retry loop reads that unchanged value as the
    saturation signal and fails the ticket (RESOURCE_CEILING_EXHAUSTED) rather
    than retrying at a size that just OOM'd — see the retry-loop test."""
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
# Persisted escalated floor — decoding work_ticket.escalated_resource_floor
# =============================================================================
#
# The column is what carries a step's learned floor across a CP restart or a
# `/run` redrive, so the decoder is the seam where a wrong shape would silently
# reset the ladder to the YAML baseline. It raises instead: an unreadable floor
# is indistinguishable from "never escalated", and quietly picking the latter is
# exactly the re-climb the column exists to prevent.


def test_parse_escalated_floor_null_is_empty():
    """A ticket that has never escalated reads NULL → no floors, which leaves
    every step sized exactly as it is today."""
    assert _parse_escalated_floor(None, work_ticket_idx=1) == {}


def test_parse_escalated_floor_decodes_both_axes():
    parsed = _parse_escalated_floor(
        {"assemble": {"mem_gb": 384, "walltime_seconds": 115200}}, work_ticket_idx=1
    )
    assert parsed == {"assemble": StepResourceFloor(mem_gb=384, walltime=timedelta(hours=32))}


def test_parse_escalated_floor_axes_are_independent():
    """A step that has only ever OOM-killed carries memory alone (and vice
    versa) — the two arms of the retry loop escalate independently."""
    parsed = _parse_escalated_floor(
        {"assemble": {"mem_gb": 384}, "qc": {"walltime_seconds": 28800}}, work_ticket_idx=1
    )
    assert parsed["assemble"] == StepResourceFloor(mem_gb=384, walltime=None)
    assert parsed["qc"] == StepResourceFloor(mem_gb=None, walltime=timedelta(hours=8))


def test_parse_escalated_floor_multiple_steps_stay_separate():
    """Per-step keying is the whole point: a floor learned for one step must
    not leak onto another (the reason this is not folded into the ticket-wide
    `resource_override`)."""
    parsed = _parse_escalated_floor(
        {"assemble": {"mem_gb": 384}, "bin_refine": {"mem_gb": 64}}, work_ticket_idx=1
    )
    assert parsed["assemble"].mem_gb == 384
    assert parsed["bin_refine"].mem_gb == 64


@pytest.mark.parametrize(
    "raw",
    [
        [{"mem_gb": 8}],  # top level must be an object, not an array
        "mem_gb=8",  # ... nor a scalar
    ],
)
def test_parse_escalated_floor_rejects_non_object_top_level(raw):
    with pytest.raises(RuntimeError, match="expected a JSON object"):
        _parse_escalated_floor(raw, work_ticket_idx=7)


def test_parse_escalated_floor_rejects_non_object_step_value():
    with pytest.raises(RuntimeError, match="expected a JSON object"):
        _parse_escalated_floor({"assemble": 384}, work_ticket_idx=7)


@pytest.mark.parametrize(
    "value",
    [
        0,  # a zero floor would resolve to "no floor" at dispatch
        -8,
        "384",  # a JSON string would compare wrong against an int baseline
        384.5,
        True,  # bool is an int subclass — must not read as a 1 GB floor
    ],
)
def test_parse_escalated_floor_rejects_bad_axis_value(value):
    with pytest.raises(RuntimeError, match="expected a positive integer"):
        _parse_escalated_floor({"assemble": {"mem_gb": value}}, work_ticket_idx=7)


def test_parse_escalated_floor_error_names_ticket_and_step():
    """A 2am post-mortem needs to know WHICH ticket and step carry the bad
    value — the column holds one object per step."""
    with pytest.raises(RuntimeError) as exc_info:
        _parse_escalated_floor({"assemble": {"walltime_seconds": -1}}, work_ticket_idx=6978)
    message = str(exc_info.value)
    assert "6978" in message
    assert "assemble" in message
    assert "walltime_seconds" in message


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
    """Once resolved walltime is at the ceiling there is no headroom: return the
    current floor unchanged. The retry loop reads that unchanged value as the
    saturation signal and fails the ticket (RESOURCE_CEILING_EXHAUSTED) rather
    than retrying at a limit that just timed out — see the retry-loop test."""
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


# =============================================================================
# plan() down-size hint — lowers below baseline; escalation still wins on retry
# =============================================================================


def test_plan_hint_lowers_mem_below_baseline():
    """A plan() memory hint below the YAML baseline down-sizes the step."""
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, plan_hint=StepPlanResponse(mem_gb=4)
    )
    assert resolved.mem_gb == 4


def test_plan_hint_lowers_walltime_below_baseline():
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=4)))
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=_CEILING,
        plan_hint=StepPlanResponse(walltime_seconds=600),
    )
    assert resolved.walltime == timedelta(seconds=600)


def test_plan_hint_above_baseline_is_noop():
    """Down-only: a hint larger than the baseline never raises the step
    (up-sizing is escalation's job, not plan's)."""
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=_CEILING,
        plan_hint=StepPlanResponse(
            mem_gb=48, walltime_seconds=int(timedelta(hours=8).total_seconds())
        ),
    )
    assert resolved.mem_gb == 12
    assert resolved.walltime == timedelta(hours=2)


def test_plan_hint_none_leaves_baseline_verbatim():
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step, bound={}, action_ceiling=_CEILING, plan_hint=None
    )
    assert resolved == FlatBaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2))


def test_plan_hint_partial_only_touches_named_axis():
    """A hint that sets only walltime leaves cpu/mem at the baseline."""
    step = _step(BaselineResources(cpu=8, mem_gb=32, walltime=timedelta(hours=4)))
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=_CEILING,
        plan_hint=StepPlanResponse(walltime_seconds=300),
    )
    assert resolved.cpu == 8
    assert resolved.mem_gb == 32
    assert resolved.walltime == timedelta(seconds=300)


def test_escalation_override_beats_plan_hint_on_retry():
    """The correctness invariant: plan() down-size is applied BEFORE the
    raise-only escalation floors, so a retry always restores at least the
    baseline — an escalated floor is grown from the baseline and so is >= any
    down-sized value. A 12 GB baseline down-sized to 4 GB by plan, with a 24 GB
    OOM escalation floor, resolves to 24 GB — the hint does not strand the retry
    below the size it needs."""
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=_CEILING,
        mem_gb_override=24,
        walltime_override=timedelta(hours=4),
        plan_hint=StepPlanResponse(mem_gb=4, walltime_seconds=600),
    )
    assert resolved.mem_gb == 24
    assert resolved.walltime == timedelta(hours=4)


def test_plan_hint_cannot_undercut_a_persisted_floor_on_attempt_zero():
    """Same invariant, on the path the persisted floor newly opens: a resumed
    run seeds its floors from `escalated_resource_floor`, so attempt 0 can carry
    an override with no OOM/TIMEOUT in THIS run. The hint must not claw that
    back — a step that has ever needed 24 GB does not get re-run at plan()'s
    optimistic 4 GB just because the process restarted."""
    step = _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)))
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=_CEILING,
        # As seeded from a persisted StepResourceFloor(mem_gb=24, walltime=4h).
        mem_gb_override=24,
        walltime_override=timedelta(hours=4),
        plan_hint=StepPlanResponse(cpu=2, mem_gb=4, walltime_seconds=600),
    )
    assert resolved.mem_gb == 24
    assert resolved.walltime == timedelta(hours=4)
    # cpu is not an escalating axis, so the hint still lowers it — the floors
    # protect only the two axes that escalate.
    assert resolved.cpu == 2


def test_plan_hint_not_applied_without_escalation_headroom():
    """A down-size is skipped on an axis where baseline == ceiling: with no room
    for escalation to grow, a down-sized attempt that OOMs/TIMEOUTs would be
    misread as RESOURCE_CEILING_EXHAUSTED and fail without ever running at the
    baseline. So the axis stays at the baseline; only axes with headroom (cpu
    here, 4 < 32) are lowered."""
    step = _step(BaselineResources(cpu=4, mem_gb=32, walltime=timedelta(hours=8)))
    # ceiling == baseline on mem_gb AND walltime -> no headroom on either; cpu
    # has headroom (4 < 32).
    tight = ActionCeiling(cpu=32, mem_gb=32, walltime=timedelta(hours=8), gpu=4)
    resolved = _resolve_baseline_for_step(
        entry=step,
        bound={},
        action_ceiling=tight,
        plan_hint=StepPlanResponse(cpu=2, mem_gb=4, walltime_seconds=600),
    )
    assert resolved.mem_gb == 32  # no headroom -> not lowered
    assert resolved.walltime == timedelta(hours=8)  # no headroom -> not lowered
    assert resolved.cpu == 2  # headroom (4 < 32) -> lowered


# =============================================================================
# _fetch_plan_hint — advisory: container steps skip; any failure -> None
# =============================================================================


def _native_step(name: str = "qc") -> WorkflowStep:
    """A native (module) step — the only kind _fetch_plan_hint queries."""
    return WorkflowStep(
        kind="step",
        name=name,
        step_type=StepType.SINGLETON,
        module="qiita_compute_orchestrator.jobs.qc",
        baseline_resources=BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)),
    )


class _FakePlanClient:
    """Stand-in for ComputeBackendClient.plan_step. `result` is returned, or an
    Exception instance is raised, to script the fetch outcome."""

    def __init__(self, result):
        self._result = result
        self.calls: list[dict] = []

    async def plan_step(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self._result, BaseException):
            raise self._result
        return self._result


async def test_fetch_plan_hint_returns_hint_for_native_step():
    client = _FakePlanClient(StepPlanResponse(walltime_seconds=600))
    hint = await _fetch_plan_hint(
        client,
        _native_step(),
        {"reads": "/scratch/r.parquet"},
        {"kind": "prep_sample", "prep_sample_idx": 5},
        work_ticket_idx=7,
    )
    assert hint == StepPlanResponse(walltime_seconds=600)
    # It forwarded the module + bound inputs so plan() sees the right Inputs.
    assert client.calls[0]["module"] == "qiita_compute_orchestrator.jobs.qc"
    assert client.calls[0]["work_ticket_idx"] == 7


async def test_fetch_plan_hint_skips_container_step():
    """A container step has no plan(); the fetch is skipped without a call."""
    client = _FakePlanClient(StepPlanResponse(mem_gb=4))
    hint = await _fetch_plan_hint(
        client,
        _step(BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2))),
        {},
        {"kind": "reference", "reference_idx": 1},
        work_ticket_idx=1,
    )
    assert hint is None
    assert client.calls == []


async def test_fetch_plan_hint_degrades_to_none_on_error():
    """Advisory: ANY failure (here an unreachable-orchestrator surrogate) must
    degrade to None so dispatch proceeds on the YAML baseline."""
    client = _FakePlanClient(RuntimeError("orchestrator down"))
    hint = await _fetch_plan_hint(
        client,
        _native_step(),
        {"reads": "/scratch/r.parquet"},
        {"kind": "prep_sample", "prep_sample_idx": 5},
        work_ticket_idx=7,
    )
    assert hint is None


def test_bind_step_inputs_paths_and_scalar_params():
    """Inputs/optional_inputs become Paths; scalar params stay strings — the
    shared shape submit and plan both send."""
    step = WorkflowStep(
        kind="step",
        name="qc",
        step_type=StepType.SINGLETON,
        module="qiita_compute_orchestrator.jobs.qc",
        inputs=["reads"],
        optional_inputs=["adapter_parquet"],
        params={"instrument_model_ctx": "instrument_model"},
        baseline_resources=BaselineResources(cpu=4, mem_gb=12, walltime=timedelta(hours=2)),
    )
    bound = {
        "reads": "/scratch/r.parquet",
        "adapter_parquet": "/scratch/a.parquet",
        "instrument_model_ctx": "NextSeq 550",
    }
    out = _bind_step_inputs(step, bound)
    assert out["reads"] == Path("/scratch/r.parquet")
    assert out["adapter_parquet"] == Path("/scratch/a.parquet")
    # scalar param: keyed by the Inputs field name, value left a string.
    assert out["instrument_model"] == "NextSeq 550"


def _prow(step_index: int, attempt: int, state: StepProgressState | None = None):
    """Minimal progress-row stand-in: these predicates read only these fields."""
    return SimpleNamespace(step_index=step_index, attempt=attempt, state=state)


def test_attempt_is_unowned():
    """Guard for the fresh-re-run attempt-dir advance. A pre-existing progress row
    for this exact (step_index, attempt) means a prior process owns the dir — the
    attempt is owned, leave it. No row means the attempt is unowned: a fresh
    re-run (e.g. a redrive whose completed prep row was invalidated, or `/run`
    having dropped the dead row), so any attempt dir on disk is orphaned and the
    runner advances past it to a fresh one rather than deleting the
    SLURM-job-owned output."""
    rows = [_prow(0, 0, StepProgressState.SUBMITTED)]
    # Pre-existing row for this exact (step_index, attempt) → adoption, owned.
    assert _attempt_is_unowned(rows, step_index=0, attempt=0) is False
    # No rows at all → fresh re-run, unowned.
    assert _attempt_is_unowned([], step_index=0, attempt=0) is True
    # A row for a different attempt of the same step → this attempt is fresh.
    assert _attempt_is_unowned(rows, step_index=0, attempt=1) is True
    # A row for a different step → unrelated to this dir, unowned.
    assert _attempt_is_unowned(rows, step_index=1, attempt=0) is True


def test_attempt_is_terminal_separates_dead_attempts_from_live_ones():
    """Only a LIVE attempt is adoptable — the invariant that keeps restart
    recovery off an ENDED job.

    The end-to-end regression (resume landing on the live attempt) is pinned in
    `test_runner.py::test_resume_skips_terminal_attempt_and_adopts_the_live_one`;
    this covers the predicate's own edges."""
    failed = [_prow(0, 0, StepProgressState.FAILED)]
    assert _attempt_is_terminal(failed, step_index=0, attempt=0) is True
    # The distinction that matters: `_attempt_is_unowned` alone reports the same
    # dead row as OWNED, because it only asks whether a row exists.
    assert _attempt_is_unowned(failed, step_index=0, attempt=0) is False

    # `completed` is terminal too. Normally consumed by the step-level
    # fast-forward before the attempt loop runs, so this arm is belt-and-braces
    # rather than a path exercised in practice.
    assert (
        _attempt_is_terminal([_prow(0, 0, StepProgressState.COMPLETED)], step_index=0, attempt=0)
        is True
    )

    # Live rows are NOT terminal — skipping one would strand a real SLURM job
    # and resubmit work already paid for.
    for live in LIVE_STEP_PROGRESS_STATES:
        assert _attempt_is_terminal([_prow(0, 0, live)], step_index=0, attempt=0) is False

    # No row at all → nothing terminal here (the orphan-dir path owns that case).
    assert _attempt_is_terminal([], step_index=0, attempt=0) is False
    # Scoping: a terminal row for another attempt or another step must not leak.
    assert _attempt_is_terminal(failed, step_index=0, attempt=1) is False
    assert _attempt_is_terminal(failed, step_index=1, attempt=0) is False


def test_terminal_and_live_step_progress_states_partition_the_enum():
    """The two sets are complements by construction — a new StepProgressState
    lands in LIVE (and so becomes adoptable) only via an explicit edit to the
    terminal tuple."""
    assert set(TERMINAL_STEP_PROGRESS_STATES) | set(LIVE_STEP_PROGRESS_STATES) == set(
        StepProgressState
    )
    assert not set(TERMINAL_STEP_PROGRESS_STATES) & set(LIVE_STEP_PROGRESS_STATES)


# =============================================================================
# Transient control-plane DB errors — classification + poll-loop resilience
# =============================================================================
#
# A per-statement command_timeout (or a brief CP-DB connection blip) on the
# runner's OWN DB calls must NOT be misclassified as a permanent step failure
# (which abandons a healthy in-flight SLURM job). These cover the classifier and
# the resilient force-fail check; the run_workflow catch-all path is covered in
# the DB-backed test_runner.py.


@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError(),  # asyncpg command_timeout surfaces as bare asyncio.TimeoutError
        asyncpg.PostgresConnectionError("connection lost"),
        asyncpg.exceptions.ConnectionDoesNotExistError("gone"),  # subclass of the above
        asyncpg.InterfaceError("pool is closing"),
    ],
)
def test_is_transient_db_error_true_for_transient(exc):
    assert _is_transient_db_error(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        asyncpg.exceptions.UniqueViolationError("dup"),  # a real SQL error stays permanent
        ValueError("bad input"),
        RuntimeError("programming bug"),
        BackendFailure(
            kind=FailureKind.OOM_KILLED,
            stage=WorkTicketFailureStage.STEP_RUN,
            step_name="x",
            reason="x",
        ),
    ],
)
def test_is_transient_db_error_false_for_non_transient(exc):
    assert _is_transient_db_error(exc) is False


class _FakePool:
    """Minimal stand-in for the asyncpg.Pool.fetchval `_raise_if_ticket_terminal`
    calls. `results` is consumed one item per call: an Exception is raised, any
    other value is returned (the ticket state)."""

    def __init__(self, *results):
        self._results = list(results)
        self.calls = 0

    async def fetchval(self, _sql, _idx, *, timeout=None):
        self.calls += 1
        item = self._results.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


async def test_raise_if_ticket_terminal_returns_when_not_terminal():
    pool = _FakePool("processing")
    await _raise_if_ticket_terminal(pool, 1)  # no raise
    assert pool.calls == 1


async def test_raise_if_ticket_terminal_aborts_when_terminal():
    pool = _FakePool("failed")
    with pytest.raises(WorkflowAborted):
        await _raise_if_ticket_terminal(pool, 1)
    assert pool.calls == 1


async def test_raise_if_ticket_terminal_retries_transient_db_error_then_succeeds(monkeypatch):
    """A transient DB error (command_timeout / connection blip) on the force-fail
    check is retried in place; a later success keeps the poll loop alive instead
    of abandoning a healthy job."""
    monkeypatch.setattr("qiita_control_plane.runner._POLL_DB_READ_BACKOFF_SECONDS", 0.0)
    pool = _FakePool(TimeoutError(), asyncpg.InterfaceError("blip"), "processing")
    await _raise_if_ticket_terminal(pool, 1)  # survives the two blips
    assert pool.calls == 3


async def test_raise_if_ticket_terminal_reraises_after_exhausting_transient_retries(monkeypatch):
    """A sustained DB outage exhausts the bounded retries and re-raises, so the
    catch-all can record it RETRIABLE (not silently swallow it forever)."""
    monkeypatch.setattr("qiita_control_plane.runner._POLL_DB_READ_BACKOFF_SECONDS", 0.0)
    pool = _FakePool(*[TimeoutError() for _ in range(_POLL_DB_READ_MAX_ATTEMPTS)])
    with pytest.raises(TimeoutError):
        await _raise_if_ticket_terminal(pool, 1)
    assert pool.calls == _POLL_DB_READ_MAX_ATTEMPTS


async def test_raise_if_ticket_terminal_propagates_non_transient_db_error():
    """A real SQL error is not transient — it propagates on the first raise,
    never retried."""
    pool = _FakePool(asyncpg.exceptions.UniqueViolationError("dup"))
    with pytest.raises(asyncpg.exceptions.UniqueViolationError):
        await _raise_if_ticket_terminal(pool, 1)
    assert pool.calls == 1


def test_every_escalating_axis_has_exactly_one_escalation_helper():
    """`ESCALATING_RESOURCE_AXES` (qiita-common) and the runner's escalation
    helpers must name the same set of axes, checked in both directions.

    The constant is what the shipped-workflow headroom guard checks against, so
    the two drifting apart breaks that guard silently and in either direction: a
    new axis added to the constant with no helper makes the guard demand headroom
    for a retry that never happens, while a new helper with no entry in the
    constant means the guard stops covering an axis that now really does
    escalate. Neither shows up as a failure anywhere else — the constant lives in
    a package that cannot import the runner, so this test is the only place the
    two can be compared.

    The second direction is only as good as the `_escalated_*` naming: a helper
    that escalates an axis under some other name is invisible to the scan below.
    That is a convention, not a guarantee, and it is the reason the map is
    written out by hand here rather than derived.
    """
    from qiita_common.actions import ESCALATING_RESOURCE_AXES

    import qiita_control_plane.runner._dispatch as dispatch

    helper_by_axis = {
        "mem_gb": "_escalated_mem_floor_after_oom",
        "walltime": "_escalated_walltime_after_timeout",
    }

    assert tuple(helper_by_axis) == ESCALATING_RESOURCE_AXES, (
        "an axis escalates but is not declared in ESCALATING_RESOURCE_AXES (or "
        "vice versa) — the shipped-workflow headroom guard reads that constant"
    )
    defined = {name for name in vars(dispatch) if name.startswith("_escalated_")}
    assert defined == set(helper_by_axis.values()), (
        "the runner's escalation helpers no longer match the axis map above; add "
        f"the new axis to ESCALATING_RESOURCE_AXES too. Found: {sorted(defined)}"
    )


# =============================================================================
# _write_adapter_parquet — reassembly order and the duplicate-position guard
# =============================================================================
#
# Sync, pure (pyarrow only), so it belongs in this tier. The BAD_INPUT wrapping
# and the partial-file unlink around it are `_resolve_qc_adapters`' job and are
# covered in tests/test_runner.py, which needs a reference row.


def test_adapter_chunks_reassemble_in_chunk_index_order():
    """Chunks arrive unordered; the sequence is their chunk_index order, and the
    Parquet carries one row per feature sorted by feature_idx."""
    import duckdb

    from qiita_control_plane.runner import _write_adapter_parquet

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "adapters.parquet"
        count = _write_adapter_parquet(
            [(9, 1, "TCTC"), (7, 2, "GGGG"), (9, 0, "CTG"), (7, 0, "AG"), (7, 1, "AT")], out
        )
        assert count == 2
        with duckdb.connect(":memory:") as conn:
            rows = conn.execute(
                f"SELECT feature_idx, sequence FROM read_parquet('{out}') ORDER BY feature_idx"
            ).fetchall()
    assert rows == [(7, "AGATGGGG"), (9, "CTGTCTC")]


def test_a_repeated_chunk_position_raises_naming_it():
    """Two rows at one (feature_idx, chunk_index) raise instead of being joined —
    the lake declares this table with no primary key, so the repeat reaches here.
    The fixture pair is one `canonical_sequence_hash_expr` folds into a single
    feature."""
    from qiita_control_plane.runner import _write_adapter_parquet

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "adapters.parquet"
        with pytest.raises(ValueError) as ei:
            _write_adapter_parquet(
                [
                    (127, 0, "AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT"),
                    (127, 0, "ACACTCTTTCCCTACACGACGCTCTTCCGATCT"),
                    (9, 0, "CTGTCTC"),
                ],
                out,
            )
        assert "1 chunk position(s)" in str(ei.value)
        assert "(feature_idx 127, chunk_index 0)" in str(ei.value)
        assert "reference_sequence_chunks" in str(ei.value)
        assert not out.exists()


def test_the_repeated_position_list_is_capped_but_the_count_is_not():
    """A reference whose every position repeats reports all of them in the count
    and at most _MAX_REPORTED in the list, so one bad reference cannot write an
    unbounded string into work_ticket.failure_reason."""
    from qiita_control_plane.runner._reference import _MAX_REPORTED, _write_adapter_parquet

    repeats = _MAX_REPORTED + 5
    rows = [(feature, 0, "ACGT") for feature in range(repeats) for _ in range(2)]
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(ValueError) as ei:
            _write_adapter_parquet(rows, Path(tmp) / "adapters.parquet")
    message = str(ei.value)
    assert f"{repeats} chunk position(s)" in message
    assert message.count("feature_idx") == _MAX_REPORTED


def test_an_empty_adapter_set_raises_before_writing_anything():
    """No sequences is a misconfiguration, not an empty-but-valid adapter set."""
    from qiita_control_plane.runner import _write_adapter_parquet

    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "adapters.parquet"
        with pytest.raises(ValueError, match="no sequences"):
            _write_adapter_parquet([], out)
        assert not out.exists()
