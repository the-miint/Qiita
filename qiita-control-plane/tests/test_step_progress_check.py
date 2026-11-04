"""`step_progress.check_step_progress`: which progress rows still name the entry at
their recorded position in an action's current `steps:` list."""

from datetime import UTC, datetime

import pytest
from qiita_common.actions import BLOCK_MASK_ACTION_ID, WorkflowAction, WorkflowStep
from qiita_common.models import ComputeTarget, StepProgressState

from qiita_control_plane import step_progress


def _action_entry(name: str) -> WorkflowAction:
    return WorkflowAction(kind="action", name=name)


def _step_entry(name: str) -> WorkflowStep:
    return WorkflowStep(
        kind="step",
        name=name,
        step_type="singleton",
        module=f"qiita_compute_orchestrator.jobs.{name.replace('-', '_')}",
        baseline_resources={"cpu": 1, "mem_gb": 1, "walltime": "PT1M"},
    )


def _row(step_index: int, name: str, target: ComputeTarget) -> step_progress.StepProgressRow:
    now = datetime.now(UTC)
    return step_progress.StepProgressRow(
        work_ticket_idx=7,
        step_index=step_index,
        attempt=0,
        step_name=name,
        compute_target=target,
        state=StepProgressState.COMPLETED,
        slurm_job_id=None,
        job_name=None,
        failure_kind=None,
        failure_reason=None,
        created_at=now,
        updated_at=now,
    )


_ROWS = [_row(0, "hash", ComputeTarget.LOCAL), _row(1, "load", ComputeTarget.CONTROL_PLANE)]


def test_rows_at_their_entries_pass():
    step_progress.check_step_progress(
        7, "some-action", _ROWS, [_step_entry("hash"), _action_entry("load"), _step_entry("end")]
    )


@pytest.mark.parametrize(
    "entries",
    [
        # The entry was removed from the end of the list.
        [_step_entry("hash")],
        # An entry was inserted ahead of it, so another name sits at its position.
        [_step_entry("hash"), _step_entry("inserted"), _action_entry("load")],
        # The name at its position now belongs to a `step:`; an `action:` wrote the row.
        [_step_entry("hash"), _step_entry("load")],
    ],
    ids=["removed", "shifted", "other kind"],
)
def test_a_row_the_entry_at_its_position_does_not_match_is_refused(entries):
    with pytest.raises(step_progress.StepProgressMismatch) as raised:
        step_progress.check_step_progress(7, "some-action", _ROWS, entries)
    message = str(raised.value)
    assert "'load' at step 1" in message
    assert "'hash'" not in message
    assert message.endswith("submit a new ticket instead.")


def test_a_step_row_at_an_action_entry_is_refused():
    with pytest.raises(step_progress.StepProgressMismatch) as raised:
        step_progress.check_step_progress(
            7, "some-action", _ROWS, [_action_entry("hash"), _action_entry("load")]
        )
    assert "'hash' at step 0" in str(raised.value)
    assert "'load'" not in str(raised.value)


def test_a_refused_block_mask_ticket_is_told_to_re_mask():
    """A fresh block plan is refused for a sample whose mask gate is pending, so a
    block-mask ticket is not told to submit a new ticket."""
    with pytest.raises(step_progress.StepProgressMismatch) as raised:
        step_progress.check_step_progress(7, BLOCK_MASK_ACTION_ID, _ROWS, [_step_entry("hash")])
    assert str(raised.value).endswith("re-mask its prep_samples under a new mask version instead.")
