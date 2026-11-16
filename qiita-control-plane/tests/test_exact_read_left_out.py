"""Which prep_samples the exact read of a deprecated alignment leaves out of the pool
cohort listing (`routes/sequencing_run._NO_RUN_TO_READ`), over every resolution the
exact resolver can return."""

import pytest
from qiita_common.models import (
    DefinitionKind,
    SampleResolution,
    SampleResolutionReason,
    SampleResolutionState,
)

from qiita_control_plane.routes.sequencing_run import exact_read_left_out

R = SampleResolutionReason
INCOMPLETE = SampleResolutionState.INCOMPLETE
NOT_PROCESSED = SampleResolutionState.NOT_PROCESSED


def _resolution(state, reason):
    return SampleResolution(
        prep_sample_idx=1,
        state=state,
        definition_kind=DefinitionKind.ALIGNMENT,
        definition_idx=None if reason is R.NO_RUN else 3,
        reason=reason,
    )


@pytest.mark.parametrize(
    ("state", "reason", "left_out"),
    [
        (INCOMPLETE, R.IN_PROGRESS, True),
        (INCOMPLETE, R.FAILED, True),
        (INCOMPLETE, R.UPSTREAM_INCOMPLETE, True),
        (NOT_PROCESSED, R.WITHDRAWN, True),
        (NOT_PROCESSED, R.UPSTREAM_WITHDRAWN, True),
        (NOT_PROCESSED, R.UPSTREAM_NO_RUN, True),
        (NOT_PROCESSED, R.NO_RUN, False),
        (NOT_PROCESSED, R.NO_DATA, False),
    ],
)
def test_an_exact_read_leaves_out_every_unprocessed_reason_but_no_run_and_no_data(
    state, reason, left_out
):
    assert bool(exact_read_left_out([_resolution(state, reason)])) is left_out


def test_a_processed_prep_sample_is_not_left_out():
    processed = SampleResolution(
        prep_sample_idx=1,
        state=SampleResolutionState.PROCESSED,
        definition_kind=DefinitionKind.ALIGNMENT,
        definition_idx=3,
    )
    assert exact_read_left_out([processed]) == []
