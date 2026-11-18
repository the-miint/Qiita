"""Which prep_samples a pool cohort listing that reads past deprecation leaves out
(the rules at `definition_resolver.NO_RUN_TO_READ`): the exact read of a deprecated
alignment, over every resolution the exact resolver can return, and the read of an
active alignment past a deprecated upstream."""

import pytest
from qiita_common.models import (
    DefinitionKind,
    SampleResolution,
    SampleResolutionReason,
    SampleResolutionState,
)

from qiita_control_plane.definition_resolver import exact_read_left_out, upstream_read_left_out

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


@pytest.mark.parametrize(
    ("state", "reason", "left_out"),
    [
        (NOT_PROCESSED, R.UPSTREAM_WITHDRAWN, True),
        (INCOMPLETE, R.IN_PROGRESS, False),
        (INCOMPLETE, R.FAILED, False),
        (INCOMPLETE, R.UPSTREAM_INCOMPLETE, False),
        (INCOMPLETE, R.UPSTREAM_MOVED, False),
        (NOT_PROCESSED, R.WITHDRAWN, False),
        (NOT_PROCESSED, R.DEPRECATED, False),
        (NOT_PROCESSED, R.UPSTREAM_DEPRECATED, False),
        (NOT_PROCESSED, R.UPSTREAM_NO_RUN, False),
        (NOT_PROCESSED, R.NO_RUN, False),
    ],
)
def test_an_upstream_read_leaves_out_only_a_withdrawn_upstream_run(state, reason, left_out):
    assert bool(upstream_read_left_out([_resolution(state, reason)])) is left_out
