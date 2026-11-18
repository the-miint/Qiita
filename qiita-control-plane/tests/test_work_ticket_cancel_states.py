"""Which work_ticket states `cancel_work_ticket` flips to `cancelled`.

This pins the state set over every `WorkTicketState`, so a state added to the enum
has to be placed on one side; the route's DB tests drive the flip itself.
"""

import pytest
from qiita_common.models import WorkTicketState

from qiita_control_plane.work_ticket_cancel import CANCELLABLE_WORK_TICKET_STATES

_FLIPS = {
    WorkTicketState.PENDING: True,
    WorkTicketState.QUEUED: True,
    WorkTicketState.PROCESSING: True,
    WorkTicketState.FAILED: True,
    WorkTicketState.COMPLETED: False,
    WorkTicketState.NO_DATA: False,
    WorkTicketState.CANCELLED: False,
}


def test_every_state_is_classified():
    assert set(_FLIPS) == set(WorkTicketState)


@pytest.mark.parametrize("state", list(WorkTicketState))
def test_cancel_flips_exactly_the_non_terminal_states_and_failed(state):
    assert (state.value in CANCELLABLE_WORK_TICKET_STATES) is _FLIPS[state]
