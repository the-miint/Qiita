"""Truth table for the download-roster staging predicate (non-DB).

`download_ticket_read_roster` stands in for a roster read that records
nothing DB-side, judging by the pool's latest download ticket lifecycle
state, so every WorkTicketState value must be classified deliberately:
roster read, not yet read, or resubmittable (the next dispatch re-reads
the roster live).
"""

from qiita_common.models import WorkTicketState

from qiita_control_plane.ena_import.registration import (
    _RESUBMITTABLE_DOWNLOAD_TICKET_STATES,
    download_ticket_read_roster,
)


def test_roster_read_state_truth_table() -> None:
    """processing / completed / no_data say the roster has been (or is being)
    read; None (no ticket), pending, queued, failed, and cancelled say it has
    not."""
    read_states = (
        WorkTicketState.PROCESSING.value,
        WorkTicketState.COMPLETED.value,
        WorkTicketState.NO_DATA.value,
    )
    not_read_states = (
        None,
        WorkTicketState.PENDING.value,
        WorkTicketState.QUEUED.value,
        WorkTicketState.FAILED.value,
        WorkTicketState.CANCELLED.value,
    )
    for state in read_states:
        assert download_ticket_read_roster(state), state
    for state in not_read_states:
        assert not download_ticket_read_roster(state), state


def test_every_work_ticket_state_classified() -> None:
    """Read, not-yet-read, and resubmittable must partition the whole
    WorkTicketState enum: a new enum value added without a classification, or
    a state dropped from (or double-assigned to) a class, fails here."""
    all_states = {state.value for state in WorkTicketState}
    read_states = {state for state in all_states if download_ticket_read_roster(state)}
    not_yet_read_states = {WorkTicketState.PENDING.value, WorkTicketState.QUEUED.value}
    assert read_states | not_yet_read_states | _RESUBMITTABLE_DOWNLOAD_TICKET_STATES == all_states
    assert read_states.isdisjoint(not_yet_read_states)
    assert read_states.isdisjoint(_RESUBMITTABLE_DOWNLOAD_TICKET_STATES)
    assert not_yet_read_states.isdisjoint(_RESUBMITTABLE_DOWNLOAD_TICKET_STATES)
