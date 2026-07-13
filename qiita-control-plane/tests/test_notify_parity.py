"""Parity between the notify sweeper's owed-set predicate, the partial index
that backs it, and the terminal-state source of truth in qiita_common.

If these drift, the planner silently stops using the partial index (a
performance cliff at scale) or the sweeper emails a state it shouldn't. No DB
needed — these read the migration file and import the Python constants.
"""

from pathlib import Path

from qiita_common.models import (
    NON_TERMINAL_WORK_TICKET_STATES,
    TERMINAL_WORK_TICKET_STATES,
    WorkTicketState,
)

from qiita_control_plane.notify.sweeper import (
    _OWED_SET_WHERE,
    _TERMINAL_STATE_LITERALS,
)

_INDEX_MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "db"
    / "migrations"
    / "20260701000001_work_ticket_notified_idx.sql"
)

# The shared retriable carve-out: completed / no_data / permanent-failed
# email; only retriable-failed is withheld.
_RETRIABLE_CARVE_OUT = "failure_type IS DISTINCT FROM 'retriable'"


def test_terminal_state_literals_match_the_source_of_truth():
    # The sweeper re-sorts the qiita_common tuple alphabetically to byte-match the
    # index predicate — assert the round-trip so a change there is reflected here.
    assert set(_TERMINAL_STATE_LITERALS) == set(TERMINAL_WORK_TICKET_STATES)
    assert list(_TERMINAL_STATE_LITERALS) == sorted(TERMINAL_WORK_TICKET_STATES)


def test_terminal_and_non_terminal_partition_every_work_ticket_state():
    # The digest's "No other work tickets of yours are still active" is an
    # AFFIRMATIVE claim, and it holds only if the two sets are exact complements
    # over WorkTicketState: what the sweeper reports as finished, plus what it
    # counts as still active, is every state a ticket can be in. Add a seventh
    # state and forget to place it, and it falls into neither — the runner never
    # finalizes it and the email tells the recipient nothing is in flight while
    # their tickets sit in it. (The retriable-FAILED carve-out is a subset of
    # the terminal side, counted separately by _HELD_COUNT_SELECT; it does not
    # break the partition, which is over STATES.)
    terminal = set(TERMINAL_WORK_TICKET_STATES)
    non_terminal = set(NON_TERMINAL_WORK_TICKET_STATES)
    assert terminal.isdisjoint(non_terminal)
    assert terminal | non_terminal == {s.value for s in WorkTicketState}


def test_owed_set_predicate_carries_all_terminal_states_and_carve_out():
    for state in TERMINAL_WORK_TICKET_STATES:
        assert f"'{state}'" in _OWED_SET_WHERE, state
    assert _RETRIABLE_CARVE_OUT in _OWED_SET_WHERE
    assert "notified_at IS NULL" in _OWED_SET_WHERE


def test_partial_index_predicate_byte_matches_the_sweep_predicate():
    sql = _INDEX_MIGRATION.read_text()
    # The index predicate must carry the same terminal states, in the same
    # sorted order the sweeper emits, plus the retriable carve-out — or the
    # planner cannot prove the query predicate implies the index predicate.
    ordered_in_list = ", ".join(f"'{s}'" for s in _TERMINAL_STATE_LITERALS)
    assert f"state IN ({ordered_in_list})" in sql
    assert _RETRIABLE_CARVE_OUT in sql
    assert "notified_at IS NULL" in sql
    assert "CREATE INDEX CONCURRENTLY qiita_work_ticket_email_owed_idx" in sql
