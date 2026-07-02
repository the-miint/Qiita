"""Parity between the notify sweeper's owed-set predicate, the partial index
that backs it, and the runner's terminal-state source of truth.

If these drift, the planner silently stops using the partial index (a
performance cliff at scale) or the sweeper emails a state it shouldn't. No DB
needed — these read the migration file and import the Python constants.
"""

from pathlib import Path

from qiita_control_plane.notify.sweeper import (
    _OWED_SET_WHERE,
    _TERMINAL_STATE_LITERALS,
)
from qiita_control_plane.runner import _TERMINAL_WORK_TICKET_STATES

_INDEX_MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "db"
    / "migrations"
    / "20260701000001_work_ticket_notified_idx.sql"
)

# The shared retriable carve-out: completed / no_data / permanent-failed
# email; only retriable-failed is withheld.
_RETRIABLE_CARVE_OUT = "failure_type IS DISTINCT FROM 'retriable'"


def test_terminal_state_literals_match_runner_source_of_truth():
    # The sweeper builds its literals from the runner frozenset, in sorted
    # order — assert the round-trip so a runner change is reflected here.
    assert set(_TERMINAL_STATE_LITERALS) == set(_TERMINAL_WORK_TICKET_STATES)
    assert list(_TERMINAL_STATE_LITERALS) == sorted(_TERMINAL_WORK_TICKET_STATES)


def test_owed_set_predicate_carries_all_terminal_states_and_carve_out():
    for state in _TERMINAL_WORK_TICKET_STATES:
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
