"""Tests for `ena_import.batch._summarize_run_failures`: an item's failure reason names
each distinct run failure once, however many runs share it."""

from qiita_control_plane.ena_import.batch import _summarize_run_failures
from qiita_control_plane.ena_import.registration import (
    EnaRunRegistrationOutcome,
    EnaRunRegistrationStatus,
)

_STRATEGY = "no prep protocol for ENA library_strategy 'OTHER', library_source 'OTHER'"


def _failed(accession: str, reason: str | None) -> EnaRunRegistrationOutcome:
    return EnaRunRegistrationOutcome(
        run_accession=accession, status=EnaRunRegistrationStatus.FAILED, failure_reason=reason
    )


def test_summarize_run_failures_groups_a_shared_reason():
    outcomes = [_failed(f"ERR{n}", _STRATEGY) for n in range(50)]

    assert _summarize_run_failures(outcomes) == f"50 runs (ERR0, ERR1, ERR2, …): {_STRATEGY}"


def test_summarize_run_failures_keeps_distinct_reasons_in_first_seen_order():
    outcomes = [
        _failed("ERR1", "platform not mapped"),
        _failed("ERR2", _STRATEGY),
        _failed("ERR3", "platform not mapped"),
        _failed("ERR4", None),
        _failed("ERR5", ""),
    ]

    assert _summarize_run_failures(outcomes) == (
        "2 runs (ERR1, ERR3): platform not mapped; "
        f"1 run (ERR2): {_STRATEGY}; "
        "2 runs (ERR4, ERR5): no reason recorded"
    )


def test_summarize_run_failures_names_every_run_up_to_the_cap():
    at_cap = [_failed(f"ERR{n}", _STRATEGY) for n in range(3)]
    over_cap = [*at_cap, _failed("ERR3", _STRATEGY)]

    assert _summarize_run_failures(at_cap) == f"3 runs (ERR0, ERR1, ERR2): {_STRATEGY}"
    assert _summarize_run_failures(over_cap) == f"4 runs (ERR0, ERR1, ERR2, …): {_STRATEGY}"
