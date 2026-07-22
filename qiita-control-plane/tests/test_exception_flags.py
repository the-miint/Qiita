"""Pure-unit tests for the sequenced-sample exception flag derivation.

`_sequenced_sample_exception_flags` turns one anomalous-sample row into the
ordered `flags` list. It must agree with the SQL WHERE in
`fetch_sequenced_pool_sample_exceptions` (every row that query returns yields at
least one flag) — these pin the flag logic without a DB.
"""

from qiita_control_plane.routes.sequencing_run import _sequenced_sample_exception_flags


def _row(**over):
    """A clean sample row (no flags); override fields to introduce anomalies."""
    base = {
        "raw_read_count_r1r2": 1000,
        "quality_filtered_read_count_r1r2": 900,
        "biosample_accession": "SAMEA0",
        "ena_sample_accession": "ERS0",
        "ena_experiment_accession": "ERX0",
        "ena_run_accession": "ERR0",
        "has_failed": False,
        "has_completed": True,
    }
    base.update(over)
    return base


def test_clean_sample_has_no_flags():
    assert _sequenced_sample_exception_flags(_row()) == []


def test_unprocessed_and_no_reads_are_mutually_exclusive():
    # No metrics → unprocessed (not no_reads).
    assert "unprocessed" in _sequenced_sample_exception_flags(
        _row(raw_read_count_r1r2=None, quality_filtered_read_count_r1r2=None)
    )
    flags = _sequenced_sample_exception_flags(_row(raw_read_count_r1r2=None))
    assert "unprocessed" in flags and "no_reads" not in flags
    # Processed but 0 survived → no_reads (not unprocessed). NULL qf on a processed
    # sample also reads as no_reads (COALESCE parity with the SQL).
    assert _sequenced_sample_exception_flags(_row(quality_filtered_read_count_r1r2=0)) == [
        "no_reads"
    ]
    assert _sequenced_sample_exception_flags(_row(quality_filtered_read_count_r1r2=None)) == [
        "no_reads"
    ]


def test_missing_accession_flags():
    assert _sequenced_sample_exception_flags(_row(biosample_accession=None)) == [
        "missing_biosample_accession"
    ]
    assert _sequenced_sample_exception_flags(_row(ena_run_accession=None)) == [
        "missing_ena_run_accession"
    ]


def test_failed_ticket_only_without_completed():
    # A FAILED ticket with a COMPLETED one is NOT flagged (succeeded on retry).
    assert _sequenced_sample_exception_flags(_row(has_failed=True, has_completed=True)) == []
    # FAILED with no COMPLETED → failed_ticket.
    assert _sequenced_sample_exception_flags(_row(has_failed=True, has_completed=False)) == [
        "failed_ticket"
    ]


def test_multiple_flags_in_order():
    flags = _sequenced_sample_exception_flags(
        _row(
            raw_read_count_r1r2=None,
            quality_filtered_read_count_r1r2=None,
            biosample_accession=None,
            ena_run_accession=None,
            has_failed=True,
            has_completed=False,
        )
    )
    assert flags == [
        "unprocessed",
        "missing_biosample_accession",
        "missing_ena_run_accession",
        "failed_ticket",
    ]
