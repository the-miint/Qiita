"""The gap check shared by the exported-identifier mints.

Exercised directly because the condition that makes it fire — a read-back taking
its own snapshot mid-transaction — cannot be staged deterministically without
racing two connections against each other.
"""

from qiita_control_plane.repositories._exported_identifier_helpers import missing_from


def test_missing_from_reports_the_gap():
    """Tests the case where some requested identifiers came back and some did not."""
    rows = [{"prep_sample_idx": 4}, {"prep_sample_idx": 9}]
    assert missing_from(rows, [4, 9], key="prep_sample_idx") == []
    assert missing_from(rows, [4, 7, 9, 2], key="prep_sample_idx") == [2, 7]
    assert missing_from([], [5], key="prep_sample_idx") == [5]
    # Deduped input must not report a phantom gap.
    assert missing_from(rows, [4, 4, 9], key="prep_sample_idx") == []


def test_missing_from_passes_over_another_kind():
    """Tests the case where the result set mixes kinds, so the keyed column is NULL
    on the rows belonging to the other one."""
    rows = [
        {"study_idx": 11, "biosample_idx": None},
        {"study_idx": None, "biosample_idx": 40},
    ]
    assert missing_from(rows, [11], key="study_idx") == []
    assert missing_from(rows, [40], key="biosample_idx") == []
    assert missing_from(rows, [11, 12], key="study_idx") == [12]
