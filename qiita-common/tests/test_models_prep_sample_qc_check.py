from pydantic import TypeAdapter

from qiita_common.models.prep_sample_qc_check import QcCheckResult


def test_strict_sidecar_accepts_json_ints_for_floats_and_nulls():
    (row,) = TypeAdapter(list[QcCheckResult]).validate_json(
        '[{"prep_sample_idx": 1, "check": "data_type", "severity": "info",'
        ' "threshold": 1, "measured_value": null, "reason": null}]'
    )
    assert row.threshold == 1.0
    assert row.measured_value is None
    assert row.reason is None
