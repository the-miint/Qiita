import json

import pytest
from pydantic import TypeAdapter, ValidationError

from qiita_common.models.prep_sample_qc_check import QcCheckResult


def test_strict_sidecar_accepts_json_ints_for_floats_and_nulls():
    (row,) = TypeAdapter(list[QcCheckResult]).validate_json(
        '[{"prep_sample_idx": 1, "check": "data_type", "severity": "info",'
        ' "threshold": 1, "measured_value": null, "reason": null}]'
    )
    assert row.threshold == 1.0
    assert row.measured_value is None
    assert row.reason is None


@pytest.mark.parametrize("metrics", ['{"k": 1}', "null", None])
def test_strict_sidecar_accepts_metrics_object_null_or_absent(metrics):
    extra = "" if metrics is None else f', "metrics": {metrics}'
    (row,) = TypeAdapter(list[QcCheckResult]).validate_json(
        f'[{{"prep_sample_idx": 1, "check": "data_type", "severity": "info"{extra}}}]'
    )
    assert row.metrics == (json.loads(metrics) if metrics else None)


@pytest.mark.parametrize("metrics", ["[1]", "3", '"x"'])
def test_strict_sidecar_rejects_non_object_metrics(metrics):
    with pytest.raises(ValidationError, match="metrics"):
        TypeAdapter(list[QcCheckResult]).validate_json(
            '[{"prep_sample_idx": 1, "check": "data_type", "severity": "info",'
            f' "metrics": {metrics}}}]'
        )
