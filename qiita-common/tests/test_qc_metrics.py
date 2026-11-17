"""The per-check metrics registry and the data-type metrics model."""

import copy

import pytest
from pydantic import ValidationError

from qiita_common.models import QcCheck
from qiita_common.models.prep_sample_qc_check import DataTypeMetrics
from qiita_common.qc_metrics import METRIC_MODELS, validate_qc_metrics

_GOOD = {
    "n_sampled": 10000,
    "fractions": {
        "marker_16s": 0.75,
        "marker_18s": 0.125,
        "marker_23s_28s": 0.0,
        "marker_5s_5_8s": 0.0,
        "unclassified": 0.125,
    },
    "dominant_class": "marker_16s",
    "marker_reference_idx": {
        "marker_16s": 11,
        "marker_18s": 12,
        "marker_23s_28s": 13,
        "marker_5s_5_8s": 14,
    },
}


_DROP = object()


def _with(path, value):
    payload = copy.deepcopy(_GOOD)
    node = payload
    *parents, leaf = path
    for key in parents:
        node = node[key]
    if value is _DROP:
        del node[leaf]
    else:
        node[leaf] = value
    return payload


def test_registry_holds_only_data_type():
    assert set(METRIC_MODELS) == {QcCheck.DATA_TYPE}
    assert METRIC_MODELS[QcCheck.DATA_TYPE] is DataTypeMetrics


def test_accepts_a_json_decoded_payload():
    assert DataTypeMetrics.model_validate(_GOOD).dominant_class == "marker_16s"


def test_no_hit_payload_has_no_dominant_class():
    fractions = {k: 0.0 for k in _GOOD["fractions"]} | {"unclassified": 1.0}
    payload = _GOOD | {"fractions": fractions, "dominant_class": None}
    assert DataTypeMetrics.model_validate(payload).dominant_class is None


@pytest.mark.parametrize(
    "payload",
    [
        _with(["fractions", "marker_18s"], _DROP),
        _with(["fractions", "unclassified"], _DROP),
        _with(["fractions", "marker_x"], 0.0),
        _with(["extra"], 1),
        _with(["fractions", "marker_18s"], -0.125),
        _with(["fractions", "marker_18s"], 1.5),
        _with(["fractions", "marker_18s"], float("nan")),
        _with(["fractions", "marker_18s"], "0.125"),
        _with(["fractions", "marker_18s"], 0.25),
        _with(["n_sampled"], 0),
        _with(["n_sampled"], True),
        _with(["marker_reference_idx", "marker_5s_5_8s"], _DROP),
        _with(["marker_reference_idx", "marker_5s_5_8s"], 0),
        _with(["dominant_class"], "marker_18s"),
        _with(["dominant_class"], None),
        _with(["dominant_class"], "marker_99s"),
    ],
)
def test_rejects_a_bad_payload(payload):
    with pytest.raises(ValidationError):
        DataTypeMetrics.model_validate(payload)


def test_helper_returns_the_canonical_dump():
    dumped = validate_qc_metrics(QcCheck.DATA_TYPE, _GOOD)
    assert dumped == _GOOD
    assert type(dumped["dominant_class"]) is str


def test_helper_rejects_a_check_without_a_model():
    with pytest.raises(ValueError, match="composition"):
        validate_qc_metrics(QcCheck.COMPOSITION, {"k": 1})


def test_helper_names_the_check_of_a_failing_payload():
    with pytest.raises(ValueError, match="data_type"):
        validate_qc_metrics(QcCheck.DATA_TYPE, _with(["n_sampled"], 0))
