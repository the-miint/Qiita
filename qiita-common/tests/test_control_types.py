"""Invariants of `control_types.yml` and its loader."""

import pytest

from qiita_common.control_types import ControlKind, load_control_types, parse_control_types


def _terms(kind):
    return {term_id for term_id, k in load_control_types().items() if k is kind}


def test_the_file_classifies_negative_positive_and_not_control():
    assert set(load_control_types().values()) == set(ControlKind)
    assert _terms(ControlKind.NEGATIVE) == {
        "control_blank",
        "control_sterile_water",
        "control_sea_water",
    }
    assert _terms(ControlKind.POSITIVE) == {"control_spike_in"}


def test_loader_is_cached():
    assert load_control_types() is load_control_types()


def test_term_listed_twice_in_one_kind_raises():
    with pytest.raises(ValueError, match="'a'.*'negative'"):
        parse_control_types("negative: [a, a]\npositive: []\nnot_control: []\n")


def test_term_listed_under_two_kinds_raises():
    with pytest.raises(ValueError, match="'a'.*'negative'.*'positive'"):
        parse_control_types("negative: [a]\npositive: [a]\nnot_control: []\n")


@pytest.mark.parametrize("text", ["negative: []\npositive: []\n", "negative: []\nother: []\n"])
def test_wrong_keys_raise(text):
    with pytest.raises(ValueError, match="keys must be"):
        parse_control_types(text)
