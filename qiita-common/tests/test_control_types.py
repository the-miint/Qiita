"""Invariants of `control_types.yml` and its loader."""

import pytest

from qiita_common.control_types import load_control_types, parse_control_types


def test_the_file_classifies_negative_positive_and_not_control():
    types = load_control_types()
    assert set(types) == {"negative", "positive", "not_control"}
    assert types["negative"] == {"control_blank", "control_sterile_water", "control_sea_water"}
    assert types["positive"] == {"control_spike_in"}


def test_loader_is_cached_and_frozen():
    types = load_control_types()
    assert load_control_types() is types
    assert all(isinstance(terms, frozenset) for terms in types.values())


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
