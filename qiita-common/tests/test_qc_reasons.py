"""Invariants of `qc_reasons.yml` and its loader."""

import re

import pytest

from qiita_common.models import QcCheck
from qiita_common.qc_reasons import load_qc_reasons, parse_qc_reasons

_SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


def test_every_key_is_a_qc_check_and_every_check_has_an_entry():
    reasons = load_qc_reasons()
    assert set(reasons) == set(QcCheck)


def test_codes_are_snake_case():
    bad = [
        code
        for codes in load_qc_reasons().values()
        for code in codes
        if not _SNAKE_CASE.match(code)
    ]
    assert not bad


def test_duplicate_reason_within_a_check_raises():
    with pytest.raises(ValueError, match="data_type_mismatch.*data_type"):
        parse_qc_reasons("data_type:\n  - data_type_mismatch\n  - data_type_mismatch\n")


def test_loader_is_cached_and_frozen():
    reasons = load_qc_reasons()
    assert load_qc_reasons() is reasons
    assert all(isinstance(codes, frozenset) for codes in reasons.values())
