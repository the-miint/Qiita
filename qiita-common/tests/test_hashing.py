"""Unit tests for qiita_common.hashing canonical-JSON hashing.

The dedup discipline behind mask_definition.params_hash (and, later, the
processing_idx hierarchy) rests on this being deterministic and order-
independent across processes.
"""

import hashlib

from qiita_common.hashing import canonical_json, canonical_params_hash


def test_canonical_json_sorts_keys():
    assert canonical_json({"b": 1, "a": 2}) == b'{"a":2,"b":1}'


def test_canonical_json_no_whitespace():
    assert b" " not in canonical_json({"a": 1, "b": [1, 2]})


def test_canonical_params_hash_is_32_bytes():
    h = canonical_params_hash({"k": "v"})
    assert isinstance(h, bytes)
    assert len(h) == 32


def test_canonical_params_hash_order_independent():
    assert canonical_params_hash({"a": 1, "b": 2}) == canonical_params_hash({"b": 2, "a": 1})


def test_canonical_params_hash_distinguishes_different_params():
    assert canonical_params_hash({"k": "v"}) != canonical_params_hash({"k": "w"})


def test_canonical_params_hash_matches_explicit_sha256():
    params = {"workflow": "host_filter", "host_refs": [1, 2]}
    expected = hashlib.sha256(canonical_json(params)).digest()
    assert canonical_params_hash(params) == expected
