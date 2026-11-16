"""The decision-set token's canonical form (`decision_set`)."""

import hashlib

from qiita_control_plane.decision_set import decision_set_token


def test_the_token_hashes_the_sorted_decided_pairs_in_compact_json():
    decided = [("mask", 3, 7, "similar"), ("alignment", 1, 2, "not_similar")]
    body = '[["alignment",1,2,"not_similar"],["mask",3,7,"similar"]]'
    assert decision_set_token(decided) == "sha256:" + hashlib.sha256(body.encode()).hexdigest()


def test_the_token_ignores_order_and_pending_pairs():
    a = ("mask", 3, 7, "similar")
    b = ("processing", 4, 9, "not_similar")
    pending = ("mask", 5, 6, "pending")
    assert decision_set_token([a, b]) == decision_set_token([pending, b, a])
    assert decision_set_token([pending]) == decision_set_token([])


def test_the_token_changes_with_a_verdict():
    assert decision_set_token([("mask", 3, 7, "similar")]) != decision_set_token(
        [("mask", 3, 7, "not_similar")]
    )
