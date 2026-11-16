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


def test_pairs_used_counts_only_live_similar_pairs_naming_a_definition_read():
    from qiita_common.models import DefinitionKind, DefinitionPairVerdict

    from qiita_control_plane.decision_set import DecisionSet, _Pair

    pairs = {
        1: _Pair(DefinitionKind.MASK, 3, 7, False),  # similar, read: counted
        2: _Pair(DefinitionKind.MASK, 3, 8, False),  # not_similar, read: joins nothing
        3: _Pair(DefinitionKind.MASK, 7, 9, True),  # similar but detached: links nothing
        4: _Pair(DefinitionKind.MASK, 20, 21, False),  # similar, not read
        5: _Pair(DefinitionKind.ALIGNMENT, 3, 7, False),  # similar, another kind's ids
    }
    verdicts = {
        1: DefinitionPairVerdict.SIMILAR,
        2: DefinitionPairVerdict.NOT_SIMILAR,
        3: DefinitionPairVerdict.SIMILAR,
        4: DefinitionPairVerdict.SIMILAR,
        5: DefinitionPairVerdict.SIMILAR,
    }
    ds = DecisionSet("sha256:" + "0" * 64, None, False, verdicts, pairs)
    used = ds.provenance({DefinitionKind.MASK: {3, 7}})
    assert used.pairs_used == 1
    assert used.pairs_used_set == decision_set_token([("mask", 3, 7, "similar")])
    exact = ds.provenance(None)
    assert (exact.pairs_used, exact.pairs_used_set) == (0, decision_set_token([]))
