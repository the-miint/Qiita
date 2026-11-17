"""The decision-set token's canonical form (`decision_set`)."""

import hashlib

import pytest

from qiita_control_plane.decision_set import decision_set_token


def test_the_token_hashes_the_sorted_decided_equivalences_in_compact_json():
    decided = [("mask", 3, 7, "similar"), ("alignment", 1, 2, "different")]
    body = '[["alignment",1,2,"different"],["mask",3,7,"similar"]]'
    assert decision_set_token(decided) == "sha256:" + hashlib.sha256(body.encode()).hexdigest()


def test_the_token_ignores_order_and_pending_equivalences():
    a = ("mask", 3, 7, "similar")
    b = ("processing", 4, 9, "different")
    pending = ("mask", 5, 6, "pending")
    assert decision_set_token([a, b]) == decision_set_token([pending, b, a])
    assert decision_set_token([pending]) == decision_set_token([])


@pytest.mark.parametrize(
    "a, b",
    [
        ("similar", "different"),
        ("same", "similar"),
        ("different", "not_benchmarked"),
        ("same", "not_benchmarked"),
    ],
)
def test_the_token_changes_with_a_verdict(a, b):
    """Every decided verdict is its own value in the token, so two verdicts that behave
    alike (`same` and `similar` both join) still give different tokens."""
    assert decision_set_token([("mask", 3, 7, a)]) != decision_set_token([("mask", 3, 7, b)])


def test_equivalences_used_counts_only_live_joining_equivalences_naming_a_definition_read():
    from qiita_common.models import DefinitionKind, OutputEquivalenceVerdict

    from qiita_control_plane.decision_set import DecisionSet, _Equivalence

    equivalences = {
        1: _Equivalence(DefinitionKind.MASK, 3, 7, False),  # similar, read: counted
        2: _Equivalence(DefinitionKind.MASK, 3, 8, False),  # different, read: joins nothing
        3: _Equivalence(DefinitionKind.MASK, 7, 9, True),  # similar but detached: links nothing
        4: _Equivalence(DefinitionKind.MASK, 20, 21, False),  # similar, not read
        5: _Equivalence(DefinitionKind.ALIGNMENT, 3, 7, False),  # similar, another kind's ids
        6: _Equivalence(DefinitionKind.MASK, 7, 11, False),  # same, read: counted
        7: _Equivalence(DefinitionKind.MASK, 3, 12, False),  # not_benchmarked: joins nothing
    }
    verdicts = {
        1: OutputEquivalenceVerdict.SIMILAR,
        2: OutputEquivalenceVerdict.DIFFERENT,
        3: OutputEquivalenceVerdict.SIMILAR,
        4: OutputEquivalenceVerdict.SIMILAR,
        5: OutputEquivalenceVerdict.SIMILAR,
        6: OutputEquivalenceVerdict.SAME,
        7: OutputEquivalenceVerdict.NOT_BENCHMARKED,
    }
    ds = DecisionSet("sha256:" + "0" * 64, None, False, verdicts, equivalences)
    used = ds.provenance({DefinitionKind.MASK: {3, 7}})
    assert used.equivalences_used == 2
    assert used.equivalences_used_set == decision_set_token(
        [("mask", 3, 7, "similar"), ("mask", 7, 11, "same")]
    )
    exact = ds.provenance(None)
    assert (exact.equivalences_used, exact.equivalences_used_set) == (0, decision_set_token([]))
