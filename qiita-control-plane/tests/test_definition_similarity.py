"""Unit tests for the definition-similarity rules: the change between two
definitions, `derived_only`, the group rule (derived edges, explicit decisions, the
third-member rule), and each refusal. No database: definitions and output equivalences
are built in memory and handed to `compute_groups` / the `check_*` functions.

`JOIN` and `NON_JOIN` are a joining and a non-joining verdict. Every test runs once per
row of `_VERDICT_ROWS`, which together cover all four decided verdicts, so each rule is
shown to treat `same` as `similar` and `not_benchmarked` as `different`."""

import sys
from dataclasses import replace
from itertools import count

import pytest
from qiita_common.models import (
    JOINING_VERDICTS,
    NON_JOINING_VERDICTS,
    DefinitionKind,
    OutputEquivalenceRefusal,
    OutputEquivalenceVerdict,
)

from qiita_control_plane.definition_similarity import (
    EquivalenceRefused,
    EquivalenceState,
    check_decision,
    check_open,
    check_revoke,
    compute_groups,
    is_derived_only,
    param_diff,
)
from qiita_control_plane.repositories.output_equivalence import DefinitionRow

MASK = DefinitionKind.MASK
ALIGNMENT = DefinitionKind.ALIGNMENT
PROCESSING = DefinitionKind.PROCESSING
PENDING = OutputEquivalenceVerdict.PENDING
# Set per test by `_verdicts`.
JOIN = OutputEquivalenceVerdict.SIMILAR
NON_JOIN = OutputEquivalenceVerdict.DIFFERENT

# (joining, non-joining): each decided verdict appears in one row.
_VERDICT_ROWS = [
    (OutputEquivalenceVerdict.SIMILAR, OutputEquivalenceVerdict.DIFFERENT),
    (OutputEquivalenceVerdict.SAME, OutputEquivalenceVerdict.NOT_BENCHMARKED),
]


def test_the_verdict_rows_cover_every_decided_verdict():
    assert {j for j, _ in _VERDICT_ROWS} == JOINING_VERDICTS
    assert {n for _, n in _VERDICT_ROWS} == NON_JOINING_VERDICTS


@pytest.fixture(autouse=True, params=_VERDICT_ROWS, ids=lambda row: f"{row[0]}-{row[1]}")
def _verdicts(request, monkeypatch):
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "JOIN", request.param[0])
    monkeypatch.setattr(module, "NON_JOIN", request.param[1])


_equivalence_ids = count(1000)


def mask(idx, *, deprecated=False, workflow="read-mask", **params):
    return DefinitionRow(
        kind=MASK,
        idx=idx,
        params={"filter_workflow": workflow, "rype": 0.05, **params},
        deprecated=deprecated,
        workflow=workflow,
    )


def assembly(idx, mask_idx, *, assembler="myloasm", deprecated=False):
    return DefinitionRow(
        kind=PROCESSING,
        idx=idx,
        params={
            "workflow": "long-read-assembly",
            "version": "1.0.0",
            "mask_idx": mask_idx,
            "assembler": assembler,
        },
        deprecated=deprecated,
        workflow="long-read-assembly",
    )


def block(idx, mask_idx, *, shard_ids=(0, 1), reference_idx=7, aligner="minimap2", **extra):
    return DefinitionRow(
        kind=ALIGNMENT,
        idx=idx,
        params={
            "reference_idx": reference_idx,
            "aligner": aligner,
            "mask_idx": mask_idx,
            "shard_ids": list(shard_ids),
            **extra,
        },
        deprecated=False,
        workflow=None,
    )


def denovo(idx, mask_idx, processing_idx, *, min_identity=0.9):
    return DefinitionRow(
        kind=ALIGNMENT,
        idx=idx,
        params={
            "subject": "assembly",
            "workflow": "align-denovo",
            "version": "1.0.0",
            "aligner": "minimap2",
            "mask_idx": mask_idx,
            "processing_idx": processing_idx,
            "min_identity": min_identity,
        },
        deprecated=False,
        workflow=None,
    )


def equivalence(definitions, kind, a, b, verdict=PENDING):
    """An equivalence as `open_equivalence` would record it, from the in-memory definitions."""
    by_key = {(d.kind, d.idx): d for d in definitions}
    low, high = sorted((a, b))
    lo, hi = by_key[(kind, low)], by_key[(kind, high)]
    change = param_diff(lo.params, hi.params)
    return EquivalenceState(
        idx=next(_equivalence_ids),
        kind=kind,
        low=low,
        high=high,
        verdict=verdict,
        change=change,
        derived_only=is_derived_only(kind, lo.params, hi.params, change),
    )


def refused(reason, fn, *args):
    with pytest.raises(EquivalenceRefused) as ei:
        fn(*args)
    assert ei.value.reason is reason, str(ei.value)
    return ei.value


# ---------------------------------------------------------------------------
# Change
# ---------------------------------------------------------------------------


def test_param_diff_walks_nested_objects_into_json_pointer_paths():
    low = {"a": 1, "qc": {"min_length": 100, "tail": True}, "same": "x"}
    high = {"a": 1, "qc": {"min_length": 200, "tail": True}, "same": "x"}
    assert param_diff(low, high) == {"/qc/min_length": [100, 200]}


def test_param_diff_compares_lists_whole():
    assert param_diff({"shard_ids": [0, 1]}, {"shard_ids": [0, 1, 2]}) == {
        "/shard_ids": [[0, 1], [0, 1, 2]]
    }


def test_param_diff_treats_absent_and_null_as_one_value():
    assert param_diff({"a": None, "b": {"c": None}}, {}) == {}
    assert param_diff({"a": None}, {"a": 3}) == {"/a": [None, 3]}


def test_param_diff_compares_as_canonical_json():
    """1, 1.0 and true hash differently, so they are different values here too."""
    assert param_diff({"a": 1}, {"a": 1.0}) == {"/a": [1, 1.0]}
    assert param_diff({"a": 1}, {"a": True}) == {"/a": [1, True]}


def test_param_diff_compares_a_null_object_against_its_leaves():
    assert param_diff({"lima": None}, {"lima": {"version": "2.13.0"}}) == {
        "/lima/version": [None, "2.13.0"]
    }


def test_param_diff_gives_every_key_its_own_path():
    """A key holding `.`, `/` or `~` cannot collide with a nested path: `a.b` and
    `a: {b}` are two paths, and a `/` in a key is escaped."""
    assert param_diff({"a.b": 1, "a": {"b": 1}}, {"a.b": 2, "a": {"b": 3}}) == {
        "/a/b": [1, 3],
        "/a.b": [1, 2],
    }
    assert param_diff({"x/y": 1, "x": {"y": 1}, "t~": 1}, {"x/y": 2, "x": {"y": 1}, "t~": 2}) == {
        "/t~0": [1, 2],
        "/x~1y": [1, 2],
    }


def test_groups_form_over_a_param_key_holding_a_dot():
    """A dotted key is an ordinary key to the derived grouping: two assemblies whose
    params differ only in grouped masks are grouped, and a dotted key that differs
    keeps them apart."""
    defs = [mask(1), mask(2, rype=0.1)]
    p1, p2 = assembly(10, mask_idx=1), assembly(11, mask_idx=2)
    p1 = replace(p1, params={**p1.params, "knob.v": 1})
    p2 = replace(p2, params={**p2.params, "knob.v": 1})
    p3 = replace(assembly(12, mask_idx=2), params={**p2.params, "knob.v": 2})
    equivalences = [equivalence(defs, MASK, 1, 2)]
    groups = compute_groups([*defs, p1, p2, p3], [replace(equivalences[0], verdict=JOIN)])
    assert groups.group_of(PROCESSING, 10) == {10, 11}
    assert groups.group_of(PROCESSING, 12) == {12}


def test_derived_only_is_a_change_of_upstream_references_alone():
    p1, p2 = assembly(1, mask_idx=10), assembly(2, mask_idx=11)
    change = param_diff(p1.params, p2.params)
    assert is_derived_only(PROCESSING, p1.params, p2.params, change)

    p3 = assembly(3, mask_idx=11, assembler="hifiasm")
    change = param_diff(p1.params, p3.params)
    assert not is_derived_only(PROCESSING, p1.params, p3.params, change)


def test_derived_only_counts_a_de_novo_processing_idx_but_not_a_block_one():
    d1, d2 = denovo(1, 10, 20), denovo(2, 10, 21)
    assert is_derived_only(ALIGNMENT, d1.params, d2.params, param_diff(d1.params, d2.params))

    # A block alignment carries no processing_idx upstream reference.
    b1, b2 = block(3, 10, processing_idx=20), block(4, 10, processing_idx=21)
    assert not is_derived_only(ALIGNMENT, b1.params, b2.params, param_diff(b1.params, b2.params))


def test_derived_only_needs_an_id_on_both_sides():
    low, high = {"mask_idx": None, "trim": 5}, {"mask_idx": 10, "trim": 5}
    assert not is_derived_only(PROCESSING, low, high, param_diff(low, high))


def test_no_change_is_not_derived_only():
    assert not is_derived_only(PROCESSING, {}, {}, {})


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


def test_masks_group_transitively_over_joining_equivalences_only():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2), mask(4, rype=0.3)]
    equivalences = [
        equivalence(defs, MASK, 1, 2, JOIN),
        equivalence(defs, MASK, 2, 3, JOIN),
        equivalence(defs, MASK, 3, 4, NON_JOIN),
    ]
    groups = compute_groups(defs, equivalences)
    assert groups.group_of(MASK, 1) == frozenset({1, 2, 3})
    assert groups.group_of(MASK, 4) == frozenset({4})
    assert groups.same_group(MASK, 1, 3)
    assert not groups.contradictions


def test_masks_with_identical_flattened_params_are_not_derived_together():
    defs = [mask(1, extra=None), mask(2)]
    assert compute_groups(defs, []).group_of(MASK, 1) == frozenset({1})


def test_an_unknown_definition_is_a_group_of_one():
    assert compute_groups([], []).group_of(ALIGNMENT, 99) == frozenset({99})


def test_processings_on_grouped_masks_group_by_derivation():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2)]
    assert not compute_groups(defs, []).same_group(PROCESSING, 10, 11)
    groups = compute_groups(defs, [equivalence(defs, MASK, 1, 2, JOIN)])
    assert groups.same_group(PROCESSING, 10, 11)


def test_a_non_upstream_difference_blocks_derivation():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    groups = compute_groups(defs, [equivalence(defs, MASK, 1, 2, JOIN)])
    assert not groups.same_group(PROCESSING, 10, 11)


def test_de_novo_alignments_need_both_upstreams_grouped():
    defs = [
        mask(1),
        mask(2, rype=0.1),
        assembly(10, 1),
        assembly(11, 1, assembler="hifiasm"),
        denovo(20, 1, 10),
        denovo(21, 2, 11),
    ]
    masks_only = compute_groups(defs, [equivalence(defs, MASK, 1, 2, JOIN)])
    assert not masks_only.same_group(ALIGNMENT, 20, 21)

    both = compute_groups(
        defs,
        [equivalence(defs, MASK, 1, 2, JOIN), equivalence(defs, PROCESSING, 10, 11, JOIN)],
    )
    assert both.same_group(ALIGNMENT, 20, 21)


def test_membership_ignores_deprecation():
    defs = [mask(1), mask(2, rype=0.1, deprecated=True)]
    groups = compute_groups(defs, [equivalence(defs, MASK, 1, 2, JOIN)])
    assert groups.same_group(MASK, 1, 2)


def _three_masks_three_alignments():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2), block(31, 1), block(32, 2), block(33, 3)]
    mask_equivalences = [equivalence(defs, MASK, 1, 2, JOIN), equivalence(defs, MASK, 2, 3, JOIN)]
    return defs, mask_equivalences


def test_non_joining_removes_both_sides_from_the_derived_class():
    """A third member of the derived class would still connect 31 and 32, so both
    leave it; 33 then has no derived partner left."""
    defs, mask_equivalences = _three_masks_three_alignments()
    assert compute_groups(defs, mask_equivalences).group_of(ALIGNMENT, 31) == frozenset(
        {31, 32, 33}
    )

    groups = compute_groups(
        defs, [*mask_equivalences, equivalence(defs, ALIGNMENT, 31, 32, NON_JOIN)]
    )
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32})
    assert groups.group_of(ALIGNMENT, 33) == frozenset({33})
    assert not groups.contradictions


def test_non_joining_sides_keep_their_explicit_joining_edges():
    defs, mask_equivalences = _three_masks_three_alignments()
    groups = compute_groups(
        defs,
        [
            *mask_equivalences,
            equivalence(defs, ALIGNMENT, 31, 32, NON_JOIN),
            equivalence(defs, ALIGNMENT, 31, 33, JOIN),
        ],
    )
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31, 33})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32})


def test_non_joining_connected_through_a_derived_and_an_explicit_edge_also_separates():
    """31 ~ 32 by derivation and 32 ~ 34 explicitly would connect the non-joining output equivalence
    31 / 34; 31 loses its derived edges and the equivalence stays apart."""
    defs = [
        mask(1),
        mask(2, rype=0.1),
        block(31, 1),
        block(32, 2),
        block(34, 2, shard_ids=(0, 1, 2)),
    ]
    groups = compute_groups(
        defs,
        [
            equivalence(defs, MASK, 1, 2, JOIN),
            equivalence(defs, ALIGNMENT, 32, 34, JOIN),
            equivalence(defs, ALIGNMENT, 31, 34, NON_JOIN),
        ],
    )
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32, 34})
    assert not groups.contradictions


def test_non_joining_joined_by_explicit_equivalences_alone_is_a_contradiction():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p13 = equivalence(defs, MASK, 1, 3, NON_JOIN)
    groups = compute_groups(
        defs, [equivalence(defs, MASK, 1, 2, JOIN), equivalence(defs, MASK, 2, 3, JOIN), p13]
    )
    assert groups.contradictions == frozenset({p13.idx})


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def test_joining_with_a_deprecated_side_is_refused_and_non_joining_is_not():
    defs = [mask(1), mask(2, rype=0.1, deprecated=True)]
    p = equivalence(defs, MASK, 1, 2)
    groups = compute_groups(defs, [p])
    refused(OutputEquivalenceRefusal.DEPRECATED_SIDE, check_decision, groups, p, JOIN)
    check_decision(groups, p, NON_JOIN)


def test_joining_on_a_shard_set_change_is_refused():
    defs = [mask(1), block(31, 1, alpha=1), block(32, 1, shard_ids=(0, 1, 2), alpha=2)]
    p = equivalence(defs, ALIGNMENT, 31, 32)
    assert "/shard_ids" in p.change
    refused(
        OutputEquivalenceRefusal.SHARD_IDS_CHANGE,
        check_decision,
        compute_groups(defs, [p]),
        p,
        JOIN,
    )


@pytest.mark.parametrize(
    ("low_syndna", "high_syndna"),
    [
        ({"reference_idx": 7}, {"reference_idx": 8}),
        ({"reference_idx": 7}, None),
        (None, {"reference_idx": 8}),
    ],
    ids=["two-references", "low-only", "high-only"],
)
def test_masks_differing_in_syndna_reference_are_never_joining(low_syndna, high_syndna):
    defs = [mask(1, resolved_syndna=low_syndna), mask(2, resolved_syndna=high_syndna)]
    p = equivalence(defs, MASK, 1, 2)
    groups = compute_groups(defs, [p])
    refused(OutputEquivalenceRefusal.SYNDNA_REFERENCE_CHANGE, check_decision, groups, p, JOIN)
    check_decision(groups, p, NON_JOIN)


@pytest.mark.parametrize("stored", [7, {"reference_idx": "7"}, {}])
def test_a_malformed_syndna_param_is_refused(stored):
    defs = [mask(1, resolved_syndna=stored), mask(2, resolved_syndna={"reference_idx": 7})]
    p = equivalence(defs, MASK, 1, 2)
    with pytest.raises(ValueError, match="malformed resolved_syndna"):
        check_decision(compute_groups(defs, [p]), p, JOIN)


def test_masks_with_one_syndna_reference_may_be_joining():
    defs = [
        mask(1, resolved_syndna={"reference_idx": 7}),
        mask(2, rype=0.1, resolved_syndna={"reference_idx": 7}),
    ]
    p = equivalence(defs, MASK, 1, 2)
    check_decision(compute_groups(defs, [p]), p, JOIN)


def test_a_derived_only_equivalence_is_decidable_non_joining_only():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2)]
    pm = equivalence(defs, MASK, 1, 2, JOIN)
    pp = equivalence(defs, PROCESSING, 10, 11)
    assert pp.derived_only
    groups = compute_groups(defs, [pm, pp])
    refused(OutputEquivalenceRefusal.DERIVED_ONLY, check_decision, groups, pp, JOIN)
    check_decision(groups, pp, NON_JOIN)


def test_joining_needs_the_changed_upstream_references_grouped():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    pm = equivalence(defs, MASK, 1, 2)
    pp = equivalence(defs, PROCESSING, 10, 11)
    refused(
        OutputEquivalenceRefusal.UPSTREAM_NOT_GROUPED,
        check_decision,
        compute_groups(defs, [pm, pp]),
        pp,
        JOIN,
    )
    pm_joined = equivalence(defs, MASK, 1, 2, JOIN)
    check_decision(compute_groups(defs, [pm_joined, pp]), pp, JOIN)


def test_a_decision_making_a_contradiction_is_refused():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p12 = equivalence(defs, MASK, 1, 2, JOIN)
    p13 = equivalence(defs, MASK, 1, 3, NON_JOIN)
    p23 = equivalence(defs, MASK, 2, 3)
    err = refused(
        OutputEquivalenceRefusal.CONTRADICTION,
        check_decision,
        compute_groups(defs, [p12, p13, p23]),
        p23,
        JOIN,
    )
    assert err.output_equivalence_idx == [p13.idx]


def test_non_joining_on_an_equivalence_already_grouped_by_joining_equivalences_is_refused():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p12, p23 = equivalence(defs, MASK, 1, 2, JOIN), equivalence(defs, MASK, 2, 3, JOIN)
    p13 = equivalence(defs, MASK, 1, 3)
    refused(
        OutputEquivalenceRefusal.CONTRADICTION,
        check_decision,
        compute_groups(defs, [p12, p23, p13]),
        p13,
        NON_JOIN,
    )


def test_a_mask_joining_that_derives_a_non_joining_equivalence_together_is_allowed():
    defs = [mask(1), mask(2, rype=0.1), block(31, 1), block(32, 2)]
    pm = equivalence(defs, MASK, 1, 2)
    pa = equivalence(defs, ALIGNMENT, 31, 32, NON_JOIN)
    check_decision(compute_groups(defs, [pm, pa]), pm, JOIN)


def _processing_relying_on_masks():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    pm = equivalence(defs, MASK, 1, 2, JOIN)
    pp = equivalence(defs, PROCESSING, 10, 11, JOIN)
    return defs, pm, pp


def test_revoking_an_upstream_equivalence_a_downstream_joining_relies_on_is_refused():
    defs, pm, pp = _processing_relying_on_masks()
    err = refused(
        OutputEquivalenceRefusal.RELIED_ON, check_revoke, compute_groups(defs, [pm, pp]), pm
    )
    assert err.output_equivalence_idx == [pp.idx]


def test_redeciding_a_relied_on_equivalence_non_joining_is_refused():
    defs, pm, pp = _processing_relying_on_masks()
    refused(
        OutputEquivalenceRefusal.RELIED_ON,
        check_decision,
        compute_groups(defs, [pm, pp]),
        pm,
        NON_JOIN,
    )


def test_revoking_the_downstream_equivalence_first_frees_the_upstream_one():
    defs, pm, pp = _processing_relying_on_masks()
    pp_pending = replace(pp, verdict=PENDING)
    check_revoke(compute_groups(defs, [pm, pp_pending]), pm)


def test_reliance_follows_a_chain_from_mask_through_assembly_to_de_novo():
    """Masks 1 ~ 2 group assemblies 10 / 11 by derivation; de novo 20 ~ 21 is an
    explicit joining one that needs both. Revoking the mask equivalence breaks the de novo one."""
    defs = [
        mask(1),
        mask(2, rype=0.1),
        assembly(10, 1),
        assembly(11, 2),
        denovo(20, 1, 10),
        denovo(21, 2, 11, min_identity=0.95),
    ]
    pm = equivalence(defs, MASK, 1, 2, JOIN)
    pd = equivalence(defs, ALIGNMENT, 20, 21, JOIN)
    groups = compute_groups(defs, [pm, pd])
    assert groups.upstream_grouped(pd)
    err = refused(OutputEquivalenceRefusal.RELIED_ON, check_revoke, groups, pm)
    assert err.output_equivalence_idx == [pd.idx]


def test_non_joining_on_a_derived_assembly_equivalence_a_de_novo_joining_relies_on_is_refused():
    defs = [
        mask(1),
        mask(2, rype=0.1),
        assembly(10, 1),
        assembly(11, 2),
        denovo(20, 1, 10),
        denovo(21, 2, 11, min_identity=0.95),
    ]
    pm = equivalence(defs, MASK, 1, 2, JOIN)
    pp = equivalence(defs, PROCESSING, 10, 11)
    pd = equivalence(defs, ALIGNMENT, 20, 21, JOIN)
    refused(
        OutputEquivalenceRefusal.RELIED_ON,
        check_decision,
        compute_groups(defs, [pm, pp, pd]),
        pp,
        NON_JOIN,
    )


def test_revoking_a_pending_equivalence_is_refused():
    defs = [mask(1), mask(2, rype=0.1)]
    p = equivalence(defs, MASK, 1, 2)
    refused(OutputEquivalenceRefusal.NOT_DECIDED, check_revoke, compute_groups(defs, [p]), p)


def test_check_decision_rejects_pending_as_a_verdict():
    defs = [mask(1), mask(2, rype=0.1)]
    p = equivalence(defs, MASK, 1, 2)
    with pytest.raises(ValueError):
        check_decision(compute_groups(defs, [p]), p, PENDING)


@pytest.mark.parametrize(
    "low, high",
    [
        (mask(1), mask(2, workflow="fastq-to-parquet")),
        (assembly(10, 1), DefinitionRow(PROCESSING, 11, {"trim": 5}, False, "amplicon")),
        (block(31, 1), block(32, 1, reference_idx=8)),
        (block(31, 1), block(32, 1, aligner="bowtie2")),
        (block(31, 1), denovo(32, 1, 10)),
    ],
    ids=[
        "mask-workflow",
        "processing-workflow",
        "block-reference",
        "block-aligner",
        "block-denovo",
    ],
)
def test_open_refuses_an_equivalence_across_comparison_scopes(low, high):
    refused(OutputEquivalenceRefusal.CROSS_SCOPE, check_open, low, high)


def test_open_refuses_two_definitions_with_the_same_params():
    refused(OutputEquivalenceRefusal.NO_DIFFERENCE, check_open, mask(1), mask(2))


def test_open_returns_the_change():
    assert check_open(mask(1), mask(2, rype=0.1)) == {"/rype": [0.05, 0.1]}


def test_same_and_similar_join_one_group_together():
    """A `same` and a `similar` output equivalence chain masks into one group, and their
    groups are the groups two `similar` ones give."""
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2), mask(4, rype=0.3)]
    same = OutputEquivalenceVerdict.SAME
    similar = OutputEquivalenceVerdict.SIMILAR
    mixed = compute_groups(
        defs, [equivalence(defs, MASK, 1, 2, same), equivalence(defs, MASK, 2, 3, similar)]
    )
    plain = compute_groups(
        defs, [equivalence(defs, MASK, 1, 2, similar), equivalence(defs, MASK, 2, 3, similar)]
    )
    for idx in (1, 2, 3, 4):
        assert mixed.group_of(MASK, idx) == plain.group_of(MASK, idx)
    assert mixed.group_of(MASK, 1) == frozenset({1, 2, 3})


def test_different_and_not_benchmarked_each_contradict_a_join():
    """Both non-joining verdicts make the same contradiction when a join connects their
    two sides."""
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    for verdict in sorted(NON_JOINING_VERDICTS):
        p13 = equivalence(defs, MASK, 1, 3, verdict)
        groups = compute_groups(
            defs, [equivalence(defs, MASK, 1, 2, JOIN), equivalence(defs, MASK, 2, 3, JOIN), p13]
        )
        assert groups.contradictions == frozenset({p13.idx})
