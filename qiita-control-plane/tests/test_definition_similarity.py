"""Unit tests for the definition-similarity rules: the change between two
definitions, `derived_only`, the group rule (derived edges, explicit decisions, the
third-member rule), and each refusal. No database: definitions and pairs are built
in memory and handed to `compute_groups` / the `check_*` functions."""

from dataclasses import replace
from itertools import count

import pytest
from qiita_common.models import DefinitionKind, DefinitionPairRefusal, DefinitionPairVerdict

from qiita_control_plane.definition_similarity import (
    PairRefused,
    PairState,
    check_decision,
    check_open,
    check_revoke,
    compute_groups,
    is_derived_only,
    param_diff,
)
from qiita_control_plane.repositories.definition_pair import DefinitionRow

MASK = DefinitionKind.MASK
ALIGNMENT = DefinitionKind.ALIGNMENT
PROCESSING = DefinitionKind.PROCESSING
SIMILAR = DefinitionPairVerdict.SIMILAR
NOT_SIMILAR = DefinitionPairVerdict.NOT_SIMILAR
PENDING = DefinitionPairVerdict.PENDING

_pair_ids = count(1000)


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


def pair(definitions, kind, a, b, verdict=PENDING):
    """A pair as `open_pair` would record it, from the in-memory definitions."""
    by_key = {(d.kind, d.idx): d for d in definitions}
    low, high = sorted((a, b))
    lo, hi = by_key[(kind, low)], by_key[(kind, high)]
    change = param_diff(lo.params, hi.params)
    return PairState(
        idx=next(_pair_ids),
        kind=kind,
        low=low,
        high=high,
        verdict=verdict,
        change=change,
        derived_only=is_derived_only(kind, lo.params, hi.params, change),
    )


def refused(reason, fn, *args):
    with pytest.raises(PairRefused) as ei:
        fn(*args)
    assert ei.value.reason is reason, str(ei.value)
    return ei.value


# ---------------------------------------------------------------------------
# Change
# ---------------------------------------------------------------------------


def test_param_diff_walks_nested_objects_into_dotted_paths():
    low = {"a": 1, "qc": {"min_length": 100, "tail": True}, "same": "x"}
    high = {"a": 1, "qc": {"min_length": 200, "tail": True}, "same": "x"}
    assert param_diff(low, high) == {"qc.min_length": [100, 200]}


def test_param_diff_compares_lists_whole():
    assert param_diff({"shard_ids": [0, 1]}, {"shard_ids": [0, 1, 2]}) == {
        "shard_ids": [[0, 1], [0, 1, 2]]
    }


def test_param_diff_treats_absent_and_null_as_one_value():
    assert param_diff({"a": None, "b": {"c": None}}, {}) == {}
    assert param_diff({"a": None}, {"a": 3}) == {"a": [None, 3]}


def test_param_diff_compares_as_canonical_json():
    """1, 1.0 and true hash differently, so they are different values here too."""
    assert param_diff({"a": 1}, {"a": 1.0}) == {"a": [1, 1.0]}
    assert param_diff({"a": 1}, {"a": True}) == {"a": [1, True]}


def test_param_diff_compares_a_null_object_against_its_leaves():
    assert param_diff({"lima": None}, {"lima": {"version": "2.13.0"}}) == {
        "lima.version": [None, "2.13.0"]
    }


def test_param_diff_refuses_a_key_containing_a_dot():
    with pytest.raises(ValueError, match="contains '.'"):
        param_diff({"a.b": 1}, {})


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


def test_masks_group_transitively_over_similar_pairs_only():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2), mask(4, rype=0.3)]
    pairs = [
        pair(defs, MASK, 1, 2, SIMILAR),
        pair(defs, MASK, 2, 3, SIMILAR),
        pair(defs, MASK, 3, 4, NOT_SIMILAR),
    ]
    groups = compute_groups(defs, pairs)
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
    groups = compute_groups(defs, [pair(defs, MASK, 1, 2, SIMILAR)])
    assert groups.same_group(PROCESSING, 10, 11)


def test_a_non_upstream_difference_blocks_derivation():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    groups = compute_groups(defs, [pair(defs, MASK, 1, 2, SIMILAR)])
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
    masks_only = compute_groups(defs, [pair(defs, MASK, 1, 2, SIMILAR)])
    assert not masks_only.same_group(ALIGNMENT, 20, 21)

    both = compute_groups(
        defs,
        [pair(defs, MASK, 1, 2, SIMILAR), pair(defs, PROCESSING, 10, 11, SIMILAR)],
    )
    assert both.same_group(ALIGNMENT, 20, 21)


def test_membership_ignores_deprecation():
    defs = [mask(1), mask(2, rype=0.1, deprecated=True)]
    groups = compute_groups(defs, [pair(defs, MASK, 1, 2, SIMILAR)])
    assert groups.same_group(MASK, 1, 2)


def _three_masks_three_alignments():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2), block(31, 1), block(32, 2), block(33, 3)]
    mask_pairs = [pair(defs, MASK, 1, 2, SIMILAR), pair(defs, MASK, 2, 3, SIMILAR)]
    return defs, mask_pairs


def test_not_similar_removes_both_sides_from_the_derived_class():
    """A third member of the derived class would still connect 31 and 32, so both
    leave it; 33 then has no derived partner left."""
    defs, mask_pairs = _three_masks_three_alignments()
    assert compute_groups(defs, mask_pairs).group_of(ALIGNMENT, 31) == frozenset({31, 32, 33})

    groups = compute_groups(defs, [*mask_pairs, pair(defs, ALIGNMENT, 31, 32, NOT_SIMILAR)])
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32})
    assert groups.group_of(ALIGNMENT, 33) == frozenset({33})
    assert not groups.contradictions


def test_not_similar_sides_keep_their_explicit_similar_edges():
    defs, mask_pairs = _three_masks_three_alignments()
    groups = compute_groups(
        defs,
        [
            *mask_pairs,
            pair(defs, ALIGNMENT, 31, 32, NOT_SIMILAR),
            pair(defs, ALIGNMENT, 31, 33, SIMILAR),
        ],
    )
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31, 33})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32})


def test_not_similar_connected_through_a_derived_and_an_explicit_edge_also_separates():
    """31 ~ 32 by derivation and 32 ~ 34 explicitly would connect the not_similar pair
    31 / 34; 31 loses its derived edges and the pair stays apart."""
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
            pair(defs, MASK, 1, 2, SIMILAR),
            pair(defs, ALIGNMENT, 32, 34, SIMILAR),
            pair(defs, ALIGNMENT, 31, 34, NOT_SIMILAR),
        ],
    )
    assert groups.group_of(ALIGNMENT, 31) == frozenset({31})
    assert groups.group_of(ALIGNMENT, 32) == frozenset({32, 34})
    assert not groups.contradictions


def test_not_similar_joined_by_explicit_pairs_alone_is_a_contradiction():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p13 = pair(defs, MASK, 1, 3, NOT_SIMILAR)
    groups = compute_groups(
        defs, [pair(defs, MASK, 1, 2, SIMILAR), pair(defs, MASK, 2, 3, SIMILAR), p13]
    )
    assert groups.contradictions == frozenset({p13.idx})


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def test_similar_with_a_deprecated_side_is_refused_and_not_similar_is_not():
    defs = [mask(1), mask(2, rype=0.1, deprecated=True)]
    p = pair(defs, MASK, 1, 2)
    groups = compute_groups(defs, [p])
    refused(DefinitionPairRefusal.DEPRECATED_SIDE, check_decision, groups, p, SIMILAR)
    check_decision(groups, p, NOT_SIMILAR)


def test_similar_on_a_shard_set_change_is_refused():
    defs = [mask(1), block(31, 1, alpha=1), block(32, 1, shard_ids=(0, 1, 2), alpha=2)]
    p = pair(defs, ALIGNMENT, 31, 32)
    assert "shard_ids" in p.change
    refused(
        DefinitionPairRefusal.SHARD_IDS_CHANGE,
        check_decision,
        compute_groups(defs, [p]),
        p,
        SIMILAR,
    )


def test_a_derived_only_pair_is_decidable_not_similar_only():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2)]
    pm = pair(defs, MASK, 1, 2, SIMILAR)
    pp = pair(defs, PROCESSING, 10, 11)
    assert pp.derived_only
    groups = compute_groups(defs, [pm, pp])
    refused(DefinitionPairRefusal.DERIVED_ONLY, check_decision, groups, pp, SIMILAR)
    check_decision(groups, pp, NOT_SIMILAR)


def test_similar_needs_the_changed_upstream_references_grouped():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    pm = pair(defs, MASK, 1, 2)
    pp = pair(defs, PROCESSING, 10, 11)
    refused(
        DefinitionPairRefusal.UPSTREAM_NOT_GROUPED,
        check_decision,
        compute_groups(defs, [pm, pp]),
        pp,
        SIMILAR,
    )
    pm_similar = pair(defs, MASK, 1, 2, SIMILAR)
    check_decision(compute_groups(defs, [pm_similar, pp]), pp, SIMILAR)


def test_a_decision_making_a_contradiction_is_refused():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p12 = pair(defs, MASK, 1, 2, SIMILAR)
    p13 = pair(defs, MASK, 1, 3, NOT_SIMILAR)
    p23 = pair(defs, MASK, 2, 3)
    err = refused(
        DefinitionPairRefusal.CONTRADICTION,
        check_decision,
        compute_groups(defs, [p12, p13, p23]),
        p23,
        SIMILAR,
    )
    assert err.definition_pair_idx == [p13.idx]


def test_not_similar_on_a_pair_already_grouped_by_similar_pairs_is_refused():
    defs = [mask(1), mask(2, rype=0.1), mask(3, rype=0.2)]
    p12, p23 = pair(defs, MASK, 1, 2, SIMILAR), pair(defs, MASK, 2, 3, SIMILAR)
    p13 = pair(defs, MASK, 1, 3)
    refused(
        DefinitionPairRefusal.CONTRADICTION,
        check_decision,
        compute_groups(defs, [p12, p23, p13]),
        p13,
        NOT_SIMILAR,
    )


def test_a_mask_similar_that_derives_a_not_similar_pair_together_is_allowed():
    defs = [mask(1), mask(2, rype=0.1), block(31, 1), block(32, 2)]
    pm = pair(defs, MASK, 1, 2)
    pa = pair(defs, ALIGNMENT, 31, 32, NOT_SIMILAR)
    check_decision(compute_groups(defs, [pm, pa]), pm, SIMILAR)


def _processing_relying_on_masks():
    defs = [mask(1), mask(2, rype=0.1), assembly(10, 1), assembly(11, 2, assembler="hifiasm")]
    pm = pair(defs, MASK, 1, 2, SIMILAR)
    pp = pair(defs, PROCESSING, 10, 11, SIMILAR)
    return defs, pm, pp


def test_revoking_an_upstream_pair_a_downstream_similar_relies_on_is_refused():
    defs, pm, pp = _processing_relying_on_masks()
    err = refused(DefinitionPairRefusal.RELIED_ON, check_revoke, compute_groups(defs, [pm, pp]), pm)
    assert err.definition_pair_idx == [pp.idx]


def test_redeciding_a_relied_on_pair_not_similar_is_refused():
    defs, pm, pp = _processing_relying_on_masks()
    refused(
        DefinitionPairRefusal.RELIED_ON,
        check_decision,
        compute_groups(defs, [pm, pp]),
        pm,
        NOT_SIMILAR,
    )


def test_revoking_the_downstream_pair_first_frees_the_upstream_one():
    defs, pm, pp = _processing_relying_on_masks()
    pp_pending = replace(pp, verdict=PENDING)
    check_revoke(compute_groups(defs, [pm, pp_pending]), pm)


def test_reliance_follows_a_chain_from_mask_through_assembly_to_de_novo():
    """Masks 1 ~ 2 group assemblies 10 / 11 by derivation; de novo 20 ~ 21 is an
    explicit similar that needs both. Revoking the mask pair breaks the de novo one."""
    defs = [
        mask(1),
        mask(2, rype=0.1),
        assembly(10, 1),
        assembly(11, 2),
        denovo(20, 1, 10),
        denovo(21, 2, 11, min_identity=0.95),
    ]
    pm = pair(defs, MASK, 1, 2, SIMILAR)
    pd = pair(defs, ALIGNMENT, 20, 21, SIMILAR)
    groups = compute_groups(defs, [pm, pd])
    assert groups.upstream_grouped(pd)
    err = refused(DefinitionPairRefusal.RELIED_ON, check_revoke, groups, pm)
    assert err.definition_pair_idx == [pd.idx]


def test_not_similar_on_a_derived_assembly_pair_a_de_novo_similar_relies_on_is_refused():
    defs = [
        mask(1),
        mask(2, rype=0.1),
        assembly(10, 1),
        assembly(11, 2),
        denovo(20, 1, 10),
        denovo(21, 2, 11, min_identity=0.95),
    ]
    pm = pair(defs, MASK, 1, 2, SIMILAR)
    pp = pair(defs, PROCESSING, 10, 11)
    pd = pair(defs, ALIGNMENT, 20, 21, SIMILAR)
    refused(
        DefinitionPairRefusal.RELIED_ON,
        check_decision,
        compute_groups(defs, [pm, pp, pd]),
        pp,
        NOT_SIMILAR,
    )


def test_revoking_a_pending_pair_is_refused():
    defs = [mask(1), mask(2, rype=0.1)]
    p = pair(defs, MASK, 1, 2)
    refused(DefinitionPairRefusal.NOT_DECIDED, check_revoke, compute_groups(defs, [p]), p)


def test_check_decision_rejects_pending_as_a_verdict():
    defs = [mask(1), mask(2, rype=0.1)]
    p = pair(defs, MASK, 1, 2)
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
def test_open_refuses_a_pair_across_comparison_scopes(low, high):
    refused(DefinitionPairRefusal.CROSS_SCOPE, check_open, low, high)


def test_open_refuses_two_definitions_with_the_same_params():
    refused(DefinitionPairRefusal.NO_DIFFERENCE, check_open, mask(1), mask(2))


def test_open_returns_the_change():
    assert check_open(mask(1), mask(2, rype=0.1)) == {"rype": [0.05, 0.1]}
