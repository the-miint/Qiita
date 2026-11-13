"""Unit tests for the per-sample group resolver: which member a sample resolves to,
and each incomplete / not-processed reason. No database: definitions, pairs and run
observations are built in memory and handed to the pure core."""

from itertools import count

import pytest
from qiita_common.models import (
    DefinitionGroupRefusal,
    DefinitionKind,
    DefinitionPairVerdict,
    SampleResolutionReason,
    SampleResolutionState,
    WorkTicketState,
)

from qiita_control_plane.definition_resolver import (
    RunObservation,
    build_observations,
    cohort_refusal,
    match_assembly_tickets,
    needed_definitions,
    resolve_sample,
)
from qiita_control_plane.definition_similarity import (
    PairState,
    compute_groups,
    is_derived_only,
    param_diff,
)
from qiita_control_plane.feature_table import denovo_alignment_processing_idx
from qiita_control_plane.repositories.definition_pair import DefinitionRow
from qiita_control_plane.repositories.definition_run import (
    SOURCE_BLOCK_TICKET,
    SOURCE_GATE,
    SOURCE_SAMPLE_TICKET,
    AssemblyTicketCandidate,
    RunRow,
)

MASK = DefinitionKind.MASK
ALIGNMENT = DefinitionKind.ALIGNMENT
PROCESSING = DefinitionKind.PROCESSING
PROCESSED = SampleResolutionState.PROCESSED
INCOMPLETE = SampleResolutionState.INCOMPLETE
NOT_PROCESSED = SampleResolutionState.NOT_PROCESSED
R = SampleResolutionReason
S = 900  # the prep_sample every test resolves

_pair_ids = count(1000)


def mask(idx, *, deprecated=False):
    return DefinitionRow(
        kind=MASK,
        idx=idx,
        params={"filter_workflow": "read-mask", "rype": idx / 1000},
        deprecated=deprecated,
        workflow="read-mask",
    )


def assembly(idx, mask_idx, *, deprecated=False):
    return DefinitionRow(
        kind=PROCESSING,
        idx=idx,
        params={
            "workflow": "long-read-assembly",
            "version": "1.0.0",
            "mask_idx": mask_idx,
            "assembler": "myloasm",
        },
        deprecated=deprecated,
        workflow="long-read-assembly",
    )


def block(idx, mask_idx, *, deprecated=False):
    return DefinitionRow(
        kind=ALIGNMENT,
        idx=idx,
        params={"reference_idx": 7, "aligner": "minimap2", "mask_idx": mask_idx, "shard_ids": [0]},
        deprecated=deprecated,
        workflow=None,
    )


def denovo(idx, mask_idx, processing_idx):
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
        },
        deprecated=False,
        workflow=None,
    )


def similar(definitions, kind, a, b):
    by_key = {(d.kind, d.idx): d for d in definitions}
    low, high = sorted((a, b))
    lo, hi = by_key[(kind, low)], by_key[(kind, high)]
    change = param_diff(lo.params, hi.params)
    return PairState(
        idx=next(_pair_ids),
        kind=kind,
        low=low,
        high=high,
        verdict=DefinitionPairVerdict.SIMILAR,
        change=change,
        derived_only=is_derived_only(kind, lo.params, hi.params, change),
    )


def done():
    return RunObservation(gate="completed")


def gate(state, *tickets):
    return RunObservation(gate=state, unfinished=tuple(tickets))


def resolve(definitions, pairs, kind, idx, observations):
    groups = compute_groups(definitions, pairs)
    return resolve_sample(groups, kind, idx, S, observations)


# Masks 5 and 12 decided similar; alignments 50 (on 5) and 120 (on 12) are then one
# group by derivation.
DEFS = [mask(5), mask(12), block(50, 5), block(120, 12)]
PAIRS = [similar(DEFS, MASK, 5, 12)]


# ---------------------------------------------------------------------------
# Which member
# ---------------------------------------------------------------------------


def test_highest_id_member_with_a_run_is_used():
    obs = {MASK: {(5, S): done(), (12, S): done()}}
    r = resolve(DEFS, PAIRS, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason) == (PROCESSED, 12, None)


def test_a_sample_with_a_run_only_under_the_older_member_resolves_to_it():
    obs = {MASK: {(5, S): done()}}
    r = resolve(DEFS, PAIRS, MASK, 12, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 5)


def test_no_member_with_a_run_is_no_run():
    r = resolve(DEFS, PAIRS, MASK, 5, {})
    assert (r.state, r.definition_idx, r.reason) == (NOT_PROCESSED, None, R.NO_RUN)


def test_a_definition_outside_any_group_resolves_alone():
    defs = [mask(5), mask(12)]
    obs = {MASK: {(12, S): done()}}
    assert resolve(defs, [], MASK, 5, obs).reason is R.NO_RUN


# ---------------------------------------------------------------------------
# Incomplete
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ticket_state", [WorkTicketState.PENDING, WorkTicketState.QUEUED, WorkTicketState.PROCESSING]
)
def test_a_waiting_or_running_ticket_under_the_newest_member_is_in_progress(ticket_state):
    obs = {MASK: {(5, S): done(), (12, S): gate("pending", (77, ticket_state))}}
    r = resolve(DEFS, PAIRS, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason, r.work_ticket_idx) == (
        INCOMPLETE,
        12,
        R.IN_PROGRESS,
        77,
    )


def test_a_failed_ticket_is_incomplete_failed_and_does_not_fall_back():
    obs = {MASK: {(5, S): done(), (12, S): gate(None, (78, WorkTicketState.FAILED))}}
    r = resolve(DEFS, PAIRS, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason, r.work_ticket_idx) == (
        INCOMPLETE,
        12,
        R.FAILED,
        78,
    )


def test_running_wins_over_failed_when_naming_the_ticket():
    obs = {
        MASK: {
            (12, S): gate("pending", (70, WorkTicketState.PROCESSING), (90, WorkTicketState.FAILED))
        }
    }
    r = resolve(DEFS, PAIRS, MASK, 12, obs)
    assert (r.reason, r.work_ticket_idx) == (R.IN_PROGRESS, 70)


def test_a_same_version_rerun_over_a_completed_gate_is_incomplete():
    obs = {ALIGNMENT: {(120, S): gate("completed", (80, WorkTicketState.PROCESSING))}}
    obs[MASK] = {(12, S): done()}
    r = resolve(DEFS, PAIRS, ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.reason) == (INCOMPLETE, 120, R.IN_PROGRESS)


def test_a_rearmed_block_reads_as_incomplete():
    """rerun-block sets the completed gates back to pending and re-arms the block's
    ticket in place."""
    rows = [
        RunRow(SOURCE_GATE, 120, S, "pending", None),
        RunRow(SOURCE_BLOCK_TICKET, 120, S, "pending", 81),
    ]
    obs = {ALIGNMENT: build_observations(rows), MASK: {(12, S): done()}}
    r = resolve(DEFS, PAIRS, ALIGNMENT, 120, obs)
    assert (r.state, r.reason, r.work_ticket_idx) == (INCOMPLETE, R.IN_PROGRESS, 81)


def test_a_pending_gate_whose_tickets_were_cancelled_is_not_a_run():
    """Cancelled tickets never reach the observations (the SQL drops them), so the
    gate they left pending has no unfinished ticket and is ignored."""
    obs = {
        MASK: {(5, S): done(), **build_observations([RunRow(SOURCE_GATE, 12, S, "pending", None)])}
    }
    r = resolve(DEFS, PAIRS, MASK, 12, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 5)


# ---------------------------------------------------------------------------
# Not processed
# ---------------------------------------------------------------------------


def test_a_withdrawn_run_is_not_processed_with_no_fallback():
    obs = {MASK: {(5, S): done(), (12, S): gate("invalidated")}}
    r = resolve(DEFS, PAIRS, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason) == (NOT_PROCESSED, 12, R.WITHDRAWN)


def test_a_deprecated_definition_is_not_processed():
    defs = [mask(5, deprecated=True)]
    r = resolve(defs, [], MASK, 5, {MASK: {(5, S): done()}})
    assert (r.state, r.reason) == (NOT_PROCESSED, R.DEPRECATED)


def test_a_deprecated_upstream_mask_is_upstream_deprecated():
    defs = [mask(5, deprecated=True), block(50, 5)]
    obs = {MASK: {(5, S): done()}, ALIGNMENT: {(50, S): done()}}
    r = resolve(defs, [], ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.reason) == (NOT_PROCESSED, 50, R.UPSTREAM_DEPRECATED)
    assert (r.upstream_kind, r.upstream_definition_idx) == (MASK, 5)


def test_a_withdrawn_upstream_run_is_upstream_withdrawn():
    defs = [mask(5), block(50, 5)]
    obs = {MASK: {(5, S): gate("invalidated")}, ALIGNMENT: {(50, S): done()}}
    r = resolve(defs, [], ALIGNMENT, 50, obs)
    assert (r.state, r.reason, r.upstream_definition_idx) == (
        NOT_PROCESSED,
        R.UPSTREAM_WITHDRAWN,
        5,
    )


def test_an_alignment_with_no_upstream_mask_run_is_upstream_no_run():
    defs = [mask(5), block(50, 5)]
    r = resolve(defs, [], ALIGNMENT, 50, {ALIGNMENT: {(50, S): done()}})
    assert (r.state, r.reason) == (NOT_PROCESSED, R.UPSTREAM_NO_RUN)


# ---------------------------------------------------------------------------
# Downstream counts only on the resolved upstream
# ---------------------------------------------------------------------------


def test_an_alignment_on_a_mask_the_sample_moved_off_is_upstream_moved():
    """The sample is masked under 5 and 12 (so its mask resolves to 12) but aligned
    only on 5: it needs aligning on 12."""
    obs = {MASK: {(5, S): done(), (12, S): done()}, ALIGNMENT: {(50, S): done()}}
    r = resolve(DEFS, PAIRS, ALIGNMENT, 120, obs)
    assert (r.state, r.definition_idx, r.reason, r.mask_idx) == (
        INCOMPLETE,
        50,
        R.UPSTREAM_MOVED,
        5,
    )
    assert (r.upstream_kind, r.upstream_definition_idx) == (MASK, 12)


def test_an_alignment_on_the_resolved_mask_is_processed_and_names_its_mask():
    obs = {MASK: {(5, S): done(), (12, S): done()}, ALIGNMENT: {(50, S): done(), (120, S): done()}}
    r = resolve(DEFS, PAIRS, ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.mask_idx) == (PROCESSED, 120, 12)


def test_an_incomplete_upstream_makes_the_alignment_incomplete():
    obs = {
        MASK: {(5, S): done(), (12, S): gate("pending", (60, WorkTicketState.QUEUED))},
        ALIGNMENT: {(50, S): done()},
    }
    r = resolve(DEFS, PAIRS, ALIGNMENT, 50, obs)
    assert (r.state, r.reason, r.work_ticket_idx, r.upstream_definition_idx) == (
        INCOMPLETE,
        R.UPSTREAM_INCOMPLETE,
        60,
        12,
    )


# ---------------------------------------------------------------------------
# Assembly and de novo alignment
# ---------------------------------------------------------------------------


def test_assembly_no_data_counts_as_completed():
    defs = [mask(5), assembly(30, 5)]
    obs = {MASK: {(5, S): done()}, PROCESSING: {(30, S): gate("no_data")}}
    r = resolve(defs, [], PROCESSING, 30, obs)
    assert (r.state, r.definition_idx, r.mask_idx) == (PROCESSED, 30, 5)


def _denovo_world(*, processing_deprecated=False):
    defs = [mask(5), assembly(30, 5, deprecated=processing_deprecated), denovo(60, 5, 30)]
    obs = {
        MASK: {(5, S): done()},
        PROCESSING: {(30, S): done()},
        ALIGNMENT: {(60, S): done()},
    }
    return defs, obs


def test_a_denovo_alignment_with_valid_upstreams_is_processed():
    defs, obs = _denovo_world()
    r = resolve(defs, [], ALIGNMENT, 60, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 60)


def test_a_denovo_alignment_on_a_deprecated_assembly_is_upstream_deprecated():
    defs, obs = _denovo_world(processing_deprecated=True)
    r = resolve(defs, [], ALIGNMENT, 60, obs)
    assert (r.reason, r.upstream_kind, r.upstream_definition_idx) == (
        R.UPSTREAM_DEPRECATED,
        PROCESSING,
        30,
    )


def test_a_denovo_alignment_whose_assembly_is_rerunning_is_upstream_incomplete():
    defs, obs = _denovo_world()
    obs[PROCESSING] = {(30, S): gate("completed", (44, WorkTicketState.FAILED))}
    r = resolve(defs, [], ALIGNMENT, 60, obs)
    assert (r.state, r.reason, r.work_ticket_idx) == (INCOMPLETE, R.UPSTREAM_INCOMPLETE, 44)


def test_needed_definitions_follows_upstream_groups():
    defs = [*DEFS, assembly(30, 5), denovo(60, 5, 30)]
    groups = compute_groups(defs, PAIRS)
    assert needed_definitions(groups, ALIGNMENT, 50) == {ALIGNMENT: {50, 120}, MASK: {5, 12}}
    assert needed_definitions(groups, ALIGNMENT, 60) == {
        ALIGNMENT: {60},
        MASK: {5, 12},
        PROCESSING: {30},
    }


# ---------------------------------------------------------------------------
# Observations from rows
# ---------------------------------------------------------------------------


def test_only_the_newest_per_sample_ticket_counts():
    """A failed ticket a later completed one superseded does not leave the sample
    incomplete; a failed re-run after a completed one does."""
    superseded = build_observations(
        [
            RunRow(SOURCE_GATE, 12, S, "completed", None),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, "failed", 10),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, "completed", 11),
        ]
    )
    assert superseded[(12, S)].unfinished == ()
    rerun_failed = build_observations(
        [
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, "completed", 10),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, "failed", 11),
        ]
    )
    assert rerun_failed[(12, S)].unfinished == ((11, WorkTicketState.FAILED),)


def test_every_covering_block_ticket_counts():
    obs = build_observations(
        [
            RunRow(SOURCE_BLOCK_TICKET, 12, S, "completed", 10),
            RunRow(SOURCE_BLOCK_TICKET, 12, S, "failed", 11),
            RunRow(SOURCE_BLOCK_TICKET, 12, S, "processing", 12),
        ]
    )
    assert obs[(12, S)].unfinished == (
        (11, WorkTicketState.FAILED),
        (12, WorkTicketState.PROCESSING),
    )


def test_a_ticket_row_without_a_ticket_id_is_refused():
    with pytest.raises(ValueError, match="no work_ticket_idx"):
        build_observations([RunRow(SOURCE_SAMPLE_TICKET, 12, S, "failed", None)])


def _candidate(**context):
    return AssemblyTicketCandidate(
        work_ticket_idx=99,
        prep_sample_idx=S,
        state="processing",
        action_id="long-read-assembly",
        action_version="1.0.0",
        action_context=context,
        mask_idx=5,
        context_schema={"properties": {"assembler": {"default": "myloasm"}}},
    )


def test_an_assembly_ticket_matches_the_processing_its_params_mint():
    """An omitted assembler is the action's default, as the runner hashes it."""
    params = {30: assembly(30, 5).params, 31: {**assembly(31, 5).params, "assembler": "flye"}}
    rows = match_assembly_tickets([_candidate(mask_idx=5)], params)
    assert [(r.definition_idx, r.work_ticket_idx, r.source) for r in rows] == [
        (30, 99, SOURCE_SAMPLE_TICKET)
    ]
    rows = match_assembly_tickets([_candidate(mask_idx=5, assembler="flye")], params)
    assert [r.definition_idx for r in rows] == [31]
    assert match_assembly_tickets([_candidate(mask_idx=5, assembler="hifiasm")], params) == []


# ---------------------------------------------------------------------------
# Cohort refusal and the de novo arm check
# ---------------------------------------------------------------------------


def test_cohort_refusal_codes():
    obs = {MASK: {(5, S): done(), (12, S): done()}}
    processed_12 = resolve(DEFS, PAIRS, MASK, 5, obs)
    assert cohort_refusal(processed_12, 12) is None
    assert cohort_refusal(processed_12, 5) is DefinitionGroupRefusal.RESOLVED_ELSEWHERE
    no_run = resolve(DEFS, PAIRS, MASK, 5, {})
    assert cohort_refusal(no_run, 5) is DefinitionGroupRefusal.NOT_PROCESSED
    running = resolve(
        DEFS, PAIRS, MASK, 5, {MASK: {(5, S): gate("pending", (1, WorkTicketState.QUEUED))}}
    )
    assert cohort_refusal(running, 5) is DefinitionGroupRefusal.INCOMPLETE


def test_denovo_arms_on_masks_of_one_group_are_accepted_when_the_group_is_given():
    denovo_params = denovo(60, 12, 30).params
    reference_params = block(50, 5).params
    with pytest.raises(ValueError, match="both arms must read the same masked pass-set"):
        denovo_alignment_processing_idx(
            denovo_alignment_idx=60, denovo_params=denovo_params, reference_params=reference_params
        )
    groups = compute_groups(DEFS, PAIRS)
    assert (
        denovo_alignment_processing_idx(
            denovo_alignment_idx=60,
            denovo_params=denovo_params,
            reference_params=reference_params,
            reference_mask_group=groups.group_of(MASK, 5),
        )
        == 30
    )


def test_denovo_arms_on_masks_of_different_groups_are_refused_with_the_group():
    groups = compute_groups([mask(5), mask(12)], [])
    with pytest.raises(ValueError, match="not in the mask group"):
        denovo_alignment_processing_idx(
            denovo_alignment_idx=60,
            denovo_params=denovo(60, 12, 30).params,
            reference_params=block(50, 5).params,
            reference_mask_group=groups.group_of(MASK, 5),
        )
    with pytest.raises(ValueError, match="does not contain the reference arm"):
        denovo_alignment_processing_idx(
            denovo_alignment_idx=60,
            denovo_params=denovo(60, 12, 30).params,
            reference_params=block(50, 5).params,
            reference_mask_group=groups.group_of(MASK, 12),
        )
