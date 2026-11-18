"""Unit tests for the per-sample group resolver: which member a sample resolves to,
and each incomplete / not-processed reason. No database: definitions, equivalences and run
observations are built in memory and handed to the pure core."""

from datetime import UTC, datetime, timedelta
from itertools import count

import pytest
from qiita_common.models import (
    NON_JOINING_VERDICTS,
    DefinitionGroupRefusal,
    DefinitionKind,
    OutputEquivalenceVerdict,
    SampleResolutionReason,
    SampleResolutionState,
    WorkTicketState,
)

from qiita_control_plane.definition_resolver import (
    DefinitionNotObservable,
    RunObservation,
    build_observations,
    cohort_refusal,
    needed_definitions,
    require_observable,
    resolve_sample,
)
from qiita_control_plane.definition_similarity import (
    EquivalenceState,
    compute_groups,
    is_derived_only,
    param_diff,
)
from qiita_control_plane.feature_table import denovo_alignment_processing_idx
from qiita_control_plane.repositories.alignment_definition import (
    ALIGNMENT_SAMPLE_COMPLETED,
    ALIGNMENT_SAMPLE_PENDING,
)
from qiita_control_plane.repositories.assembly import (
    ASSEMBLY_SAMPLE_COMPLETED,
    ASSEMBLY_SAMPLE_NO_DATA,
)
from qiita_control_plane.repositories.block import (
    MASK_SAMPLE_COMPLETED,
    MASK_SAMPLE_INVALIDATED,
    MASK_SAMPLE_PENDING,
)
from qiita_control_plane.repositories.definition_run import (
    SOURCE_BLOCK_TICKET,
    SOURCE_GATE,
    SOURCE_SAMPLE_TICKET,
    RunRow,
)
from qiita_control_plane.repositories.output_equivalence import DefinitionRow

MASK = DefinitionKind.MASK
ALIGNMENT = DefinitionKind.ALIGNMENT
PROCESSING = DefinitionKind.PROCESSING
PROCESSED = SampleResolutionState.PROCESSED
INCOMPLETE = SampleResolutionState.INCOMPLETE
NOT_PROCESSED = SampleResolutionState.NOT_PROCESSED
R = SampleResolutionReason
S = 900  # the prep_sample every test resolves
T0 = datetime(2026, 1, 1, tzinfo=UTC)  # a row's `at` where the order of changes is moot

_equivalence_ids = count(1000)


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


def decided(definitions, kind, a, b, verdict=OutputEquivalenceVerdict.SIMILAR):
    """A decided output equivalence of `a` and `b`; `verdict` is `similar` unless given."""
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


def done():
    return RunObservation(gate=MASK_SAMPLE_COMPLETED)


def gate(state, *tickets):
    return RunObservation(gate=state, unfinished=tuple(tickets))


def resolve(definitions, equivalences, kind, idx, observations):
    groups = compute_groups(definitions, equivalences)
    return resolve_sample(groups, kind, idx, S, observations)


# Masks 5 and 12 decided similar; alignments 50 (on 5) and 120 (on 12) are then one
# group by derivation.
DEFS = [mask(5), mask(12), block(50, 5), block(120, 12)]
EQUIVALENCES = [decided(DEFS, MASK, 5, 12)]


# ---------------------------------------------------------------------------
# Which member
# ---------------------------------------------------------------------------


def test_highest_id_member_with_a_run_is_used():
    obs = {MASK: {(5, S): done(), (12, S): done()}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason) == (PROCESSED, 12, None)


def test_a_sample_with_a_run_only_under_the_older_member_resolves_to_it():
    obs = {MASK: {(5, S): done()}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 12, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 5)


def test_no_member_with_a_run_is_no_run():
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, {})
    assert (r.state, r.definition_idx, r.reason) == (NOT_PROCESSED, None, R.NO_RUN)


def test_a_definition_outside_any_group_resolves_alone():
    defs = [mask(5), mask(12)]
    obs = {MASK: {(12, S): done()}}
    assert resolve(defs, [], MASK, 5, obs).reason is R.NO_RUN


@pytest.mark.parametrize(
    "kind, idx, obs",
    [
        (MASK, 5, {MASK: {(5, S): done(), (12, S): done()}}),
        (MASK, 12, {MASK: {(5, S): done()}}),
        (MASK, 5, {}),
        (ALIGNMENT, 50, {ALIGNMENT: {(50, S): done(), (120, S): done()}}),
        (ALIGNMENT, 120, {ALIGNMENT: {(50, S): done()}}),
    ],
    ids=["mask-newest", "mask-older-only", "mask-no-run", "derived-newest", "derived-older"],
)
def test_a_same_decision_resolves_exactly_as_a_similar_one(kind, idx, obs):
    """Masks 5 and 12 decided `same` give every resolution, the derived alignment group
    included, that a `similar` decision gives."""
    same = [decided(DEFS, MASK, 5, 12, OutputEquivalenceVerdict.SAME)]
    assert resolve(DEFS, same, kind, idx, obs) == resolve(DEFS, EQUIVALENCES, kind, idx, obs)
    assert compute_groups(DEFS, same).group_of(kind, idx) == compute_groups(
        DEFS, EQUIVALENCES
    ).group_of(kind, idx)
    assert len(compute_groups(DEFS, same).group_of(kind, idx)) == 2


@pytest.mark.parametrize("verdict", sorted(NON_JOINING_VERDICTS), ids=str)
def test_a_non_joining_decision_resolves_each_definition_alone(verdict):
    apart = [decided(DEFS, MASK, 5, 12, verdict)]
    obs = {MASK: {(12, S): done()}}
    assert resolve(DEFS, apart, MASK, 5, obs).reason is R.NO_RUN
    assert compute_groups(DEFS, apart).group_of(ALIGNMENT, 50) == frozenset({50})


# ---------------------------------------------------------------------------
# Incomplete
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ticket_state", [WorkTicketState.PENDING, WorkTicketState.QUEUED, WorkTicketState.PROCESSING]
)
def test_a_waiting_or_running_ticket_under_the_newest_member_is_in_progress(ticket_state):
    obs = {MASK: {(5, S): done(), (12, S): gate(MASK_SAMPLE_PENDING, (77, ticket_state))}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason, r.work_ticket_idx) == (
        INCOMPLETE,
        12,
        R.IN_PROGRESS,
        77,
    )


def test_a_failed_ticket_is_incomplete_failed_and_does_not_fall_back():
    obs = {MASK: {(5, S): done(), (12, S): gate(None, (78, WorkTicketState.FAILED))}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, obs)
    assert (r.state, r.definition_idx, r.reason, r.work_ticket_idx) == (
        INCOMPLETE,
        12,
        R.FAILED,
        78,
    )


_EXPECTED_BY_TICKET_STATE = {
    WorkTicketState.PENDING: (INCOMPLETE, 12, R.IN_PROGRESS),
    WorkTicketState.QUEUED: (INCOMPLETE, 12, R.IN_PROGRESS),
    WorkTicketState.PROCESSING: (INCOMPLETE, 12, R.IN_PROGRESS),
    WorkTicketState.FAILED: (INCOMPLETE, 12, R.FAILED),
    WorkTicketState.COMPLETED: (PROCESSED, 5, None),
    WorkTicketState.NO_DATA: (PROCESSED, 5, None),
    WorkTicketState.CANCELLED: (PROCESSED, 5, None),
}


def test_every_ticket_state_has_an_expected_resolution():
    assert set(_EXPECTED_BY_TICKET_STATE) == set(WorkTicketState)


@pytest.mark.parametrize("ticket_state", list(WorkTicketState))
def test_a_block_ticket_in_each_state_over_a_pending_gate(ticket_state):
    rows = [
        RunRow(
            source=SOURCE_GATE,
            definition_idx=5,
            prep_sample_idx=S,
            state=MASK_SAMPLE_COMPLETED,
            work_ticket_idx=None,
            at=T0,
        ),
        RunRow(
            source=SOURCE_GATE,
            definition_idx=12,
            prep_sample_idx=S,
            state=MASK_SAMPLE_PENDING,
            work_ticket_idx=None,
            at=T0,
        ),
        RunRow(
            source=SOURCE_BLOCK_TICKET,
            definition_idx=12,
            prep_sample_idx=S,
            state=ticket_state.value,
            work_ticket_idx=77,
            at=T0,
        ),
    ]
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, {MASK: build_observations(MASK, rows)})
    assert (r.state, r.definition_idx, r.reason) == _EXPECTED_BY_TICKET_STATE[ticket_state]


def test_an_unknown_gate_state_is_refused():
    with pytest.raises(ValueError, match="unknown mask gate state"):
        resolve(DEFS, EQUIVALENCES, MASK, 5, {MASK: {(12, S): gate("archived")}})


def test_running_wins_over_failed_when_naming_the_ticket():
    obs = {
        MASK: {
            (12, S): gate(
                MASK_SAMPLE_PENDING, (70, WorkTicketState.PROCESSING), (90, WorkTicketState.FAILED)
            )
        }
    }
    r = resolve(DEFS, EQUIVALENCES, MASK, 12, obs)
    assert (r.reason, r.work_ticket_idx) == (R.IN_PROGRESS, 70)


def test_a_same_version_rerun_over_a_completed_gate_is_incomplete():
    obs = {
        ALIGNMENT: {(120, S): gate(ALIGNMENT_SAMPLE_COMPLETED, (80, WorkTicketState.PROCESSING))}
    }
    obs[MASK] = {(12, S): done()}
    r = resolve(DEFS, EQUIVALENCES, ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.reason) == (INCOMPLETE, 120, R.IN_PROGRESS)


def test_a_rearmed_block_reads_as_incomplete():
    """rerun-block sets the completed gates back to pending and re-arms the block's
    ticket in place."""
    rows = [
        RunRow(SOURCE_GATE, 120, S, ALIGNMENT_SAMPLE_PENDING, None, T0),
        RunRow(SOURCE_BLOCK_TICKET, 120, S, WorkTicketState.PENDING.value, 81, T0),
    ]
    obs = {ALIGNMENT: build_observations(ALIGNMENT, rows), MASK: {(12, S): done()}}
    r = resolve(DEFS, EQUIVALENCES, ALIGNMENT, 120, obs)
    assert (r.state, r.reason, r.work_ticket_idx) == (INCOMPLETE, R.IN_PROGRESS, 81)


def test_a_pending_gate_whose_tickets_were_cancelled_is_not_a_run():
    """Cancelled tickets never reach the observations (the SQL drops them), so the
    gate they left pending has no unfinished ticket and is ignored."""
    obs = {
        MASK: {
            (5, S): done(),
            **build_observations(MASK, [RunRow(SOURCE_GATE, 12, S, MASK_SAMPLE_PENDING, None, T0)]),
        }
    }
    r = resolve(DEFS, EQUIVALENCES, MASK, 12, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 5)


# ---------------------------------------------------------------------------
# Not processed
# ---------------------------------------------------------------------------


def test_a_withdrawn_run_is_not_processed_with_no_fallback():
    obs = {MASK: {(5, S): done(), (12, S): gate(MASK_SAMPLE_INVALIDATED)}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 5, obs)
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
    obs = {MASK: {(5, S): gate(MASK_SAMPLE_INVALIDATED)}, ALIGNMENT: {(50, S): done()}}
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
# Exact mode
# ---------------------------------------------------------------------------


def resolve_exact_sample(definitions, equivalences, kind, idx, observations):
    groups = compute_groups(definitions, equivalences)
    return resolve_sample(groups, kind, idx, S, observations, exact=True)


def resolve_past_upstream(definitions, equivalences, kind, idx, observations, admitted=None):
    groups = compute_groups(definitions, equivalences)
    return resolve_sample(
        groups,
        kind,
        idx,
        S,
        observations,
        admitted_upstream_kinds=(frozenset({MASK, PROCESSING}) if admitted is None else admitted),
    )


def test_deprecated_upstreams_mode_reads_an_alignment_on_a_deprecated_mask():
    defs = [mask(5, deprecated=True), block(50, 5)]
    obs = {MASK: {(5, S): done()}, ALIGNMENT: {(50, S): done()}}
    assert (resolve(defs, [], ALIGNMENT, 50, obs).reason) == R.UPSTREAM_DEPRECATED
    r = resolve_past_upstream(defs, [], ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 50)


def test_deprecated_upstreams_mode_reads_a_denovo_alignment_on_a_deprecated_assembly():
    defs = [mask(5, deprecated=True), assembly(7, 5, deprecated=True), denovo(70, 5, 7)]
    obs = {
        MASK: {(5, S): done()},
        PROCESSING: {(7, S): done()},
        ALIGNMENT: {(70, S): done()},
    }
    r = resolve_past_upstream(defs, [], ALIGNMENT, 70, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 70)


def test_an_admitted_assembly_does_not_admit_its_deprecated_mask():
    """`include_deprecated` alone admits a deprecated assembly run, not a deprecated
    mask under it (`admitted_deprecated_upstream_kinds`)."""
    defs = [mask(5, deprecated=True), assembly(7, 5, deprecated=True), denovo(70, 5, 7)]
    obs = {
        MASK: {(5, S): done()},
        PROCESSING: {(7, S): done()},
        ALIGNMENT: {(70, S): done()},
    }
    r = resolve_past_upstream(defs, [], ALIGNMENT, 70, obs, admitted=frozenset({PROCESSING}))
    assert (r.state, r.reason) == (NOT_PROCESSED, R.UPSTREAM_DEPRECATED)
    alone = [mask(5), assembly(7, 5, deprecated=True), denovo(70, 5, 7)]
    r = resolve_past_upstream(alone, [], ALIGNMENT, 70, obs, admitted=frozenset({PROCESSING}))
    assert (r.state, r.definition_idx) == (PROCESSED, 70)


def test_deprecated_upstreams_mode_keeps_a_deprecated_member_and_a_withdrawn_upstream():
    deprecated_member = [mask(5), block(50, 5, deprecated=True)]
    obs = {MASK: {(5, S): done()}, ALIGNMENT: {(50, S): done()}}
    assert resolve_past_upstream(deprecated_member, [], ALIGNMENT, 50, obs).reason == R.DEPRECATED
    withdrawn = {MASK: {(5, S): gate(MASK_SAMPLE_INVALIDATED)}, ALIGNMENT: {(50, S): done()}}
    r = resolve_past_upstream(
        [mask(5, deprecated=True), block(50, 5)], [], ALIGNMENT, 50, withdrawn
    )
    assert r.reason == R.UPSTREAM_WITHDRAWN


def test_exact_mode_reads_a_deprecated_definition():
    defs = [mask(5, deprecated=True)]
    r = resolve_exact_sample(defs, [], MASK, 5, {MASK: {(5, S): done()}})
    assert (r.state, r.definition_idx, r.reason) == (PROCESSED, 5, None)


def test_exact_mode_keeps_a_withdrawn_run_unreadable():
    defs = [mask(5, deprecated=True)]
    obs = {MASK: {(5, S): gate(MASK_SAMPLE_INVALIDATED)}}
    r = resolve_exact_sample(defs, [], MASK, 5, obs)
    assert (r.state, r.reason) == (NOT_PROCESSED, R.WITHDRAWN)


def test_exact_mode_ignores_a_newer_group_member():
    obs = {MASK: {(5, S): done(), (12, S): done()}}
    r = resolve_exact_sample(DEFS, EQUIVALENCES, MASK, 5, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 5)
    assert resolve(DEFS, EQUIVALENCES, MASK, 5, obs).definition_idx == 12
    r = resolve_exact_sample(DEFS, EQUIVALENCES, MASK, 5, {MASK: {(12, S): done()}})
    assert (r.state, r.reason) == (NOT_PROCESSED, R.NO_RUN)


def test_exact_mode_refuses_a_withdrawn_upstream_run_and_reads_a_deprecated_one():
    defs = [mask(5, deprecated=True), block(50, 5, deprecated=True)]
    withdrawn = {MASK: {(5, S): gate(MASK_SAMPLE_INVALIDATED)}, ALIGNMENT: {(50, S): done()}}
    r = resolve_exact_sample(defs, [], ALIGNMENT, 50, withdrawn)
    assert (r.state, r.reason, r.upstream_definition_idx) == (
        NOT_PROCESSED,
        R.UPSTREAM_WITHDRAWN,
        5,
    )
    obs = {MASK: {(5, S): done()}, ALIGNMENT: {(50, S): done()}}
    r = resolve_exact_sample(defs, [], ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.mask_idx) == (PROCESSED, 50, 5)


def test_exact_mode_does_not_move_with_the_upstream_group():
    """Aligned on 50 over mask 5, masked under 5 and 12: the group resolution says
    upstream_moved; the exact read of 50 reads it."""
    obs = {MASK: {(5, S): done(), (12, S): done()}, ALIGNMENT: {(50, S): done()}}
    r = resolve_exact_sample(DEFS, EQUIVALENCES, ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx) == (PROCESSED, 50)


def test_exact_needed_definitions_are_the_definition_and_its_upstreams():
    groups = compute_groups(DEFS, EQUIVALENCES)
    assert needed_definitions(groups, ALIGNMENT, 50, exact=True) == {ALIGNMENT: {50}, MASK: {5}}


# ---------------------------------------------------------------------------
# Downstream counts only on the resolved upstream
# ---------------------------------------------------------------------------


def test_an_alignment_on_a_mask_the_sample_moved_off_is_upstream_moved():
    """The sample is masked under 5 and 12 (so its mask resolves to 12) but aligned
    only on 5: it needs aligning on 12."""
    obs = {MASK: {(5, S): done(), (12, S): done()}, ALIGNMENT: {(50, S): done()}}
    r = resolve(DEFS, EQUIVALENCES, ALIGNMENT, 120, obs)
    assert (r.state, r.definition_idx, r.reason, r.mask_idx) == (
        INCOMPLETE,
        50,
        R.UPSTREAM_MOVED,
        5,
    )
    assert (r.upstream_kind, r.upstream_definition_idx) == (MASK, 12)


def test_an_alignment_on_the_resolved_mask_is_processed_and_names_its_mask():
    obs = {MASK: {(5, S): done(), (12, S): done()}, ALIGNMENT: {(50, S): done(), (120, S): done()}}
    r = resolve(DEFS, EQUIVALENCES, ALIGNMENT, 50, obs)
    assert (r.state, r.definition_idx, r.mask_idx) == (PROCESSED, 120, 12)


def test_an_incomplete_upstream_makes_the_alignment_incomplete():
    obs = {
        MASK: {(5, S): done(), (12, S): gate(MASK_SAMPLE_PENDING, (60, WorkTicketState.QUEUED))},
        ALIGNMENT: {(50, S): done()},
    }
    r = resolve(DEFS, EQUIVALENCES, ALIGNMENT, 50, obs)
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
    obs = {MASK: {(5, S): done()}, PROCESSING: {(30, S): gate(ASSEMBLY_SAMPLE_NO_DATA)}}
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
    obs[PROCESSING] = {(30, S): gate(ASSEMBLY_SAMPLE_COMPLETED, (44, WorkTicketState.FAILED))}
    r = resolve(defs, [], ALIGNMENT, 60, obs)
    assert (r.state, r.reason, r.work_ticket_idx) == (INCOMPLETE, R.UPSTREAM_INCOMPLETE, 44)


def _remasked_world(p2_gate):
    """Masks 5 < 12 decided similar, so assemblies 30 (on 5) and 31 (on 12) and de novo
    alignments 60 (on 30) and 61 (on 31) group by derivation. The prep_sample was
    assembled and aligned under mask 5, then re-masked under 12 and assembled again
    (31, gate `p2_gate`); 61 has no run for it."""
    defs = [mask(5), mask(12), assembly(30, 5), assembly(31, 12), denovo(60, 5, 30)]
    defs.append(denovo(61, 12, 31))
    equivalences = [decided(defs, MASK, 5, 12)]
    obs = {
        MASK: {(5, S): done(), (12, S): done()},
        PROCESSING: {(30, S): done(), (31, S): gate(p2_gate)},
        ALIGNMENT: {(60, S): done()},
    }
    return defs, equivalences, obs


def test_a_denovo_alignment_whose_assembly_produced_no_contigs_has_nothing_to_read():
    """Align-denovo ends NO_DATA without a gate row when its assembly is empty, so 61
    has no run; it is still the member that the prep_sample resolves to, with nothing
    to read, rather than 60 with its mask moved."""
    defs, equivalences, obs = _remasked_world(ASSEMBLY_SAMPLE_NO_DATA)
    for anchor in (60, 61):
        r = resolve(defs, equivalences, ALIGNMENT, anchor, obs)
        assert (r.state, r.reason, r.definition_idx, r.mask_idx) == (
            NOT_PROCESSED,
            R.NO_DATA,
            61,
            12,
        )
        assert cohort_refusal(r, 61) is DefinitionGroupRefusal.NOT_PROCESSED


def test_a_denovo_alignment_whose_assembly_has_contigs_and_no_run_still_needs_one():
    """The same world with contigs under 31: 61 has yet to be aligned, so the prep_sample
    reads as 60 with its mask moved, as before."""
    defs, equivalences, obs = _remasked_world(ASSEMBLY_SAMPLE_COMPLETED)
    r = resolve(defs, equivalences, ALIGNMENT, 60, obs)
    assert (r.state, r.reason, r.definition_idx, r.upstream_definition_idx) == (
        INCOMPLETE,
        R.UPSTREAM_MOVED,
        60,
        12,
    )


def test_an_empty_assembly_that_is_rerunning_is_not_nothing_to_read():
    defs, equivalences, obs = _remasked_world(ASSEMBLY_SAMPLE_NO_DATA)
    obs[PROCESSING][(31, S)] = gate(ASSEMBLY_SAMPLE_NO_DATA, (44, WorkTicketState.PROCESSING))
    r = resolve(defs, equivalences, ALIGNMENT, 60, obs)
    assert (r.state, r.reason, r.definition_idx) == (INCOMPLETE, R.UPSTREAM_MOVED, 60)


def test_a_group_reading_a_processing_of_another_workflow_is_refused():
    """An amplicon processing has no gate the resolver reads, named directly or as the
    upstream of a de novo alignment; an assembly one passes."""
    amplicon = DefinitionRow(
        kind=PROCESSING,
        idx=40,
        params={"workflow": "amplicon", "version": "1.0.0", "trim": 150},
        deprecated=False,
        workflow="amplicon",
    )
    groups = compute_groups([mask(5), assembly(30, 5), amplicon, denovo(61, 5, 40)], [])
    for kind, idx in ((PROCESSING, 40), (ALIGNMENT, 61)):
        with pytest.raises(DefinitionNotObservable, match="'amplicon' run"):
            require_observable(groups, kind, idx)
    require_observable(groups, PROCESSING, 30)


def test_needed_definitions_follows_upstream_groups():
    defs = [*DEFS, assembly(30, 5), denovo(60, 5, 30)]
    groups = compute_groups(defs, EQUIVALENCES)
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
        MASK,
        [
            RunRow(SOURCE_GATE, 12, S, MASK_SAMPLE_COMPLETED, None, T0),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.FAILED.value, 10, T0),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.COMPLETED.value, 11, T0),
        ],
    )
    assert superseded[(12, S)].unfinished == ()
    rerun_failed = build_observations(
        MASK,
        [
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.COMPLETED.value, 10, T0),
            RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.FAILED.value, 11, T0),
        ],
    )
    assert rerun_failed[(12, S)].unfinished == ((11, WorkTicketState.FAILED),)


def test_every_covering_block_ticket_counts():
    obs = build_observations(
        MASK,
        [
            RunRow(SOURCE_BLOCK_TICKET, 12, S, WorkTicketState.COMPLETED.value, 10, T0),
            RunRow(SOURCE_BLOCK_TICKET, 12, S, WorkTicketState.FAILED.value, 11, T0),
            RunRow(SOURCE_BLOCK_TICKET, 12, S, WorkTicketState.PROCESSING.value, 12, T0),
        ],
    )
    assert obs[(12, S)].unfinished == (
        (11, WorkTicketState.FAILED),
        (12, WorkTicketState.PROCESSING),
    )


T1, T2, T3 = (T0 + timedelta(hours=h) for h in (1, 2, 3))


def _rows(gate_state, gate_at, *tickets):
    """A gate row under mask 12 changed at `gate_at`, and `(source, state, idx, created_at)`
    tickets."""
    return [
        RunRow(SOURCE_GATE, 12, S, gate_state, None, gate_at),
        *(RunRow(src, 12, S, st.value, idx, at) for src, st, idx, at in tickets),
    ]


def _resolve_rows(rows):
    obs = {MASK: {(5, S): done(), **build_observations(MASK, rows)}}
    r = resolve(DEFS, EQUIVALENCES, MASK, 12, obs)
    return r.state, r.definition_idx, r.reason, r.work_ticket_idx


@pytest.mark.parametrize("source", [SOURCE_SAMPLE_TICKET, SOURCE_BLOCK_TICKET])
def test_a_failed_ticket_created_before_the_gate_was_restored_is_stale(source):
    """Withdraw (T1), a re-run that fails at finalize (created T2), restore (T3): the
    failed ticket is older than the gate's last change, so the restored run counts."""
    rows = _rows(MASK_SAMPLE_COMPLETED, T3, (source, WorkTicketState.FAILED, 11, T2))
    assert _resolve_rows(rows) == (PROCESSED, 12, None, None)


@pytest.mark.parametrize("source", [SOURCE_SAMPLE_TICKET, SOURCE_BLOCK_TICKET])
def test_a_failed_ticket_created_after_the_gate_completed_counts(source):
    rows = _rows(MASK_SAMPLE_COMPLETED, T1, (source, WorkTicketState.FAILED, 11, T2))
    assert _resolve_rows(rows) == (INCOMPLETE, 12, R.FAILED, 11)


def test_a_failed_ticket_created_with_the_gate_counts():
    """Same timestamp: the ticket and the gate row were written in one transaction."""
    rows = _rows(MASK_SAMPLE_COMPLETED, T1, (SOURCE_BLOCK_TICKET, WorkTicketState.FAILED, 11, T1))
    assert _resolve_rows(rows) == (INCOMPLETE, 12, R.FAILED, 11)


@pytest.mark.parametrize("state", [WorkTicketState.PENDING, WorkTicketState.PROCESSING])
@pytest.mark.parametrize("source", [SOURCE_SAMPLE_TICKET, SOURCE_BLOCK_TICKET])
def test_a_running_ticket_created_before_the_gate_change_still_counts(source, state):
    rows = _rows(MASK_SAMPLE_COMPLETED, T3, (source, state, 11, T2))
    assert _resolve_rows(rows) == (INCOMPLETE, 12, R.IN_PROGRESS, 11)


def test_a_failed_ticket_under_a_pending_gate_that_changed_later_counts():
    """rerun-block sets the gate pending after the block ticket's creation and re-arms
    that ticket in place; when it fails again, it is the run that explains the gate."""
    rows = _rows(MASK_SAMPLE_PENDING, T3, (SOURCE_BLOCK_TICKET, WorkTicketState.FAILED, 11, T2))
    assert _resolve_rows(rows) == (INCOMPLETE, 12, R.FAILED, 11)


def test_a_failed_ticket_older_than_a_withdrawal_is_stale():
    rows = _rows(
        MASK_SAMPLE_INVALIDATED, T3, (SOURCE_SAMPLE_TICKET, WorkTicketState.FAILED, 11, T2)
    )
    assert _resolve_rows(rows)[:3] == (NOT_PROCESSED, 12, R.WITHDRAWN)


def test_a_failed_ticket_with_no_gate_row_counts():
    obs = {
        MASK: {
            (5, S): done(),
            **build_observations(
                MASK, [RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.FAILED.value, 11, T2)]
            ),
        }
    }
    r = resolve(DEFS, EQUIVALENCES, MASK, 12, obs)
    assert (r.state, r.reason, r.work_ticket_idx) == (INCOMPLETE, R.FAILED, 11)


def test_a_ticket_row_without_a_ticket_id_is_refused():
    with pytest.raises(ValueError, match="no work_ticket_idx"):
        build_observations(
            MASK, [RunRow(SOURCE_SAMPLE_TICKET, 12, S, WorkTicketState.FAILED.value, None, T0)]
        )


# ---------------------------------------------------------------------------
# Cohort refusal and the de novo arm check
# ---------------------------------------------------------------------------


def test_cohort_refusal_codes():
    obs = {MASK: {(5, S): done(), (12, S): done()}}
    processed_12 = resolve(DEFS, EQUIVALENCES, MASK, 5, obs)
    assert cohort_refusal(processed_12, 12) is None
    assert cohort_refusal(processed_12, 5) is DefinitionGroupRefusal.RESOLVED_ELSEWHERE
    no_run = resolve(DEFS, EQUIVALENCES, MASK, 5, {})
    assert cohort_refusal(no_run, 5) is DefinitionGroupRefusal.NOT_PROCESSED
    running = resolve(
        DEFS,
        EQUIVALENCES,
        MASK,
        5,
        {MASK: {(5, S): gate(MASK_SAMPLE_PENDING, (1, WorkTicketState.QUEUED))}},
    )
    assert cohort_refusal(running, 5) is DefinitionGroupRefusal.INCOMPLETE


def test_denovo_arms_on_masks_of_one_group_are_accepted_when_the_group_is_given():
    denovo_params = denovo(60, 12, 30).params
    reference_params = block(50, 5).params
    with pytest.raises(ValueError, match="both arms must read the same masked pass-set"):
        denovo_alignment_processing_idx(
            denovo_alignment_idx=60, denovo_params=denovo_params, reference_params=reference_params
        )
    groups = compute_groups(DEFS, EQUIVALENCES)
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
