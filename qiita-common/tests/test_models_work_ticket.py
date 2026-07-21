"""Tests for WorkTicket-family Pydantic models."""

from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError


def test_step_type_enum():
    """StepType must expose the locked map / reduce / singleton triple."""
    from qiita_common.models import StepType

    assert StepType.MAP == "map"
    assert StepType.REDUCE == "reduce"
    assert StepType.SINGLETON == "singleton"


def test_work_ticket_state_enum():
    """WorkTicketState covers the lifecycle from PENDING through COMPLETED/FAILED."""
    from qiita_common.models import WorkTicketState

    assert WorkTicketState.PENDING == "pending"
    assert WorkTicketState.QUEUED == "queued"
    assert WorkTicketState.PROCESSING == "processing"
    assert WorkTicketState.COMPLETED == "completed"
    assert WorkTicketState.FAILED == "failed"


def test_resource_override_mem_gb_must_be_positive():
    """mem_gb is gt=0; None (the default) is the no-override case."""
    from qiita_common.models import ResourceOverride

    assert ResourceOverride().mem_gb is None
    assert ResourceOverride(mem_gb=48).mem_gb == 48
    with pytest.raises(ValidationError):
        ResourceOverride(mem_gb=0)


def test_work_ticket_create_request_optional_resource_override():
    """resource_override defaults to None and round-trips a coerced dict."""
    from qiita_common.models import ResourceOverride, WorkTicketCreateRequest

    base = {
        "action_id": "a",
        "action_version": "1.0.0",
        "scope_target": {"kind": "reference", "reference_idx": 1},
    }
    assert WorkTicketCreateRequest(**base).resource_override is None
    with_override = WorkTicketCreateRequest(**base, resource_override={"mem_gb": 48})
    assert with_override.resource_override == ResourceOverride(mem_gb=48)


def test_scope_target_dispatches_on_kind():
    """The discriminated union must select StudyPrepScopeTarget for kind='study_prep',
    ReferenceScopeTarget for kind='reference', PrepSampleScopeTarget for
    kind='prep_sample', SequencedPoolScopeTarget for kind='sequenced_pool', and
    BlockScopeTarget for kind='block'."""
    from qiita_common.models import (
        BlockScopeTarget,
        PrepSampleScopeTarget,
        ReferenceScopeTarget,
        ScopeTarget,
        SequencedPoolScopeTarget,
        StudyPrepScopeTarget,
    )

    adapter = TypeAdapter(ScopeTarget)

    sp = adapter.validate_python({"kind": "study_prep", "study_idx": 7, "prep_idx": 3})
    assert isinstance(sp, StudyPrepScopeTarget)
    assert sp.study_idx == 7
    assert sp.prep_idx == 3

    ref = adapter.validate_python({"kind": "reference", "reference_idx": 11})
    assert isinstance(ref, ReferenceScopeTarget)
    assert ref.reference_idx == 11

    ss = adapter.validate_python({"kind": "prep_sample", "prep_sample_idx": 23})
    assert isinstance(ss, PrepSampleScopeTarget)
    assert ss.prep_sample_idx == 23

    pool = adapter.validate_python(
        {"kind": "sequenced_pool", "sequenced_pool_idx": 42, "sequencing_run_idx": 7},
    )
    assert isinstance(pool, SequencedPoolScopeTarget)
    assert pool.sequenced_pool_idx == 42
    assert pool.sequencing_run_idx == 7

    block = adapter.validate_python({"kind": "block", "block_idx": 99})
    assert isinstance(block, BlockScopeTarget)
    assert block.block_idx == 99


def test_block_scope_target_requires_positive_block_idx():
    """BlockScopeTarget carries a single block_idx (gt=0). A missing idx or a
    non-positive one must raise, mirroring the other single-idx arms."""
    from qiita_common.models import ScopeTarget

    adapter = TypeAdapter(ScopeTarget)

    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "block"})
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "block", "block_idx": 0})
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "block", "block_idx": -1})


def test_sequenced_pool_scope_target_requires_both_idxs():
    """SequencedPoolScopeTarget carries both pool and run idxs so the framework
    can flow both scalars to the prep step without an extra DB lookup."""
    from qiita_common.models import ScopeTarget

    adapter = TypeAdapter(ScopeTarget)

    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "sequenced_pool", "sequenced_pool_idx": 1})
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "sequenced_pool", "sequencing_run_idx": 1})
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {"kind": "sequenced_pool", "sequenced_pool_idx": 0, "sequencing_run_idx": 1},
        )


def test_scope_target_rejects_unknown_kind():
    """An unknown discriminator must raise ValidationError."""
    from qiita_common.models import ScopeTarget

    with pytest.raises(ValidationError):
        TypeAdapter(ScopeTarget).validate_python(
            {"kind": "bogus", "reference_idx": 1},
        )


def test_scope_target_rejects_cross_kind_fields():
    """A study_prep target must not carry reference_idx (extra field rejected
    by the discriminated arm) — and vice versa."""
    from qiita_common.models import ScopeTarget

    adapter = TypeAdapter(ScopeTarget)

    # study_prep arm has no `reference_idx` field; Pydantic permits extras by
    # default, so the assertion is on the missing required field — supplying
    # only reference_idx with kind=study_prep must fail.
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "study_prep", "reference_idx": 1})

    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "reference", "study_idx": 1, "prep_idx": 1})


def test_scope_target_rejects_non_positive_idx():
    """All idx fields are gt=0; zero or negative must be rejected."""
    from qiita_common.models import ScopeTarget

    adapter = TypeAdapter(ScopeTarget)

    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "reference", "reference_idx": 0})
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {"kind": "study_prep", "study_idx": 1, "prep_idx": -2},
        )
    with pytest.raises(ValidationError):
        adapter.validate_python(
            {"kind": "prep_sample", "prep_sample_idx": 0},
        )


def test_work_ticket_round_trips():
    """WorkTicket round-trips through model_dump for both scope-target arms."""
    from qiita_common.models import (
        ReferenceScopeTarget,
        StudyPrepScopeTarget,
        WorkTicket,
        WorkTicketState,
    )

    now = datetime.now(UTC)
    wt = WorkTicket(
        work_ticket_idx=42,
        action_id="reference-add",
        action_version="1.0.0",
        originator_principal_idx=5,
        scope_target=ReferenceScopeTarget(kind="reference", reference_idx=11),
        action_context={"source_uri": "s3://refs/gg2.fa"},
        state=WorkTicketState.PENDING,
        created_at=now,
        updated_at=now,
    )
    dumped = wt.model_dump()
    assert dumped["scope_target"]["kind"] == "reference"
    assert dumped["scope_target"]["reference_idx"] == 11
    assert dumped["action_context"] == {"source_uri": "s3://refs/gg2.fa"}
    assert dumped["state"] == "pending"

    wt2 = WorkTicket.model_validate(dumped)
    assert wt2 == wt

    wt_sp = WorkTicket(
        work_ticket_idx=43,
        action_id="deblur",
        action_version="1.1.0",
        originator_principal_idx=5,
        scope_target=StudyPrepScopeTarget(kind="study_prep", study_idx=2, prep_idx=9),
        state=WorkTicketState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    assert wt_sp.action_context == {}  # default factory
    assert wt_sp.scope_target.kind == "study_prep"


def test_work_ticket_rejects_blank_action_id_or_version():
    """action_id and action_version both have min_length=1."""
    from qiita_common.models import (
        ReferenceScopeTarget,
        WorkTicket,
        WorkTicketState,
    )

    now = datetime.now(UTC)
    common = dict(
        work_ticket_idx=1,
        originator_principal_idx=1,
        scope_target=ReferenceScopeTarget(kind="reference", reference_idx=1),
        state=WorkTicketState.PENDING,
        created_at=now,
        updated_at=now,
    )
    with pytest.raises(ValidationError):
        WorkTicket(action_id="", action_version="1.0", **common)
    with pytest.raises(ValidationError):
        WorkTicket(action_id="x", action_version="", **common)


def test_work_ticket_rejects_non_positive_originator():
    """originator_principal_idx is gt=0."""
    from qiita_common.models import (
        ReferenceScopeTarget,
        WorkTicket,
        WorkTicketState,
    )

    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        WorkTicket(
            work_ticket_idx=1,
            action_id="x",
            action_version="1",
            originator_principal_idx=0,
            scope_target=ReferenceScopeTarget(kind="reference", reference_idx=1),
            state=WorkTicketState.PENDING,
            created_at=now,
            updated_at=now,
        )


def test_terminal_and_non_terminal_partition_the_enum():
    """The two sets are exact complements over WorkTicketState.

    The invariant every consumer leans on: a state in neither set is invisible to
    every gate, poll loop, and rollup that reasons about terminal-ness.
    """
    from qiita_common.models import (
        NON_TERMINAL_WORK_TICKET_STATES,
        TERMINAL_WORK_TICKET_STATES,
        WorkTicketState,
    )

    terminal = set(TERMINAL_WORK_TICKET_STATES)
    non_terminal = set(NON_TERMINAL_WORK_TICKET_STATES)
    assert terminal.isdisjoint(non_terminal)
    assert terminal | non_terminal == {s.value for s in WorkTicketState}


def test_terminal_set_carries_all_terminal_states():
    """NO_DATA is terminal (an empty-well outcome, not pending); CANCELLED is
    terminal (an operator stop). Both are outcomes, not in-flight states."""
    from qiita_common.models import TERMINAL_WORK_TICKET_STATES

    assert TERMINAL_WORK_TICKET_STATES == ("completed", "no_data", "failed", "cancelled")


def test_non_terminal_states_are_in_lifecycle_order():
    """Derived from the enum's declaration order, so a caller can render it."""
    from qiita_common.models import NON_TERMINAL_WORK_TICKET_STATES

    assert NON_TERMINAL_WORK_TICKET_STATES == ("pending", "queued", "processing")


def test_membership_works_for_plain_strings():
    """asyncpg and JSON both hand the state back as a plain str, not the enum."""
    from qiita_common.models import (
        NON_TERMINAL_WORK_TICKET_STATES,
        TERMINAL_WORK_TICKET_STATES,
    )

    assert "no_data" in TERMINAL_WORK_TICKET_STATES
    assert "processing" not in TERMINAL_WORK_TICKET_STATES
    assert "processing" in NON_TERMINAL_WORK_TICKET_STATES
    assert "no_data" not in NON_TERMINAL_WORK_TICKET_STATES
