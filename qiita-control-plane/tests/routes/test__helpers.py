"""Tests for cross-route helpers in routes/_helpers.py.

Covers detail_for_global_field_collision against synthetic
GlobalFieldSlotOccupiedError instances for each of the five concrete
subclasses, parametrized over SampleEntityKind so both biosample and
prep_sample wording is exercised. The missing-reason sub-case requires
a real missing_value_reason row because the helper resolves the reason
name via DB lookup; all sub-cases run against the same postgres_pool
fixture for uniformity even though only one branch touches the DB.

No route integration tests live here: both
write_global_metadata_or_diagnose callers (the biosample import route
and the sequenced-sample create route) always create a fresh entity per
call, so the partial unique index never fires through them today. The
typed except clauses are mechanical plumbing; the wording dispatch is
the load-bearing surface and is fully covered below.
"""

import secrets
from decimal import Decimal

import pytest
import pytest_asyncio
from fastapi import HTTPException
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    SampleEntityKind,
    SlotOccupiedByMissingReasonError,
    TransientMetadataWriteRaceError,
)
from qiita_control_plane.routes._helpers import (
    detail_for_global_field_collision,
    raise_for_transient_write_race,
)

pytestmark = pytest.mark.db


# Both entity kinds: the helper interpolates exc.entity_kind into the
# message so every sub-case must read correctly for either domain.
_ENTITY_KINDS = [SampleEntityKind.BIOSAMPLE, SampleEntityKind.PREP_SAMPLE]


def _make_exception_kwargs(
    *,
    entity_kind: SampleEntityKind,
    existing_value=None,
    existing_missing_reason_idx=None,
    attempted_value="attempted",
):
    """Build the kwargs an exception subclass __init__ takes. The four
    non-missing-reason subclasses share the same payload shape and only
    differ in attempted_value vs existing_value; the missing-reason
    subclass overrides both existing_value (None) and
    existing_missing_reason_idx (non-None).
    """
    # Same kwargs across all four typed-value subclasses; the test then
    # tweaks existing_value / attempted_value per sub-case.
    return {
        "entity_kind": entity_kind,
        "entity_idx": 42,
        "global_field_idx": 7,
        "attempted_study_idx": 3,
        "attempted_value": attempted_value,
        "data_type": FieldDataType.TEXT,
        "existing_metadata_idx": 99,
        "existing_value": existing_value,
        "existing_missing_reason_idx": existing_missing_reason_idx,
        "contributing_study_idx": 3,
    }


@pytest_asyncio.fixture
async def conn(postgres_pool):
    """Single connection for one helper-test invocation. The missing-reason
    case runs a SELECT; the other four don't touch the DB but use the
    same fixture for uniformity.
    """
    async with postgres_pool.acquire() as conn:
        yield conn


# ---------------------------------------------------------------------------
# Sub-case: DuplicateValueSameStudyError -> idempotent same-study wording
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_duplicate_value_same_study(conn, entity_kind):
    # Same study + same value: helper reports the no-op confirm wording.
    exc = DuplicateValueSameStudyError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value="hello",
            attempted_value="hello",
        )
    )
    detail = await detail_for_global_field_collision(conn, exc)

    # Wording identifies the no-action-taken sub-case and embeds the
    # entity_kind-qualified idx and the global_field_idx.
    assert "already wrote this same value" in detail
    assert "no new row was created" in detail
    assert f"{entity_kind}_idx=42" in detail
    assert "global_field_idx=7" in detail


# ---------------------------------------------------------------------------
# Sub-case: ConflictingValueSameStudyError -> caller must PATCH/DELETE
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_conflicting_value_same_study(conn, entity_kind):
    # Same study + different value: helper directs the caller to PATCH
    # rather than retry INSERT.
    exc = ConflictingValueSameStudyError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value="old",
            attempted_value="new",
        )
    )
    detail = await detail_for_global_field_collision(conn, exc)

    assert "previously wrote a different value" in detail
    assert "PATCH" in detail
    assert f"{entity_kind}_idx=42" in detail


# ---------------------------------------------------------------------------
# Sub-case: DuplicateValueDifferentStudyError -> "you don't own the row"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_duplicate_value_different_study(conn, entity_kind):
    # Different study + same value: helper identifies the contributing
    # study and warns the caller they do not own the row.
    kwargs = _make_exception_kwargs(
        entity_kind=entity_kind,
        existing_value="shared",
        attempted_value="shared",
    )
    # Caller's study and contributing study diverge.
    kwargs["attempted_study_idx"] = 5
    kwargs["contributing_study_idx"] = 11
    exc = DuplicateValueDifferentStudyError(**kwargs)
    detail = await detail_for_global_field_collision(conn, exc)

    assert "already present" in detail
    assert "your study does not own the row" in detail
    # Contributing study idx surfaces; per the security review note the
    # study name is intentionally not joined.
    assert "study_idx=11" in detail


# ---------------------------------------------------------------------------
# Sub-case: ConflictingValueDifferentStudyError -> real cross-study conflict
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_conflicting_value_different_study(conn, entity_kind):
    # Different study + different value: the real cross-study conflict
    # wording, naming the contributing study.
    kwargs = _make_exception_kwargs(
        entity_kind=entity_kind,
        existing_value="theirs",
        attempted_value="mine",
    )
    kwargs["attempted_study_idx"] = 5
    kwargs["contributing_study_idx"] = 11
    exc = ConflictingValueDifferentStudyError(**kwargs)
    detail = await detail_for_global_field_collision(conn, exc)

    assert "another study" in detail
    assert "canonical value is in dispute" in detail
    assert "study_idx=11" in detail


# ---------------------------------------------------------------------------
# Sub-case: SlotOccupiedByMissingReasonError -> resolves the reason name
# ---------------------------------------------------------------------------


async def _seed_missing_reason(conn, name: str) -> int:
    """Insert a qiita.missing_value_reason row inside the caller's
    transaction (Pattern 1: caller rolls back after). Returns the idx.
    """
    return await conn.fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        name,
    )


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_slot_occupied_by_missing_reason(postgres_pool, entity_kind):
    # Missing-reason sub-case requires a real row so the helper's
    # name-resolution SELECT has something to return. Pattern 1
    # transaction rollback keeps the seeded row out of the committed DB.
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            reason_name = f"not_collected_{secrets.token_hex(4)}"
            reason_idx = await _seed_missing_reason(conn, reason_name)

            exc = SlotOccupiedByMissingReasonError(
                **_make_exception_kwargs(
                    entity_kind=entity_kind,
                    existing_value=None,
                    existing_missing_reason_idx=reason_idx,
                    attempted_value="anything",
                )
            )
            detail = await detail_for_global_field_collision(conn, exc)

            # Wording identifies the missing-reason sub-case, embeds the
            # resolved reason name (not just the idx), and tells the
            # caller what action unblocks the write.
            assert "intentionally missing" in detail
            assert f"reason: {reason_name}" in detail
            assert "missing-reason row must be deleted" in detail
            assert f"{entity_kind}_idx=42" in detail
        finally:
            await tr.rollback()


# ---------------------------------------------------------------------------
# Numeric / date data_types do not change the wording (the value embeds
# via the exception payload, not via the helper). One smoke test confirms
# the helper handles non-TEXT data_types without dispatching on them.
# ---------------------------------------------------------------------------


async def test_detail_handles_numeric_attempted_value(conn):
    exc = DuplicateValueSameStudyError(
        entity_kind=SampleEntityKind.BIOSAMPLE,
        entity_idx=42,
        global_field_idx=7,
        attempted_study_idx=3,
        attempted_value=Decimal("3.14"),
        data_type=FieldDataType.NUMERIC,
        existing_metadata_idx=99,
        existing_value=Decimal("3.14"),
        existing_missing_reason_idx=None,
        contributing_study_idx=3,
    )
    detail = await detail_for_global_field_collision(conn, exc)

    # Same wording regardless of data_type; values are not embedded in
    # the message (they are available to clients via the exception
    # payload but not the response detail string per Plan 2 decision 1).
    assert "already wrote this same value" in detail


# ---------------------------------------------------------------------------
# raise_for_transient_write_race -> 503 + Retry-After
# ---------------------------------------------------------------------------


def test_raise_for_transient_write_race():
    """A lost write race maps to 503 (transient) carrying a Retry-After
    hint and a resubmit-the-request detail — not 409 (the slot is not
    actually in conflict) and not 500. Slot-kind-agnostic: the helper
    surfaces whatever slot_summary the diagnostic read supplied.
    """
    exc = TransientMetadataWriteRaceError(
        entity_kind=SampleEntityKind.BIOSAMPLE,
        entity_idx=42,
        slot_summary="biosample_idx=42, global_field_idx=7",
    )
    with pytest.raises(HTTPException) as excinfo:
        raise_for_transient_write_race(exc)

    raised = excinfo.value
    assert (raised.status_code, raised.detail, raised.headers) == (
        503,
        (
            "a concurrent delete raced your metadata write"
            " (biosample_idx=42, global_field_idx=7); the slot is now"
            " free — resubmit the identical request"
        ),
        {"Retry-After": "1"},
    )
