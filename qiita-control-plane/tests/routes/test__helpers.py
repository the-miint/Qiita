"""Tests for cross-route helpers in routes/_helpers.py.

Covers detail_for_slot_collision against synthetic SlotOccupiedError
instances for each concrete subclass, parametrized over
SampleEntityKind so both biosample and prep_sample wording is
exercised. Each leaf reachable from both write paths is also exercised
with global_field_idx=None so the local-path wording is covered.
The missing-reason sub-case requires a real missing_value_reason row
because the helper resolves the reason name via DB lookup; all
sub-cases run against the same postgres_pool fixture for uniformity
even though only one branch touches the DB. Also covers
parse_kv_detail and detail_for_biosample_link_rejection — pure string
helpers that turn a trigger's structured error DETAIL into an
unambiguous 422 message.

No route integration tests live here: both metadata-write callers
(the biosample import route and the sequenced-sample create route)
always create a fresh entity per call, so neither slot constraint
fires through them today. The typed except clauses are mechanical
plumbing; the wording dispatch is the load-bearing surface and is
fully covered below.
"""

import secrets
from datetime import date
from decimal import Decimal

import pytest
import pytest_asyncio
from fastapi import HTTPException
from qiita_common.models import FieldDataType, MissingReasonRef

from qiita_control_plane.repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    SampleEntityKind,
    SlotOccupiedByMissingReasonError,
    SlotOccupiedByTypedValueError,
    TransientWriteRaceError,
)
from qiita_control_plane.routes._helpers import (
    detail_for_biosample_link_rejection,
    detail_for_slot_collision,
    parse_kv_detail,
    raise_for_transient_write_race,
)

pytestmark = pytest.mark.db


# Both entity kinds: the helper interpolates exc.entity_kind into the
# message so every sub-case must read correctly for either domain.
_ENTITY_KINDS = [SampleEntityKind.BIOSAMPLE, SampleEntityKind.PREP_SAMPLE]

_DISPLAY_NAME = "my_field"
_STUDY_FIELD_IDX = 5
_GLOBAL_FIELD_IDX = 7


def _make_exception_kwargs(
    *,
    entity_kind: SampleEntityKind,
    existing_value=None,
    existing_missing_reason_idx=None,
    attempted_value="attempted",
    global_field_idx: int | None = _GLOBAL_FIELD_IDX,
):
    """Build the kwargs an exception subclass __init__ takes. The four
    non-missing-reason subclasses share the same payload shape and only
    differ in attempted_value vs existing_value; the missing-reason
    subclass overrides both existing_value (None) and
    existing_missing_reason_idx (non-None). global_field_idx defaults
    to non-None (global-path); pass None to simulate a local-path raise.
    """
    # Same kwargs across all four typed-value subclasses; the test then
    # tweaks existing_value / attempted_value per sub-case.
    return {
        "entity_kind": entity_kind,
        "entity_idx": 42,
        "display_name": _DISPLAY_NAME,
        "study_field_idx": _STUDY_FIELD_IDX,
        "global_field_idx": global_field_idx,
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
    detail = await detail_for_slot_collision(conn, exc)

    # Wording identifies the no-action-taken sub-case, leads with the
    # caller-facing display_name, and embeds the entity_kind-qualified
    # idx and the global-path slot identifier.
    assert "already wrote this same value" in detail
    assert "no new row was created" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_idx=42" in detail
    assert f"global_field_idx={_GLOBAL_FIELD_IDX}" in detail


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
    detail = await detail_for_slot_collision(conn, exc)

    assert "previously wrote a different value" in detail
    assert "PATCH" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_idx=42" in detail
    assert f"global_field_idx={_GLOBAL_FIELD_IDX}" in detail


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
    detail = await detail_for_slot_collision(conn, exc)

    assert "already present" in detail
    assert "your study does not own the row" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    # Contributing study idx surfaces; per the security review note the
    # study name is intentionally not joined.
    assert "study_idx=11" in detail
    assert f"global_field_idx={_GLOBAL_FIELD_IDX}" in detail


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
    detail = await detail_for_slot_collision(conn, exc)

    assert "another study" in detail
    assert "canonical value is in dispute" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert "study_idx=11" in detail
    assert f"global_field_idx={_GLOBAL_FIELD_IDX}" in detail


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
            detail = await detail_for_slot_collision(conn, exc)

            # Wording identifies the missing-reason sub-case, leads
            # with the caller-facing display_name, embeds the resolved
            # reason name (not just the idx), and tells the caller
            # what action unblocks the write.
            assert "intentionally missing" in detail
            assert f"reason: {reason_name}" in detail
            assert "missing-reason row must be deleted" in detail
            assert f"field {_DISPLAY_NAME!r}" in detail
            assert f"{entity_kind}_idx=42" in detail
        finally:
            await tr.rollback()


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
@pytest.mark.parametrize(
    ("existing_value", "expected_in_detail"),
    [
        ("typed_value", "'typed_value'"),
        (Decimal("1.5"), "1.5"),
        (date(2024, 1, 2), "2024-01-02"),
    ],
)
async def test_detail_for_slot_occupied_by_typed_value(
    conn, entity_kind, existing_value, expected_in_detail
):
    """Tests the case where the slot holds a typed value and the caller
    attempted a missing-reason marker. Each typed value renders as its
    natural form (quoted-string, plain numeric, ISO date) — the Python
    type constructor must not leak.
    """
    exc = SlotOccupiedByTypedValueError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value=existing_value,
            existing_missing_reason_idx=None,
            attempted_value=MissingReasonRef(idx=42, name="ignored"),
        )
    )
    detail = await detail_for_slot_collision(conn, exc)

    # Wording identifies the typed-occupant sub-case, leads with the
    # caller-facing display_name, embeds the existing typed value, and
    # tells the caller the typed row must go first.
    assert "already recorded as a typed value" in detail
    assert expected_in_detail in detail
    # No Python type constructor leaks into the on-the-wire detail.
    assert "Decimal(" not in detail
    assert "datetime.date(" not in detail
    assert "typed row must be deleted" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_idx=42" in detail


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_duplicate_value_same_study_with_missing_reason(conn, entity_kind):
    """Tests the case where the attempted value is a MissingReasonRef and
    the slot already holds the same missing-reason: the existing
    DuplicateValueSameStudyError branch emits "missing-reason marker"
    wording instead of "value".
    """
    exc = DuplicateValueSameStudyError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value=None,
            existing_missing_reason_idx=11,
            attempted_value=MissingReasonRef(idx=11, name="ignored"),
        )
    )
    detail = await detail_for_slot_collision(conn, exc)

    # The wording variant reads naturally for the missing-kind path
    # and still leads with the caller-facing display_name.
    assert "already wrote this same missing-reason marker" in detail
    assert "no new row was created" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_idx=42" in detail


# ---------------------------------------------------------------------------
# Numeric / date data_types do not change the wording (the value embeds
# via the exception payload, not via the helper). One smoke test confirms
# the helper handles non-TEXT data_types without dispatching on them.
# ---------------------------------------------------------------------------


async def test_detail_handles_numeric_attempted_value(conn):
    exc = DuplicateValueSameStudyError(
        entity_kind=SampleEntityKind.BIOSAMPLE,
        entity_idx=42,
        display_name=_DISPLAY_NAME,
        study_field_idx=_STUDY_FIELD_IDX,
        global_field_idx=_GLOBAL_FIELD_IDX,
        attempted_study_idx=3,
        attempted_value=Decimal("3.14"),
        data_type=FieldDataType.NUMERIC,
        existing_metadata_idx=99,
        existing_value=Decimal("3.14"),
        existing_missing_reason_idx=None,
        contributing_study_idx=3,
    )
    detail = await detail_for_slot_collision(conn, exc)

    # Same wording regardless of data_type; values are not embedded in
    # the message (they are available to clients via the exception
    # payload but not the response detail string).
    assert "already wrote this same value" in detail


# ---------------------------------------------------------------------------
# Local-path coverage: global_field_idx=None selects the per-entity
# study_field_idx slot identifier instead of global_field_idx, and the
# message must not template the global-only identifier.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_duplicate_value_same_study_local_path(conn, entity_kind):
    """Tests the case where DuplicateValueSameStudyError fires from the
    local write path: the message references the per-entity
    study_field_idx instead of a global_field_idx.
    """
    exc = DuplicateValueSameStudyError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value="hello",
            attempted_value="hello",
            global_field_idx=None,
        )
    )
    detail = await detail_for_slot_collision(conn, exc)

    assert "already wrote this same value" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_idx=42" in detail
    assert f"{entity_kind}_study_field_idx={_STUDY_FIELD_IDX}" in detail
    assert "global_field_idx" not in detail


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_conflicting_value_same_study_local_path(conn, entity_kind):
    """Tests the case where ConflictingValueSameStudyError fires from
    the local write path: the message points the caller at PATCH and
    references the per-entity study_field_idx.
    """
    exc = ConflictingValueSameStudyError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value="old",
            attempted_value="new",
            global_field_idx=None,
        )
    )
    detail = await detail_for_slot_collision(conn, exc)

    assert "previously wrote a different value" in detail
    assert "PATCH" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_study_field_idx={_STUDY_FIELD_IDX}" in detail
    assert "global_field_idx" not in detail


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_slot_occupied_by_missing_reason_local_path(postgres_pool, entity_kind):
    """Tests the case where SlotOccupiedByMissingReasonError fires from
    the local write path: the resolved reason name and the
    per-entity study_field_idx appear; global_field_idx does not.
    """
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
                    global_field_idx=None,
                )
            )
            detail = await detail_for_slot_collision(conn, exc)

            assert "intentionally missing" in detail
            assert f"reason: {reason_name}" in detail
            assert "missing-reason row must be deleted" in detail
            assert f"field {_DISPLAY_NAME!r}" in detail
            assert f"{entity_kind}_study_field_idx={_STUDY_FIELD_IDX}" in detail
            assert "global_field_idx" not in detail
        finally:
            await tr.rollback()


@pytest.mark.parametrize("entity_kind", _ENTITY_KINDS)
async def test_detail_for_slot_occupied_by_typed_value_local_path(conn, entity_kind):
    """Tests the case where SlotOccupiedByTypedValueError fires from
    the local write path: the existing typed value and per-entity
    study_field_idx appear; global_field_idx does not.
    """
    exc = SlotOccupiedByTypedValueError(
        **_make_exception_kwargs(
            entity_kind=entity_kind,
            existing_value="typed_value",
            existing_missing_reason_idx=None,
            attempted_value=MissingReasonRef(idx=42, name="ignored"),
            global_field_idx=None,
        )
    )
    detail = await detail_for_slot_collision(conn, exc)

    assert "already recorded as a typed value" in detail
    assert "'typed_value'" in detail
    assert "typed row must be deleted" in detail
    assert f"field {_DISPLAY_NAME!r}" in detail
    assert f"{entity_kind}_study_field_idx={_STUDY_FIELD_IDX}" in detail
    assert "global_field_idx" not in detail


# ---------------------------------------------------------------------------
# raise_for_transient_write_race -> 503 + Retry-After
# ---------------------------------------------------------------------------


def test_raise_for_transient_write_race():
    """Tests the case where a lost write race maps to 503 with a
    Retry-After hint and a resubmit-the-request detail.
    """
    exc = TransientWriteRaceError(
        row_label="biosample_metadata",
        slot_summary="biosample_idx=42, global_field_idx=7",
    )
    with pytest.raises(HTTPException) as excinfo:
        raise_for_transient_write_race(exc)

    raised = excinfo.value
    assert (raised.status_code, raised.detail, raised.headers) == (
        503,
        (
            "a concurrent delete raced your biosample_metadata write"
            " (biosample_idx=42, global_field_idx=7); the slot is now"
            " free — resubmit the identical request"
        ),
        {"Retry-After": "1"},
    )


# ---------------------------------------------------------------------------
# parse_kv_detail -> structured key=value parsing of a Postgres DETAIL
# ---------------------------------------------------------------------------


def test_parse_kv_detail_splits_pairs():
    """Tests the case where comma-separated key=value pairs parse into a
    dict and surrounding whitespace around each pair is stripped."""
    parsed = parse_kv_detail(
        "trigger=prep_sample_to_study_reject_without_biosample_link, study_idx=7, biosample_idx=42"
    )
    assert parsed == {
        "trigger": "prep_sample_to_study_reject_without_biosample_link",
        "study_idx": "7",
        "biosample_idx": "42",
    }


def test_parse_kv_detail_skips_chunks_without_equals():
    """Tests the case where a chunk carrying no '=' is dropped (rather
    than producing a bogus key) and where a None or empty input yields
    an empty dict."""
    assert parse_kv_detail("study_idx=7, garbage, biosample_idx=42") == {
        "study_idx": "7",
        "biosample_idx": "42",
    }
    assert parse_kv_detail(None) == {}
    assert parse_kv_detail("") == {}


# ---------------------------------------------------------------------------
# detail_for_biosample_link_rejection -> 422 wording naming the failing study
# ---------------------------------------------------------------------------


def test_detail_for_biosample_link_rejection_names_the_study():
    """Tests the case where the 422 detail names the exact failing
    study_idx and biosample_idx from the parsed key=value dict."""
    detail = detail_for_biosample_link_rejection({"study_idx": "7", "biosample_idx": "42"})
    assert detail == (
        "prep_sample cannot be linked to study_idx=7:"
        " biosample_idx=42 is not linked to that study"
        " (or the link is retired)"
    )


def test_detail_for_biosample_link_rejection_degrades_without_detail():
    """Tests the case where the input dict carries neither study_idx nor
    biosample_idx: the helper substitutes '?' placeholders rather than
    raising."""
    detail = detail_for_biosample_link_rejection({})
    assert "study_idx=?" in detail
    assert "biosample_idx=?" in detail
