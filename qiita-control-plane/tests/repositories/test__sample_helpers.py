"""Tests for the cross-entity helpers in repositories._sample_helpers.

Covers parse_text_for_data_type (pure-unit), write_global_metadata_or_diagnose
and write_local_metadata_or_diagnose (DB-bound): happy paths for each
supported data_type, every collision sub-case from both write functions
(five for the global path, three for the local path), the strict-mode
LocalWriteOnGloballyLinkedFieldError guard, StudyFieldConflictError and
non-target UniqueViolation pass-through, transaction rollback of a
freshly-created study_field on collision, and a prep_sample-spec sanity
test that proves PREP_SAMPLE_METADATA_SPEC's identifiers and callables
are correctly bound.

Biosample tests use the ctx fixture (Pattern 2: committed rows + FK-reverse
cleanup) so the diagnostic SELECT sees the prior writer's committed row.
The prep_sample sanity test uses Pattern 1 (per-test transaction rollback)
because the prep_sample side has no committed-fixture pattern yet.
parse_text_for_data_type tests are pure-unit and need no fixture.

Known coverage gap: the collision sub-cases are driven from a single
thread against an already-committed occupant row; the savepoint /
concurrent-delete race (the slot occupant vanishing between the savepoint
rollback and the diagnostic SELECT, yielding TransientMetadataWriteRaceError)
is not exercised under genuinely concurrent writers.
"""

import secrets
from datetime import date
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    LocalConflictingValueError,
    LocalDuplicateValueError,
    LocalSlotOccupiedByMissingReasonError,
    LocalWriteOnGloballyLinkedFieldError,
    MetadataParseError,
    SampleEntityKind,
    SlotOccupiedByMissingReasonError,
    StudyFieldConflictError,
    TransientMetadataWriteRaceError,
    _fetch_global_field_slot_occupant,
    _fetch_local_slot_occupant,
    parse_text_for_data_type,
    write_global_metadata_or_diagnose,
    write_local_metadata_or_diagnose,
)
from qiita_control_plane.repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
)
from qiita_control_plane.repositories.prep_sample_metadata import (
    PREP_SAMPLE_METADATA_SPEC,
)
from qiita_control_plane.testing.db_seeds import seed_biosample_global_field

from .conftest import (
    _create_biosample_with_link,
    _seed_study,
    _unique_field_name,
)

pytestmark = pytest.mark.db


# ---------------------------------------------------------------------------
# Helpers used across multiple tests in this module
# ---------------------------------------------------------------------------


async def _seed_global(ctx, data_type: FieldDataType, label: str = "gf") -> int:
    """Create a biosample_global_field of the given data_type and track it
    for cleanup. Used by every test in this module that needs a global
    field to target.
    """
    # Token suffix defends against unique-name collisions across re-runs.
    suffix = secrets.token_hex(4)
    gf_idx = await seed_biosample_global_field(
        ctx["pool"],
        internal_name=f"{label}_{suffix}",
        display_name=f"{label} {suffix}",
        data_type=data_type,
        created_by_idx=ctx["principal_idx"],
    )
    ctx["created"]["biosample_global_field"].append(gf_idx)
    return gf_idx


async def _commit_write(
    ctx,
    *,
    bs_idx: int,
    study_idx: int,
    gf_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value,
    caller_idx: int | None = None,
):
    """Run write_global_metadata_or_diagnose inside its own committed
    transaction, track the resulting rows for cleanup, and return the
    SampleMetadataWriteResult.
    """
    caller_idx = caller_idx if caller_idx is not None else ctx["principal_idx"]
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await write_global_metadata_or_diagnose(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=bs_idx,
                study_idx=study_idx,
                global_field_idx=gf_idx,
                display_name=display_name,
                data_type=data_type,
                value=value,
                caller_idx=caller_idx,
            )
    ctx["created"]["biosample_study_field"].append(result.study_field_idx)
    ctx["created"]["biosample_metadata"].append(result.metadata_idx)
    return result


async def _create_second_study_and_link_biosample(ctx, bs_idx):
    """Seed a second study and link the biosample to it (active link).
    Used by the two different-study collision tests.
    """
    # Second study owned by the ctx principal so role-typed FK triggers pass.
    second_study_idx = await _seed_study(
        ctx["pool"], ctx["principal_idx"], f"second-{secrets.token_hex(4)}"
    )
    ctx["created"]["studies"].append(second_study_idx)
    async with ctx["pool"].acquire() as conn:
        await conn.execute(
            "INSERT INTO qiita.biosample_to_study"
            " (biosample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
            bs_idx,
            second_study_idx,
            ctx["principal_idx"],
        )
    ctx["created"]["biosample_to_study"].append((bs_idx, second_study_idx))
    return second_study_idx


async def _fetch_metadata_row(pool, metadata_idx):
    """Return the full biosample_metadata row as a dict, for full-row asserts."""
    row = await pool.fetchrow(
        "SELECT biosample_idx, biosample_study_field_idx, global_field_idx,"
        " value_text, value_numeric, value_boolean, value_date,"
        " value_terminology_term_idx, value_missing_reason_idx,"
        " is_owner_biosample_id, created_by_idx"
        " FROM qiita.biosample_metadata WHERE idx = $1",
        metadata_idx,
    )
    return dict(row)


def _expected_metadata_row(
    *,
    bs_idx,
    study_field_idx,
    gf_idx,
    data_type: FieldDataType,
    value,
    caller_idx,
):
    """Build the expected biosample_metadata row dict for a value-bearing
    INSERT through the orchestrator. The value lands in the column named
    by GLOBAL_METADATA_VALUE_COLUMN[data_type]; the other typed columns
    are NULL, and missing-reason / terminology / is_owner_biosample_id
    are also NULL/false because the orchestrator does not set them.
    """
    # Start with everything cleared, then fill the one value column.
    row = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": study_field_idx,
        "global_field_idx": gf_idx,
        "value_text": None,
        "value_numeric": None,
        "value_boolean": None,
        "value_date": None,
        "value_terminology_term_idx": None,
        "value_missing_reason_idx": None,
        "is_owner_biosample_id": False,
        "created_by_idx": caller_idx,
    }
    column_for_type = {
        FieldDataType.TEXT: "value_text",
        FieldDataType.NUMERIC: "value_numeric",
        FieldDataType.DATE: "value_date",
    }
    row[column_for_type[data_type]] = value
    return row


# ---------------------------------------------------------------------------
# Happy path: each supported data_type lands the row and returns the result
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_text_returns_result_and_persists(ctx):
    """First global-linked write of a text value: get-or-create makes the
    study field, the value lands in value_text, and the result flags the
    study field as newly created.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "text_happy")
    display_name = _unique_field_name("happy_text")

    # Single call, fresh display_name -> get-or-create creates the study_field.
    result = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="hello",
    )

    # Result names the new study_field as created and the persisted row
    # carries the typed value plus the denormalized global_field_idx.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=gf_idx,
        data_type=FieldDataType.TEXT,
        value="hello",
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


async def test_write_global_metadata_or_diagnose_numeric_returns_result_and_persists(ctx):
    """Global-linked NUMERIC happy path: the Decimal lands in
    value_numeric and the sibling value_* columns stay NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.NUMERIC, "num_happy")
    display_name = _unique_field_name("happy_num")

    # Single call, fresh display_name -> get-or-create creates the study_field.
    result = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("3.14"),
    )

    # Result names the new study_field as created and the persisted row
    # carries the typed Decimal value with siblings NULL.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=gf_idx,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("3.14"),
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


async def test_write_global_metadata_or_diagnose_date_returns_result_and_persists(ctx):
    """Global-linked DATE happy path: the date lands in value_date and
    the sibling value_* columns stay NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.DATE, "date_happy")
    display_name = _unique_field_name("happy_date")

    # Single call, fresh display_name -> get-or-create creates the study_field.
    result = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name,
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 15),
    )

    # Result names the new study_field as created and the persisted row
    # carries the typed date with siblings NULL.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=gf_idx,
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 15),
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


# ---------------------------------------------------------------------------
# Collision: same-study sub-cases (caller is the contributing study)
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_raises_duplicate_value_same_study(ctx):
    """One biosample + global field holds only one value across all
    studies. The same study re-writing that field with the same value through
    a second field name is rejected by the cross-study unique index and
    classified as a same-study duplicate (idempotent re-confirm; nothing
    written).
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "dup_same_study")

    # Two display_names in the SAME study, both bound to the same global
    # field. The second write goes through a different study_field
    # (different display_name) so unique_per_field does not fire first;
    # only the cross-study partial unique index can reject the second
    # INSERT.
    display_name_first = _unique_field_name("dup_same_study_a")
    display_name_second = _unique_field_name("dup_same_study_b")

    # First write commits the (biosample, global field) slot under ctx's study.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_first,
        data_type=FieldDataType.TEXT,
        value="v1",
    )

    # Second write under the SAME study and SAME global field, but via
    # a DIFFERENT display_name (different study_field). Same study + same
    # value -> DuplicateValueSameStudyError.
    with pytest.raises(DuplicateValueSameStudyError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    global_field_idx=gf_idx,
                    display_name=display_name_second,
                    data_type=FieldDataType.TEXT,
                    value="v1",
                    caller_idx=ctx["principal_idx"],
                )

    # Payload names the existing slot occupant and the contributing study.
    exc = excinfo.value
    assert exc.entity_kind == SampleEntityKind.BIOSAMPLE
    assert exc.entity_idx == bs_idx
    assert exc.global_field_idx == gf_idx
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value == "v1"
    assert exc.attempted_value == "v1"
    assert exc.contributing_study_idx == ctx["study_idx"]
    assert exc.attempted_study_idx == ctx["study_idx"]


async def test_write_global_metadata_or_diagnose_raises_conflicting_value_same_study(ctx):
    """As the same-study duplicate case, but the same study writes a
    different value through a second field name: classified as a
    same-study conflict (an INSERT was asked for where a row exists;
    correction needs PATCH or DELETE+INSERT).
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "conf_same_study")

    # Two display_names in the SAME study, both bound to the same global
    # field. Different display_name on the second write means a
    # different study_field, so unique_per_field does not fire first;
    # the cross-study partial unique index rejects the second INSERT.
    display_name_first = _unique_field_name("conf_same_study_a")
    display_name_second = _unique_field_name("conf_same_study_b")

    # First write commits the slot with value "v1".
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_first,
        data_type=FieldDataType.TEXT,
        value="v1",
    )

    # Second write under the SAME study and SAME global field with a
    # DIFFERENT value -> ConflictingValueSameStudyError.
    with pytest.raises(ConflictingValueSameStudyError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    global_field_idx=gf_idx,
                    display_name=display_name_second,
                    data_type=FieldDataType.TEXT,
                    value="v2",
                    caller_idx=ctx["principal_idx"],
                )

    # Payload names existing v1 and the attempted v2.
    exc = excinfo.value
    assert exc.existing_value == "v1"
    assert exc.attempted_value == "v2"
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.contributing_study_idx == ctx["study_idx"]


# ---------------------------------------------------------------------------
# Collision: different-study sub-cases (caller's study != contributing study)
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_raises_duplicate_value_cross_study(ctx):
    """A second study writes the same value for the same biosample +
    global field; classified as a duplicate contributed by another
    study — the desired state exists but this study does not own the row.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    second_study_idx = await _create_second_study_and_link_biosample(ctx, bs_idx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "dup_diff_study")
    display_name_a = _unique_field_name("dup_diff_study_a")
    display_name_b = _unique_field_name("dup_diff_study_b")

    # First study writes the slot; ctx["study_idx"] is the contributing study.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_a,
        data_type=FieldDataType.TEXT,
        value="vX",
    )

    # Second study attempts to write the SAME value through ITS OWN field;
    # collides on the partial unique index -> DuplicateValueDifferentStudyError.
    with pytest.raises(DuplicateValueDifferentStudyError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=second_study_idx,
                    global_field_idx=gf_idx,
                    display_name=display_name_b,
                    data_type=FieldDataType.TEXT,
                    value="vX",
                    caller_idx=ctx["principal_idx"],
                )

    # Payload distinguishes the contributing study from the caller's study.
    exc = excinfo.value
    assert exc.existing_value == "vX"
    assert exc.attempted_value == "vX"
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.contributing_study_idx == ctx["study_idx"]
    assert exc.attempted_study_idx == second_study_idx


async def test_write_global_metadata_or_diagnose_raises_conflicting_value_cross_study(ctx):
    """A second study writes a different value for the same biosample +
    global field; the genuine cross-study conflict — the field's
    canonical value is in dispute.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    second_study_idx = await _create_second_study_and_link_biosample(ctx, bs_idx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "conf_diff_study")
    display_name_a = _unique_field_name("conf_diff_study_a")
    display_name_b = _unique_field_name("conf_diff_study_b")

    # First study writes the slot with vX.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_a,
        data_type=FieldDataType.TEXT,
        value="vX",
    )

    # Second study attempts to write a DIFFERENT value -> the real cross-
    # study conflict.
    with pytest.raises(ConflictingValueDifferentStudyError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=second_study_idx,
                    global_field_idx=gf_idx,
                    display_name=display_name_b,
                    data_type=FieldDataType.TEXT,
                    value="vY",
                    caller_idx=ctx["principal_idx"],
                )

    # Payload names the existing vX, the attempted vY, and both studies.
    exc = excinfo.value
    assert exc.existing_value == "vX"
    assert exc.attempted_value == "vY"
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.contributing_study_idx == ctx["study_idx"]
    assert exc.attempted_study_idx == second_study_idx


# ---------------------------------------------------------------------------
# Collision: slot held by missing-reason row
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_slot_held_by_missing_reason_raises(ctx):
    """The slot is already held by a row that records the value as
    intentionally missing (value_missing_reason_idx set); a later write
    is rejected with no typed value to compare against.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "missing_reason")

    # Two display_names in the same study, both bound to the same global
    # field. The pre-seeded missing-reason row goes through the first
    # study_field; the write_global_metadata_or_diagnose call goes through
    # the second so unique_per_field does not fire first.
    display_name_first = _unique_field_name("missing_reason_a")
    display_name_second = _unique_field_name("missing_reason_b")

    # Pre-seed a globally-linked study_field so the missing-reason metadata
    # row has a study_field to attach to.
    study_field_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, biosample_global_field_idx, display_name, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ctx["study_idx"],
        gf_idx,
        display_name_first,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_study_field"].append(study_field_idx)

    # Seed a missing-value reason so the metadata row has something to
    # reference.
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    # Seed the missing-reason metadata row directly. The trigger denormalizes
    # global_field_idx onto the row from the study_field; this occupies the
    # partial unique slot.
    seeded_meta_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_metadata"
        "  (biosample_idx, biosample_study_field_idx,"
        "   value_missing_reason_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        bs_idx,
        study_field_idx,
        reason_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_metadata"].append(seeded_meta_idx)

    # write_global_metadata_or_diagnose now sees the slot occupied via the
    # second display_name's path; the diagnostic SELECT sees
    # value_missing_reason_idx populated and raises
    # SlotOccupiedByMissingReasonError.
    with pytest.raises(SlotOccupiedByMissingReasonError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    global_field_idx=gf_idx,
                    display_name=display_name_second,
                    data_type=FieldDataType.TEXT,
                    value="anything",
                    caller_idx=ctx["principal_idx"],
                )

    # Payload carries the missing-reason idx and no typed existing_value.
    exc = excinfo.value
    assert exc.existing_metadata_idx == seeded_meta_idx
    assert exc.existing_value is None
    assert exc.existing_missing_reason_idx == reason_idx


# ---------------------------------------------------------------------------
# Pass-through: StudyFieldConflictError and non-target UniqueViolationError
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_propagates_study_field_conflict_error(ctx):
    """When the field name already exists in the study but is bound to a
    different global field, get-or-create raises StudyFieldConflictError
    and the write passes it through unclassified.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_a = await _seed_global(ctx, FieldDataType.TEXT, "sfconf_a")
    gf_b = await _seed_global(ctx, FieldDataType.TEXT, "sfconf_b")
    display_name = _unique_field_name("sfconf")

    # Pre-create a study_field at this (study, display_name) bound to gf_a.
    study_field_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, biosample_global_field_idx, display_name, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ctx["study_idx"],
        gf_a,
        display_name,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_study_field"].append(study_field_idx)

    # write_global_metadata_or_diagnose for gf_b at the same display_name
    # surfaces StudyFieldConflictError from get_or_create unchanged.
    with pytest.raises(StudyFieldConflictError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    global_field_idx=gf_b,
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="anything",
                    caller_idx=ctx["principal_idx"],
                )
    assert excinfo.value.expected_global_field_idx == gf_b
    assert excinfo.value.found_global_field_idx == gf_a


async def test_write_global_metadata_or_diagnose_propagates_non_target_unique_violation(ctx):
    """The "non-target" case: a UniqueViolation on a constraint other
    than the cross-study global-field slot index this function diagnoses
    (here the per-field uniqueness constraint, hit by re-writing through
    the same field name). It is re-raised verbatim, not classified into a
    typed subclass.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "non_target")
    display_name = _unique_field_name("non_target")

    # First write commits a (biosample, study_field) row.
    await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="v1",
    )

    # Second write via the same study_field collides on
    # biosample_metadata_unique_per_field. That constraint is NOT the spec's
    # global_field_unique_index_name, so the orchestrator re-raises the
    # original UniqueViolationError without classifying.
    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    global_field_idx=gf_idx,
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="v2",
                    caller_idx=ctx["principal_idx"],
                )

    # The constraint name is NOT the global-field partial index, and the
    # exception is NOT one of our typed subclasses.
    assert excinfo.value.constraint_name != (BIOSAMPLE_METADATA_SPEC.global_field_unique_index_name)
    assert not isinstance(excinfo.value, DuplicateValueSameStudyError)
    assert not isinstance(excinfo.value, ConflictingValueSameStudyError)


# ---------------------------------------------------------------------------
# Rollback: a freshly-created study_field rolls back with the failed write
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_rolls_back_new_study_field_on_collision(ctx):
    """When the colliding write goes through a brand-new field that
    get-or-create just created, the typed exception propagating out of
    the caller's transaction also rolls that new study_field row back —
    it must not survive.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    second_study_idx = await _create_second_study_and_link_biosample(ctx, bs_idx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "rollback")

    # Seed: first study writes the slot. ctx["study_idx"] is contributing.
    display_name_a = _unique_field_name("rollback_a")
    await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_a,
        data_type=FieldDataType.TEXT,
        value="vX",
    )

    # Second study attempts to write with a NEW display_name. The get-or-create
    # would create a new study_field in the second study; the INSERT then
    # collides; the typed exception propagates; the transaction rolls back;
    # the freshly-created study_field must NOT survive.
    display_name_b = _unique_field_name("rollback_b")
    with pytest.raises(ConflictingValueDifferentStudyError):
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=second_study_idx,
                    global_field_idx=gf_idx,
                    display_name=display_name_b,
                    data_type=FieldDataType.TEXT,
                    value="vY",
                    caller_idx=ctx["principal_idx"],
                )

    # No study_field exists at (second_study_idx, display_name_b) — the
    # outer transaction rolled the get-or-create's INSERT back.
    row = await ctx["pool"].fetchrow(
        "SELECT idx FROM qiita.biosample_study_field WHERE study_idx = $1 AND display_name = $2",
        second_study_idx,
        display_name_b,
    )
    assert row is None


# ---------------------------------------------------------------------------
# prep_sample spec sanity (Pattern 1: transaction-rollback)
# ---------------------------------------------------------------------------


async def test_write_global_metadata_or_diagnose_prep_sample_spec(postgres_pool):
    """Drive write_global_metadata_or_diagnose against PREP_SAMPLE_METADATA_SPEC
    to confirm its identifiers and callables are correctly bound: one
    happy-path write, then one same-study collision diagnoses against the
    prep_sample tables (proving the diagnostic SELECT's interpolated
    spec.metadata_table / study_field_table / study_field_idx_column /
    entity_key_column resolve to the prep_sample side).
    """
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            # Minimal user-kind principal + study + biosample + biosample_to_study
            # link, all inline. Pattern 1: nothing commits.
            token = secrets.token_hex(4)
            principal_idx = await conn.fetchval(
                "INSERT INTO qiita.principal (display_name, created_by_idx)"
                " VALUES ($1, $2) RETURNING idx",
                f"ps-{token}",
                SYSTEM_PRINCIPAL_IDX,
            )
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                principal_idx,
                f"ps-{token}@test.local",
            )
            study_idx = await conn.fetchval(
                "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
                " VALUES ($1, $2, $1) RETURNING idx",
                principal_idx,
                f"ps-{token}",
            )
            biosample_idx = await conn.fetchval(
                "INSERT INTO qiita.biosample (owner_idx, created_by_idx)"
                " VALUES ($1, $1) RETURNING idx",
                principal_idx,
            )
            await conn.execute(
                "INSERT INTO qiita.biosample_to_study"
                " (biosample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
                biosample_idx,
                study_idx,
                principal_idx,
            )

            # Resolve the seeded prep_protocol and create a sequenced prep_sample
            # + its per-study link. The biosample_to_study link above satisfies
            # the prep_sample_to_study_reject_without_biosample_link trigger.
            protocol_idx = await conn.fetchval(
                "SELECT idx FROM qiita.prep_protocol WHERE name = 'short_read_metagenomics'",
            )
            prep_sample_idx = await conn.fetchval(
                "INSERT INTO qiita.prep_sample"
                "  (biosample_idx, owner_idx, prep_protocol_idx,"
                "   processing_kind, created_by_idx)"
                " VALUES ($1, $2, $3, 'sequenced'::qiita.processing_kind, $2)"
                " RETURNING idx",
                biosample_idx,
                principal_idx,
                protocol_idx,
            )
            await conn.execute(
                "INSERT INTO qiita.prep_sample_to_study"
                " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
                prep_sample_idx,
                study_idx,
                principal_idx,
            )

            # Seed a TEXT-typed prep_sample_global_field for the write to target.
            gf_idx = await conn.fetchval(
                "INSERT INTO qiita.prep_sample_global_field"
                "  (internal_name, display_name, data_type, created_by_idx)"
                " VALUES ($1, $2, 'text', $3) RETURNING idx",
                f"gf_{token}",
                f"GF {token}",
                principal_idx,
            )

            # Happy path: PREP_SAMPLE_METADATA_SPEC drives the write via the
            # first display_name.
            display_name_first = f"PS Field A {token}"
            display_name_second = f"PS Field B {token}"
            result = await write_global_metadata_or_diagnose(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                entity_idx=prep_sample_idx,
                study_idx=study_idx,
                global_field_idx=gf_idx,
                display_name=display_name_first,
                data_type=FieldDataType.TEXT,
                value="prep_v1",
                caller_idx=principal_idx,
            )
            assert result.study_field_created is True

            # Second write under the SAME study and SAME global field via
            # a DIFFERENT display_name (different study_field) so the
            # cross-study partial unique index fires instead of
            # unique_per_field. Same value -> DuplicateValueSameStudyError.
            # Proves the diagnostic SELECT's interpolated identifiers
            # (prep_sample_metadata table, prep_sample_idx key column,
            # prep_sample_study_field table, prep_sample_study_field_idx
            # column) resolve to the prep_sample side.
            with pytest.raises(DuplicateValueSameStudyError) as excinfo:
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=PREP_SAMPLE_METADATA_SPEC,
                    entity_idx=prep_sample_idx,
                    study_idx=study_idx,
                    global_field_idx=gf_idx,
                    display_name=display_name_second,
                    data_type=FieldDataType.TEXT,
                    value="prep_v1",
                    caller_idx=principal_idx,
                )
            assert excinfo.value.entity_kind == SampleEntityKind.PREP_SAMPLE
            assert excinfo.value.entity_idx == prep_sample_idx
            assert excinfo.value.existing_value == "prep_v1"
            assert excinfo.value.contributing_study_idx == study_idx
        finally:
            # Pattern 1: never commit; transaction rolls back at finally.
            await tr.rollback()


# ---------------------------------------------------------------------------
# write_local_metadata_or_diagnose: helper + happy path
# ---------------------------------------------------------------------------


async def _commit_local_write(
    ctx,
    *,
    bs_idx: int,
    study_idx: int,
    display_name: str,
    data_type: FieldDataType,
    value,
    caller_idx: int | None = None,
    required: bool = False,
):
    """Run write_local_metadata_or_diagnose inside its own committed
    transaction, track the resulting rows for cleanup, and return the
    SampleMetadataWriteResult.
    """
    caller_idx = caller_idx if caller_idx is not None else ctx["principal_idx"]
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            result = await write_local_metadata_or_diagnose(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=bs_idx,
                study_idx=study_idx,
                display_name=display_name,
                data_type=data_type,
                value=value,
                caller_idx=caller_idx,
                required=required,
            )
    ctx["created"]["biosample_study_field"].append(result.study_field_idx)
    ctx["created"]["biosample_metadata"].append(result.metadata_idx)
    return result


async def test_write_local_metadata_or_diagnose_text_returns_result_and_persists(ctx):
    """First local (study-private) text write: get-or-create makes a
    local study field, the value lands in value_text, and
    global_field_idx stays NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_text_happy")

    # Single call, fresh display_name -> get-or-create creates a purely-
    # local study_field; INSERT writes value_text and leaves
    # global_field_idx NULL.
    result = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="hello",
    )

    # Result names the new study_field as created and the persisted row
    # carries the typed value with global_field_idx NULL.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=None,
        data_type=FieldDataType.TEXT,
        value="hello",
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


async def test_write_local_metadata_or_diagnose_numeric_returns_result_and_persists(ctx):
    """Local NUMERIC happy path: the Decimal lands in value_numeric and
    global_field_idx stays NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_num_happy")

    # Single call writes a Decimal value into the value_numeric column.
    result = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("3.14"),
    )

    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=None,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("3.14"),
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


async def test_write_local_metadata_or_diagnose_date_returns_result_and_persists(ctx):
    """Local DATE happy path: the date lands in value_date and
    global_field_idx stays NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_date_happy")

    # Single call writes a date value into the value_date column.
    result = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 15),
    )

    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = _expected_metadata_row(
        bs_idx=bs_idx,
        study_field_idx=result.study_field_idx,
        gf_idx=None,
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 15),
        caller_idx=ctx["principal_idx"],
    )
    assert actual == expected


# ---------------------------------------------------------------------------
# write_local_metadata_or_diagnose: collision sub-cases
# ---------------------------------------------------------------------------


async def test_write_local_metadata_or_diagnose_raises_duplicate_value(ctx):
    """Re-writing the same value through the same local field hits the
    per-field uniqueness constraint; classified as a local duplicate.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_dup")

    # First write commits a value through the new local study_field.
    first = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="v1",
    )

    # Second write via the same display_name -> get-or-create returns the
    # same study_field; INSERT collides on unique_per_field; same value
    # -> LocalDuplicateValueError.
    with pytest.raises(LocalDuplicateValueError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="v1",
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.entity_kind == SampleEntityKind.BIOSAMPLE
    assert exc.entity_idx == bs_idx
    assert exc.study_idx == ctx["study_idx"]
    assert exc.study_field_idx == first.study_field_idx
    assert exc.display_name == display_name
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value == "v1"
    assert exc.attempted_value == "v1"


async def test_write_local_metadata_or_diagnose_raises_conflicting_value(ctx):
    """Re-writing a different value through the same local field;
    classified as a local conflict.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_conf")

    # First write commits value "v1".
    first = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="v1",
    )

    # Second write with a different value -> LocalConflictingValueError.
    with pytest.raises(LocalConflictingValueError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="v2",
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_value == "v1"
    assert exc.attempted_value == "v2"
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.study_field_idx == first.study_field_idx


async def test_write_local_metadata_or_diagnose_slot_held_by_missing_reason_raises(ctx):
    """The local slot is already held by a missing-reason row; a later
    write is rejected with no typed value to compare against.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    display_name = _unique_field_name("local_missing_reason")

    # Pre-seed a purely-local study_field directly (skipping the
    # orchestrator) and a missing-reason metadata row pointing at it.
    study_field_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, display_name, data_type, required, created_by_idx)"
        " VALUES ($1, $2, 'text', false, $3) RETURNING idx",
        ctx["study_idx"],
        display_name,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_study_field"].append(study_field_idx)

    # Seed a missing-value reason so the metadata row has something to
    # reference.
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    # Seed the missing-reason metadata row directly to occupy the slot.
    seeded_meta_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_metadata"
        "  (biosample_idx, biosample_study_field_idx,"
        "   value_missing_reason_idx, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        bs_idx,
        study_field_idx,
        reason_idx,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_metadata"].append(seeded_meta_idx)

    # write_local_metadata_or_diagnose collides on unique_per_field; the
    # diagnostic SELECT sees the missing-reason row -> raises
    # LocalSlotOccupiedByMissingReasonError.
    with pytest.raises(LocalSlotOccupiedByMissingReasonError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="anything",
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_metadata_idx == seeded_meta_idx
    assert exc.existing_value is None
    assert exc.existing_missing_reason_idx == reason_idx


# ---------------------------------------------------------------------------
# write_local_metadata_or_diagnose: strict-mode guard
# ---------------------------------------------------------------------------


async def test_write_local_metadata_or_diagnose_raises_on_globally_linked_field(ctx):
    """Strict-mode guard: a local-only write is refused before any
    INSERT when the resolved field turns out to be bound to a global
    field (writing through it would silently enter the cross-study slot).
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "strict_mode")
    display_name = _unique_field_name("strict_mode")

    # Pre-seed a study_field at this (study, display_name) that is bound
    # to a global field. write_local then resolves this row in its
    # get-or-create lookup branch.
    study_field_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.biosample_study_field"
        "  (study_idx, biosample_global_field_idx, display_name, created_by_idx)"
        " VALUES ($1, $2, $3, $4) RETURNING idx",
        ctx["study_idx"],
        gf_idx,
        display_name,
        ctx["principal_idx"],
    )
    ctx["created"]["biosample_study_field"].append(study_field_idx)

    # write_local sees the resolved row's biosample_global_field_idx is
    # non-None and refuses the write before any INSERT runs.
    with pytest.raises(LocalWriteOnGloballyLinkedFieldError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="anything",
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.entity_kind == SampleEntityKind.BIOSAMPLE
    assert exc.study_idx == ctx["study_idx"]
    assert exc.display_name == display_name
    assert exc.study_field_idx == study_field_idx
    assert exc.found_global_field_idx == gf_idx

    # No metadata row landed: the strict-mode raise fires before any INSERT.
    count = await ctx["pool"].fetchval(
        "SELECT count(*) FROM qiita.biosample_metadata"
        " WHERE biosample_idx = $1 AND biosample_study_field_idx = $2",
        bs_idx,
        study_field_idx,
    )
    assert count == 0


# ---------------------------------------------------------------------------
# write_local_metadata_or_diagnose: prep_sample spec sanity
# ---------------------------------------------------------------------------


async def test_write_local_metadata_or_diagnose_prep_sample_spec(postgres_pool):
    """Drive write_local_metadata_or_diagnose against PREP_SAMPLE_METADATA_SPEC
    to confirm the new spec fields and the prep_sample get-or-create-local
    are correctly bound: one happy-path write followed by one same-value
    collision diagnoses against the prep_sample tables.
    """
    async with postgres_pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            # Minimal user-kind principal + study + biosample + biosample_to_study
            # link, all inline. Pattern 1: nothing commits.
            token = secrets.token_hex(4)
            principal_idx = await conn.fetchval(
                "INSERT INTO qiita.principal (display_name, created_by_idx)"
                " VALUES ($1, $2) RETURNING idx",
                f"psl-{token}",
                SYSTEM_PRINCIPAL_IDX,
            )
            await conn.execute(
                "INSERT INTO qiita.user (principal_idx, email) VALUES ($1, $2)",
                principal_idx,
                f"psl-{token}@test.local",
            )
            study_idx = await conn.fetchval(
                "INSERT INTO qiita.study (owner_idx, title, created_by_idx)"
                " VALUES ($1, $2, $1) RETURNING idx",
                principal_idx,
                f"psl-{token}",
            )
            biosample_idx = await conn.fetchval(
                "INSERT INTO qiita.biosample (owner_idx, created_by_idx)"
                " VALUES ($1, $1) RETURNING idx",
                principal_idx,
            )
            await conn.execute(
                "INSERT INTO qiita.biosample_to_study"
                " (biosample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
                biosample_idx,
                study_idx,
                principal_idx,
            )

            # Resolve the seeded prep_protocol and create a sequenced prep_sample
            # + its per-study link.
            protocol_idx = await conn.fetchval(
                "SELECT idx FROM qiita.prep_protocol WHERE name = 'short_read_metagenomics'",
            )
            prep_sample_idx = await conn.fetchval(
                "INSERT INTO qiita.prep_sample"
                "  (biosample_idx, owner_idx, prep_protocol_idx,"
                "   processing_kind, created_by_idx)"
                " VALUES ($1, $2, $3, 'sequenced'::qiita.processing_kind, $2)"
                " RETURNING idx",
                biosample_idx,
                principal_idx,
                protocol_idx,
            )
            await conn.execute(
                "INSERT INTO qiita.prep_sample_to_study"
                " (prep_sample_idx, study_idx, created_by_idx) VALUES ($1, $2, $3)",
                prep_sample_idx,
                study_idx,
                principal_idx,
            )

            # Happy path: PREP_SAMPLE_METADATA_SPEC drives the local write.
            display_name = f"PSL Field {token}"
            result = await write_local_metadata_or_diagnose(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                entity_idx=prep_sample_idx,
                study_idx=study_idx,
                display_name=display_name,
                data_type=FieldDataType.TEXT,
                value="local_v1",
                caller_idx=principal_idx,
            )
            assert result.study_field_created is True

            # Second write via the same display_name with the same value
            # -> LocalDuplicateValueError via the prep_sample-spec
            # diagnostic. Proves the SELECT's interpolated identifiers
            # resolve to the prep_sample side.
            with pytest.raises(LocalDuplicateValueError) as excinfo:
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=PREP_SAMPLE_METADATA_SPEC,
                    entity_idx=prep_sample_idx,
                    study_idx=study_idx,
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value="local_v1",
                    caller_idx=principal_idx,
                )
            assert excinfo.value.entity_kind == SampleEntityKind.PREP_SAMPLE
            assert excinfo.value.entity_idx == prep_sample_idx
            assert excinfo.value.existing_value == "local_v1"
            assert excinfo.value.study_idx == study_idx
        finally:
            # Pattern 1: never commit; transaction rolls back at finally.
            await tr.rollback()


# ---------------------------------------------------------------------------
# parse_text_for_data_type (pure-unit; shared by biosample and prep_sample
# composer pre-flights)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "data_type, text_value, expected",
    [
        (FieldDataType.TEXT, "hello", "hello"),
        (FieldDataType.TEXT, "  spaced  ", "spaced"),
        (FieldDataType.NUMERIC, "3.14", Decimal("3.14")),
        (FieldDataType.NUMERIC, " 42 ", Decimal("42")),
        (FieldDataType.DATE, "2026-05-06", date(2026, 5, 6)),
        (FieldDataType.DATE, " 2026-01-01 ", date(2026, 1, 1)),
    ],
)
def test_parse_text_for_data_type_returns_expected(data_type, text_value, expected):
    """Valid text coerces to the typed Python value (str / Decimal /
    date) with surrounding whitespace stripped.
    """
    assert parse_text_for_data_type("field_x", data_type, text_value) == expected


@pytest.mark.parametrize(
    "data_type, text_value",
    [
        (FieldDataType.NUMERIC, "abc"),
        (FieldDataType.NUMERIC, ""),
        (FieldDataType.DATE, "not-a-date"),
        (FieldDataType.DATE, "2026/05/06"),
    ],
)
def test_parse_text_for_data_type_raises_parse_error(data_type, text_value):
    """Uncoercible text raises MetadataParseError carrying display_name,
    data_type, and the raw text for a field-scoped 422.
    """
    with pytest.raises(MetadataParseError) as excinfo:
        parse_text_for_data_type("field_x", data_type, text_value)
    assert excinfo.value.display_name == "field_x"
    assert excinfo.value.data_type == data_type
    assert excinfo.value.text_value == text_value


@pytest.mark.parametrize(
    "data_type",
    [FieldDataType.BOOLEAN, FieldDataType.TERMINOLOGY],
)
def test_parse_text_for_data_type_raises_not_implemented(data_type):
    """BOOLEAN and TERMINOLOGY are deliberately unsupported and raise
    NotImplementedError.
    """
    with pytest.raises(NotImplementedError):
        parse_text_for_data_type("field_x", data_type, "x")


# ---------------------------------------------------------------------------
# Diagnostic-fetch lost-race: occupant vanished before it could be inspected
# ---------------------------------------------------------------------------


async def test__fetch_global_field_slot_occupant_raises_transient_write_race(ctx):
    """A diagnostic SELECT that finds no occupant means the colliding row
    was concurrently deleted-and-committed between the savepoint rollback
    and this read; the helper raises TransientMetadataWriteRaceError (a
    benign retry signal), not RuntimeError (a schema-corruption claim).
    """
    # Sentinel idxs with no metadata row model the occupant having been
    # deleted-and-committed in the race window; the SELECT returns nothing.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(TransientMetadataWriteRaceError) as excinfo:
            await _fetch_global_field_slot_occupant(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=2_000_000_000,
                global_field_idx=2_000_000_000,
            )
    assert (excinfo.value.entity_kind, excinfo.value.entity_idx) == (
        SampleEntityKind.BIOSAMPLE,
        2_000_000_000,
    )


async def test__fetch_local_slot_occupant_raises_transient_write_race(ctx):
    """Local-path twin: a no-occupant diagnostic SELECT on the
    unique-per-field constraint path raises TransientMetadataWriteRaceError
    rather than RuntimeError, for the same concurrent-delete reason.
    """
    # Sentinel idxs with no metadata row; same race model as the global twin.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(TransientMetadataWriteRaceError) as excinfo:
            await _fetch_local_slot_occupant(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                entity_idx=2_000_000_000,
                study_field_idx=2_000_000_000,
            )
    assert (excinfo.value.entity_kind, excinfo.value.entity_idx) == (
        SampleEntityKind.PREP_SAMPLE,
        2_000_000_000,
    )
