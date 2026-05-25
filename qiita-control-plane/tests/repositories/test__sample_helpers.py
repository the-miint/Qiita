"""Tests for the cross-entity helpers in repositories._sample_helpers.

Covers parse_text_for_data_type (pure-unit), write_global_metadata_or_diagnose
and write_local_metadata_or_diagnose (DB-bound): happy paths for each
supported data_type, every collision sub-case from the global write
function (six SlotOccupiedError leaves) and the same-study + cross-
kind subset reachable from the local write function (four leaves; the
*DifferentStudy variants are unreachable on a single-study local row),
the strict-mode LocalWriteOnGloballyLinkedFieldError guard,
StudyFieldConflictError and non-target UniqueViolation pass-through,
transaction rollback of a freshly-created study_field on collision,
and a prep_sample-spec sanity test that proves
PREP_SAMPLE_METADATA_SPEC's identifiers and callables are correctly
bound.

Biosample tests use the ctx fixture (Pattern 2: committed rows + FK-reverse
cleanup) so the diagnostic SELECT sees the prior writer's committed row.
The prep_sample sanity test uses Pattern 1 (per-test transaction rollback)
because the prep_sample side has no committed-fixture pattern yet.
parse_text_for_data_type tests are pure-unit and need no fixture.

Known coverage gap: the collision sub-cases are driven from a single
thread against an already-committed occupant row; the savepoint /
concurrent-delete race (the slot occupant vanishing between the savepoint
rollback and the diagnostic SELECT, yielding TransientWriteRaceError)
is not exercised under genuinely concurrent writers.
"""

import secrets
from datetime import date
from decimal import Decimal

import asyncpg
import pytest
from qiita_common.auth_constants import SYSTEM_PRINCIPAL_IDX
from qiita_common.models import FieldDataType, MissingReasonRef

from qiita_control_plane.repositories._sample_helpers import (
    ConflictingValueDifferentStudyError,
    ConflictingValueSameStudyError,
    DuplicateValueDifferentStudyError,
    DuplicateValueSameStudyError,
    GlobalFieldRow,
    GlobalMetadataRow,
    LocalWriteOnGloballyLinkedFieldError,
    MetadataParseError,
    MetadataUnknownFieldsError,
    SampleEntityKind,
    SlotOccupiedByMissingReasonError,
    SlotOccupiedByTypedValueError,
    StudyFieldConflictError,
    TransientWriteRaceError,
    _fetch_slot_occupant,
    _get_or_create_globally_linked_study_field,
    _get_or_create_local_study_field,
    _insert_metadata,
    fetch_global_fields_by_display_names,
    fetch_global_metadata,
    fetch_missing_value_reason_idxs_by_names,
    insert_entity_to_study,
    link_entity_to_studies,
    parse_text_for_data_type,
    preflight_global_metadata,
    validate_primary_secondary_studies,
    write_global_metadata_entries,
    write_global_metadata_or_diagnose,
    write_local_metadata_or_diagnose,
)
from qiita_control_plane.repositories.biosample_metadata import (
    BIOSAMPLE_METADATA_SPEC,
    insert_owner_biosample_id_metadata,
)
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.testing.db_seeds import (
    retire_biosample_to_study_link,
    retire_prep_sample_to_study_link,
    seed_biosample_global_field,
    seed_prep_sample_global_field,
)

from .conftest import (
    _create_biosample_with_link,
    _create_local_field,
    _create_prep_sample_with_link,
    _seed_global_field_for_spec,
    _seed_secondary_studies_for_entity,
    _seed_study,
    _seed_unlinked_entity_for_spec,
    _track_to_study_link,
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


async def test_write_global_metadata_or_diagnose_missing_reason_persists(ctx):
    """Tests the case where the caller writes a MissingReasonRef against a
    typed global field: the row carries value_missing_reason_idx populated
    and every value_* typed column NULL, regardless of the field's
    data_type.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    # Use a NUMERIC field so any "leak" into a typed column would violate
    # the data-type contract; the missing-reason exemption is what allows
    # this row to land.
    gf_idx = await _seed_global(ctx, FieldDataType.NUMERIC, "missing_happy")
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")
    display_name = _unique_field_name("missing_global")

    result = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
        value=MissingReasonRef(idx=reason_idx, name="any_name"),
    )

    # value_missing_reason_idx populated; every typed value column NULL.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": result.study_field_idx,
        "global_field_idx": gf_idx,
        "value_text": None,
        "value_numeric": None,
        "value_boolean": None,
        "value_date": None,
        "value_terminology_term_idx": None,
        "value_missing_reason_idx": reason_idx,
        "is_owner_biosample_id": False,
        "created_by_idx": ctx["principal_idx"],
    }
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


async def test_write_global_metadata_or_diagnose_missing_reason_dup_same_study(ctx):
    """Tests the case where the slot already holds a missing-reason row
    and the caller re-attempts the same missing-reason from the same
    study: the write raises DuplicateValueSameStudyError with the existing
    missing-reason idx reported as the existing value.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.NUMERIC, "miss_dup_same")
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")
    display_name_first = _unique_field_name("miss_dup_same_a")
    display_name_second = _unique_field_name("miss_dup_same_b")

    # First write seeds the missing-reason row through the first display_name.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_first,
        data_type=FieldDataType.NUMERIC,
        value=MissingReasonRef(idx=reason_idx, name="ignored"),
    )

    # Second write under the same study via a different display_name with
    # the same missing-reason -> DuplicateValueSameStudyError.
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
                    data_type=FieldDataType.NUMERIC,
                    value=MissingReasonRef(idx=reason_idx, name="ignored"),
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value is None
    assert exc.existing_missing_reason_idx == reason_idx
    assert exc.contributing_study_idx == ctx["study_idx"]


async def test_write_global_metadata_or_diagnose_missing_reason_conflict_diff_study(ctx):
    """Tests the case where the slot holds a missing-reason from one study
    and a different study attempts a different missing-reason: the write
    raises ConflictingValueDifferentStudyError with the original study
    reported as the contributing study.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.NUMERIC, "miss_conf_x")
    reason_a_idx = await _seed_missing_value_reason(ctx, f"reason_a_{secrets.token_hex(4)}")
    reason_b_idx = await _seed_missing_value_reason(ctx, f"reason_b_{secrets.token_hex(4)}")
    display_name_first = _unique_field_name("miss_conf_a")
    display_name_second = _unique_field_name("miss_conf_b")

    # First write through the original study seeds missing-reason A.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_first,
        data_type=FieldDataType.NUMERIC,
        value=MissingReasonRef(idx=reason_a_idx, name="ignored_a"),
    )

    # Link biosample to a second study so the cross-study path is reachable.
    second_study_idx = await _create_second_study_and_link_biosample(ctx, bs_idx)

    # Second write from the second study attempts a different
    # missing-reason -> ConflictingValueDifferentStudyError.
    with pytest.raises(ConflictingValueDifferentStudyError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_global_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=second_study_idx,
                    global_field_idx=gf_idx,
                    display_name=display_name_second,
                    data_type=FieldDataType.NUMERIC,
                    value=MissingReasonRef(idx=reason_b_idx, name="ignored_b"),
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value is None
    assert exc.existing_missing_reason_idx == reason_a_idx
    assert exc.contributing_study_idx == ctx["study_idx"]


async def test_write_global_metadata_or_diagnose_raises_slot_occupied_by_typed_value(ctx):
    """Tests the case where the slot holds a typed value and the caller
    attempts to record a missing-reason marker: the write raises
    SlotOccupiedByTypedValueError carrying the typed existing value.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    gf_idx = await _seed_global(ctx, FieldDataType.TEXT, "typed_then_missing")
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")
    display_name_first = _unique_field_name("typed_first")
    display_name_second = _unique_field_name("missing_second")

    # First write seeds a typed value through the first display_name.
    first = await _commit_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        gf_idx=gf_idx,
        display_name=display_name_first,
        data_type=FieldDataType.TEXT,
        value="typed_value",
    )

    # Second write through a different display_name attempts a
    # missing-reason against the typed-occupant slot ->
    # SlotOccupiedByTypedValueError.
    with pytest.raises(SlotOccupiedByTypedValueError) as excinfo:
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
                    value=MissingReasonRef(idx=reason_idx, name="ignored"),
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value == "typed_value"
    assert exc.existing_missing_reason_idx is None


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


async def test_write_local_metadata_or_diagnose_missing_reason_persists(ctx):
    """Tests the case where the caller writes a MissingReasonRef through a
    purely-local study field: value_missing_reason_idx is populated and
    every value_* typed column stays NULL.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")
    display_name = _unique_field_name("local_missing")

    result = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value=MissingReasonRef(idx=reason_idx, name="any_name"),
    )

    # Full-row assert: missing-reason column populated; global_field_idx
    # and every typed value column NULL.
    assert result.study_field_created is True
    actual = await _fetch_metadata_row(ctx["pool"], result.metadata_idx)
    expected = {
        "biosample_idx": bs_idx,
        "biosample_study_field_idx": result.study_field_idx,
        "global_field_idx": None,
        "value_text": None,
        "value_numeric": None,
        "value_boolean": None,
        "value_date": None,
        "value_terminology_term_idx": None,
        "value_missing_reason_idx": reason_idx,
        "is_owner_biosample_id": False,
        "created_by_idx": ctx["principal_idx"],
    }
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
    # -> DuplicateValueSameStudyError (single-study local row -> the
    # contributing study is trivially the caller's).
    with pytest.raises(DuplicateValueSameStudyError) as excinfo:
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
    assert exc.attempted_study_idx == ctx["study_idx"]
    assert exc.contributing_study_idx == ctx["study_idx"]
    assert exc.study_field_idx == first.study_field_idx
    assert exc.display_name == display_name
    assert exc.global_field_idx is None
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

    # Second write with a different value -> ConflictingValueSameStudyError.
    with pytest.raises(ConflictingValueSameStudyError) as excinfo:
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
    # SlotOccupiedByMissingReasonError.
    with pytest.raises(SlotOccupiedByMissingReasonError) as excinfo:
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


async def test_write_local_metadata_or_diagnose_raises_slot_occupied_by_typed_value(ctx):
    """Tests the case where the local slot holds a typed value and the
    caller attempts a missing-reason write: the write raises
    SlotOccupiedByTypedValueError carrying the typed existing value.
    """
    bs_idx = await _create_biosample_with_link(ctx)
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")
    display_name = _unique_field_name("local_typed_first")

    # First write commits a typed value through the new local study_field.
    first = await _commit_local_write(
        ctx,
        bs_idx=bs_idx,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        value="typed_value",
    )

    # Second write via the same display_name attempts a missing-reason
    # against the typed-occupant slot -> SlotOccupiedByTypedValueError.
    with pytest.raises(SlotOccupiedByTypedValueError) as excinfo:
        async with ctx["pool"].acquire() as conn:
            async with conn.transaction():
                await write_local_metadata_or_diagnose(
                    conn,
                    spec=BIOSAMPLE_METADATA_SPEC,
                    entity_idx=bs_idx,
                    study_idx=ctx["study_idx"],
                    display_name=display_name,
                    data_type=FieldDataType.TEXT,
                    value=MissingReasonRef(idx=reason_idx, name="ignored"),
                    caller_idx=ctx["principal_idx"],
                )

    exc = excinfo.value
    assert exc.existing_metadata_idx == first.metadata_idx
    assert exc.existing_value == "typed_value"
    assert exc.existing_missing_reason_idx is None


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
            # -> DuplicateValueSameStudyError via the prep_sample-spec
            # diagnostic. Proves the SELECT's interpolated identifiers
            # resolve to the prep_sample side.
            with pytest.raises(DuplicateValueSameStudyError) as excinfo:
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
            assert excinfo.value.attempted_study_idx == study_idx
            assert excinfo.value.contributing_study_idx == study_idx
            assert excinfo.value.global_field_idx is None
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


async def test__fetch_slot_occupant_global_path_raises_transient_write_race(ctx):
    """A diagnostic SELECT that finds no occupant means the colliding row
    was concurrently deleted-and-committed between the savepoint rollback
    and this read; the helper raises TransientWriteRaceError (a benign
    retry signal), not RuntimeError (a schema-corruption claim). The
    global-path discriminator (global_field_idx kwarg) selects the
    global_field_idx WHERE filter and slot_summary label.
    """
    # Sentinel idxs with no metadata row model the occupant having been
    # deleted-and-committed in the race window; the SELECT returns nothing.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(TransientWriteRaceError) as excinfo:
            await _fetch_slot_occupant(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=2_000_000_000,
                global_field_idx=2_000_000_000,
            )
    assert (excinfo.value.row_label, excinfo.value.slot_summary) == (
        "biosample_metadata",
        "biosample_idx=2000000000, global_field_idx=2000000000",
    )


async def test__fetch_slot_occupant_local_path_raises_transient_write_race(ctx):
    """Local-path twin: a no-occupant diagnostic SELECT on the
    unique-per-field constraint path raises TransientWriteRaceError
    rather than RuntimeError, for the same concurrent-delete reason.
    The local-path discriminator (study_field_idx kwarg) selects the
    spec.study_field_idx_column WHERE filter and slot_summary label.
    """
    # Sentinel idxs with no metadata row; same race model as the global twin.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(TransientWriteRaceError) as excinfo:
            await _fetch_slot_occupant(
                conn,
                spec=PREP_SAMPLE_METADATA_SPEC,
                entity_idx=2_000_000_000,
                study_field_idx=2_000_000_000,
            )
    assert (excinfo.value.row_label, excinfo.value.slot_summary) == (
        "prep_sample_metadata",
        "prep_sample_idx=2000000000, prep_sample_study_field_idx=2000000000",
    )


async def test__fetch_slot_occupant_rejects_both_or_neither_idx(ctx):
    """Tests the case where the caller passes both or neither of the
    two idx kwargs: the XOR guard rejects the call with ValueError
    before any DB roundtrip.
    """
    async with ctx["pool"].acquire() as conn:
        # Both kwargs passed -> caller bug, no DB roundtrip.
        with pytest.raises(ValueError, match="exactly one of"):
            await _fetch_slot_occupant(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=1,
                global_field_idx=1,
                study_field_idx=1,
            )
        # Neither kwarg passed -> caller bug, no DB roundtrip.
        with pytest.raises(ValueError, match="exactly one of"):
            await _fetch_slot_occupant(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=1,
            )


# ---------------------------------------------------------------------------
# fetch_global_fields_by_display_names (spec-parameterized over both entities)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec, seed_global_field, created_key",
    [
        (BIOSAMPLE_METADATA_SPEC, seed_biosample_global_field, "biosample_global_field"),
        (PREP_SAMPLE_METADATA_SPEC, seed_prep_sample_global_field, "prep_sample_global_field"),
    ],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_fields_by_display_names_returns_matching(
    ctx, spec, seed_global_field, created_key
):
    # Seed two global fields with collision-resistant names.
    suffix = secrets.token_hex(4)
    name_a = f"Test Field A {suffix}"
    name_b = f"Test Field B {suffix}"
    idx_a = await seed_global_field(
        ctx["pool"],
        internal_name=f"tfa_{suffix}",
        display_name=name_a,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    idx_b = await seed_global_field(
        ctx["pool"],
        internal_name=f"tfb_{suffix}",
        display_name=name_b,
        data_type=FieldDataType.NUMERIC,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"][created_key].extend([idx_a, idx_b])

    # Fetch both names; verify the dict carries both rows with correct fields.
    async with ctx["pool"].acquire() as conn:
        result = await fetch_global_fields_by_display_names(
            conn, spec=spec, display_names=[name_a, name_b]
        )

    expected = {
        name_a: GlobalFieldRow(idx=idx_a, display_name=name_a, data_type=FieldDataType.TEXT),
        name_b: GlobalFieldRow(idx=idx_b, display_name=name_b, data_type=FieldDataType.NUMERIC),
    }
    assert result == expected


@pytest.mark.parametrize(
    "spec, seed_global_field, created_key",
    [
        (BIOSAMPLE_METADATA_SPEC, seed_biosample_global_field, "biosample_global_field"),
        (PREP_SAMPLE_METADATA_SPEC, seed_prep_sample_global_field, "prep_sample_global_field"),
    ],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_fields_by_display_names_omits_unknown(
    ctx, spec, seed_global_field, created_key
):
    # Seed one global field; ask for it plus a name that does not exist.
    suffix = secrets.token_hex(4)
    known_name = f"Known Field {suffix}"
    unknown_name = f"Unknown Field {suffix}"
    idx = await seed_global_field(
        ctx["pool"],
        internal_name=f"kf_{suffix}",
        display_name=known_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"][created_key].append(idx)

    async with ctx["pool"].acquire() as conn:
        result = await fetch_global_fields_by_display_names(
            conn, spec=spec, display_names=[known_name, unknown_name]
        )

    # Only the known name appears; unknown is silently absent.
    expected = {
        known_name: GlobalFieldRow(idx=idx, display_name=known_name, data_type=FieldDataType.TEXT),
    }
    assert result == expected


# ---------------------------------------------------------------------------
# fetch_missing_value_reason_idxs_by_names
# ---------------------------------------------------------------------------


async def _seed_missing_value_reason(ctx, name: str) -> int:
    """Insert one qiita.missing_value_reason row, track for cleanup, return idx."""
    idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        name,
    )
    ctx["created"]["missing_value_reason"].append(idx)
    return idx


async def test_fetch_missing_value_reason_idxs_by_names_returns_idxs(ctx):
    """Tests the case where every requested name has a matching row: the
    returned dict carries name -> idx for each one.
    """
    suffix = secrets.token_hex(4)
    name_a = f"mv_a_{suffix}"
    name_b = f"mv_b_{suffix}"
    idx_a = await _seed_missing_value_reason(ctx, name_a)
    idx_b = await _seed_missing_value_reason(ctx, name_b)

    async with ctx["pool"].acquire() as conn:
        result = await fetch_missing_value_reason_idxs_by_names(conn, [name_a, name_b])

    assert result == {name_a: idx_a, name_b: idx_b}


async def test_fetch_missing_value_reason_idxs_by_names_empty_input(ctx):
    """Tests the case where the names iterable is empty: returns an empty
    dict.
    """
    async with ctx["pool"].acquire() as conn:
        result = await fetch_missing_value_reason_idxs_by_names(conn, [])

    assert result == {}


async def test_fetch_missing_value_reason_idxs_by_names_no_matches(ctx):
    """Tests the case where no requested name has a matching row: returns
    an empty dict.
    """
    suffix = secrets.token_hex(4)
    async with ctx["pool"].acquire() as conn:
        result = await fetch_missing_value_reason_idxs_by_names(
            conn, [f"no_such_reason_{suffix}_x", f"no_such_reason_{suffix}_y"]
        )

    assert result == {}


# ---------------------------------------------------------------------------
# _get_or_create_globally_linked_study_field (parametrized over both specs)
# ---------------------------------------------------------------------------


async def _seed_global_field(
    ctx,
    *,
    spec,
    internal_name: str,
    display_name: str,
    data_type: FieldDataType,
) -> int:
    """Seed a *_global_field row for the entity named by spec, track for
    cleanup, return its idx. Branches on spec.entity_kind because the two
    seed helpers take entity-specific table identifiers internally.
    """
    seeder = (
        seed_biosample_global_field
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else seed_prep_sample_global_field
    )
    idx = await seeder(
        ctx["pool"],
        internal_name=internal_name,
        display_name=display_name,
        data_type=data_type,
        created_by_idx=SYSTEM_PRINCIPAL_IDX,
    )
    ctx["created"][f"{spec.entity_kind}_global_field"].append(idx)
    return idx


async def _seed_local_study_field(ctx, *, spec, display_name: str, required: bool = False) -> int:
    """Seed a purely-local *_study_field row via the shared upsert, track
    for cleanup.
    """
    async with ctx["pool"].acquire() as conn, conn.transaction():
        idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            required=required,
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(idx)
    return idx


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_globally_linked_study_field_creates_new_row(ctx, spec):
    # Seed a global field the new study_field will be linked to.
    suffix = secrets.token_hex(4)
    display_name = f"Linked Field {suffix}"
    global_idx = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=f"link_{suffix}",
        display_name=display_name,
        data_type=FieldDataType.NUMERIC,
    )

    # Upsert a globally-linked study field at the same display_name.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        idx, created = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(idx)

    # First call inserts the row; created flag must report True.
    assert created is True

    # Verify the inheritance-CHECK-compliant row shape: global link populated,
    # data_type / required / terminology_idx / tier_override all NULL.
    row = await ctx["pool"].fetchrow(
        f"SELECT study_idx, {spec.study_field_global_fk_column},"
        f" display_name, description, data_type, required,"
        f" terminology_idx, tier_override, created_by_idx"
        f" FROM {spec.study_field_table} WHERE idx = $1",
        idx,
    )
    expected = {
        "study_idx": ctx["study_idx"],
        spec.study_field_global_fk_column: global_idx,
        "display_name": display_name,
        "description": None,
        "data_type": None,
        "required": None,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_globally_linked_study_field_returns_existing(ctx, spec):
    # Seed a global field and call the upsert twice with the same args.
    suffix = secrets.token_hex(4)
    display_name = f"Linked Field {suffix}"
    global_idx = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=f"link_{suffix}",
        display_name=display_name,
        data_type=FieldDataType.TEXT,
    )

    async with ctx["pool"].acquire() as conn, conn.transaction():
        first_idx, first_created = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
        second_idx, second_created = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(first_idx)

    # First call inserts; second call resolves via the fallback SELECT branch.
    assert first_created is True
    assert second_created is False
    assert first_idx == second_idx


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_globally_linked_study_field_raises_on_local_collision(ctx, spec):
    # Pre-seed a purely-local row at (study_idx, display_name) via the
    # per-entity local helper. The upsert must detect the existing row is
    # purely-local (FK column IS NULL) and raise.
    suffix = secrets.token_hex(4)
    display_name = f"Collision Field {suffix}"
    await _seed_local_study_field(ctx, spec=spec, display_name=display_name, required=True)

    # Seed a global field the caller wants to link to.
    global_idx = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=f"col_{suffix}",
        display_name=f"Global {suffix}",
        data_type=FieldDataType.TEXT,
    )

    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(StudyFieldConflictError) as excinfo:
            await _get_or_create_globally_linked_study_field(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                global_field_idx=global_idx,
                display_name=display_name,
                created_by_idx=ctx["principal_idx"],
            )
    assert excinfo.value.entity_kind == spec.entity_kind
    assert excinfo.value.found_global_field_idx is None
    assert excinfo.value.expected_global_field_idx == global_idx
    assert excinfo.value.display_name == display_name


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_globally_linked_study_field_raises_on_global_mismatch(ctx, spec):
    # Two distinct global fields; pre-seed a study field bound to the first.
    suffix = secrets.token_hex(4)
    global_a = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=f"ma_{suffix}",
        display_name=f"Global A {suffix}",
        data_type=FieldDataType.TEXT,
    )
    global_b = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=f"mb_{suffix}",
        display_name=f"Global B {suffix}",
        data_type=FieldDataType.TEXT,
    )

    # Shared study-local display_name pointing at global_a.
    display_name = f"Field {suffix}"
    async with ctx["pool"].acquire() as conn, conn.transaction():
        existing_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_a,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(existing_idx)

    # Asking for the same display_name with global_b must raise; the row
    # already binds to global_a.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(StudyFieldConflictError) as excinfo:
            await _get_or_create_globally_linked_study_field(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                global_field_idx=global_b,
                display_name=display_name,
                created_by_idx=ctx["principal_idx"],
            )
    assert excinfo.value.entity_kind == spec.entity_kind
    assert excinfo.value.found_global_field_idx == global_a
    assert excinfo.value.expected_global_field_idx == global_b


# ---------------------------------------------------------------------------
# _get_or_create_local_study_field (parametrized over both specs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_local_study_field_creates_purely_local(ctx, spec):
    field_name = _unique_field_name()

    # Create a new local field with required=True (composer's intended use).
    async with ctx["pool"].acquire() as conn, conn.transaction():
        idx, created, resolved_global_field_idx = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
            required=True,
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(idx)

    # First call inserts the row; created flag reports True and the resolved
    # global_field_idx is None because the create branch always produces a
    # purely-local row.
    assert created is True
    assert resolved_global_field_idx is None

    # Verify the row reflects the local-field defaults plus the explicit required.
    row = await ctx["pool"].fetchrow(
        f"SELECT study_idx, {spec.study_field_global_fk_column},"
        f" display_name, description, data_type, required,"
        f" terminology_idx, tier_override, created_by_idx"
        f" FROM {spec.study_field_table} WHERE idx = $1",
        idx,
    )
    expected = {
        "study_idx": ctx["study_idx"],
        spec.study_field_global_fk_column: None,
        "display_name": field_name,
        "description": None,
        "data_type": "text",
        "required": True,
        "terminology_idx": None,
        "tier_override": None,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__get_or_create_local_study_field_returns_existing(ctx, spec):
    field_name = _unique_field_name()

    # First call inserts; second call with the same (study_idx, display_name)
    # must return the same idx without inserting a new row.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        (
            first_idx,
            first_created,
            first_global_field_idx,
        ) = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
        (
            second_idx,
            second_created,
            second_global_field_idx,
        ) = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=field_name,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(first_idx)

    # First call inserts (created=True); second call resolves via the
    # fallback SELECT branch (created=False) and converges on the same idx.
    # Both calls resolve to a purely-local row, so the global_field_idx
    # element is None in both.
    assert first_created is True
    assert second_created is False
    assert first_idx == second_idx
    assert first_global_field_idx is None
    assert second_global_field_idx is None

    # Confirm the DB only has one row for this (study, display_name).
    count = await ctx["pool"].fetchval(
        f"SELECT count(*) FROM {spec.study_field_table} WHERE study_idx = $1 AND display_name = $2",
        ctx["study_idx"],
        field_name,
    )
    assert count == 1


# ---------------------------------------------------------------------------
# _insert_metadata (parametrized over both specs and all data_types)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
@pytest.mark.parametrize(
    "data_type, value, value_column",
    [
        (FieldDataType.TEXT, "TYPED-VAL", "value_text"),
        (FieldDataType.NUMERIC, Decimal("3.14"), "value_numeric"),
        (FieldDataType.DATE, date(2026, 5, 6), "value_date"),
    ],
    ids=["text", "numeric", "date"],
)
async def test__insert_metadata_writes_typed_value(ctx, spec, data_type, value, value_column):
    """Happy path: one insert per (spec, data_type); the row lands in
    exactly the matching value_* column and the others stay NULL.
    """
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )

    # Local field of the matching data_type so the field-contract trigger
    # accepts the value column choice.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name(value_column),
            created_by_idx=ctx["principal_idx"],
            data_type=data_type,
            required=True,
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(field_idx)

    # Insert via the shared typed inserter.
    async with ctx["pool"].acquire() as conn:
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=data_type,
            value=value,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_metadata"].append(meta_idx)

    # Full-row assert: the matching value_* column carries the value, the
    # other two typed columns are NULL. The spec drives both table name
    # and entity key column so one assertion shape covers both arms.
    row = await ctx["pool"].fetchrow(
        f"SELECT {spec.entity_key_column}, {spec.study_field_idx_column},"
        f" value_text, value_numeric, value_date, created_by_idx"
        f" FROM {spec.metadata_table} WHERE idx = $1",
        meta_idx,
    )
    expected = {
        spec.entity_key_column: entity_idx,
        spec.study_field_idx_column: field_idx,
        "value_text": None,
        "value_numeric": None,
        "value_date": None,
        "created_by_idx": ctx["principal_idx"],
    }
    expected[value_column] = value
    assert dict(row) == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__insert_metadata_writes_missing_reason_value(ctx, spec):
    """Tests the case where the value is a MissingReasonRef: the row lands
    with value_missing_reason_idx populated and every typed value column
    NULL, regardless of the field's data_type.
    """
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )
    reason_idx = await _seed_missing_value_reason(ctx, f"reason_{secrets.token_hex(4)}")

    # Local NUMERIC field so a typed-column write would violate the
    # data-type contract; the missing-reason exemption is what lets the
    # row land.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=_unique_field_name("missing_insert"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.NUMERIC,
            required=True,
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(field_idx)

    async with ctx["pool"].acquire() as conn:
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.NUMERIC,
            value=MissingReasonRef(idx=reason_idx, name="ignored_in_insert"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_metadata"].append(meta_idx)

    # Full-row assert: missing-reason column carries the FK; every typed
    # value column is NULL.
    row = await ctx["pool"].fetchrow(
        f"SELECT {spec.entity_key_column}, {spec.study_field_idx_column},"
        f" value_text, value_numeric, value_date, value_missing_reason_idx,"
        f" created_by_idx"
        f" FROM {spec.metadata_table} WHERE idx = $1",
        meta_idx,
    )
    expected = {
        spec.entity_key_column: entity_idx,
        spec.study_field_idx_column: field_idx,
        "value_text": None,
        "value_numeric": None,
        "value_date": None,
        "value_missing_reason_idx": reason_idx,
        "created_by_idx": ctx["principal_idx"],
    }
    assert dict(row) == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test__insert_metadata_unsupported_data_type_raises(ctx, spec):
    """Closed-set guard: BOOLEAN and TERMINOLOGY are not yet decodable
    via GLOBAL_METADATA_VALUE_COLUMN, so the shared inserter raises
    NotImplementedError rather than silently writing NULL into every
    value column. Exercised without touching the DB because the guard
    fires before any INSERT.
    """
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(NotImplementedError):
            await _insert_metadata(
                conn,
                spec=spec,
                entity_idx=1,
                study_field_idx=1,
                data_type=FieldDataType.BOOLEAN,
                value="ignored",
                created_by_idx=ctx["principal_idx"],
            )


# ---------------------------------------------------------------------------
# fetch_global_metadata (parametrized over both specs)
# ---------------------------------------------------------------------------


async def _seed_globally_linked_metadata(
    ctx,
    *,
    spec,
    entity_idx: int,
    internal_name: str,
    display_name: str,
    description: str | None,
    data_type: FieldDataType,
    value,
):
    """Test helper: seed a *_global_field for the entity named by spec,
    link a study field to it, write one metadata row of the matching
    typed-column flavor via the shared inserter, and track the
    *_global_field / *_study_field / *_metadata idxs for fixture cleanup.
    Returns the global field idx for callers that want to inspect or
    extend the row.
    """
    # Seed the global field; *_global_field rows persist beyond the test
    # so the helper tracks them on the cleanup dict.
    global_idx = await _seed_global_field(
        ctx,
        spec=spec,
        internal_name=internal_name,
        display_name=display_name,
        data_type=data_type,
    )
    # The seed helper omits description; set it via UPDATE when needed so
    # the seed helper surface stays small. spec.global_field_table is a
    # frozen constant, never caller-reached, so identifier interpolation
    # is safe.
    if description is not None:
        await ctx["pool"].execute(
            f"UPDATE {spec.global_field_table} SET description = $2 WHERE idx = $1",
            global_idx,
            description,
        )

    # Link a per-study field to the global field via the shared upsert,
    # then write the typed value via the shared inserter; the field-
    # contract trigger rejects mismatches so the typed-column choice
    # must agree with data_type.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=data_type,
            value=value,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][f"{spec.entity_kind}_study_field"].append(field_idx)
    ctx["created"][f"{spec.entity_kind}_metadata"].append(meta_idx)
    return global_idx


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_metadata_text_numeric_date(ctx, spec):
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )
    suffix = secrets.token_hex(4)

    # Three globally-linked rows, one per supported data_type, on the same
    # entity. Names are tagged with a suffix to dodge UNIQUE collisions.
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"host_subject_id_{suffix}",
        display_name=f"Host Subject ID {suffix}",
        description="Host's stable identifier",
        data_type=FieldDataType.TEXT,
        value="HOST-7",
    )
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"latitude_{suffix}",
        display_name=f"Latitude {suffix}",
        description=None,
        data_type=FieldDataType.NUMERIC,
        value=Decimal("32.7"),
    )
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"collection_date_{suffix}",
        display_name=f"Collection Date {suffix}",
        description="Date the sample was collected",
        data_type=FieldDataType.DATE,
        value=date(2026, 5, 6),
    )

    result = await fetch_global_metadata(ctx["pool"], spec=spec, entity_idx=entity_idx)

    expected = {
        f"host_subject_id_{suffix}": GlobalMetadataRow(
            internal_name=f"host_subject_id_{suffix}",
            display_name=f"Host Subject ID {suffix}",
            description="Host's stable identifier",
            data_type=FieldDataType.TEXT,
            value="HOST-7",
        ),
        f"latitude_{suffix}": GlobalMetadataRow(
            internal_name=f"latitude_{suffix}",
            display_name=f"Latitude {suffix}",
            description=None,
            data_type=FieldDataType.NUMERIC,
            value=Decimal("32.7"),
        ),
        f"collection_date_{suffix}": GlobalMetadataRow(
            internal_name=f"collection_date_{suffix}",
            display_name=f"Collection Date {suffix}",
            description="Date the sample was collected",
            data_type=FieldDataType.DATE,
            value=date(2026, 5, 6),
        ),
    }
    assert result == expected


@pytest.mark.parametrize("spec", [BIOSAMPLE_METADATA_SPEC], ids=["biosample"])
async def test_fetch_global_metadata_excludes_purely_local_rows(ctx, spec):
    bs_idx = await _create_biosample_with_link(ctx)
    suffix = secrets.token_hex(4)

    # One globally-linked row to confirm appears in the result.
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=bs_idx,
        internal_name=f"global_only_{suffix}",
        display_name=f"Global Only {suffix}",
        description=None,
        data_type=FieldDataType.TEXT,
        value="KEEP",
    )

    # One purely-local field plus a metadata row (not flagged as
    # owner-id) and one purely-local field flagged as owner-id. Both
    # are biosample_metadata rows with global_field_idx IS NULL via the
    # field-contract trigger; both must be filtered out by the read.
    local_field_idx = await _create_local_field(ctx, suffix=f"plain_{suffix}")
    owner_id_field_idx = await _create_local_field(ctx, suffix=f"owner_{suffix}")
    async with ctx["pool"].acquire() as conn:
        local_meta = await _insert_metadata(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=bs_idx,
            study_field_idx=local_field_idx,
            data_type=FieldDataType.TEXT,
            value="LOCAL-VAL",
            created_by_idx=ctx["principal_idx"],
        )
        owner_meta = await insert_owner_biosample_id_metadata(
            conn,
            biosample_idx=bs_idx,
            biosample_study_field_idx=owner_id_field_idx,
            value_text="OWNER-ID-VAL",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["biosample_metadata"].extend([local_meta, owner_meta])

    result = await fetch_global_metadata(ctx["pool"], spec=spec, entity_idx=bs_idx)

    # Only the globally-linked row appears; both purely-local rows are filtered.
    expected = {
        f"global_only_{suffix}": GlobalMetadataRow(
            internal_name=f"global_only_{suffix}",
            display_name=f"Global Only {suffix}",
            description=None,
            data_type=FieldDataType.TEXT,
            value="KEEP",
        ),
    }
    assert result == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_metadata_preserves_link_retired_rows(ctx, spec):
    is_biosample = spec.entity_kind is SampleEntityKind.BIOSAMPLE
    entity_idx = await (
        _create_biosample_with_link(ctx) if is_biosample else _create_prep_sample_with_link(ctx)
    )
    suffix = secrets.token_hex(4)

    # Seed one globally-linked metadata row, then retire the underlying
    # entity-to-study link. The retirement does not touch global_field_idx
    # on the metadata row — the canonical global value is preserved so
    # studies other than the retiring one (and admins) continue to read it
    # through the global field. Per-study read access on the retired link
    # is governed by the study_access predicate at the route boundary, not
    # by schema mutation here.
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"preserved_{suffix}",
        display_name=f"Preserved {suffix}",
        description=None,
        data_type=FieldDataType.TEXT,
        value="PRESERVED",
    )
    if is_biosample:
        await retire_biosample_to_study_link(
            ctx["pool"],
            biosample_idx=entity_idx,
            study_idx=ctx["study_idx"],
            retired_by_idx=ctx["principal_idx"],
        )
    else:
        await retire_prep_sample_to_study_link(
            ctx["pool"],
            prep_sample_idx=entity_idx,
            study_idx=ctx["study_idx"],
            retired_by_idx=ctx["principal_idx"],
        )

    result = await fetch_global_metadata(ctx["pool"], spec=spec, entity_idx=entity_idx)

    # The row still satisfies global_field_idx IS NOT NULL and surfaces
    # unchanged through the cross-study read.
    expected = {
        f"preserved_{suffix}": GlobalMetadataRow(
            internal_name=f"preserved_{suffix}",
            display_name=f"Preserved {suffix}",
            description=None,
            data_type=FieldDataType.TEXT,
            value="PRESERVED",
        ),
    }
    assert result == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_metadata_empty_when_none_exist(ctx, spec):
    # An entity with no metadata rows of any kind returns an empty dict.
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )

    result = await fetch_global_metadata(ctx["pool"], spec=spec, entity_idx=entity_idx)
    assert result == {}


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_fetch_global_metadata_surfaces_missing_reason_rows(ctx, spec):
    """Tests the case where a globally-linked metadata row records an
    intentionally-missing entry (value_missing_reason_idx populated): the
    fetch returns a MissingReasonRef carrying the reason's idx and name.
    """
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )
    suffix = secrets.token_hex(4)
    reason_name = f"reason_{suffix}"
    reason_idx = await _seed_missing_value_reason(ctx, reason_name)

    # Seed two globally-linked rows on the same entity: one typed value
    # and one intentionally-missing entry on a different field. Both are
    # written through _insert_metadata so the production dispatch handles
    # the value-column routing.
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"host_subject_id_{suffix}",
        display_name=f"Host Subject ID {suffix}",
        description=None,
        data_type=FieldDataType.TEXT,
        value="HOST-9",
    )
    await _seed_globally_linked_metadata(
        ctx,
        spec=spec,
        entity_idx=entity_idx,
        internal_name=f"latitude_{suffix}",
        display_name=f"Latitude {suffix}",
        description=None,
        data_type=FieldDataType.NUMERIC,
        value=MissingReasonRef(idx=reason_idx, name=reason_name),
    )

    result = await fetch_global_metadata(ctx["pool"], spec=spec, entity_idx=entity_idx)

    expected = {
        f"host_subject_id_{suffix}": GlobalMetadataRow(
            internal_name=f"host_subject_id_{suffix}",
            display_name=f"Host Subject ID {suffix}",
            description=None,
            data_type=FieldDataType.TEXT,
            value="HOST-9",
        ),
        f"latitude_{suffix}": GlobalMetadataRow(
            internal_name=f"latitude_{suffix}",
            display_name=f"Latitude {suffix}",
            description=None,
            data_type=FieldDataType.NUMERIC,
            value=MissingReasonRef(idx=reason_idx, name=reason_name),
        ),
    }
    assert result == expected


async def test_missing_value_reason_name_rejects_empty_string(postgres_pool):
    """Tests the case where a direct insert tries to seed an empty-string
    reason name: the layered CHECK rejects it with CheckViolationError,
    pairing the application-side MissingReasonRef.name min_length=1 guard.
    """
    with pytest.raises(asyncpg.CheckViolationError):
        await postgres_pool.execute("INSERT INTO qiita.missing_value_reason (name) VALUES ('')")


# ---------------------------------------------------------------------------
# validate_primary_secondary_studies (pure-unit)
# ---------------------------------------------------------------------------


def test_validate_primary_secondary_studies_accepts_disjoint():
    # Primary not in secondaries; the guard returns silently.
    validate_primary_secondary_studies(1, [2, 3])


def test_validate_primary_secondary_studies_accepts_empty_secondaries():
    # Empty list is the common single-study case; guard returns silently.
    validate_primary_secondary_studies(1, [])


def test_validate_primary_secondary_studies_rejects_primary_in_secondaries():
    # Primary present in the secondary list; the guard raises with the
    # canonical message format the composers share.
    with pytest.raises(ValueError, match="must not appear in secondary_study_idxs"):
        validate_primary_secondary_studies(1, [2, 1, 3])


# ---------------------------------------------------------------------------
# insert_entity_to_study (parametrized over both specs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_insert_entity_to_study_links_entity(ctx, spec):
    # Seed an entity not yet linked to ctx['study_idx'].
    entity_idx = await _seed_unlinked_entity_for_spec(ctx, spec)

    # Write the (entity, study) link via the parameterised helper.
    async with ctx["pool"].acquire() as conn:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    _track_to_study_link(ctx, spec, entity_idx, ctx["study_idx"])

    # Verify the link row matches the expected non-retired shape. The
    # column list is built from the spec so the assertion stays
    # entity-agnostic.
    row = await ctx["pool"].fetchrow(
        f"SELECT {spec.link_entity_key_column}, study_idx, created_by_idx, retired"
        f" FROM {spec.link_table}"
        f" WHERE {spec.link_entity_key_column} = $1 AND study_idx = $2",
        entity_idx,
        ctx["study_idx"],
    )
    expected = {
        spec.link_entity_key_column: entity_idx,
        "study_idx": ctx["study_idx"],
        "created_by_idx": ctx["principal_idx"],
        "retired": False,
    }
    assert dict(row) == expected


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_insert_entity_to_study_rejects_duplicate(ctx, spec):
    # First link succeeds via the parameterised helper.
    entity_idx = await _seed_unlinked_entity_for_spec(ctx, spec)
    async with ctx["pool"].acquire() as conn:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    _track_to_study_link(ctx, spec, entity_idx, ctx["study_idx"])

    # Second insert of the same (entity, study) pair must raise on the PK.
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(asyncpg.UniqueViolationError):
            await insert_entity_to_study(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=ctx["study_idx"],
                created_by_idx=ctx["principal_idx"],
            )


# ---------------------------------------------------------------------------
# link_entity_to_studies (parametrized over both specs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_link_entity_to_studies_links_primary_and_unique_secondaries(ctx, spec):
    """Helper links the entity to primary_study_idx plus every unique
    secondary. Passing a repeated secondary does not trip the link
    table's primary key — the helper dedups before iterating, which is
    the bug-prevention layer the composers share.
    """
    # Seed the entity and two additional studies it can be linked to.
    entity_idx = await _seed_unlinked_entity_for_spec(ctx, spec)
    sec_a, sec_b = await _seed_secondary_studies_for_entity(ctx, spec, entity_idx, count=2)

    # Pass sec_a twice; the helper must collapse the duplicate so the
    # second sec_a INSERT is never attempted.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            await link_entity_to_studies(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                primary_study_idx=ctx["study_idx"],
                secondary_study_idxs=[sec_a, sec_b, sec_a],
                caller_idx=ctx["principal_idx"],
            )
    for st in [ctx["study_idx"], sec_a, sec_b]:
        _track_to_study_link(ctx, spec, entity_idx, st)

    # The persisted link set is exactly {primary, sec_a, sec_b}.
    rows = await ctx["pool"].fetch(
        f"SELECT study_idx FROM {spec.link_table} WHERE {spec.link_entity_key_column} = $1",
        entity_idx,
    )
    assert {r["study_idx"] for r in rows} == {ctx["study_idx"], sec_a, sec_b}


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_link_entity_to_studies_empty_secondaries_links_primary_only(ctx, spec):
    # Common single-study case; helper writes one link row.
    entity_idx = await _seed_unlinked_entity_for_spec(ctx, spec)

    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            await link_entity_to_studies(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                primary_study_idx=ctx["study_idx"],
                secondary_study_idxs=[],
                caller_idx=ctx["principal_idx"],
            )
    _track_to_study_link(ctx, spec, entity_idx, ctx["study_idx"])

    rows = await ctx["pool"].fetch(
        f"SELECT study_idx FROM {spec.link_table} WHERE {spec.link_entity_key_column} = $1",
        entity_idx,
    )
    assert [r["study_idx"] for r in rows] == [ctx["study_idx"]]


# ---------------------------------------------------------------------------
# preflight_global_metadata (parametrized over both specs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_returns_parsed_pairs(ctx, spec):
    """Resolves each display_name to its global-field row and parses each
    text value into the typed Python form matching its data_type. The
    returned list mirrors input order."""
    gf_row = await _seed_global_field_for_spec(ctx, spec, FieldDataType.TEXT)

    metadata = {gf_row.display_name: "  hello  "}
    async with ctx["pool"].acquire() as conn:
        result = await preflight_global_metadata(conn, spec=spec, metadata=metadata)

    # parse_text_for_data_type strips outer whitespace for the TEXT arm.
    assert result == [(gf_row, "hello")]


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_raises_unknown_with_spec_entity_kind(ctx, spec):
    """Unknown display_names raise MetadataUnknownFieldsError carrying
    spec.entity_kind so the error message names the right domain."""
    metadata = {"definitely_not_a_field_xyz123": "value"}

    async with ctx["pool"].acquire() as conn:
        with pytest.raises(MetadataUnknownFieldsError) as excinfo:
            await preflight_global_metadata(conn, spec=spec, metadata=metadata)

    # The unknown-name list is preserved verbatim, and the exception
    # message interpolates spec.entity_kind so it reads naturally for
    # whichever entity the call targeted.
    assert excinfo.value.unknown_display_names == ["definitely_not_a_field_xyz123"]
    assert spec.entity_kind.value in str(excinfo.value)


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_raises_parse_error(ctx, spec):
    """Bad text-for-data_type input raises MetadataParseError after the
    unknown-name check passes."""
    gf_row = await _seed_global_field_for_spec(ctx, spec, FieldDataType.NUMERIC)

    metadata = {gf_row.display_name: "not_a_number"}
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(MetadataParseError):
            await preflight_global_metadata(conn, spec=spec, metadata=metadata)


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_routes_missing_marker(ctx, spec):
    """Tests the case where a metadata text value matches a known
    missing-reason name: the corresponding entry resolves to a
    MissingReasonRef carrying the reason's idx and name.
    """
    # Seed a NUMERIC field: missing-marker recognition is the only way
    # for a non-numeric text to resolve in this slot.
    gf_row = await _seed_global_field_for_spec(ctx, spec, FieldDataType.NUMERIC)
    suffix = secrets.token_hex(4)
    reason_name = f"mv_marker_{suffix}"
    reason_idx = await _seed_missing_value_reason(ctx, reason_name)

    metadata = {gf_row.display_name: reason_name}
    async with ctx["pool"].acquire() as conn:
        result = await preflight_global_metadata(
            conn,
            spec=spec,
            metadata=metadata,
            known_missing_reasons={reason_name: reason_idx},
        )

    assert result == [(gf_row, MissingReasonRef(idx=reason_idx, name=reason_name))]


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_unchanged_for_typed_values_with_empty_map(ctx, spec):
    """Tests the case where known_missing_reasons is empty: typed parsing
    runs and produces typed Python values.
    """
    gf_row = await _seed_global_field_for_spec(ctx, spec, FieldDataType.TEXT)

    metadata = {gf_row.display_name: "  hello  "}
    async with ctx["pool"].acquire() as conn:
        result = await preflight_global_metadata(
            conn,
            spec=spec,
            metadata=metadata,
            known_missing_reasons={},
        )

    # Outer whitespace is stripped from the TEXT value.
    assert result == [(gf_row, "hello")]


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_preflight_global_metadata_recognizes_padded_marker(ctx, spec):
    """Tests the case where a TEXT-field metadata value is a missing-reason
    name with surrounding whitespace: the value resolves to a MissingReasonRef
    (carrying the stripped reason name), not a literal text value.
    """
    # TEXT field: without stripped marker recognition, a padded marker
    # would be silently stored as a literal text value via parse_text_for_data_type.
    gf_row = await _seed_global_field_for_spec(ctx, spec, FieldDataType.TEXT)
    suffix = secrets.token_hex(4)
    reason_name = f"mv_marker_{suffix}"
    reason_idx = await _seed_missing_value_reason(ctx, reason_name)

    metadata = {gf_row.display_name: f"  {reason_name}  "}
    async with ctx["pool"].acquire() as conn:
        result = await preflight_global_metadata(
            conn,
            spec=spec,
            metadata=metadata,
            known_missing_reasons={reason_name: reason_idx},
        )

    assert result == [(gf_row, MissingReasonRef(idx=reason_idx, name=reason_name))]


def test_parse_text_for_data_type_unchanged_for_missing_reason_name():
    """Tests the case where a text value matches a known missing-reason
    name: parse_text_for_data_type raises MetadataParseError when the
    field is NUMERIC. Marker recognition is not performed at this layer.
    """
    with pytest.raises(MetadataParseError):
        parse_text_for_data_type("temp_c", FieldDataType.NUMERIC, "not collected")


# ---------------------------------------------------------------------------
# write_global_metadata_entries (parametrized over both specs)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_write_global_metadata_entries_writes_each_entry(ctx, spec):
    """Each parsed entry produces one metadata row bound to the input
    study_idx; values land in the value_text column for TEXT entries."""
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )
    gf_first = await _seed_global_field_for_spec(ctx, spec, FieldDataType.TEXT)
    gf_second = await _seed_global_field_for_spec(ctx, spec, FieldDataType.TEXT)
    parsed_metadata = [(gf_first, "alpha"), (gf_second, "beta")]

    # Write all entries inside one committed transaction so the rows
    # persist for the post-call SELECT below.
    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            await write_global_metadata_entries(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=ctx["study_idx"],
                caller_idx=ctx["principal_idx"],
                parsed_metadata=parsed_metadata,
            )

    # Recover the persisted rows and track each for FK-reverse cleanup;
    # the metadata-side teardown depends on the matching study_field row
    # being recorded too.
    rows = await ctx["pool"].fetch(
        f"SELECT m.idx AS metadata_idx, m.value_text,"
        f" m.{spec.study_field_idx_column} AS study_field_idx"
        f" FROM {spec.metadata_table} m"
        f" WHERE m.{spec.entity_key_column} = $1"
        f" ORDER BY m.value_text",
        entity_idx,
    )
    metadata_key = (
        "biosample_metadata"
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else "prep_sample_metadata"
    )
    study_field_key = (
        "biosample_study_field"
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else "prep_sample_study_field"
    )
    seen_field_idxs: set[int] = set()
    for r in rows:
        ctx["created"][metadata_key].append(r["metadata_idx"])
        if r["study_field_idx"] not in seen_field_idxs:
            ctx["created"][study_field_key].append(r["study_field_idx"])
            seen_field_idxs.add(r["study_field_idx"])

    assert [r["value_text"] for r in rows] == ["alpha", "beta"]


@pytest.mark.parametrize(
    "spec",
    [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC],
    ids=["biosample", "prep_sample"],
)
async def test_write_global_metadata_entries_empty_input_is_noop(ctx, spec):
    """Empty parsed_metadata writes nothing; the helper short-circuits
    without touching the DB."""
    entity_idx = await (
        _create_biosample_with_link(ctx)
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else _create_prep_sample_with_link(ctx)
    )

    async with ctx["pool"].acquire() as conn:
        async with conn.transaction():
            await write_global_metadata_entries(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=ctx["study_idx"],
                caller_idx=ctx["principal_idx"],
                parsed_metadata=[],
            )

    count = await ctx["pool"].fetchval(
        f"SELECT COUNT(*) FROM {spec.metadata_table} WHERE {spec.entity_key_column} = $1",
        entity_idx,
    )
    assert count == 0
