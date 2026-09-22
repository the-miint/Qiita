"""Integration tests for the triggers guarding the *_metadata surface.

Covers the structurally identical biosample and prep_sample trigger twins
in one suite, parameterized over EntityMetadataSpec so every branch is
exercised against both stacks. The final section is the exception: it
drives a migration rather than a trigger directly, and runs against the
biosample stack alone for the reason its header gives.

The SQL UPDATE/SELECT statements that drive the triggers interpolate
identifiers from frozen module-level spec fields (metadata_table,
study_field_table, study_field_global_fk_column, link_table); the spec
carries every identifier that differs between the two stacks.
"""

import secrets
from datetime import date
from decimal import Decimal
from pathlib import Path

import asyncpg
import pytest
from qiita_common.models import FieldDataType, MissingReasonRef
from qiita_common.models.biosample import FieldWriteOutcome

from qiita_control_plane.repositories._sample_helpers import (
    MissingValueOnUniqueFieldError,
    StudyUniqueValueConflictError,
    UniqueInStudyViolation,
    _get_or_create_globally_linked_study_field,
    _get_or_create_local_study_field,
    _insert_metadata,
    classify_unique_in_study_violation,
    fetch_study_field,
    insert_entity_to_study,
    update_study_field,
    write_local_metadata_or_diagnose,
)
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.routes._helpers import parse_kv_detail
from qiita_control_plane.testing.db_seeds import seed_sequenced_prep_sample
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    _create_linked_entity_for_spec,
    _seed_global_field_for_spec,
    _seed_secondary_studies_for_entity,
    _track_to_study_link,
)

pytestmark = pytest.mark.db


# Both stacks run every test; pytest reports ids as [biosample] / [prep_sample].
SPECS = [BIOSAMPLE_METADATA_SPEC, PREP_SAMPLE_METADATA_SPEC]


def _spec_id(spec):
    """Pytest id for the parametrize decorator: spec.entity_kind value."""
    return spec.entity_kind.value


def _study_field_tracking_key(spec):
    """Cleanup-dict key for the *_study_field rows seeded by a test."""
    return spec.study_field_table.split(".")[-1]


def _metadata_tracking_key(spec):
    """Cleanup-dict key for the *_metadata rows seeded by a test."""
    return spec.metadata_table.split(".")[-1]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_propagate_link_upgrade_null_to_non_null_propagates_to_metadata(ctx, spec):
    # NULL -> non-NULL transition (upgrade local to global): the UPDATE on
    # the study_field row succeeds and the trigger denormalizes the new
    # global_field_idx into any existing metadata row through this field.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)

    # Seed a TEXT global field the study_field will be upgraded to.
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    # Create a purely-local TEXT field and write one metadata row through it.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name("upgrade"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="kept",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    ctx["created"][_metadata_tracking_key(spec)].append(meta_idx)

    # Upgrade the field to global: clear the inherited columns too so the
    # *_study_field_inheritance_consistent CHECK passes after the UPDATE
    # (the linked-row branch requires data_type / required NULL).
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f" SET {spec.study_field_global_fk_column} = $1,"
        f"     data_type = NULL,"
        f"     required = NULL,"
        f"     terminology_idx = NULL,"
        f"     tier_override = NULL"
        f" WHERE idx = $2",
        gf.idx,
        field_idx,
    )

    # The pre-existing metadata row's global_field_idx now reflects the
    # upgrade; the typed value column is untouched.
    row = await ctx["pool"].fetchrow(
        f"SELECT global_field_idx, value_text FROM {spec.metadata_table} WHERE idx = $1",
        meta_idx,
    )
    assert dict(row) == {"global_field_idx": gf.idx, "value_text": "kept"}


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_propagate_link_unlink_with_no_metadata_succeeds(ctx, spec):
    # non-NULL -> NULL transition (unlink) with no metadata through the
    # field: the UPDATE succeeds because the unlink has no rows to strand.
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    # Create a globally-linked study_field with no metadata rows yet.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf.idx,
            display_name=unique_field_name("unlink_empty"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    # Unlink the field. The propagate trigger has nothing to update; the
    # CHECK requires data_type / required non-NULL once unlinked, so the
    # UPDATE supplies both alongside the unlink.
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f" SET {spec.study_field_global_fk_column} = NULL,"
        f"     data_type = 'text',"
        f"     required = false"
        f" WHERE idx = $1",
        field_idx,
    )

    row = await ctx["pool"].fetchrow(
        f"SELECT {spec.study_field_global_fk_column} AS gf_idx, data_type, required"
        f" FROM {spec.study_field_table} WHERE idx = $1",
        field_idx,
    )
    assert dict(row) == {
        "gf_idx": None,
        "data_type": "text",
        "required": False,
    }


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_propagate_link_unlink_with_metadata_raises(ctx, spec):
    # non-NULL -> NULL transition (unlink) with at least one metadata row
    # through the field: the trigger raises rather than silently strand
    # the globally-linked rows.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf.idx,
            display_name=unique_field_name("unlink_full"),
            created_by_idx=ctx["principal_idx"],
        )
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="published",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    ctx["created"][_metadata_tracking_key(spec)].append(meta_idx)

    # Attempt to unlink — trigger refuses, the UPDATE rolls back.
    with pytest.raises(asyncpg.RaiseError, match="cannot unlink"):
        await ctx["pool"].execute(
            f"UPDATE {spec.study_field_table}"
            f" SET {spec.study_field_global_fk_column} = NULL,"
            f"     data_type = 'text',"
            f"     required = false"
            f" WHERE idx = $1",
            field_idx,
        )

    # Field row remains globally-linked; metadata row is untouched.
    row = await ctx["pool"].fetchrow(
        f"SELECT {spec.study_field_global_fk_column} AS gf_idx, data_type, required"
        f" FROM {spec.study_field_table} WHERE idx = $1",
        field_idx,
    )
    assert dict(row) == {
        "gf_idx": gf.idx,
        "data_type": None,
        "required": None,
    }
    meta_row = await ctx["pool"].fetchrow(
        f"SELECT global_field_idx, value_text FROM {spec.metadata_table} WHERE idx = $1",
        meta_idx,
    )
    assert dict(meta_row) == {"global_field_idx": gf.idx, "value_text": "published"}


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_propagate_link_rebind_raises_unconditionally(ctx, spec):
    # non-NULL -> different non-NULL transition (rebind): trigger rejects
    # regardless of metadata presence, because rebinding mutates the
    # field's identity rather than evolving it. This test exercises the
    # no-metadata case so the rejection is provably unconditional.
    gf_a = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)
    gf_b = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf_a.idx,
            display_name=unique_field_name("rebind"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    # Attempt to rebind from gf_a to gf_b. Trigger raises even though no
    # metadata exists through this field.
    with pytest.raises(asyncpg.RaiseError, match="cannot rebind"):
        await ctx["pool"].execute(
            f"UPDATE {spec.study_field_table}"
            f" SET {spec.study_field_global_fk_column} = $1 WHERE idx = $2",
            gf_b.idx,
            field_idx,
        )

    # Field row remains bound to the original global field.
    bound = await ctx["pool"].fetchval(
        f"SELECT {spec.study_field_global_fk_column} FROM {spec.study_field_table} WHERE idx = $1",
        field_idx,
    )
    assert bound == gf_a.idx


async def _retire_link(ctx, spec, entity_idx):
    """Retire the entity's link to ctx['study_idx'].

    The retirement CHECK requires retired_at and retired_by_idx alongside the
    flag, so all three are set in one UPDATE.
    """
    await ctx["pool"].execute(
        f"UPDATE {spec.link_table}"
        f"   SET retired = true, retired_at = now(), retired_by_idx = $1"
        f" WHERE {spec.link_entity_key_column} = $2 AND study_idx = $3",
        ctx["principal_idx"],
        entity_idx,
        ctx["study_idx"],
    )


async def _seed_global_value(ctx, spec, entity_idx, value):
    """Write one globally-linked TEXT metadata value while the link is active.

    Returns (metadata_idx, global_field_idx), both tracked for cleanup.
    """
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf.idx,
            display_name=unique_field_name("retired_link"),
            created_by_idx=ctx["principal_idx"],
        )
        metadata_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value=value,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    ctx["created"][_metadata_tracking_key(spec)].append(metadata_idx)
    return metadata_idx, gf.idx


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_reject_if_link_retired_blocks_value_update(ctx, spec):
    """Tests the case where a metadata value is overwritten after the writing
    study's link to the entity has been retired: the trigger rejects it, so an
    overwrite cannot slip through a link that no longer permits writes.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx, _ = await _seed_global_value(ctx, spec, entity_idx, "before")

    await _retire_link(ctx, spec, entity_idx)

    # The value UPDATE the upsert path performs is now refused.
    with pytest.raises(asyncpg.RaiseError, match="is retired"):
        await ctx["pool"].execute(
            f"UPDATE {spec.metadata_table} SET value_text = $1 WHERE idx = $2",
            "after",
            metadata_idx,
        )

    # The stored value is unchanged.
    stored_value = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {spec.metadata_table} WHERE idx = $1",
        metadata_idx,
    )
    assert stored_value == "before"


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_reject_if_link_retired_detail_identifies_the_trigger_on_update(ctx, spec):
    """Tests the case where a route needs to tell this rejection from another
    guard sharing its SQLSTATE: the error DETAIL names the raising function plus
    the entity and study, so no route has to match message prose.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx, _ = await _seed_global_value(ctx, spec, entity_idx, "before")

    await _retire_link(ctx, spec, entity_idx)

    with pytest.raises(asyncpg.RaiseError) as excinfo:
        await ctx["pool"].execute(
            f"UPDATE {spec.metadata_table} SET value_text = $1 WHERE idx = $2",
            "after",
            metadata_idx,
        )

    expected_detail = {
        "trigger": spec.metadata_retired_link_trigger,
        spec.entity_key_column: str(entity_idx),
        "study_idx": str(ctx["study_idx"]),
    }
    assert parse_kv_detail(excinfo.value.detail) == expected_detail


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_reject_if_link_retired_detail_identifies_the_trigger_on_insert(ctx, spec):
    """Tests the case where the first value for a slot is written through a
    retired link: the INSERT rejection carries the same DETAIL as the overwrite
    rejection, so one dispatch covers both statement kinds.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _retire_link(ctx, spec, entity_idx)
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    # The raise escapes the transaction block, so the study field created
    # alongside the refused row rolls back with it and needs no cleanup.
    with pytest.raises(asyncpg.RaiseError) as excinfo:
        async with ctx["pool"].acquire() as conn, conn.transaction():
            field_idx, _ = await _get_or_create_globally_linked_study_field(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                global_field_idx=gf.idx,
                display_name=unique_field_name("retired_link_insert"),
                created_by_idx=ctx["principal_idx"],
            )
            await _insert_metadata(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=field_idx,
                data_type=FieldDataType.TEXT,
                value="x",
                created_by_idx=ctx["principal_idx"],
            )

    expected_detail = {
        "trigger": spec.metadata_retired_link_trigger,
        spec.entity_key_column: str(entity_idx),
        "study_idx": str(ctx["study_idx"]),
    }
    assert parse_kv_detail(excinfo.value.detail) == expected_detail


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_reject_if_link_retired_allows_value_update_on_active_link(ctx, spec):
    """Tests the case where a metadata value is overwritten while the link is
    still active: the guard does not over-reject the ordinary upsert.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx, _ = await _seed_global_value(ctx, spec, entity_idx, "before")

    await ctx["pool"].execute(
        f"UPDATE {spec.metadata_table} SET value_text = $1 WHERE idx = $2",
        "after",
        metadata_idx,
    )

    stored_value = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {spec.metadata_table} WHERE idx = $1",
        metadata_idx,
    )
    assert stored_value == "after"


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_reject_if_link_retired_allows_global_link_propagation(ctx, spec):
    """Tests the case where a study_field is upgraded local -> global while an
    entity holding a value through it has a retired link: the propagated
    global_field_idx write must still succeed, which is what scoping the guard
    to the value columns preserves.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    # A purely-local field carrying one value, written while the link is active.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name("retired_upgrade"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
        metadata_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="kept",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    ctx["created"][_metadata_tracking_key(spec)].append(metadata_idx)

    await _retire_link(ctx, spec, entity_idx)

    # Upgrading the field to global fires the propagate trigger, which writes
    # global_field_idx onto the existing metadata row — a column outside the
    # retired-link guard's scope, so the upgrade is unaffected by retirement.
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f"   SET {spec.study_field_global_fk_column} = $1,"
        f"       data_type = NULL,"
        f"       required = NULL,"
        f"       terminology_idx = NULL,"
        f"       tier_override = NULL"
        f" WHERE idx = $2",
        gf.idx,
        field_idx,
    )

    row = await ctx["pool"].fetchrow(
        f"SELECT global_field_idx, value_text FROM {spec.metadata_table} WHERE idx = $1",
        metadata_idx,
    )
    assert dict(row) == {"global_field_idx": gf.idx, "value_text": "kept"}


async def _fetch_timestamps(ctx, spec, metadata_idx):
    """Return the metadata row's (created_at, updated_at) pair."""
    row = await ctx["pool"].fetchrow(
        f"SELECT created_at, updated_at FROM {spec.metadata_table} WHERE idx = $1",
        metadata_idx,
    )
    return row["created_at"], row["updated_at"]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_set_updated_at_matches_created_at_on_insert(ctx, spec):
    """Tests the case where a metadata row has only ever been inserted: both
    timestamps default to the same transaction clock, so an untouched row
    reports an updated_at that claims no edit it did not receive.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx, _ = await _seed_global_value(ctx, spec, entity_idx, "fresh")

    created_at, updated_at = await _fetch_timestamps(ctx, spec, metadata_idx)
    assert updated_at == created_at


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_set_updated_at_bumps_on_value_update(ctx, spec):
    """Tests the case where a stored value is overwritten: the trigger advances
    updated_at past the insert-time value and leaves created_at alone, which is
    what makes the column usable as the row's version.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx, _ = await _seed_global_value(ctx, spec, entity_idx, "before")
    created_at, seeded_updated_at = await _fetch_timestamps(ctx, spec, metadata_idx)

    # A separate transaction from the seed, so now() has advanced.
    await ctx["pool"].execute(
        f"UPDATE {spec.metadata_table} SET value_text = $1 WHERE idx = $2",
        "after",
        metadata_idx,
    )

    bumped_created_at, bumped_updated_at = await _fetch_timestamps(ctx, spec, metadata_idx)
    assert bumped_updated_at > seeded_updated_at
    assert bumped_created_at == created_at


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_set_updated_at_bumps_on_global_link_propagation(ctx, spec):
    """Tests the case where a study_field is upgraded local -> global and the
    propagate trigger denormalizes global_field_idx onto an existing metadata
    row: updated_at bumps even though no value changed, because the trigger is
    unscoped and the column tracks any change to the row.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)

    # A purely-local field carrying one value, so the upgrade below has a row
    # to propagate into.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name("upgrade_bump"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
        metadata_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="kept",
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    ctx["created"][_metadata_tracking_key(spec)].append(metadata_idx)
    _, seeded_updated_at = await _fetch_timestamps(ctx, spec, metadata_idx)

    # The inherited columns are cleared alongside the link so the
    # *_study_field_inheritance_consistent CHECK still holds after the UPDATE.
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table}"
        f"   SET {spec.study_field_global_fk_column} = $1,"
        f"       data_type = NULL,"
        f"       required = NULL,"
        f"       terminology_idx = NULL,"
        f"       tier_override = NULL"
        f" WHERE idx = $2",
        gf.idx,
        field_idx,
    )

    _, bumped_updated_at = await _fetch_timestamps(ctx, spec, metadata_idx)
    assert bumped_updated_at > seeded_updated_at


# =============================================================================
# unique_in_study
#
# The flag is switched on by raw UPDATE throughout: the repository helpers do
# not carry the column, so the study_field table is the only way to set it.
# =============================================================================


async def _create_flagged_field(ctx, spec, *, study_idx, data_type, suffix):
    """Create a purely-local study field in `study_idx` and switch its
    unique_in_study flag on. Returns the field idx.
    """
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=study_idx,
            display_name=unique_field_name(suffix),
            created_by_idx=ctx["principal_idx"],
            data_type=data_type,
            required=False,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table} SET unique_in_study = true WHERE idx = $1",
        field_idx,
    )
    return field_idx


async def _write_value(ctx, spec, *, entity_idx, field_idx, data_type, value):
    """Write one metadata row and track it for cleanup. Returns the row idx."""
    async with ctx["pool"].acquire() as conn, conn.transaction():
        meta_idx = await _insert_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_field_idx=field_idx,
            data_type=data_type,
            value=value,
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_metadata_tracking_key(spec)].append(meta_idx)
    return meta_idx


# The three eligible data_types and a colliding value for each, so the
# duplicate-rejection case is stated once rather than per type.
_ELIGIBLE_TYPE_VALUES = [
    (FieldDataType.TEXT, "Sample 1"),
    (FieldDataType.NUMERIC, Decimal("42.5")),
    (FieldDataType.DATE, date(2026, 3, 1)),
]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize("data_type,value", _ELIGIBLE_TYPE_VALUES, ids=lambda v: str(v))
async def test_unique_in_study_rejects_duplicate(ctx, spec, data_type, value):
    # Tests the case where two entities in one study are given the same value
    # through a flagged field: the second write hits the partial unique index.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_flagged_field(
        ctx, spec, study_idx=ctx["study_idx"], data_type=data_type, suffix="dup"
    )

    await _write_value(
        ctx,
        spec,
        entity_idx=first_entity_idx,
        field_idx=field_idx,
        data_type=data_type,
        value=value,
    )

    with pytest.raises(asyncpg.UniqueViolationError):
        await _write_value(
            ctx,
            spec,
            entity_idx=second_entity_idx,
            field_idx=field_idx,
            data_type=data_type,
            value=value,
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_allows_duplicate_when_flag_false(ctx, spec):
    # Tests the case where the flag is left at its default: the same value
    # through the same field for two entities is accepted, as before.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name("unflagged"),
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    for entity_idx in (first_entity_idx, second_entity_idx):
        await _write_value(
            ctx,
            spec,
            entity_idx=entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="Sample 1",
        )

    written = await ctx["pool"].fetchval(
        f"SELECT COUNT(*) FROM {spec.metadata_table}"
        f" WHERE {spec.study_field_idx_column} = $1 AND value_text = $2",
        field_idx,
        "Sample 1",
    )
    assert written == 2


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_allows_same_value_in_another_study(ctx, spec):
    # Tests the case where one entity carries the same value through two
    # studies' own flagged fields: uniqueness is scoped to the field, and a
    # purely-local field belongs to exactly one study, so there is no clash.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    (second_study_idx,) = await _seed_secondary_studies_for_entity(ctx, spec, entity_idx, 1)
    async with ctx["pool"].acquire() as conn:
        await insert_entity_to_study(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=second_study_idx,
            created_by_idx=ctx["principal_idx"],
        )
    _track_to_study_link(ctx, spec, entity_idx, second_study_idx)

    for study_idx, suffix in ((ctx["study_idx"], "own"), (second_study_idx, "other")):
        field_idx = await _create_flagged_field(
            ctx, spec, study_idx=study_idx, data_type=FieldDataType.TEXT, suffix=suffix
        )
        await _write_value(
            ctx,
            spec,
            entity_idx=entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="Sample 1",
        )

    written = await ctx["pool"].fetchval(
        f"SELECT COUNT(*) FROM {spec.metadata_table}"
        f" WHERE {spec.entity_key_column} = $1 AND value_text = $2",
        entity_idx,
        "Sample 1",
    )
    assert written == 2


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_rejects_missing_reason(ctx, spec):
    # Tests the case where a flagged field is given a missing-value marker:
    # a field whose job is to tell the study's samples apart cannot hold a
    # sample that declines to be told apart.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_flagged_field(
        ctx, spec, study_idx=ctx["study_idx"], data_type=FieldDataType.TEXT, suffix="missing"
    )

    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    with pytest.raises(asyncpg.CheckViolationError):
        await _write_value(
            ctx,
            spec,
            entity_idx=entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value=MissingReasonRef(idx=reason_idx, name="not applicable"),
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_retired_entity_holds_the_slot(ctx, spec):
    # Tests the case where the entity holding a value is retired: the slot
    # stays occupied, matching the permanently-held semantics of the
    # cross-study global field slot.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_flagged_field(
        ctx, spec, study_idx=ctx["study_idx"], data_type=FieldDataType.TEXT, suffix="retired"
    )

    await _write_value(
        ctx,
        spec,
        entity_idx=first_entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )

    entity_table = spec.metadata_table.replace("_metadata", "")
    await ctx["pool"].execute(
        f"UPDATE {entity_table}"
        f" SET retired = true, retired_at = now(), retired_by_idx = $1"
        f" WHERE idx = $2",
        ctx["principal_idx"],
        first_entity_idx,
    )

    with pytest.raises(asyncpg.UniqueViolationError):
        await _write_value(
            ctx,
            spec,
            entity_idx=second_entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="Sample 1",
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_rejected_on_globally_linked_field(ctx, spec):
    # Tests the case where the flag is set on a globally-linked field: one
    # metadata row is shared across every study linked to the global field,
    # so grouping by the study field would not describe any single study.
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf.idx,
            display_name=unique_field_name("linked"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    with pytest.raises(asyncpg.CheckViolationError):
        await ctx["pool"].execute(
            f"UPDATE {spec.study_field_table} SET unique_in_study = true WHERE idx = $1",
            field_idx,
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize(
    "data_type", [FieldDataType.BOOLEAN, FieldDataType.TERMINOLOGY], ids=lambda v: v.value
)
async def test_unique_in_study_rejected_on_ineligible_data_type(ctx, spec, data_type):
    # Tests the case where the flag is set on a closed-value-set field:
    # such a field caps the study at as many samples as it has values.
    #
    # A terminology field needs a vocabulary to point at; the *_study_field
    # CHECK couples terminology_idx to the data_type either way.
    terminology_idx = None
    if data_type is FieldDataType.TERMINOLOGY:
        terminology_idx = await ctx["pool"].fetchval(
            "INSERT INTO qiita.terminology (name, version, loaded_at)"
            " VALUES ($1, $2, now()) RETURNING idx",
            f"term_{secrets.token_hex(4)}",
            "v1",
        )
        ctx["created"]["terminology"].append(terminology_idx)
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name("ineligible"),
            created_by_idx=ctx["principal_idx"],
            data_type=data_type,
            required=False,
            terminology_idx=terminology_idx,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    with pytest.raises(asyncpg.CheckViolationError):
        await ctx["pool"].execute(
            f"UPDATE {spec.study_field_table} SET unique_in_study = true WHERE idx = $1",
            field_idx,
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_duplicate_raises_typed_error(ctx, spec):
    # Tests the case where a duplicate reaches the write path rather than a
    # raw INSERT: the index violation is translated, not propagated, so the
    # route layer has something to map instead of a 500.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    display_name = unique_field_name("typed-dup")

    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
            unique_in_study=True,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    await _write_value(
        ctx,
        spec,
        entity_idx=first_entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )

    with pytest.raises(StudyUniqueValueConflictError) as excinfo:
        async with ctx["pool"].acquire() as conn, conn.transaction():
            await write_local_metadata_or_diagnose(
                conn,
                spec=spec,
                entity_idx=second_entity_idx,
                study_idx=ctx["study_idx"],
                display_name=display_name,
                data_type=FieldDataType.TEXT,
                value="Sample 1",
                caller_idx=ctx["principal_idx"],
            )

    assert excinfo.value.display_name == display_name
    assert excinfo.value.attempted_value == "Sample 1"
    assert excinfo.value.study_field_idx == field_idx


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_missing_marker_raises_typed_error(ctx, spec):
    # Tests the case where a missing-value marker reaches the write path for a
    # flagged field: the CHECK violation is translated on its constraint name.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    display_name = unique_field_name("typed-missing")

    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
            unique_in_study=True,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)

    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)

    with pytest.raises(MissingValueOnUniqueFieldError) as excinfo:
        async with ctx["pool"].acquire() as conn, conn.transaction():
            await write_local_metadata_or_diagnose(
                conn,
                spec=spec,
                entity_idx=entity_idx,
                study_idx=ctx["study_idx"],
                display_name=display_name,
                data_type=FieldDataType.TEXT,
                value=MissingReasonRef(idx=reason_idx, name=reason_name),
                caller_idx=ctx["principal_idx"],
            )

    assert excinfo.value.display_name == display_name
    assert excinfo.value.study_field_idx == field_idx


# =============================================================================
# study-field updated_at and unique_in_study propagation
# =============================================================================


async def _create_plain_field(ctx, spec, *, suffix, data_type=FieldDataType.TEXT):
    """Create a purely-local study field with no uniqueness policy. Returns
    the field idx.
    """
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=unique_field_name(suffix),
            created_by_idx=ctx["principal_idx"],
            data_type=data_type,
            required=False,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    return field_idx


async def _set_unique_in_study(ctx, spec, field_idx, value):
    """Flip a study field's unique_in_study, driving the propagation trigger."""
    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table} SET unique_in_study = $1 WHERE idx = $2",
        value,
        field_idx,
    )


async def _read_flags(ctx, spec, field_idx):
    """Return the field's stored flag and the flags on its metadata rows."""
    field_flag = await ctx["pool"].fetchval(
        f"SELECT unique_in_study FROM {spec.study_field_table} WHERE idx = $1",
        field_idx,
    )
    metadata_flags = await ctx["pool"].fetch(
        f"SELECT unique_in_study FROM {spec.metadata_table}"
        f" WHERE {spec.study_field_idx_column} = $1",
        field_idx,
    )
    return field_flag, [r["unique_in_study"] for r in metadata_flags]


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_study_field_set_updated_at_bumps_on_update(ctx, spec):
    # Tests the case where a study field row is edited: the shared
    # set_updated_at() function is attached to this table too, so the column
    # moves without the caller setting it.
    field_idx = await _create_plain_field(ctx, spec, suffix="touch")
    before = await ctx["pool"].fetchval(
        f"SELECT updated_at FROM {spec.study_field_table} WHERE idx = $1", field_idx
    )

    await ctx["pool"].execute(
        f"UPDATE {spec.study_field_table} SET description = $1 WHERE idx = $2",
        "edited",
        field_idx,
    )

    after = await ctx["pool"].fetchval(
        f"SELECT updated_at FROM {spec.study_field_table} WHERE idx = $1", field_idx
    )
    assert after > before


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_flip_on_propagates_to_metadata(ctx, spec):
    # Tests the case where a field with existing values is switched to
    # unique: the denormalized flag reaches every row already written through
    # it, so the indexes describe the field's current policy.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="flip-on")
    meta_idx = await _write_value(
        ctx,
        spec,
        entity_idx=entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )
    before = await ctx["pool"].fetchval(
        f"SELECT updated_at FROM {spec.metadata_table} WHERE idx = $1", meta_idx
    )

    await _set_unique_in_study(ctx, spec, field_idx, True)

    assert await _read_flags(ctx, spec, field_idx) == (True, [True])
    after = await ctx["pool"].fetchval(
        f"SELECT updated_at FROM {spec.metadata_table} WHERE idx = $1", meta_idx
    )
    assert after > before


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_flip_off_propagates_to_metadata(ctx, spec):
    # Tests the case where a unique field is relaxed: the flag is cleared on
    # its metadata rows, so duplicates become writable again.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_flagged_field(
        ctx, spec, study_idx=ctx["study_idx"], data_type=FieldDataType.TEXT, suffix="flip-off"
    )
    await _write_value(
        ctx,
        spec,
        entity_idx=entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )

    await _set_unique_in_study(ctx, spec, field_idx, False)

    assert await _read_flags(ctx, spec, field_idx) == (False, [False])
    # The relaxed field now accepts the value a second time.
    await _write_value(
        ctx,
        spec,
        entity_idx=second_entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_flip_on_rejected_when_duplicates_exist(ctx, spec):
    # Tests the case where a field already holding a repeated value is
    # switched to unique: the propagation trips the partial unique index and
    # the flag change rolls back rather than leaving the policy half-applied.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="dup-flip")
    for entity_idx in (first_entity_idx, second_entity_idx):
        await _write_value(
            ctx,
            spec,
            entity_idx=entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="Sample 1",
        )

    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        await _set_unique_in_study(ctx, spec, field_idx, True)

    # Name the constraint: the study field's own eligibility CHECK would also
    # refuse a flag change, and this must be the metadata index instead.
    assert excinfo.value.constraint_name in spec.unique_in_study_index_names
    assert await _read_flags(ctx, spec, field_idx) == (False, [False, False])


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_unique_in_study_flip_on_rejected_when_missing_marker_exists(ctx, spec):
    # Tests the case where a field holding a missing-value marker is switched
    # to unique: the propagation trips the no-missing-value CHECK and rolls
    # back, so the field cannot claim to identify samples it has not named.
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_plain_field(ctx, spec, suffix="miss-flip")

    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    await _write_value(
        ctx,
        spec,
        entity_idx=entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value=MissingReasonRef(idx=reason_idx, name=reason_name),
    )

    with pytest.raises(asyncpg.CheckViolationError) as excinfo:
        await _set_unique_in_study(ctx, spec, field_idx, True)

    assert excinfo.value.constraint_name == spec.unique_in_study_no_missing_constraint
    assert await _read_flags(ctx, spec, field_idx) == (False, [False])


# =============================================================================
# update_study_field, the locking read, and the violation classifier
# =============================================================================


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_update_study_field_writes_and_returns_resolved_row(ctx, spec):
    # Tests the case where a purely-local field is edited: the write lands and
    # the returned row carries the full read shape, not the flat RETURNING the
    # shared composer would give on its own.
    field_idx = await _create_plain_field(ctx, spec, suffix="upd")

    async with ctx["pool"].acquire() as conn, conn.transaction():
        updated = await update_study_field(
            conn,
            spec=spec,
            idx=field_idx,
            fields={"description": "edited", "unique_in_study": True},
        )

    assert updated["description"] == "edited"
    assert updated["unique_in_study"] is True
    # The resolved shape, not the composer's RETURNING list.
    assert updated["data_type"] == FieldDataType.TEXT
    assert updated["updated_at"] is not None


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_update_study_field_returns_none_for_absent_row(ctx, spec):
    # Tests the case where the row vanished between a caller's preflight and
    # its write: the composer matches nothing and the absence is reported
    # rather than masked as a successful no-op.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        updated = await update_study_field(
            conn, spec=spec, idx=2_000_000_000, fields={"description": "edited"}
        )

    assert updated is None


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_fetch_study_field_for_update_returns_the_row(ctx, spec):
    # Tests the case where a preflight locks the row it is about to edit: the
    # locking read returns the same shape the plain read does.
    field_idx = await _create_plain_field(ctx, spec, suffix="lock")

    async with ctx["pool"].acquire() as conn, conn.transaction():
        locked = await fetch_study_field(conn, spec=spec, idx=field_idx, for_update=True)
    plain = await fetch_study_field(ctx["pool"], spec=spec, idx=field_idx)

    assert dict(locked) == dict(plain)


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_classify_unique_in_study_violation_names_each_rule(ctx, spec):
    # Tests the case where each of the two study-local uniqueness rules is
    # broken: the classifier names which one, so a caller can word its own
    # answer without re-deriving the constraint names.
    first_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    second_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    field_idx = await _create_flagged_field(
        ctx, spec, study_idx=ctx["study_idx"], data_type=FieldDataType.TEXT, suffix="classify"
    )
    await _write_value(
        ctx,
        spec,
        entity_idx=first_entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value="Sample 1",
    )

    with pytest.raises(asyncpg.UniqueViolationError) as dup_exc:
        await _write_value(
            ctx,
            spec,
            entity_idx=second_entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value="Sample 1",
        )

    reason_name = f"reason_{secrets.token_hex(4)}"
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        reason_name,
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    with pytest.raises(asyncpg.CheckViolationError) as missing_exc:
        await _write_value(
            ctx,
            spec,
            entity_idx=second_entity_idx,
            field_idx=field_idx,
            data_type=FieldDataType.TEXT,
            value=MissingReasonRef(idx=reason_idx, name=reason_name),
        )

    assert (
        classify_unique_in_study_violation(dup_exc.value, spec=spec)
        is UniqueInStudyViolation.DUPLICATE_VALUE
    )
    assert (
        classify_unique_in_study_violation(missing_exc.value, spec=spec)
        is UniqueInStudyViolation.MISSING_VALUE_MARKER
    )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_classify_unique_in_study_violation_ignores_other_constraints(ctx, spec):
    # Tests the case where an unrelated constraint fires: the classifier
    # reports neither rule, so a caller re-raises instead of answering for a
    # cause it did not diagnose.
    display_name = unique_field_name("collide")
    await _create_plain_field(ctx, spec, suffix="collide-a")
    async with ctx["pool"].acquire() as conn, conn.transaction():
        first_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(first_idx)
    second_idx = await _create_plain_field(ctx, spec, suffix="collide-b")

    # Rename the second field onto the first's name: the (study_idx,
    # display_name) unique constraint, which is neither uniqueness rule.
    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        await ctx["pool"].execute(
            f"UPDATE {spec.study_field_table} SET display_name = $1 WHERE idx = $2",
            display_name,
            second_idx,
        )

    assert classify_unique_in_study_violation(excinfo.value, spec=spec) is None


# ---------------------------------------------------------------------------
# Overwriting a value on a unique_in_study field
# ---------------------------------------------------------------------------


async def _write_local(ctx, spec, *, entity_idx, display_name, value, on_conflict="raise"):
    """Drive one study-local metadata write for `spec` in its own transaction."""
    async with ctx["pool"].acquire() as conn, conn.transaction():
        result = await write_local_metadata_or_diagnose(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            data_type=FieldDataType.TEXT,
            value=value,
            caller_idx=ctx["principal_idx"],
            on_conflict=on_conflict,
        )
    ctx["created"][_metadata_tracking_key(spec)].append(result.metadata_idx)
    return result


async def _seed_unique_field(ctx, spec, *, suffix):
    """Create one purely-local unique_in_study text field; return its display name."""
    display_name = unique_field_name(suffix)
    async with ctx["pool"].acquire() as conn, conn.transaction():
        field_idx, _, _ = await _get_or_create_local_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
            data_type=FieldDataType.TEXT,
            required=False,
            unique_in_study=True,
        )
    ctx["created"][_study_field_tracking_key(spec)].append(field_idx)
    return display_name, field_idx


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_upsert_onto_duplicate_value_raises_typed_error(ctx, spec):
    # Tests the case where a sample that already holds a value is overwritten
    # to a value another sample in the study holds: the overwrite trips the
    # uniqueness index the insert never reached, and is translated the same way.
    display_name, field_idx = await _seed_unique_field(ctx, spec, suffix="upsert-dup")
    holder_idx = await _create_linked_entity_for_spec(ctx, spec)
    writer_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _write_local(ctx, spec, entity_idx=holder_idx, display_name=display_name, value="TAKEN")
    await _write_local(ctx, spec, entity_idx=writer_idx, display_name=display_name, value="MINE")

    with pytest.raises(StudyUniqueValueConflictError) as excinfo:
        await _write_local(
            ctx,
            spec,
            entity_idx=writer_idx,
            display_name=display_name,
            value="TAKEN",
            on_conflict="upsert",
        )

    assert excinfo.value.display_name == display_name
    assert excinfo.value.study_field_idx == field_idx
    assert excinfo.value.attempted_value == "TAKEN"


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_upsert_onto_free_value_overwrites(ctx, spec):
    # Control for the two cases above: the same overwrite path succeeds when
    # the new value collides with nothing, so their failures are the
    # uniqueness rules and not the overwrite itself.
    display_name, _ = await _seed_unique_field(ctx, spec, suffix="upsert-free")
    writer_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _write_local(ctx, spec, entity_idx=writer_idx, display_name=display_name, value="MINE")

    result = await _write_local(
        ctx,
        spec,
        entity_idx=writer_idx,
        display_name=display_name,
        value="FRESH",
        on_conflict="upsert",
    )

    assert result.outcome is FieldWriteOutcome.UPDATED
    stored = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {spec.metadata_table} WHERE idx = $1", result.metadata_idx
    )
    assert stored == "FRESH"


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_fetch_study_field_for_update_locks_the_row(ctx, spec):
    # Tests the case where a second reader wants the row a preflight is holding:
    # the lock is what stops two edits both clearing the same ETag, so NOWAIT
    # from a second connection must be refused rather than served.
    field_idx = await _create_plain_field(ctx, spec, suffix="lock-excl")
    nowait_sql = f"SELECT idx FROM {spec.study_field_table} WHERE idx = $1 FOR UPDATE NOWAIT"

    async with ctx["pool"].acquire() as holder, holder.transaction():
        await fetch_study_field(holder, spec=spec, idx=field_idx, for_update=True)
        async with ctx["pool"].acquire() as other, other.transaction():
            with pytest.raises(asyncpg.LockNotAvailableError):
                await other.fetchval(nowait_sql, field_idx)

    # Control: with the holder's transaction closed, the same read is served,
    # so the refusal above is the lock and not a broken query.
    async with ctx["pool"].acquire() as after, after.transaction():
        assert await after.fetchval(nowait_sql, field_idx) == field_idx


# =============================================================================
# The migration that brings pre-rule owner-id fields up to unique_in_study
#
# Biosample-only, so these are not parameterized over both stacks: the owner
# biosample id lives on biosample_metadata and the prep_sample tables carry no
# counterpart. The migration flips the field flag, and everything that decides
# whether the flip lands -- the propagation trigger and the partial unique
# index -- is the subject of this module.
# =============================================================================


def _owner_id_migration_sql():
    """The migration's `migrate:up` body, read from the file so the test tracks
    the real migration rather than a hand-copied duplicate."""
    path = (
        Path(__file__).resolve().parents[2]
        / "db"
        / "migrations"
        / "20260911000000_owner_biosample_id_unique_in_study.sql"
    )
    text = path.read_text()
    return text.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)[0].strip()


async def _flag_as_owner_id(ctx, metadata_idx):
    """Mark one metadata row as the owner's identifier for its biosample. The
    flag is application-maintained, and the path that sets it now also mints the
    field with unique_in_study, so the pre-rule state is reachable only by raw
    UPDATE."""
    await ctx["pool"].execute(
        "UPDATE qiita.biosample_metadata SET is_owner_biosample_id = true WHERE idx = $1",
        metadata_idx,
    )


async def _write_owner_id(ctx, *, field_idx, value):
    """Write one value through `field_idx` and mark it an owner id. Returns the
    metadata row idx."""
    spec = BIOSAMPLE_METADATA_SPEC
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    metadata_idx = await _write_value(
        ctx,
        spec,
        entity_idx=entity_idx,
        field_idx=field_idx,
        data_type=FieldDataType.TEXT,
        value=value,
    )
    await _flag_as_owner_id(ctx, metadata_idx)
    return metadata_idx


async def _seed_owner_id_migration_cases(ctx):
    """Seed one field per case the migration's predicate distinguishes: a local
    field carrying an owner id, a local field carrying none, a globally-linked
    field carrying one, and a field the study already declared unique.

    Each owner-id row goes on its own biosample: at most one row per biosample
    may carry the flag.
    """
    spec = BIOSAMPLE_METADATA_SPEC

    owner_field = await _create_plain_field(ctx, spec, suffix="owner-id")
    owner_metadata_idx = await _write_owner_id(ctx, field_idx=owner_field, value="OWNER-1")

    # A field whose value is not an owner id: the marker, not the field's name
    # or its locality, is what the predicate reads.
    plain_field = await _create_plain_field(ctx, spec, suffix="no-owner-id")
    plain_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _write_value(
        ctx,
        spec,
        entity_idx=plain_entity_idx,
        field_idx=plain_field,
        data_type=FieldDataType.TEXT,
        value="42",
    )

    # A globally-linked field carrying an owner id. The write path refuses to
    # put one through a linked field, but a field that once held them could have
    # been upgraded since.
    gf = await _seed_global_field_for_spec(ctx, spec, data_type=FieldDataType.TEXT)
    async with ctx["pool"].acquire() as conn, conn.transaction():
        linked_field, _ = await _get_or_create_globally_linked_study_field(
            conn,
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=gf.idx,
            display_name=unique_field_name("linked-owner-id"),
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"][_study_field_tracking_key(spec)].append(linked_field)
    await _write_owner_id(ctx, field_idx=linked_field, value="OWNER-LINKED")

    # An owner-id field a study already declared unique through the edit route.
    _, already_field = await _seed_unique_field(ctx, spec, suffix="prior-owner-id")
    await _write_owner_id(ctx, field_idx=already_field, value="PRIOR")

    return {
        "owner_field": owner_field,
        "plain_field": plain_field,
        "linked_field": linked_field,
        "already_field": already_field,
        "owner_metadata_idx": owner_metadata_idx,
    }


async def _flags_by_case(ctx, cases):
    """The unique_in_study flag of every seeded field, keyed by the case it
    stands for."""
    case_names = ("owner_field", "plain_field", "linked_field", "already_field")
    rows = await ctx["pool"].fetch(
        "SELECT idx, unique_in_study FROM qiita.biosample_study_field WHERE study_idx = $1",
        ctx["study_idx"],
    )
    by_idx = {row["idx"]: row["unique_in_study"] for row in rows}
    return {name: by_idx[cases[name]] for name in case_names}


async def test_owner_id_migration_flags_only_local_owner_id_fields(ctx):
    # Tests the case where one study holds all four kinds of field: only the
    # purely-local field carrying an owner id is changed, and the other three --
    # no owner id, globally linked, already declared unique -- come through as
    # they went in.
    cases = await _seed_owner_id_migration_cases(ctx)
    assert await _flags_by_case(ctx, cases) == {
        "owner_field": False,
        "plain_field": False,
        "linked_field": False,
        "already_field": True,
    }

    await ctx["pool"].execute(_owner_id_migration_sql())

    assert await _flags_by_case(ctx, cases) == {
        "owner_field": True,
        "plain_field": False,
        "linked_field": False,
        "already_field": True,
    }


async def test_owner_id_migration_propagates_the_flag_to_the_fields_values(ctx):
    # Tests the case where the flipped field already holds values: the policy
    # reaches each of them, which is what makes the partial unique index govern
    # the rows written before the flip.
    cases = await _seed_owner_id_migration_cases(ctx)

    await ctx["pool"].execute(_owner_id_migration_sql())

    flagged = await ctx["pool"].fetchval(
        "SELECT unique_in_study FROM qiita.biosample_metadata WHERE idx = $1",
        cases["owner_metadata_idx"],
    )
    assert flagged is True


async def test_owner_id_migration_is_idempotent(ctx):
    # Tests the case where the statement is replayed -- by a re-run after a
    # partial deploy, or by a later hand-run: the second pass matches nothing,
    # since NOT unique_in_study excludes what the first pass set.
    cases = await _seed_owner_id_migration_cases(ctx)

    await ctx["pool"].execute(_owner_id_migration_sql())
    await ctx["pool"].execute(_owner_id_migration_sql())

    assert await _flags_by_case(ctx, cases) == {
        "owner_field": True,
        "plain_field": False,
        "linked_field": False,
        "already_field": True,
    }


async def test_owner_id_migration_aborts_when_two_samples_share_an_owner_id(ctx):
    # Tests the case where a study's samples already answer to the same owner
    # id: the partial unique index rejects the propagated flag and the whole
    # statement rolls back, which is the migration's intended report about the
    # data rather than a defect in it.
    cases = await _seed_owner_id_migration_cases(ctx)
    await _write_owner_id(ctx, field_idx=cases["owner_field"], value="OWNER-1")

    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        await ctx["pool"].execute(_owner_id_migration_sql())

    assert excinfo.value.constraint_name == "biosample_metadata_unique_in_study_text"
    assert await _flags_by_case(ctx, cases) == {
        "owner_field": False,
        "plain_field": False,
        "linked_field": False,
        "already_field": True,
    }


async def _publish_prep_for_biosample(ctx, biosample_idx):
    """Give `biosample_idx` a sequenced prep_sample whose study link is
    published, which freezes that biosample's metadata rows."""
    prep_sample_idx = await seed_sequenced_prep_sample(
        ctx["pool"],
        biosample_idx=biosample_idx,
        owner_idx=ctx["principal_idx"],
    )
    async with ctx["pool"].acquire() as conn, conn.transaction():
        await insert_entity_to_study(
            conn,
            spec=PREP_SAMPLE_METADATA_SPEC,
            entity_idx=prep_sample_idx,
            study_idx=ctx["study_idx"],
            created_by_idx=ctx["principal_idx"],
        )
    ctx["created"]["prep_sample"].append(prep_sample_idx)
    ctx["created"]["prep_sample_to_study"].append((prep_sample_idx, ctx["study_idx"]))

    # The publish action, which no write path performs yet.
    await ctx["pool"].execute(
        "UPDATE qiita.prep_sample_to_study SET is_published = true"
        " WHERE prep_sample_idx = $1 AND study_idx = $2",
        prep_sample_idx,
        ctx["study_idx"],
    )
    return prep_sample_idx


_UNCHANGED_MIGRATION_FLAGS = {
    "owner_field": False,
    "plain_field": False,
    "linked_field": False,
    "already_field": True,
}


async def test_owner_id_migration_aborts_on_a_non_owner_duplicate(ctx):
    # Tests the case where the repeated value is not itself an owner id: the
    # flag reaches every row written through the field, so an ordinary value
    # matching an owner id collides exactly as a second owner id would.
    spec = BIOSAMPLE_METADATA_SPEC
    cases = await _seed_owner_id_migration_cases(ctx)
    other_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _write_value(
        ctx,
        spec,
        entity_idx=other_entity_idx,
        field_idx=cases["owner_field"],
        data_type=FieldDataType.TEXT,
        value="OWNER-1",
    )

    with pytest.raises(asyncpg.UniqueViolationError) as excinfo:
        await ctx["pool"].execute(_owner_id_migration_sql())

    assert excinfo.value.constraint_name == "biosample_metadata_unique_in_study_text"
    assert await _flags_by_case(ctx, cases) == _UNCHANGED_MIGRATION_FLAGS


async def test_owner_id_migration_aborts_on_a_missing_value_marker(ctx):
    # Tests the case where another row in the owner-id field declines to give a
    # value: the no-missing-value CHECK rejects the propagated flag, so the
    # abort reports a rule the duplicate-value query would never surface.
    spec = BIOSAMPLE_METADATA_SPEC
    cases = await _seed_owner_id_migration_cases(ctx)
    reason_idx = await ctx["pool"].fetchval(
        "INSERT INTO qiita.missing_value_reason (name) VALUES ($1) RETURNING idx",
        f"reason_{secrets.token_hex(4)}",
    )
    ctx["created"]["missing_value_reason"].append(reason_idx)
    other_entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    await _write_value(
        ctx,
        spec,
        entity_idx=other_entity_idx,
        field_idx=cases["owner_field"],
        data_type=FieldDataType.TEXT,
        value=MissingReasonRef(idx=reason_idx, name="not applicable"),
    )

    with pytest.raises(asyncpg.CheckViolationError) as excinfo:
        await ctx["pool"].execute(_owner_id_migration_sql())

    assert excinfo.value.constraint_name == "biosample_metadata_unique_in_study_no_missing_value"
    assert await _flags_by_case(ctx, cases) == _UNCHANGED_MIGRATION_FLAGS


async def test_owner_id_migration_aborts_on_a_published_biosample(ctx):
    # Tests the case where a row in the owner-id field sits on a biosample a
    # published prep freezes: the propagated UPDATE trips the publication lock,
    # which no data change can resolve, unlike the other two abort causes.
    cases = await _seed_owner_id_migration_cases(ctx)
    biosample_idx = await ctx["pool"].fetchval(
        "SELECT biosample_idx FROM qiita.biosample_metadata WHERE idx = $1",
        cases["owner_metadata_idx"],
    )
    await _publish_prep_for_biosample(ctx, biosample_idx)

    with pytest.raises(asyncpg.RaiseError) as excinfo:
        await ctx["pool"].execute(_owner_id_migration_sql())

    assert excinfo.value.sqlstate == "P0001"
    assert "published prep_sample" in str(excinfo.value)
    assert await _flags_by_case(ctx, cases) == _UNCHANGED_MIGRATION_FLAGS
