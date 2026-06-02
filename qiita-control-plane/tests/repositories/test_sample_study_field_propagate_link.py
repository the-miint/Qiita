"""Integration tests for the *_study_field_propagate_global_link triggers.

Covers the structurally identical
biosample_study_field_propagate_global_link and
prep_sample_study_field_propagate_global_link triggers in one suite,
parameterized over EntityMetadataSpec so every transition branch
(upgrade, unlink-empty, unlink-with-metadata-raises, rebind-raises) is
exercised against both the biosample and prep_sample stacks.

The SQL UPDATE/SELECT statements that drive the triggers interpolate
identifiers from frozen module-level spec fields (study_field_table,
study_field_global_fk_column, metadata_table); the spec carries every
identifier that differs between the two stacks.
"""

import asyncpg
import pytest
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories._sample_helpers import (
    _get_or_create_globally_linked_study_field,
    _get_or_create_local_study_field,
    _insert_metadata,
)
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.prep_sample_metadata import PREP_SAMPLE_METADATA_SPEC
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import (
    _create_linked_entity_for_spec,
    _seed_global_field_for_spec,
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
