"""DB-bound tests: a sample-metadata write refuses what the caller's tier
cannot read.

Every field that the body names must require no more than the caller's tier on
the study, and so must every value that the write would touch: an exception on
one value can hide it even when its field is reachable. The rule is defined in
docs/architecture/data-model.md, "Metadata visibility tiers".
"""

import secrets

import pytest
from qiita_common.models import FieldDataType, Tier

from qiita_control_plane.ena_import.registration import register_ena_study
from qiita_control_plane.repositories._sample_helpers import (
    EntityMetadataSpec,
    MetadataFieldTierError,
    MetadataParseError,
    MetadataValueTierError,
    SampleEntityKind,
    preflight_sample_metadata,
    write_resolved_metadata_entries,
    write_sample_metadata,
)
from qiita_control_plane.repositories.biosample import import_biosample_from_owner_biosample_id
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC
from qiita_control_plane.repositories.sequenced_sample import import_sequenced_prep_sample
from qiita_control_plane.repositories.study_access import CallerMetadataTiers
from qiita_control_plane.testing.db_seeds import (
    seed_field_exception,
    seed_global_field_for_spec,
    seed_globally_linked_study_field,
    seed_local_study_field,
    seed_metadata_text_value,
)
from qiita_control_plane.testing.tiers import WRITE_CHECKS, apply_write_checks
from qiita_control_plane.testing.unique_names import unique_field_name

from .conftest import SPECS, _create_linked_entity_for_spec, _spec_id

pytestmark = pytest.mark.db


def _tiers_on(study_idx: int, tier: Tier) -> CallerMetadataTiers:
    """A caller holding `tier` on one study and nothing anywhere else."""
    return CallerMetadataTiers(study_tiers={study_idx: tier}, default=Tier.PUBLIC)


async def _local_field(
    ctx, spec: EntityMetadataSpec, *, tier_override, data_type=FieldDataType.TEXT
) -> tuple[int, str]:
    """Seed a purely-local field on ctx's study; return (idx, display_name)."""
    display_name = unique_field_name("tier_write_local")
    field_idx = await seed_local_study_field(
        ctx["pool"],
        spec=spec,
        study_idx=ctx["study_idx"],
        display_name=display_name,
        created_by_idx=ctx["principal_idx"],
        data_type=data_type,
        tier_override=tier_override,
    )
    return field_idx, display_name


async def _global_field(
    ctx, spec: EntityMetadataSpec, *, default_tier: Tier, alias: bool
) -> tuple[int, str, int | None]:
    """Seed a text global field at default_tier, tracked for cleanup.

    Returns (global_field_idx, display_name, alias_idx). With alias, the field
    is also linked on ctx's study under its own display_name; without, the
    study has no row for it yet and a write would mint one.
    """
    suffix = secrets.token_hex(4)
    display_name = f"Tier Write {suffix}"
    global_field_idx = await seed_global_field_for_spec(
        ctx["pool"],
        spec=spec,
        internal_name=f"tier_write_{suffix}",
        display_name=display_name,
        data_type=FieldDataType.TEXT,
        created_by_idx=ctx["principal_idx"],
        default_tier=default_tier,
    )
    table_key = (
        "biosample_global_field"
        if spec.entity_kind is SampleEntityKind.BIOSAMPLE
        else "prep_sample_global_field"
    )
    ctx["created"][table_key].append(global_field_idx)
    alias_idx = None
    if alias:
        alias_idx = await seed_globally_linked_study_field(
            ctx["pool"],
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_field_idx,
            display_name=display_name,
            created_by_idx=ctx["principal_idx"],
        )
    return global_field_idx, display_name, alias_idx


async def _metadata_rows(ctx, spec: EntityMetadataSpec, entity_idx: int) -> list[dict]:
    rows = await ctx["pool"].fetch(
        f"SELECT idx, global_field_idx, value_text, updated_at FROM {spec.metadata_table}"
        f" WHERE {spec.entity_key_column} = $1 ORDER BY idx",
        entity_idx,
    )
    return [dict(r) for r in rows]


async def _write(ctx, spec, *, entity_idx, metadata, tiers):
    """One write in its own transaction, as a route issues it."""
    async with ctx["pool"].acquire() as conn, conn.transaction():
        return await write_sample_metadata(
            conn,
            spec=spec,
            entity_idx=entity_idx,
            study_idx=ctx["study_idx"],
            metadata=metadata,
            caller_idx=ctx["principal_idx"],
            allow_local=True,
            on_conflict="upsert",
            tiers=tiers,
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_preflight_refuses_fields_above_caller_tier(ctx, spec):
    """A member naming a field that requires admin is refused before anything
    is written, and the refusal names only that field and the tier it needs."""
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    _, l_admin = await _local_field(ctx, spec, tier_override=Tier.ADMIN)
    _, l_viewer = await _local_field(ctx, spec, tier_override=Tier.VIEWER)
    _, g_member, _ = await _global_field(ctx, spec, default_tier=Tier.MEMBER, alias=True)
    before = await _metadata_rows(ctx, spec, entity_idx)

    with pytest.raises(MetadataFieldTierError) as excinfo:
        await _write(
            ctx,
            spec,
            entity_idx=entity_idx,
            metadata={l_admin: "a", g_member: "b", l_viewer: "c"},
            tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
        )

    assert excinfo.value.refused == {l_admin: Tier.ADMIN}
    assert excinfo.value.study_idx == ctx["study_idx"]
    assert excinfo.value.caller_tier == Tier.MEMBER
    assert await _metadata_rows(ctx, spec, entity_idx) == before


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_preflight_tier_check_covers_an_unaliased_global_key(ctx, spec):
    """A global field the study has not linked yet is judged on its own
    default tier, not let through because the study has no row for it."""
    _, g_admin, _ = await _global_field(ctx, spec, default_tier=Tier.ADMIN, alias=False)

    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(MetadataFieldTierError) as excinfo:
            await preflight_sample_metadata(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                metadata={g_admin: "x"},
                allow_local=True,
                tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
            )

    assert excinfo.value.refused == {g_admin: Tier.ADMIN}


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_preflight_tier_check_precedes_parsing(ctx, spec):
    """A value that would not parse against a field the caller cannot reach is
    refused for the field's tier, so the answer says nothing about the field's
    type."""
    _, l_admin_numeric = await _local_field(
        ctx, spec, tier_override=Tier.ADMIN, data_type=FieldDataType.NUMERIC
    )

    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(MetadataFieldTierError):
            await preflight_sample_metadata(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                metadata={l_admin_numeric: "not a number"},
                allow_local=True,
                tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
            )

    # The same body from an admin reaches the parse and fails there instead.
    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(MetadataParseError):
            await preflight_sample_metadata(
                conn,
                spec=spec,
                study_idx=ctx["study_idx"],
                metadata={l_admin_numeric: "not a number"},
                allow_local=True,
                tiers=_tiers_on(ctx["study_idx"], Tier.ADMIN),
            )


async def _public_value_under_exception(ctx, spec, *, exception_tier=Tier.ADMIN):
    """An entity holding a value on a public global field, written through
    ctx's study, with an exception at exception_tier on that value.

    Returns (entity_idx, display_name, metadata_idx).
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    global_field_idx, display_name, alias_idx = await _global_field(
        ctx, spec, default_tier=Tier.PUBLIC, alias=True
    )
    metadata_idx = await seed_metadata_text_value(
        ctx["pool"],
        spec=spec,
        entity_idx=entity_idx,
        study_field_idx=alias_idx,
        created_by_idx=ctx["principal_idx"],
    )
    await seed_field_exception(
        ctx["pool"],
        spec=spec,
        entity_idx=entity_idx,
        global_field_idx=global_field_idx,
        tier_override=exception_tier,
        created_by_idx=ctx["principal_idx"],
    )
    return entity_idx, display_name, metadata_idx


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize("write_checks", WRITE_CHECKS)
async def test_write_refuses_value_under_exception_and_rolls_back(
    ctx, spec, monkeypatch, write_checks
):
    """A member may write the field, but not this one value of it: the write is
    refused and the stored value and its updated_at are as they were. Under
    "post-write-only" the check of the written values refuses it alone."""
    apply_write_checks(monkeypatch, write_checks)
    entity_idx, display_name, _ = await _public_value_under_exception(ctx, spec)
    before = await _metadata_rows(ctx, spec, entity_idx)

    with pytest.raises(MetadataValueTierError) as excinfo:
        await _write(
            ctx,
            spec,
            entity_idx=entity_idx,
            metadata={display_name: "rewritten"},
            tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
        )

    assert excinfo.value.field_keys == [display_name]
    assert await _metadata_rows(ctx, spec, entity_idx) == before


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_write_refuses_an_unchanged_value_under_exception(ctx, spec):
    """Re-sending the hidden value is refused too. Answering "unchanged" would
    confirm what the value is."""
    entity_idx, display_name, metadata_idx = await _public_value_under_exception(ctx, spec)
    stored = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {spec.metadata_table} WHERE idx = $1", metadata_idx
    )

    with pytest.raises(MetadataValueTierError):
        await _write(
            ctx,
            spec,
            entity_idx=entity_idx,
            metadata={display_name: stored},
            tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
        )


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize("write_checks", WRITE_CHECKS)
async def test_write_refuses_into_empty_slot_carrying_exception(
    ctx, spec, monkeypatch, write_checks
):
    """An exception set before any value exists still governs the first value
    written into its slot. Under "post-write-only" the check of the written
    values refuses it alone."""
    apply_write_checks(monkeypatch, write_checks)
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    global_field_idx, display_name, _ = await _global_field(
        ctx, spec, default_tier=Tier.PUBLIC, alias=True
    )
    await seed_field_exception(
        ctx["pool"],
        spec=spec,
        entity_idx=entity_idx,
        global_field_idx=global_field_idx,
        tier_override=Tier.ADMIN,
        created_by_idx=ctx["principal_idx"],
    )

    with pytest.raises(MetadataValueTierError):
        await _write(
            ctx,
            spec,
            entity_idx=entity_idx,
            metadata={display_name: "first"},
            tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
        )

    assert await _metadata_rows(ctx, spec, entity_idx) == []


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
@pytest.mark.parametrize(
    "global_exception_key",
    ["global_field", "link_row", "other_link_row"],
    ids=["on-global-field", "on-link-row", "on-other-link-row"],
)
@pytest.mark.parametrize(
    ("global_held", "local_held"),
    [(True, False), (False, True), (True, True), (False, False)],
    ids=["global-held", "local-held", "both-held", "neither-held"],
)
async def test_refusal_names_the_same_keys_whether_a_restricted_value_exists(
    ctx, spec, global_exception_key, global_held, local_held
):
    """Tests the case where one body names two values under exceptions above
    the caller's tier, one on a global field and one on a local field, and a
    third value that no exception restricts. The body reaches the global field
    by its own name. Its exception is keyed on the global field, on the
    study's link row at that name, or on another of the study's rows linked to
    the global, through which the study's value is then held. The refusal
    names the two restricted keys whichever of them already holds a value, so
    it does not show which hidden values exist.
    """
    entity_idx = await _create_linked_entity_for_spec(ctx, spec)
    global_field_idx, global_name, alias_idx = await _global_field(
        ctx, spec, default_tier=Tier.PUBLIC, alias=True
    )
    local_field_idx, local_name = await _local_field(ctx, spec, tier_override=Tier.PUBLIC)
    _, open_name = await _local_field(ctx, spec, tier_override=Tier.PUBLIC)
    global_row_idx = alias_idx
    if global_exception_key == "other_link_row":
        global_row_idx = await seed_globally_linked_study_field(
            ctx["pool"],
            spec=spec,
            study_idx=ctx["study_idx"],
            global_field_idx=global_field_idx,
            display_name=f"{global_name} other",
            created_by_idx=ctx["principal_idx"],
        )
    for held, study_field_idx in ((global_held, global_row_idx), (local_held, local_field_idx)):
        if held:
            await seed_metadata_text_value(
                ctx["pool"],
                spec=spec,
                entity_idx=entity_idx,
                study_field_idx=study_field_idx,
                created_by_idx=ctx["principal_idx"],
            )
    global_key = (
        {"global_field_idx": global_field_idx}
        if global_exception_key == "global_field"
        else {"study_field_idx": global_row_idx}
    )
    for key in (global_key, {"study_field_idx": local_field_idx}):
        await seed_field_exception(
            ctx["pool"],
            spec=spec,
            entity_idx=entity_idx,
            tier_override=Tier.ADMIN,
            created_by_idx=ctx["principal_idx"],
            **key,
        )
    before = await _metadata_rows(ctx, spec, entity_idx)

    with pytest.raises(MetadataValueTierError) as refused:
        await _write(
            ctx,
            spec,
            entity_idx=entity_idx,
            metadata={global_name: "sent", open_name: "sent", local_name: "sent"},
            tiers=_tiers_on(ctx["study_idx"], Tier.MEMBER),
        )

    assert refused.value.field_keys == [global_name, local_name]
    assert await _metadata_rows(ctx, spec, entity_idx) == before


@pytest.mark.parametrize("spec", SPECS, ids=_spec_id)
async def test_write_at_the_exception_tier_lands(ctx, spec):
    """A caller whose tier meets the exception writes the value."""
    entity_idx, display_name, metadata_idx = await _public_value_under_exception(ctx, spec)

    await _write(
        ctx,
        spec,
        entity_idx=entity_idx,
        metadata={display_name: "rewritten"},
        tiers=_tiers_on(ctx["study_idx"], Tier.ADMIN),
    )

    stored = await ctx["pool"].fetchval(
        f"SELECT value_text FROM {spec.metadata_table} WHERE idx = $1", metadata_idx
    )
    assert stored == "rewritten"


async def test_write_requires_a_transaction(ctx):
    """The values are checked again once written, and a refusal then undoes
    nothing outside a transaction, so the write refuses to start without one."""
    async with ctx["pool"].acquire() as conn:
        with pytest.raises(RuntimeError, match="transaction"):
            await write_resolved_metadata_entries(
                conn,
                spec=BIOSAMPLE_METADATA_SPEC,
                entity_idx=1,
                study_idx=ctx["study_idx"],
                caller_idx=ctx["principal_idx"],
                resolved_metadata=[],
                tiers=_tiers_on(ctx["study_idx"], Tier.ADMIN),
            )


async def test_imports_refuse_tiers_below_admin(ctx):
    """An import requires admin on its study, so each import refuses, before it
    writes anything, tiers that do not hold admin there."""
    member = _tiers_on(ctx["study_idx"], Tier.MEMBER)
    caller_idx = ctx["principal_idx"]

    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="an import needs"):
            await import_biosample_from_owner_biosample_id(
                conn,
                primary_study_idx=ctx["study_idx"],
                owner_idx=caller_idx,
                owner_biosample_id_field_name=unique_field_name("tier_write_owner_id"),
                owner_biosample_id_value="unused",
                caller_idx=caller_idx,
                metadata={},
                tiers=member,
            )
    async with ctx["pool"].acquire() as conn, conn.transaction():
        with pytest.raises(RuntimeError, match="an import needs"):
            await import_sequenced_prep_sample(
                conn,
                sequenced_pool_idx=1,
                biosample_idx=1,
                prep_protocol_idx=1,
                owner_idx=caller_idx,
                sequenced_pool_item_id="unused",
                metadata={},
                primary_study_idx=ctx["study_idx"],
                caller_idx=caller_idx,
                tiers=member,
            )
    # Refused before it reads any of its inputs, so none are built.
    with pytest.raises(RuntimeError, match="an import needs"):
        await register_ena_study(
            ctx["pool"],
            study_idx=ctx["study_idx"],
            study_header=None,
            ena_runs=[],
            sample_attributes=[],
            owner_idx=caller_idx,
            caller_idx=caller_idx,
            tiers=member,
        )
