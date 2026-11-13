"""Shared bodies of the field-exception routes: read, set and clear the
exception that restricts one biosample's or prep_sample's value of one study
field (docs/architecture/data-model.md, "Metadata visibility tiers").

Each entity's routes are thin bindings over these; the contract is stated here.
In every detail the field is named by its own entity kind (`spec.entity_kind`)
and the sample by the noun that its route uses for it, since a sequenced_sample's
metadata lives on its prep_sample.
"""

from collections.abc import Awaitable, Callable
from typing import NamedTuple, NoReturn

import asyncpg
from fastapi import HTTPException
from qiita_common.models import (
    SampleFieldExceptionPutRequest,
    SampleFieldExceptionResponse,
    Tier,
)

from ..auth.guards import resource_retired_detail, tier_at_least
from ..repositories._sample_helpers import (
    EntityMetadataSpec,
    FieldExceptionSlot,
    delete_field_exception,
    fetch_field_exception,
    fetch_global_value_study_idx,
    fetch_study_field_tier,
    insert_field_exception,
    update_field_exception,
)
from ..repositories.study_access import CallerMetadataTiers
from ._helpers import (
    detail_for_unlinked_entity,
    etag_for_updated_at,
    fetch_study_field_in_study,
    require_etag_match,
    require_if_match,
    resolve_linked_study_entity,
)


class _Target(NamedTuple):
    slot: FieldExceptionSlot
    field: asyncpg.Record
    # A field linked to a global field whose value on the entity no study has
    # written yet.
    linked_slot_empty: bool


def _field_label(spec: EntityMetadataSpec, field: asyncpg.Record) -> str:
    return f"{spec.entity_kind} field {field['display_name']!r}"


async def _resolve_target(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    study_field_idx: int,
    noun: str,
    tiers: CallerMetadataTiers,
    write: bool,
) -> _Target:
    """Resolve the (entity, field) slot that a field-exception route
    addresses, and refuse a caller who may not act on it.

    The field must be one of the study's (else 404, as a field read answers)
    and the entity linked to the study (404; a retired entity is 404 to a read
    and 409 to a write). A linked field's exception keys on the global field
    and follows the value into every study that links it, so when another study
    wrote that value, the caller also needs admin there: the exception decides
    who reads that study's value, and its reason may describe the value. That
    403 comes before any read of the exception itself, so it is the same on
    every verb and whether or not an exception is set, and it names neither
    the study nor the reason.
    """
    field = await fetch_study_field_in_study(
        conn, spec=spec, study_idx=study_idx, study_field_idx=study_field_idx
    )
    if field is None:
        raise HTTPException(
            status_code=404, detail=f"{spec.entity_kind} field {study_field_idx} not found"
        )
    _row, metadata_entity_idx = await resolve_linked_study_entity(
        conn,
        spec=spec,
        fetch_row=fetch_row,
        entity_idx=entity_idx,
        metadata_idx_column=metadata_idx_column,
        study_idx=study_idx,
        noun=noun,
        retired_status=409 if write else 404,
        retired_detail=(
            resource_retired_detail(noun, entity_idx) if write else f"{noun} {entity_idx} not found"
        ),
        unlinked_detail=detail_for_unlinked_entity(
            noun=noun, entity_idx=entity_idx, study_idx=study_idx
        ),
    )
    global_field_idx = field[spec.study_field_global_fk_column]
    if global_field_idx is None:
        slot = FieldExceptionSlot(
            entity_idx=metadata_entity_idx, global_field_idx=None, study_field_idx=study_field_idx
        )
        return _Target(slot=slot, field=field, linked_slot_empty=False)

    value_study_idx = await fetch_global_value_study_idx(
        conn, spec=spec, entity_idx=metadata_entity_idx, global_field_idx=global_field_idx
    )
    if value_study_idx is not None and not tier_at_least(
        tiers.tier_on(value_study_idx), Tier.ADMIN
    ):
        raise HTTPException(
            status_code=403,
            detail=(
                f"{_field_label(spec, field)}: another study wrote the value on this {noun},"
                " so its field exception needs admin on that study"
            ),
        )
    slot = FieldExceptionSlot(
        entity_idx=metadata_entity_idx, global_field_idx=global_field_idx, study_field_idx=None
    )
    return _Target(slot=slot, field=field, linked_slot_empty=value_study_idx is None)


def _raise_not_set(
    spec: EntityMetadataSpec, field: asyncpg.Record, *, noun: str, entity_idx: int
) -> NoReturn:
    raise HTTPException(
        status_code=404,
        detail=f"no field exception is set on {_field_label(spec, field)} of {noun} {entity_idx}",
    )


def _raise_already_set(spec: EntityMetadataSpec, field: asyncpg.Record, *, noun: str) -> NoReturn:
    raise HTTPException(
        status_code=428,
        detail=(
            f"{_field_label(spec, field)} already carries a field exception on this {noun};"
            " replacing it needs If-Match"
        ),
    )


def _raise_on_unique_field(spec: EntityMetadataSpec, field: asyncpg.Record) -> NoReturn:
    # The migration that makes an exception and unique_in_study exclude each
    # other says why.
    raise HTTPException(
        status_code=409,
        detail=(
            f"{_field_label(spec, field)} declares unique_in_study;"
            " a field exception cannot be set on it"
        ),
    )


def _raise_vanished(spec: EntityMetadataSpec, slot: FieldExceptionSlot) -> NoReturn:
    raise RuntimeError(f"{spec.field_exception_table} row for {slot} vanished under its row lock")


async def _lock_matching(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    target: _Target,
    if_match: str,
    noun: str,
    entity_idx: int,
) -> asyncpg.Record:
    """Lock the exception on the target's slot to commit and check If-Match
    against it: none set is 404, a stale tag 412."""
    row = await fetch_field_exception(conn, spec=spec, slot=target.slot, for_update=True)
    if row is None:
        _raise_not_set(spec, target.field, noun=noun, entity_idx=entity_idx)
    require_etag_match(row, if_match=if_match, label=f"{noun} field exception", row_idx=entity_idx)
    return row


def _map(
    row: asyncpg.Record, *, slot: FieldExceptionSlot
) -> tuple[SampleFieldExceptionResponse, str]:
    """Shape one exception row, and return it with its ETag."""
    response = SampleFieldExceptionResponse.model_validate({**dict(row), "keyed_on": slot.keyed_on})
    return response, etag_for_updated_at(row["updated_at"])


async def read_and_map_field_exception(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    study_field_idx: int,
    noun: str,
    tiers: CallerMetadataTiers,
) -> tuple[SampleFieldExceptionResponse, str]:
    """Read the field exception on one entity's value of one study field, with
    the ETag that a replace or clear sends as If-Match. The gates are
    `_resolve_target`'s; none set is 404.
    """
    target = await _resolve_target(
        conn,
        spec=spec,
        fetch_row=fetch_row,
        entity_idx=entity_idx,
        metadata_idx_column=metadata_idx_column,
        study_idx=study_idx,
        study_field_idx=study_field_idx,
        noun=noun,
        tiers=tiers,
        write=False,
    )
    row = await fetch_field_exception(conn, spec=spec, slot=target.slot)
    if row is None:
        _raise_not_set(spec, target.field, noun=noun, entity_idx=entity_idx)
    return _map(row, slot=target.slot)


async def put_and_map_field_exception(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    study_field_idx: int,
    noun: str,
    body: SampleFieldExceptionPutRequest,
    if_match: str | None,
    caller_idx: int,
    tiers: CallerMetadataTiers,
) -> tuple[SampleFieldExceptionResponse, str, bool]:
    """Set the field exception on one entity's value of one study field; return
    it, its ETag, and whether it was created.

    After `_resolve_target`'s gates, in order:
    - a linked field with no value on the entity yet is 409: the exception
      would bind whichever study writes first;
    - a field declaring unique_in_study is 409;
    - a tier_override at or below the field's tier is 422, restricting nothing.

    Without If-Match the PUT only creates: an exception already there, or one
    that a concurrent request created first, is 428, so a stale caller cannot
    undo another admin's exception unseen. With If-Match it only replaces: none
    set is 404, a stale tag 412. The caller owns the transaction.
    """
    target = await _resolve_target(
        conn,
        spec=spec,
        fetch_row=fetch_row,
        entity_idx=entity_idx,
        metadata_idx_column=metadata_idx_column,
        study_idx=study_idx,
        study_field_idx=study_field_idx,
        noun=noun,
        tiers=tiers,
        write=True,
    )
    field = target.field
    if target.linked_slot_empty:
        raise HTTPException(
            status_code=409,
            detail=(
                f"{_field_label(spec, field)} holds no value on this {noun}; a field exception"
                " on a field linked to a global field restricts the value whichever study"
                " writes it, so it can be set once a value is there"
            ),
        )
    if field["unique_in_study"]:
        _raise_on_unique_field(spec, field)
    field_tier = await fetch_study_field_tier(conn, spec=spec, study_field_idx=study_field_idx)
    if tier_at_least(field_tier, body.tier_override):
        raise HTTPException(
            status_code=422,
            detail=(
                f"{_field_label(spec, field)} already requires {field_tier.value}; a field"
                " exception at or below the field's tier restricts nothing"
            ),
        )

    if if_match is None:
        try:
            row = await insert_field_exception(
                conn,
                spec=spec,
                slot=target.slot,
                tier_override=body.tier_override,
                reason=body.reason,
                created_by_idx=caller_idx,
            )
        except asyncpg.CheckViolationError as exc:
            # unique_in_study was declared on the field after the check above.
            if exc.constraint_name == spec.exception_on_unique_field_constraint:
                _raise_on_unique_field(spec, field)
            raise
        if row is None:
            # One is already set, or a concurrent create committed first.
            _raise_already_set(spec, field, noun=noun)
        response, etag = _map(row, slot=target.slot)
        return response, etag, True

    await _lock_matching(
        conn, spec=spec, target=target, if_match=if_match, noun=noun, entity_idx=entity_idx
    )
    row = await update_field_exception(
        conn,
        spec=spec,
        slot=target.slot,
        tier_override=body.tier_override,
        reason=body.reason,
        updated_by_idx=caller_idx,
    )
    if row is None:
        _raise_vanished(spec, target.slot)
    response, etag = _map(row, slot=target.slot)
    return response, etag, False


async def delete_field_exception_or_raise(
    conn: asyncpg.Connection,
    *,
    spec: EntityMetadataSpec,
    fetch_row: Callable[[asyncpg.Connection, int], Awaitable[asyncpg.Record | None]],
    entity_idx: int,
    metadata_idx_column: str,
    study_idx: int,
    study_field_idx: int,
    noun: str,
    if_match: str | None,
    tiers: CallerMetadataTiers,
) -> None:
    """Clear the field exception on one entity's value of one study field.

    If-Match is required (428 without), then `_resolve_target`'s gates; none
    set is 404, a stale tag 412. The caller owns the transaction.
    """
    if_match = require_if_match(if_match)
    target = await _resolve_target(
        conn,
        spec=spec,
        fetch_row=fetch_row,
        entity_idx=entity_idx,
        metadata_idx_column=metadata_idx_column,
        study_idx=study_idx,
        study_field_idx=study_field_idx,
        noun=noun,
        tiers=tiers,
        write=True,
    )
    await _lock_matching(
        conn, spec=spec, target=target, if_match=if_match, noun=noun, entity_idx=entity_idx
    )
    if not await delete_field_exception(conn, spec=spec, slot=target.slot):
        _raise_vanished(spec, target.slot)
