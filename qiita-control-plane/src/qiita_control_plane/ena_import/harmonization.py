"""ENA sample-attribute harmonization: turns one BioSample's ENA
submitter-defined attribute map into cross-study-comparable metadata on a
`qiita.biosample` row.

`attribute_mapping.map_ena_attributes` splits the map into a curated set that
lands on an existing `biosample_global_field` (one canonical value shared
cross-study) and everything else, retained as raw study-local TEXT rather than
dropped. `known_missing_reasons` is wired into `preflight_sample_metadata` so an
INSDC missing-value string resolves as a `MissingReasonRef` instead of raising --
mapped values only.

Unlike the other biosample composers, this one deliberately never calls
`assert_required_global_fields_supplied` -- ENA declining to supply a
checklist-required field is not an error here.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import asyncpg
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories import require_transaction
from qiita_control_plane.repositories._sample_helpers import (
    fetch_missing_value_reason_idxs_by_names,
    preflight_sample_metadata,
    write_local_metadata_or_diagnose,
    write_resolved_metadata_entries,
)
from qiita_control_plane.repositories.biosample import update_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC

from .attribute_mapping import map_ena_attributes


@dataclass(frozen=True)
class HarmonizationResult:
    """One biosample's harmonization outcome.

    `mapped_count`: attributes written as globally-linked metadata.
    `retained_unmapped`: raw ENA tags written as study-local metadata (not
    dropped).
    """

    mapped_count: int
    retained_unmapped: list[str] = field(default_factory=list)


async def harmonize_biosample_attributes(
    conn: asyncpg.Connection,
    *,
    biosample_idx: int,
    study_idx: int,
    attributes: dict[str, str],
    caller_idx: int,
    metadata_checklist_idx: int,
) -> HarmonizationResult:
    """Harmonize one BioSample's ENA attributes onto an existing biosample.

    Inside the caller's transaction: split via `map_ena_attributes`; pre-flight
    resolve + parse the mapped attributes against `biosample_global_field`
    (raising before any write on an unknown display_name or parse failure); write
    mapped attributes as globally-linked metadata and unmapped ones as raw
    study-local TEXT (retained, not dropped); and bind the biosample's checklist.

    Caller must wrap the call in `async with conn.transaction():`; RuntimeError
    otherwise so a partial write cannot leave orphan rows.
    """
    require_transaction(conn)

    mapped, unmapped = map_ena_attributes(attributes)

    # Resolve INSDC missing-value markers so preflight_sample_metadata treats
    # them as MissingReasonRef instead of raising. Mapped values only.
    candidate_texts = {v.strip() for v in mapped.values()}
    known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(conn, candidate_texts)

    # Pre-flight: resolve + parse every mapped attribute against
    # biosample_global_field only (allow_local=False). map_ena_attributes keys
    # on display_name, not internal_name, so global_internal_names=False.
    # Raises before any write on an unrecognized display_name or an
    # unparseable value -- a mapping/data bug, not a gap.
    resolved_metadata = await preflight_sample_metadata(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        study_idx=study_idx,
        metadata=mapped,
        known_missing_reasons=known_missing_reasons,
        allow_local=False,
        global_internal_names=False,
    )

    # Mapped: globally-linked write, cross-study comparable. on_conflict
    # stays "raise" (write-once): a second study sharing this biosample must
    # not overwrite the first import's value through the shared global slot.
    # Deliberately no assert_required_global_fields_supplied (see module
    # docstring).
    write_results = await write_resolved_metadata_entries(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        entity_idx=biosample_idx,
        study_idx=study_idx,
        caller_idx=caller_idx,
        resolved_metadata=resolved_metadata,
    )

    # Unmapped: retained as raw, study-local TEXT metadata -- never dropped.
    for tag, value in unmapped.items():
        await write_local_metadata_or_diagnose(
            conn,
            spec=BIOSAMPLE_METADATA_SPEC,
            entity_idx=biosample_idx,
            study_idx=study_idx,
            display_name=tag,
            data_type=FieldDataType.TEXT,
            value=value,
            caller_idx=caller_idx,
        )

    # Bind the biosample to its checklist.
    await update_biosample(
        conn, biosample_idx, fields={"metadata_checklist_idx": metadata_checklist_idx}
    )

    return HarmonizationResult(
        mapped_count=len(write_results),
        retained_unmapped=sorted(unmapped),
    )
