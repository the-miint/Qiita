"""ENA sample-attribute harmonization: turns one BioSample's ENA
submitter-defined attribute map into cross-study-comparable metadata on a
`qiita.biosample` row.

`attribute_mapping.map_ena_attributes` splits the map into a curated set that
lands on an existing `biosample_global_field` (one canonical value shared
cross-study) and everything else, retained as raw study-local TEXT rather than
dropped. `known_missing_reasons` is wired into `preflight_global_metadata` so an
INSDC missing-value string resolves as a `MissingReasonRef` instead of raising --
mapped values only.

Unlike the other biosample composers, this one deliberately never calls
`assert_required_global_fields_supplied`: a checklist-required field ENA did not
supply is reported on `HarmonizationResult.missing_required`, never rejected.
Only a genuine parse/type/collision failure raises -- the caller
(`ena_import.registration`) isolates that per-run.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import asyncpg
from qiita_common.models import FieldDataType

from qiita_control_plane.repositories import require_transaction
from qiita_control_plane.repositories._sample_helpers import (
    fetch_missing_value_reason_idxs_by_names,
    preflight_global_metadata,
    write_global_metadata_entries,
    write_local_metadata_or_diagnose,
)
from qiita_control_plane.repositories.biosample import update_biosample
from qiita_control_plane.repositories.biosample_metadata import BIOSAMPLE_METADATA_SPEC

from .attribute_mapping import map_ena_attributes


@dataclass(frozen=True)
class HarmonizationResult:
    """One biosample's harmonization outcome.

    `mapped_count`: attributes written as globally-linked metadata.
    `retained_unmapped`: raw ENA tags written as study-local metadata (not
    dropped). `checklist_name` / `missing_required`: the bound checklist and its
    required fields (by display_name) still without a value -- a report, never a
    rejection.
    """

    mapped_count: int
    retained_unmapped: list[str] = field(default_factory=list)
    checklist_name: str = ""
    missing_required: list[str] = field(default_factory=list)


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
    study-local TEXT (retained, not dropped); bind the biosample's checklist; and
    compute the non-raising missing-required report.

    Caller must wrap the call in `async with conn.transaction():`; RuntimeError
    otherwise so a partial write cannot leave orphan rows.
    """
    require_transaction(conn)

    mapped, unmapped = map_ena_attributes(attributes)

    # Resolve INSDC missing-value markers so preflight_global_metadata treats
    # them as MissingReasonRef instead of raising. Mapped values only.
    candidate_texts = {v.strip() for v in mapped.values()}
    known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(conn, candidate_texts)

    # Pre-flight: resolve + parse every mapped attribute against
    # biosample_global_field. Raises before any write on an unrecognized
    # display_name or an unparseable value -- a mapping/data bug, not a gap.
    parsed_metadata = await preflight_global_metadata(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        metadata=mapped,
        known_missing_reasons=known_missing_reasons,
    )

    # Mapped: globally-linked write, cross-study comparable. Deliberately no
    # assert_required_global_fields_supplied -- a checklist gap is reported
    # below, never rejected here (see module docstring).
    await write_global_metadata_entries(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        entity_idx=biosample_idx,
        study_idx=study_idx,
        caller_idx=caller_idx,
        parsed_metadata=parsed_metadata,
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
    checklist_name = await conn.fetchval(
        "SELECT name FROM qiita.metadata_checklist WHERE idx = $1", metadata_checklist_idx
    )

    # Non-raising required-field gap report: every checklist-required
    # biosample_global_field the biosample still has no value for after the
    # writes above. ENA declining to supply a required field is reported.
    missing_rows = await conn.fetch(
        "SELECT gf.display_name"
        " FROM qiita.metadata_checklist_field mcf"
        " JOIN qiita.biosample_global_field gf ON gf.idx = mcf.biosample_global_field_idx"
        " WHERE mcf.metadata_checklist_idx = $1"
        "   AND mcf.biosample_global_field_idx IS NOT NULL"
        "   AND NOT EXISTS ("
        "     SELECT 1 FROM qiita.biosample_metadata bm"
        "      WHERE bm.biosample_idx = $2 AND bm.global_field_idx = gf.idx"
        "   )"
        " ORDER BY gf.display_name",
        metadata_checklist_idx,
        biosample_idx,
    )
    missing_required = [r["display_name"] for r in missing_rows]

    return HarmonizationResult(
        mapped_count=len(parsed_metadata),
        retained_unmapped=sorted(unmapped),
        checklist_name=checklist_name,
        missing_required=missing_required,
    )
