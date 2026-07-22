"""ENA sample-attribute harmonization into Qiita's shared biosample metadata
model: turns one BioSample's ENA submitter-defined attribute map
(`EnaSampleAttributes.attributes`) into cross-study-comparable metadata on a
`qiita.biosample` row.

Splits the attribute map via `attribute_mapping.map_ena_attributes` into a
curated set that lands on an existing `biosample_global_field` (globally
linked -- one canonical value shared cross-study) and everything else, which
is retained as raw study-local TEXT metadata rather than dropped.
Wires `known_missing_reasons` into `preflight_global_metadata` so an
ENA/INSDC missing-value string (`not applicable`, `not collected`, ...)
resolves as a `MissingReasonRef` instead of raising `MetadataParseError` --
mapped values only; unmapped values are written as raw TEXT, unparsed.

Unlike `import_biosample_from_owner_biosample_id` /
`import_sequenced_prep_sample`, this composer deliberately never calls
`assert_required_global_fields_supplied`: a checklist-required field ENA did
not supply is reported on the returned `HarmonizationResult.missing_required`,
never rejected. Only a genuine parse/type/collision failure (an
unrecognized display_name, an unparseable typed value, a cross-study slot
collision) raises -- the caller (`ena_import.registration`) isolates that
per-run, exactly like a platform- or protocol-mapping failure; a
required-field gap is never one of those failures.
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

    `mapped_count` is the number of attributes written as globally-linked
    metadata; `retained_unmapped` names the raw ENA tags written as
    study-local metadata instead of being dropped; `checklist_name` and
    `missing_required` describe the checklist the biosample was bound to and
    which of its required fields (by display_name) still have no value after
    this write -- always a report, never a rejection.
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

    Order of operations, all inside the caller's transaction:

      1. Split `attributes` via `attribute_mapping.map_ena_attributes`.
      2. Resolve every mapped value that could plausibly be an INSDC
         missing-value marker in one round trip
         (`fetch_missing_value_reason_idxs_by_names`), then pre-flight
         resolve + parse every mapped attribute against
         `biosample_global_field` (`preflight_global_metadata`, with that
         lookup wired in as `known_missing_reasons`) -- unknown-display-name
         and parse-failure cases raise before any write.
      3. Write every mapped attribute as globally-linked metadata against
         `study_idx` (`write_global_metadata_entries`) -- no
         `assert_required_global_fields_supplied` call; see module
         docstring.
      4. Write every unmapped attribute as raw, study-local TEXT metadata
         (`write_local_metadata_or_diagnose`) -- retained, not dropped.
      5. Bind `biosample.metadata_checklist_idx = metadata_checklist_idx`.
      6. Compute the non-raising missing-required report: every
         `metadata_checklist_field` row for `metadata_checklist_idx` whose
         target global field the biosample still has no value for.

    Caller must wrap the call in `async with conn.transaction():`;
    RuntimeError otherwise so a partial write cannot leave orphan rows.
    """
    require_transaction(conn)

    mapped, unmapped = map_ena_attributes(attributes)

    # known_missing_reasons wiring: an INSDC missing-value marker resolves as
    # a MissingReasonRef via preflight_global_metadata rather than raising
    # MetadataParseError. Scoped to mapped values only -- unmapped values are
    # written as raw TEXT below, unparsed.
    candidate_texts = {v.strip() for v in mapped.values()}
    known_missing_reasons = await fetch_missing_value_reason_idxs_by_names(conn, candidate_texts)

    # Pre-flight: resolve + parse every mapped attribute against
    # biosample_global_field. Raises before any write on an unrecognized
    # display_name or a value that fails to parse for its field's data_type
    # -- a genuine mapping/data bug, not a missing-required-field gap.
    parsed_metadata = await preflight_global_metadata(
        conn,
        spec=BIOSAMPLE_METADATA_SPEC,
        metadata=mapped,
        known_missing_reasons=known_missing_reasons,
    )

    # Mapped: globally-linked write, cross-study comparable. Deliberately no
    # assert_required_global_fields_supplied call -- see module docstring;
    # a checklist gap is reported below, never rejected here.
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
    # biosample_global_field the biosample still has no value for, after the
    # writes above. Never raises -- ENA declining to supply a required field
    # is reported, not rejected.
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
