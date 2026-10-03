"""Row bodies for the ENA objects this package submits, and the mapping into them.

One model per `ena` catalog table, each field named for the ENA column it
populates, so a dumped model is addressable by column name and no second
statement of the ordering exists to drift from it.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from pydantic import BaseModel, Field
from qiita_common.models import (
    BIOSAMPLE_FIELD_TAXON_ID,
    MetadataChecklistRef,
    MissingReasonRef,
    NonBlankText,
    SampleMetadataValue,
    StudyResponse,
    TerminologyTermRef,
)

from qiita_control_plane.repositories._sample_helpers import (
    ChecklistRequirementRow,
    GlobalMetadataRow,
)

# ENA's `project name` attribute, sourced from the study rather than the sample.
_PROJECT_NAME_ATTRIBUTE = "project name"

# The one ENA checklist with no `project name` field, so the attribute is
# withheld for it and sent to every other. Deliberately a single value, not a
# set: a second checklist needing this carve-out means the per-checklist
# requirement belongs in the database, not in a wider constant here.
_CHECKLIST_WITHOUT_PROJECT_NAME = "ERC000011"


class EnaMappingError(Exception):
    """A biosample or study could not be mapped into an ENA row body."""


class EnaProjectRow(BaseModel):
    """One `ena.projects` row body."""

    alias: NonBlankText
    title: str | None = None
    description: str | None = None
    project_type: str | None = None


class EnaSampleRow(BaseModel):
    """One `ena.samples` row body.

    `checklist` has to carry content: an empty string disables miint's
    client-side checklist validation instead of failing.
    """

    alias: NonBlankText
    taxon_id: int
    checklist: NonBlankText
    attributes: dict[str, str] = Field(default_factory=dict)
    attribute_units: dict[str, str] = Field(default_factory=dict)


def ena_alias(idx: int) -> str:
    """Render a Qiita idx as the ENA alias for the object it identifies.

    The same idx must always yield the same alias, for the life of the object:
    an alias is unique per (submission account, object type) on ENA's side, so
    a value derived from anything editable would address a different object
    after an edit.

    Sending an idx past the Qiita boundary is approved here specifically. The
    alternative handles are worse: sample_name often carries PII, and an
    accession does not exist yet at submission time. These aliases are
    permanent once deposited.
    """
    return str(idx)


def _render_value(value: SampleMetadataValue, *, field_name: str) -> str:
    """Render one metadata value as the text ENA's attribute map carries."""
    # The reason name is already the INSDC vocabulary ENA reads.
    if isinstance(value, MissingReasonRef):
        return value.name
    if isinstance(value, str):
        return value
    # Fixed-point to prevent exponent notation for Decimal values,
    # as ENA's coordinate fields expect fixed-point notation.
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    # No field any seeded checklist requires is boolean or terminology-typed, so
    # reaching here means a newly required field needs a rendering decision.
    raise EnaMappingError(f"no ENA rendering for {field_name!r} of type {type(value).__name__}")


def _taxon_id_of(metadata: dict[str, GlobalMetadataRow]) -> int:
    """Extract the NCBI taxon id ena.samples requires of every sample.

    The field is bound to the NCBI Taxonomy terminology, so the value arrives
    as a resolved term and its term_id is an id that vocabulary issued. That
    binding is what establishes the id names a real taxon.
    """
    # Fills ena.samples' own taxon_id column rather than becoming an
    # attribute: the SAMPLE record requires it independently of any checklist.
    entry = metadata.get(BIOSAMPLE_FIELD_TAXON_ID)
    if entry is None:
        raise EnaMappingError("biosample has no taxon id, which ena.samples requires")
    if not isinstance(entry.value, TerminologyTermRef):
        raise EnaMappingError(
            f"taxon id is {type(entry.value).__name__}, not a resolved terminology term"
        )
    try:
        taxon_id = int(entry.value.term_id)
    except ValueError as exc:
        raise EnaMappingError(f"taxon id {entry.value.term_id!r} is not an integer") from exc
    return taxon_id


def map_study_to_ena_project_row(
    study: StudyResponse, *, project_type: str | None = None
) -> EnaProjectRow:
    """Build the `ena.projects` row body for `study`."""
    row = EnaProjectRow(
        alias=ena_alias(study.study_idx),
        title=study.title,
        description=study.description,
        project_type=project_type,
    )
    return row


def map_biosample_to_ena_sample_row(
    *,
    biosample_idx: int,
    checklist: MetadataChecklistRef | None,
    requirements: list[ChecklistRequirementRow],
    metadata: dict[str, GlobalMetadataRow],
    study_title: str,
) -> EnaSampleRow:
    """Build the `ena.samples` row body for one biosample.

    `requirements` fixes what is sent: every field the checklist requires and
    nothing else, so a value Qiita holds for an unrequired field is not
    submitted and a required field Qiita lacks raises here rather than being
    rejected by the whole envelope at submit. `project name` is the one
    attribute drawn from the study instead of the sample.
    """
    if checklist is None:
        raise EnaMappingError(
            f"biosample {biosample_idx} claims no metadata checklist, which ena.samples requires"
        )
    if not requirements:
        raise EnaMappingError(
            f"checklist {checklist.name} has no seeded requirements, so a sample "
            "under it would be submitted carrying nothing"
        )

    attributes: dict[str, str] = {}
    attribute_units: dict[str, str] = {}
    for requirement in requirements:
        entry = metadata.get(requirement.internal_name)
        if entry is None:
            raise EnaMappingError(
                f"biosample {biosample_idx} has no {requirement.internal_name!r}, "
                f"which checklist {checklist.name} requires"
            )
        attributes[requirement.checklist_field_name] = _render_value(
            entry.value, field_name=requirement.internal_name
        )
        if requirement.unit is not None:
            attribute_units[requirement.checklist_field_name] = requirement.unit

    # Sourced from the study, so it is not a requirement row and is sent to
    # every checklist that defines the field.
    if checklist.name != _CHECKLIST_WITHOUT_PROJECT_NAME:
        attributes[_PROJECT_NAME_ATTRIBUTE] = study_title

    row = EnaSampleRow(
        alias=ena_alias(biosample_idx),
        taxon_id=_taxon_id_of(metadata),
        checklist=checklist.name,
        attributes=attributes,
        attribute_units=attribute_units,
    )
    return row
