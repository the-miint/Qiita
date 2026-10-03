"""Tests for the study and biosample mapping into ENA row bodies."""

from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from qiita_common.models import (
    FieldDataType,
    MetadataChecklistRef,
    MissingReasonRef,
    StudyResponse,
    TerminologyTermRef,
    Tier,
)

from qiita_control_plane.ena_submission.mapping import (
    EnaMappingError,
    EnaProjectRow,
    EnaSampleRow,
    ena_alias,
    map_biosample_to_ena_sample_row,
    map_study_to_ena_project_row,
)
from qiita_control_plane.repositories._sample_helpers import (
    ChecklistRequirementRow,
    GlobalMetadataRow,
)

_HUMAN_GUT = MetadataChecklistRef(idx=4, name="ERC000015")
_ENA_DEFAULT = MetadataChecklistRef(idx=1, name="ERC000011")
_STUDY_TITLE = "Gut microbiome of resident penguins"
_BIOSAMPLE_IDX = 30002
_TAXON_ID = 408170

# The requirement most tests use, since its value kind is the uninteresting one
# and the test's own subject is whatever it adds beside it.
_COLLECTION_DATE = ChecklistRequirementRow(
    checklist_field_name="collection date",
    internal_name="collection_date",
    unit=None,
)
_LATITUDE = ChecklistRequirementRow(
    checklist_field_name="geographic location (latitude)",
    internal_name="geographic_location_latitude",
    unit="DD",
)
_DEPTH = ChecklistRequirementRow(
    checklist_field_name="depth",
    internal_name="depth_m",
    unit="m",
)


def _metadata_row(internal_name: str, value, data_type: FieldDataType) -> GlobalMetadataRow:
    """One resolved global metadata entry, with the cosmetic columns the
    mapping never reads filled in flatly."""
    return GlobalMetadataRow(
        display_name=internal_name,
        description=None,
        data_type=data_type,
        value=value,
        internal_name=internal_name,
    )


_TAXON_TERM = TerminologyTermRef(idx=7, term_id="408170", label="human gut metagenome")


def _taxon_metadata() -> dict[str, GlobalMetadataRow]:
    """The taxon entry every sample needs, independent of any checklist."""
    return {"taxon_id": _metadata_row("taxon_id", _TAXON_TERM, FieldDataType.TERMINOLOGY)}


def _expected_sample(
    *,
    checklist: MetadataChecklistRef,
    attributes: dict[str, str],
    attribute_units: dict[str, str] | None = None,
) -> EnaSampleRow:
    """The row every mapping test expects, varying only in what that test is
    about: the alias and taxon id are fixed by the inputs each one shares."""
    return EnaSampleRow(
        alias=str(_BIOSAMPLE_IDX),
        taxon_id=_TAXON_ID,
        checklist=checklist.name,
        attributes=attributes,
        attribute_units=attribute_units or {},
    )


def _study(*, description: str | None) -> StudyResponse:
    """A study carrying the three fields the project mapping reads; the rest
    are the model's required columns, filled flatly."""
    stamp = datetime(2026, 3, 11, tzinfo=UTC)
    return StudyResponse(
        study_idx=25001,
        owner_idx=1,
        principal_investigator_idx=None,
        title=_STUDY_TITLE,
        alias=None,
        description=description,
        abstract=None,
        funding=None,
        ena_study_accession=None,
        bioproject_accession=None,
        notes=None,
        last_submission_at=None,
        submission_error=None,
        extra_metadata=None,
        default_tier=Tier.MEMBER,
        created_by_idx=1,
        created_at=stamp,
        updated_at=stamp,
    )


def test_ena_alias():
    """Tests the case where an idx is rendered as an alias: it is the idx
    verbatim, since retry safety depends on it being derivable again."""
    assert ena_alias(25001) == "25001"


def test_map_study_to_ena_project_row():
    """Tests the case where a study is mapped with no project type supplied:
    the alias is the study idx and project_type stays unset rather than being
    given a placeholder."""
    study = _study(description="Resident colony, sampled monthly")

    result = map_study_to_ena_project_row(study)

    expected = EnaProjectRow(
        alias="25001",
        title=_STUDY_TITLE,
        description="Resident colony, sampled monthly",
        project_type=None,
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row():
    """Tests the case where every required field is present and one carries a
    unit: the attributes are keyed by the checklist's own field names, only the
    unit-bearing field appears in attribute_units, and project name is drawn
    from the study."""
    requirements = [
        _COLLECTION_DATE,
        _LATITUDE,
    ]
    metadata = _taxon_metadata() | {
        "collection_date": _metadata_row("collection_date", "2026-03-11", FieldDataType.TEXT),
        "geographic_location_latitude": _metadata_row(
            "geographic_location_latitude", Decimal("-64.80000"), FieldDataType.NUMERIC
        ),
    }

    result = map_biosample_to_ena_sample_row(
        biosample_idx=_BIOSAMPLE_IDX,
        checklist=_HUMAN_GUT,
        requirements=requirements,
        metadata=metadata,
        study_title=_STUDY_TITLE,
    )

    expected = _expected_sample(
        checklist=_HUMAN_GUT,
        attributes={
            "collection date": "2026-03-11",
            "geographic location (latitude)": "-64.80000",
            "project name": _STUDY_TITLE,
        },
        attribute_units={"geographic location (latitude)": "DD"},
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row_root_checklist():
    """Tests the case where the checklist is the one with no project name
    field: the attribute is withheld rather than sent and rejected."""
    requirements = [_COLLECTION_DATE]
    metadata = _taxon_metadata() | {
        "collection_date": _metadata_row("collection_date", "2026-03-11", FieldDataType.TEXT),
    }

    result = map_biosample_to_ena_sample_row(
        biosample_idx=_BIOSAMPLE_IDX,
        checklist=_ENA_DEFAULT,
        requirements=requirements,
        metadata=metadata,
        study_title=_STUDY_TITLE,
    )

    expected = _expected_sample(
        checklist=_ENA_DEFAULT,
        attributes={"collection date": "2026-03-11"},
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row_missing_value():
    """Tests the case where a required field holds an intentionally-missing
    marker: it is submitted as the reason name, which is already the vocabulary
    ENA expects, rather than being dropped or rendered as a Python repr."""
    requirements = [_COLLECTION_DATE]
    metadata = _taxon_metadata() | {
        "collection_date": _metadata_row(
            "collection_date",
            MissingReasonRef(idx=2, name="not collected"),
            FieldDataType.TEXT,
        ),
    }

    result = map_biosample_to_ena_sample_row(
        biosample_idx=_BIOSAMPLE_IDX,
        checklist=_ENA_DEFAULT,
        requirements=requirements,
        metadata=metadata,
        study_title=_STUDY_TITLE,
    )

    expected = _expected_sample(
        checklist=_ENA_DEFAULT,
        attributes={"collection date": "not collected"},
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row_small_numeric():
    """Tests the case where a numeric value is small enough that a float would
    render in scientific notation: it is submitted in plain decimal, because
    ENA parses the text and `1e-05` is not a number to it."""
    requirements = [_DEPTH]
    metadata = _taxon_metadata() | {
        "depth_m": _metadata_row("depth_m", Decimal("0.00001"), FieldDataType.NUMERIC),
    }

    result = map_biosample_to_ena_sample_row(
        biosample_idx=_BIOSAMPLE_IDX,
        checklist=_ENA_DEFAULT,
        requirements=requirements,
        metadata=metadata,
        study_title=_STUDY_TITLE,
    )

    expected = _expected_sample(
        checklist=_ENA_DEFAULT,
        attributes={"depth": "0.00001"},
        attribute_units={"depth": "m"},
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row_date_value():
    """Tests the case where a required field holds a typed date: it is
    submitted in ISO 8601 rather than a locale-dependent rendering."""
    requirements = [_COLLECTION_DATE]
    metadata = _taxon_metadata() | {
        "collection_date": _metadata_row("collection_date", date(2026, 3, 11), FieldDataType.DATE),
    }

    result = map_biosample_to_ena_sample_row(
        biosample_idx=_BIOSAMPLE_IDX,
        checklist=_ENA_DEFAULT,
        requirements=requirements,
        metadata=metadata,
        study_title=_STUDY_TITLE,
    )

    expected = _expected_sample(
        checklist=_ENA_DEFAULT,
        attributes={"collection date": "2026-03-11"},
    )
    assert result == expected


def test_map_biosample_to_ena_sample_row_no_checklist():
    """Tests the case where the biosample claims no checklist: mapping raises
    rather than passing an empty checklist, which would silently disable the
    client-side validation instead of failing."""
    with pytest.raises(EnaMappingError, match="claims no metadata checklist"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=None,
            requirements=[],
            metadata=_taxon_metadata(),
            study_title=_STUDY_TITLE,
        )


def test_map_biosample_to_ena_sample_row_no_requirements():
    """Tests the case where the checklist exists but nothing is seeded for it:
    mapping raises, since a sample submitted under a real checklist carrying no
    attributes is a gap in the seed rather than a sample with nothing to say."""
    with pytest.raises(EnaMappingError, match="no seeded requirements"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=_HUMAN_GUT,
            requirements=[],
            metadata=_taxon_metadata(),
            study_title=_STUDY_TITLE,
        )


def test_map_biosample_to_ena_sample_row_missing_required_field():
    """Tests the case where the biosample lacks a field its checklist requires:
    mapping raises naming the field, rather than submitting an envelope that
    fails validation for every sample in it."""
    requirements = [_COLLECTION_DATE]

    with pytest.raises(EnaMappingError, match="collection_date"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=_ENA_DEFAULT,
            requirements=requirements,
            metadata=_taxon_metadata(),
            study_title=_STUDY_TITLE,
        )


def test_map_biosample_to_ena_sample_row_no_taxon():
    """Tests the case where the biosample has no taxon id: mapping raises,
    because ena.samples requires one of every sample whatever its checklist."""
    with pytest.raises(EnaMappingError, match="no taxon id"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=_ENA_DEFAULT,
            requirements=[_COLLECTION_DATE],
            metadata={
                "collection_date": _metadata_row(
                    "collection_date", "2026-03-11", FieldDataType.TEXT
                )
            },
            study_title=_STUDY_TITLE,
        )


def test_map_biosample_to_ena_sample_row_taxon_not_resolved():
    """Tests the case where the taxon id carries a missing-reason marker rather
    than a resolved term: mapping raises, because a marker names no taxon and
    ena.samples needs the id itself. A populated missing reason supersedes the
    field's terminology typing, so ordinary data reaches this path."""
    metadata = {
        "taxon_id": _metadata_row(
            "taxon_id",
            MissingReasonRef(idx=2, name="not collected"),
            FieldDataType.TERMINOLOGY,
        ),
        "collection_date": _metadata_row("collection_date", "2026-03-11", FieldDataType.TEXT),
    }

    with pytest.raises(EnaMappingError, match="not a resolved terminology term"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=_ENA_DEFAULT,
            requirements=[_COLLECTION_DATE],
            metadata=metadata,
            study_title=_STUDY_TITLE,
        )


def test_map_biosample_to_ena_sample_row_unrenderable_value():
    """Tests the case where a required field holds a kind with no ENA
    rendering: mapping raises rather than guessing. No field any seeded
    checklist requires is of such a kind, so this fires only when a newly
    required field needs a rendering decision."""
    requirements = [_COLLECTION_DATE]
    metadata = _taxon_metadata() | {
        "collection_date": _metadata_row("collection_date", True, FieldDataType.BOOLEAN),
    }

    with pytest.raises(EnaMappingError, match="no ENA rendering"):
        map_biosample_to_ena_sample_row(
            biosample_idx=_BIOSAMPLE_IDX,
            checklist=_ENA_DEFAULT,
            requirements=requirements,
            metadata=metadata,
            study_title=_STUDY_TITLE,
        )
