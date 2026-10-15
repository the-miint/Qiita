"""Wire shapes for POST /biosample/resolve-roster and the tube normalizer."""

import pytest
from pydantic import ValidationError

from qiita_common.models import (
    BiosampleRef,
    RosterProblem,
    RosterResolveFailure,
    RosterResolveRequest,
    RosterResolveRow,
    StudyRef,
    normalize_matrix_tube_id,
)

_BIOSAMPLE = {"biosample_accession": "SAMN1"}
_PRIMARY = {"bioproject_accession": "PRJNA1"}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0363157924", "0363157924"),
        # A spreadsheet drops the leading zero; the tube is still that tube.
        ("363157924", "0363157924"),
        (" 363157924 ", "0363157924"),
        ("1", "0000000001"),
    ],
)
def test_normalize_matrix_tube_id(raw: str, expected: str) -> None:
    assert normalize_matrix_tube_id(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "800805.001.V1.Plasma", "control sample", "12345678901", "36315792a", "-363157924"],
)
def test_normalize_matrix_tube_id_rejects(raw: str) -> None:
    with pytest.raises(ValueError, match="matrix tube id"):
        normalize_matrix_tube_id(raw)


@pytest.mark.parametrize("ref_model", [BiosampleRef, StudyRef])
def test__requires_an_identifier(ref_model) -> None:
    """Tests the case where a ref carries no identifier: it is refused."""
    with pytest.raises(ValidationError, match="no identifier"):
        ref_model()


@pytest.mark.parametrize("missing", ["biosample", "primary_project"])
def test_roster_resolve_row_requires_biosample_and_primary(missing: str) -> None:
    """Tests the case where a row omits its biosample or its primary project."""
    body = {"item_id": "1", "biosample": _BIOSAMPLE, "primary_project": _PRIMARY}
    del body[missing]
    with pytest.raises(ValidationError, match=missing):
        RosterResolveRow.model_validate(body)


def test_study_ref_refuses_a_tube() -> None:
    """Tests the case where a study ref carries a biosample-only identifier."""
    with pytest.raises(ValidationError, match="matrix_tube_id"):
        StudyRef.model_validate({"export_entity_id": "QS1", "matrix_tube_id": "1"})


def test_biosample_ref_keeps_a_malformed_tube() -> None:
    """Tests the case where the tube is malformed: the wire keeps it, so the
    route reports it as that row's problem rather than as a request-level 422."""
    ref = BiosampleRef(matrix_tube_id="800805.001.V1.Plasma")
    assert ref.model_dump(exclude_none=True) == {"matrix_tube_id": "800805.001.V1.Plasma"}


def test_roster_resolve_request_refuses_duplicate_item_ids() -> None:
    """Tests the case where two rows share an item_id."""
    row = {"item_id": "1", "biosample": _BIOSAMPLE, "primary_project": _PRIMARY}
    with pytest.raises(ValidationError, match="duplicate item_id"):
        RosterResolveRequest.model_validate({"rows": [row, row]})


def test_request_refuses_empty_and_extra() -> None:
    with pytest.raises(ValidationError):
        RosterResolveRequest(rows=[])
    with pytest.raises(ValidationError):
        RosterResolveRow.model_validate(
            {"item_id": "1", "biosample": _BIOSAMPLE, "primary_project": _PRIMARY, "tube": "1"}
        )


def test_failure_round_trips() -> None:
    failure = RosterResolveFailure(
        message="1 of 2 rows did not resolve",
        problems=[RosterProblem(item_id="1", message="no biosample has matrix tube 0363157924")],
    )
    again = RosterResolveFailure.model_validate(failure.model_dump(mode="json"))
    assert again == failure
