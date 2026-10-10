"""Wire shapes for POST /biosample/resolve-roster and the tube normalizer."""

import pytest
from pydantic import ValidationError

from qiita_common.models import (
    RosterProblem,
    RosterResolveFailure,
    RosterResolveRequest,
    RosterResolveRow,
    normalize_matrix_tube_id,
)


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


def test_row_carries_either_identity() -> None:
    tube_row = RosterResolveRow(item_id="1", matrix_tube_id="363157924")
    assert tube_row.biosample_accession is None
    assert tube_row.secondary_project_accessions == []
    acc_row = RosterResolveRow(
        item_id="2", biosample_accession="SAMN1", primary_project_accession="PRJNA1"
    )
    assert acc_row.matrix_tube_id is None


def test_row_keeps_a_malformed_tube_for_the_problem_report() -> None:
    # The wire accepts any non-blank tube text: a malformed tube is a per-row
    # problem the route reports alongside every other, not a request-level 422.
    row = RosterResolveRow(item_id="1", matrix_tube_id="800805.001.V1.Plasma")
    assert row.matrix_tube_id == "800805.001.V1.Plasma"


def test_request_refuses_duplicate_item_ids() -> None:
    with pytest.raises(ValidationError, match="duplicate item_id"):
        RosterResolveRequest(
            rows=[
                RosterResolveRow(item_id="1", matrix_tube_id="1"),
                RosterResolveRow(item_id="1", matrix_tube_id="2"),
            ]
        )


def test_request_refuses_empty_and_extra() -> None:
    with pytest.raises(ValidationError):
        RosterResolveRequest(rows=[])
    with pytest.raises(ValidationError):
        RosterResolveRow.model_validate({"item_id": "1", "tube": "1"})


def test_failure_round_trips() -> None:
    failure = RosterResolveFailure(
        message="1 of 2 rows did not resolve",
        problems=[RosterProblem(item_id="1", message="no biosample has matrix tube 0363157924")],
    )
    again = RosterResolveFailure.model_validate(failure.model_dump(mode="json"))
    assert again == failure
