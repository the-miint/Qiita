"""Tests for the batch ENA-import wire models (`models.ena_import`).

Pure Pydantic coverage: request validation and the response/status shapes the
`/ena-import-batch` route returns. No DB, no HTTP.
"""

import pytest
from pydantic import ValidationError


def test_batch_import_request_defaults():
    from qiita_common.models.ena_import import BatchImportRequest

    req = BatchImportRequest(accessions=["PRJEB1234"])
    assert req.accessions == ["PRJEB1234"]


def test_batch_import_request_accepts_multiple_accessions():
    from qiita_common.models.ena_import import BatchImportRequest

    req = BatchImportRequest(accessions=["PRJEB1234", "PRJNA5678"])
    assert req.accessions == ["PRJEB1234", "PRJNA5678"]


@pytest.mark.parametrize("unknown_field", ["backend", "source"])
def test_batch_import_request_rejects_unknown_fields(unknown_field):
    """Every *Request model in this repo pins extra="forbid", so a field the
    model does not declare fails loud rather than being silently dropped.
    `backend` and `source` are the two this PR removed, so they are the useful
    cases to pin."""
    from qiita_common.models.ena_import import BatchImportRequest

    with pytest.raises(ValidationError):
        BatchImportRequest(accessions=["PRJEB1234"], **{unknown_field: "whatever"})


def test_batch_import_request_rejects_empty_accessions():
    from qiita_common.models.ena_import import BatchImportRequest

    with pytest.raises(ValidationError):
        BatchImportRequest(accessions=[])


def test_batch_item_state_values():
    from qiita_common.models.ena_import import BatchItemState

    assert {s.value for s in BatchItemState} == {
        "pending",
        "resolving",
        "registered",
        "downloading",
        "done",
        "failed",
    }


def test_batch_import_item_defaults():
    from qiita_common.models.ena_import import BatchImportItem, BatchItemState

    item = BatchImportItem(ena_study_accession="PRJEB1234", state=BatchItemState.PENDING)
    assert item.study_idx is None
    assert item.failure_reason is None
    assert item.download_work_ticket_idxs == []


def test_batch_import_response_shape():
    from qiita_common.models.ena_import import (
        BatchImportItem,
        BatchImportResponse,
        BatchItemState,
    )

    resp = BatchImportResponse(
        ena_import_batch_idx=1,
        items=[BatchImportItem(ena_study_accession="PRJEB1234", state=BatchItemState.PENDING)],
    )
    assert resp.ena_import_batch_idx == 1
    assert resp.items[0].ena_study_accession == "PRJEB1234"


def test_batch_import_status_shape():
    from qiita_common.models.ena_import import (
        BatchImportItem,
        BatchImportStatus,
        BatchItemState,
    )

    status = BatchImportStatus(
        ena_import_batch_idx=1,
        items=[
            BatchImportItem(
                ena_study_accession="PRJEB1234",
                state=BatchItemState.DONE,
                study_idx=25000,
                download_work_ticket_idxs=[7, 8],
            )
        ],
    )
    assert status.items[0].state == BatchItemState.DONE
    assert status.items[0].download_work_ticket_idxs == [7, 8]
