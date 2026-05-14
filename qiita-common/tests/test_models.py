"""Tests for shared Pydantic models."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER


def test_reference_status_enum():
    """ReferenceStatus must have the expected values."""
    from qiita_common.models import ReferenceStatus

    assert ReferenceStatus.PENDING == "pending"
    assert ReferenceStatus.HASHING == "hashing"
    assert ReferenceStatus.MINTING == "minting"
    assert ReferenceStatus.LOADING == "loading"
    assert ReferenceStatus.ACTIVE == "active"
    assert ReferenceStatus.FAILED == "failed"


def test_reference_create_request_valid():
    """ReferenceCreateRequest must accept valid input."""
    from qiita_common.models import ReferenceCreateRequest

    req = ReferenceCreateRequest(
        name="greengenes2",
        version="2024.09",
        kind="sequence_reference",
    )
    assert req.name == "greengenes2"
    assert req.version == "2024.09"
    assert req.kind == "sequence_reference"


def test_reference_create_request_rejects_invalid_kind():
    """ReferenceCreateRequest must reject invalid kind values."""
    from qiita_common.models import ReferenceCreateRequest

    with pytest.raises(ValidationError):
        ReferenceCreateRequest(
            name="test",
            version="1.0",
            kind="bogus",
        )


def test_reference_create_request_rejects_empty_name():
    """ReferenceCreateRequest must reject empty name."""
    from qiita_common.models import ReferenceCreateRequest

    with pytest.raises(ValidationError):
        ReferenceCreateRequest(
            name="",
            version="1.0",
            kind="sequence_reference",
        )


def test_reference_create_request_rejects_empty_version():
    """ReferenceCreateRequest must reject empty version."""
    from qiita_common.models import ReferenceCreateRequest

    with pytest.raises(ValidationError):
        ReferenceCreateRequest(
            name="test",
            version="",
            kind="sequence_reference",
        )


def test_reference_response_round_trips():
    """ReferenceResponse must round-trip through model_dump."""
    from qiita_common.models import ReferenceResponse

    now = datetime.now(UTC)
    resp = ReferenceResponse(
        reference_idx=1,
        name="greengenes2",
        version="2024.09",
        kind="sequence_reference",
        status="pending",
        created_by_idx=42,
        created_at=now,
    )
    d = resp.model_dump()
    assert d["reference_idx"] == 1
    assert d["status"] == "pending"
    assert d["created_at"] == now


def test_reference_response_rejects_invalid_status():
    """ReferenceResponse must reject invalid status values."""
    from qiita_common.models import ReferenceResponse

    with pytest.raises(ValidationError):
        ReferenceResponse(
            reference_idx=1,
            name="test",
            version="1.0",
            kind="sequence_reference",
            status="bogus",
            created_by_idx=42,
            created_at=datetime.now(UTC),
        )


def test_reference_response_rejects_naive_datetime():
    """ReferenceResponse must reject naive (non-timezone-aware) datetimes."""
    from qiita_common.models import ReferenceResponse

    with pytest.raises(ValidationError):
        ReferenceResponse(
            reference_idx=1,
            name="test",
            version="1.0",
            kind="sequence_reference",
            status="pending",
            created_by_idx=42,
            created_at=datetime.now(),  # naive — no timezone
        )


def test_reference_response_rejects_zero_idx():
    """ReferenceResponse must reject reference_idx <= 0."""
    from qiita_common.models import ReferenceResponse

    with pytest.raises(ValidationError):
        ReferenceResponse(
            reference_idx=0,
            name="test",
            version="1.0",
            kind="sequence_reference",
            status="pending",
            created_by_idx=42,
            created_at=datetime.now(UTC),
        )


def test_feature_hash_entry_rejects_source_without_id():
    """genome_source set without genome_source_id must fail validation."""
    from qiita_common.models import FeatureHashEntry

    with pytest.raises(ValidationError):
        FeatureHashEntry(
            sequence_hash=UUID("a0000000-0000-0000-0000-000000000001"),
            genome_source="genbank",
        )


def test_feature_hash_entry_rejects_id_without_source():
    """genome_source_id set without genome_source must fail validation."""
    from qiita_common.models import FeatureHashEntry

    with pytest.raises(ValidationError):
        FeatureHashEntry(
            sequence_hash=UUID("a0000000-0000-0000-0000-000000000001"),
            genome_source_id="GCF_123",
        )


# --- StepRunRequest runtime-selection validator ---------------------------
# Mirrors WorkflowStep's exactly-one(container, module) rule at the wire
# boundary. Pydantic raises a 422 at FastAPI deserialization, before any
# backend code runs — single enforcement point, no per-backend drift risk.


def _minimal_step_run_kwargs() -> dict:
    """Smallest valid kwargs for StepRunRequest — container form."""
    return dict(
        step_name="hash",
        inputs={"fasta_path": "/data/in.fa"},
        workspace="/workspace",
        reference_idx=1,
        work_ticket_idx=1,
        container=REFERENCE_HASH_CONTAINER,
    )


def test_step_run_request_container_form_validates():
    from qiita_common.models import StepRunRequest

    req = StepRunRequest(**_minimal_step_run_kwargs())
    assert req.container == REFERENCE_HASH_CONTAINER
    assert req.module is None


def test_step_run_request_module_form_validates():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    del kwargs["container"]
    kwargs["module"] = "qiita_compute_orchestrator.jobs.fastq_to_parquet"
    req = StepRunRequest(**kwargs)
    assert req.module == "qiita_compute_orchestrator.jobs.fastq_to_parquet"
    assert req.container is None


def test_step_run_request_rejects_both_container_and_module():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    kwargs["module"] = "qiita_compute_orchestrator.jobs.x"
    with pytest.raises(ValidationError) as exc_info:
        StepRunRequest(**kwargs)
    assert "exactly one" in str(exc_info.value)


def test_step_run_request_rejects_neither_container_nor_module():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    del kwargs["container"]
    with pytest.raises(ValidationError) as exc_info:
        StepRunRequest(**kwargs)
    assert "exactly one" in str(exc_info.value)


def test_step_run_request_rejects_entrypoint_without_container():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    del kwargs["container"]
    kwargs["module"] = "qiita_compute_orchestrator.jobs.x"
    kwargs["entrypoint"] = "/bin/sh"
    with pytest.raises(ValidationError) as exc_info:
        StepRunRequest(**kwargs)
    assert "entrypoint" in str(exc_info.value).lower()


def test_step_run_request_entrypoint_with_container_ok():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    kwargs["entrypoint"] = "/usr/local/bin/qiita-hash"
    req = StepRunRequest(**kwargs)
    assert req.entrypoint == "/usr/local/bin/qiita-hash"


# ---------------------------------------------------------------------------
# SequenceRangeMintRequest / SequenceRange
# ---------------------------------------------------------------------------


def test_sequence_range_mint_request_accepts_valid_input():
    from qiita_common.models import SequenceRangeMintRequest

    req = SequenceRangeMintRequest(prep_sample_idx=7, count=100)
    assert req.prep_sample_idx == 7
    assert req.count == 100


@pytest.mark.parametrize("bad_count", [0, -1, -1000])
def test_sequence_range_mint_request_rejects_nonpositive_count(bad_count):
    from qiita_common.models import SequenceRangeMintRequest

    with pytest.raises(ValidationError):
        SequenceRangeMintRequest(prep_sample_idx=1, count=bad_count)


@pytest.mark.parametrize("bad_idx", [0, -1])
def test_sequence_range_mint_request_rejects_nonpositive_prep_sample_idx(bad_idx):
    from qiita_common.models import SequenceRangeMintRequest

    with pytest.raises(ValidationError):
        SequenceRangeMintRequest(prep_sample_idx=bad_idx, count=10)


def test_sequence_range_mint_request_rejects_extra_fields():
    """ConfigDict(extra='forbid') so an unknown key at the API boundary
    fails fast rather than being silently dropped."""
    from qiita_common.models import SequenceRangeMintRequest

    with pytest.raises(ValidationError):
        SequenceRangeMintRequest.model_validate(
            {"prep_sample_idx": 1, "count": 10, "extra": "smuggled"}
        )


def test_sequence_range_round_trips_through_json():
    from qiita_common.models import SequenceRange

    payload = {
        "prep_sample_idx": 7,
        "sequence_idx_start": 1,
        "sequence_idx_stop": 100,
        "created_at": "2026-05-14T12:00:00+00:00",
    }
    model = SequenceRange.model_validate(payload)
    assert model.sequence_idx_start == 1
    assert model.sequence_idx_stop == 100
    assert model.prep_sample_idx == 7
    # Aware datetime (qiita_common convention).
    assert model.created_at.tzinfo is not None
