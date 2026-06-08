"""Tests for shared Pydantic models."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from qiita_common.testing.containers import REFERENCE_HASH_CONTAINER
from qiita_common.testing.native_steps import FASTQ_TO_PARQUET_MODULE


def test_reference_status_enum():
    """ReferenceStatus must have the expected values."""
    from qiita_common.models import ReferenceStatus

    assert ReferenceStatus.PENDING == "pending"
    assert ReferenceStatus.HASHING == "hashing"
    assert ReferenceStatus.MINTING == "minting"
    assert ReferenceStatus.LOADING == "loading"
    assert ReferenceStatus.INDEXING == "indexing"
    assert ReferenceStatus.ACTIVE == "active"
    assert ReferenceStatus.FAILED == "failed"


def test_reference_status_indexing_transitions():
    """`indexing` sits between `loading` and `active`; `loading` keeps its
    direct `→ active` edge so the existing (non-host) reference-add flow is
    unchanged, and `indexing` is only reachable from `loading`."""
    from qiita_common.models import VALID_STATUS_TRANSITIONS, ReferenceStatus

    assert VALID_STATUS_TRANSITIONS[ReferenceStatus.LOADING] == {
        ReferenceStatus.INDEXING,
        ReferenceStatus.ACTIVE,
        ReferenceStatus.FAILED,
    }
    assert VALID_STATUS_TRANSITIONS[ReferenceStatus.INDEXING] == {
        ReferenceStatus.ACTIVE,
        ReferenceStatus.FAILED,
    }


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


def test_reference_create_request_is_host_defaults_false():
    """is_host is an orthogonal flag; absent means a regular reference."""
    from qiita_common.models import ReferenceCreateRequest

    req = ReferenceCreateRequest(name="greengenes2", version="2024.09", kind="sequence_reference")
    assert req.is_host is False

    host = ReferenceCreateRequest(
        name="human", version="t2t-chm13v2.0", kind="sequence_reference", is_host=True
    )
    assert host.is_host is True


def test_reference_response_carries_is_host():
    """ReferenceResponse surfaces the is_host flag from the DB row."""
    from qiita_common.models import ReferenceResponse

    resp = ReferenceResponse(
        reference_idx=1,
        name="human",
        version="t2t-chm13v2.0",
        kind="sequence_reference",
        status="active",
        is_host=True,
        created_by_idx=42,
        created_at=datetime.now(UTC),
    )
    assert resp.is_host is True
    assert resp.model_dump()["is_host"] is True


def test_reference_index_model_round_trips():
    """ReferenceIndex describes a built index: where it is + how it was made."""
    from qiita_common.models import ReferenceIndex

    now = datetime.now(UTC)
    idx = ReferenceIndex(
        reference_index_idx=5,
        reference_idx=1,
        index_type="rype",
        fs_path="/srv/qiita/references/1/rype/index.ryxdi",
        params={"k": 64, "w": 25, "bucket_name": "reference_1"},
        created_at=now,
    )
    d = idx.model_dump()
    assert d["reference_idx"] == 1
    assert d["index_type"] == "rype"
    assert d["params"]["k"] == 64
    assert d["created_at"] == now


def test_reference_index_rejects_zero_idx():
    """reference_idx must be positive."""
    from qiita_common.models import ReferenceIndex

    with pytest.raises(ValidationError):
        ReferenceIndex(
            reference_index_idx=1,
            reference_idx=0,
            index_type="rype",
            fs_path="/x",
            params={},
            created_at=datetime.now(UTC),
        )


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
        is_host=False,
        created_by_idx=42,
        created_at=now,
    )
    d = resp.model_dump()
    assert d["reference_idx"] == 1
    assert d["status"] == "pending"
    assert d["is_host"] is False
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
        scope_target={"kind": "reference", "reference_idx": 1},
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
    kwargs["module"] = FASTQ_TO_PARQUET_MODULE
    req = StepRunRequest(**kwargs)
    assert req.module == FASTQ_TO_PARQUET_MODULE
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


# --- StepRunRequest scope_target discriminated-union validator -----------
# `_validate_scope_target` delegates to the ScopeTarget union; these tests
# ensure each kind validates with its required scalars, that the dispatch
# discriminator is enforced, and that per-kind required idx fields can't
# be omitted (the cases the existing tests above don't cover — they all
# pin scope_target = {"kind": "reference", "reference_idx": 1}).


def test_step_run_request_scope_target_prep_sample_validates():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    kwargs["scope_target"] = {"kind": "prep_sample", "prep_sample_idx": 42}
    # Native form pairs naturally with the prep_sample target shape
    # (fastq_to_parquet is the canonical native+prep_sample step).
    del kwargs["container"]
    kwargs["module"] = FASTQ_TO_PARQUET_MODULE
    req = StepRunRequest(**kwargs)
    assert req.scope_target == {"kind": "prep_sample", "prep_sample_idx": 42}


def test_step_run_request_scope_target_study_prep_validates():
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    kwargs["scope_target"] = {"kind": "study_prep", "study_idx": 7, "prep_idx": 11}
    req = StepRunRequest(**kwargs)
    assert req.scope_target == {"kind": "study_prep", "study_idx": 7, "prep_idx": 11}


@pytest.mark.parametrize(
    ("bad_scope_target", "expected_in_error"),
    [
        # Unknown discriminator value.
        ({"kind": "bogus", "reference_idx": 1}, "kind"),
        # Missing discriminator entirely.
        ({"reference_idx": 1}, "kind"),
        # prep_sample missing its required scalar.
        ({"kind": "prep_sample"}, "prep_sample_idx"),
        # study_prep missing one of its required scalars.
        ({"kind": "study_prep", "study_idx": 1}, "prep_idx"),
        ({"kind": "study_prep", "prep_idx": 2}, "study_idx"),
        # Wrong-kind scalar (reference_idx on a prep_sample shape).
        # Pydantic's discriminated union flags the missing prep_sample_idx;
        # extra wrong-kind fields are silently ignored by default. We
        # assert the missing-field message — that's the load-bearing signal.
        ({"kind": "prep_sample", "reference_idx": 1}, "prep_sample_idx"),
        # gt=0 enforcement on each kind's idx scalar.
        ({"kind": "reference", "reference_idx": 0}, "greater than"),
        ({"kind": "prep_sample", "prep_sample_idx": 0}, "greater than"),
        ({"kind": "study_prep", "study_idx": 0, "prep_idx": 1}, "greater than"),
        # Empty dict — no kind, no fields.
        ({}, "kind"),
    ],
)
def test_step_run_request_scope_target_rejects_invalid_shapes(bad_scope_target, expected_in_error):
    from qiita_common.models import StepRunRequest

    kwargs = _minimal_step_run_kwargs()
    kwargs["scope_target"] = bad_scope_target
    with pytest.raises(ValidationError) as exc_info:
        StepRunRequest(**kwargs)
    assert expected_in_error in str(exc_info.value)


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


def test_dedupe_secondary_study_idxs():
    """Duplicate entries in secondary_study_idxs are collapsed
    (order-preserving) at the wire boundary; this is benign normalization,
    distinct from primary appearing in secondary, which stays a hard
    rejection."""
    from qiita_common.models import SequencedSampleCreateRequest

    req = SequencedSampleCreateRequest(
        biosample_idx=1,
        prep_protocol_idx=1,
        owner_idx=1,
        sequenced_pool_item_id="X",
        primary_study_idx=1,
        secondary_study_idxs=[5, 5, 3],
    )
    assert req.secondary_study_idxs == [5, 3]


def test_missing_reason_ref_rejects_empty_name():
    """Tests the case where MissingReasonRef is constructed with an
    empty name: validation fails so the empty marker never reaches the
    wire boundary.
    """
    from qiita_common.models import MissingReasonRef

    with pytest.raises(ValidationError):
        MissingReasonRef(idx=1, name="")


def test_metadata_checklist_ref_from_row_populated():
    """Tests the case where from_row gets a non-null idx + name: it builds
    the ref carrying both."""
    from qiita_common.models import MetadataChecklistRef

    assert MetadataChecklistRef.from_row(3, "ERC000015") == MetadataChecklistRef(
        idx=3, name="ERC000015"
    )


def test_metadata_checklist_ref_from_row_none_idx():
    """Tests the case where the row has no checklist (null idx): from_row
    yields None rather than a ref."""
    from qiita_common.models import MetadataChecklistRef

    assert MetadataChecklistRef.from_row(None, None) is None


def test_metadata_checklist_ref_rejects_zero_idx():
    """Tests the case where idx is non-positive: construction fails."""
    from qiita_common.models import MetadataChecklistRef

    with pytest.raises(ValidationError):
        MetadataChecklistRef(idx=0, name="ERC000015")


def test_metadata_checklist_ref_rejects_empty_name():
    """Tests the case where MetadataChecklistRef is constructed with an
    empty name: validation fails so the empty ref never reaches the wire
    boundary."""
    from qiita_common.models import MetadataChecklistRef

    with pytest.raises(ValidationError):
        MetadataChecklistRef(idx=1, name="")
