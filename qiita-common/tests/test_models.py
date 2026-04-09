"""Tests for shared Pydantic models."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError


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
        created_by=UUID("a0000000-0000-0000-0000-000000000001"),
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
            created_by=UUID("a0000000-0000-0000-0000-000000000001"),
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
            created_by=UUID("a0000000-0000-0000-0000-000000000001"),
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
            created_by=UUID("a0000000-0000-0000-0000-000000000001"),
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


def test_feature_mint_request_rejects_duplicate_hashes():
    """FeatureMintRequest must reject duplicate sequence_hash values."""
    from qiita_common.models import FeatureHashEntry, FeatureMintRequest

    h = UUID("a0000000-0000-0000-0000-000000000001")
    with pytest.raises(ValidationError):
        FeatureMintRequest(
            entries=[FeatureHashEntry(sequence_hash=h), FeatureHashEntry(sequence_hash=h)]
        )


def test_phylogeny_tip_entry_valid():
    """PhylogenyTipEntry must accept valid input."""
    from qiita_common.models import PhylogenyTipEntry

    entry = PhylogenyTipEntry(reference_idx=1, node_index=0, feature_idx=100)
    assert entry.reference_idx == 1
    assert entry.node_index == 0
    assert entry.feature_idx == 100


def test_phylogeny_tip_request_rejects_duplicates():
    """PhylogenyTipRequest must reject duplicate (reference_idx, node_index) pairs."""
    from qiita_common.models import PhylogenyTipEntry, PhylogenyTipRequest

    with pytest.raises(ValidationError, match="Duplicate"):
        PhylogenyTipRequest(
            entries=[
                PhylogenyTipEntry(reference_idx=1, node_index=0, feature_idx=100),
                PhylogenyTipEntry(reference_idx=1, node_index=0, feature_idx=200),
            ]
        )


def test_phylogeny_tip_request_allows_different_nodes():
    """PhylogenyTipRequest must accept entries with distinct node_index values."""
    from qiita_common.models import PhylogenyTipEntry, PhylogenyTipRequest

    req = PhylogenyTipRequest(
        entries=[
            PhylogenyTipEntry(reference_idx=1, node_index=0, feature_idx=100),
            PhylogenyTipEntry(reference_idx=1, node_index=1, feature_idx=200),
        ]
    )
    assert len(req.entries) == 2
