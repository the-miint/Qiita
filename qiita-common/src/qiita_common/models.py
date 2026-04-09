"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, Field, model_validator


class HealthResponse(BaseModel):
    status: str
    service: str


class ReferenceStatus(StrEnum):
    PENDING = "pending"
    HASHING = "hashing"
    MINTING = "minting"
    LOADING = "loading"
    ACTIVE = "active"
    FAILED = "failed"


ReferenceKind = Literal["sequence_reference", "taxonomy_authority"]


class ReferenceCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    version: str = Field(min_length=1, max_length=100)
    kind: ReferenceKind


class ReferenceResponse(BaseModel):
    reference_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    kind: ReferenceKind
    status: ReferenceStatus
    created_by: UUID
    created_at: AwareDatetime


class FeatureHashEntry(BaseModel):
    sequence_hash: UUID
    genome_source: str | None = None
    genome_source_id: str | None = None

    @model_validator(mode="after")
    def genome_fields_consistent(self):
        if (self.genome_source is None) != (self.genome_source_id is None):
            raise ValueError("genome_source and genome_source_id must both be set or both be null")
        return self


class FeatureMintRequest(BaseModel):
    entries: list[FeatureHashEntry] = Field(min_length=1)

    @model_validator(mode="after")
    def no_duplicate_hashes(self):
        hashes = [e.sequence_hash for e in self.entries]
        if len(hashes) != len(set(hashes)):
            raise ValueError("entries must not contain duplicate sequence_hash values")
        return self


class FeatureMintResponse(BaseModel):
    mapping: dict[UUID, int]
    minted: int
    reused: int


# Valid status transitions for references.
VALID_STATUS_TRANSITIONS: dict[ReferenceStatus, set[ReferenceStatus]] = {
    ReferenceStatus.PENDING: {ReferenceStatus.HASHING, ReferenceStatus.FAILED},
    ReferenceStatus.HASHING: {ReferenceStatus.MINTING, ReferenceStatus.FAILED},
    ReferenceStatus.MINTING: {ReferenceStatus.LOADING, ReferenceStatus.FAILED},
    ReferenceStatus.LOADING: {ReferenceStatus.ACTIVE, ReferenceStatus.FAILED},
    # ACTIVE is a terminal success state. To remediate a broken active reference,
    # delete it and re-create. No direct transition to FAILED — that path is only
    # for in-progress references that encounter errors during ingestion.
    ReferenceStatus.ACTIVE: set(),
    ReferenceStatus.FAILED: {ReferenceStatus.PENDING},
}


class ReferenceStatusUpdate(BaseModel):
    status: ReferenceStatus


class PhylogenyTipEntry(BaseModel):
    reference_idx: Annotated[int, Field(gt=0)]
    node_index: Annotated[int, Field(ge=0)]
    feature_idx: Annotated[int, Field(gt=0)]


class PhylogenyTipRequest(BaseModel):
    entries: list[PhylogenyTipEntry] = Field(min_length=1)

    @model_validator(mode="after")
    def no_duplicate_node_indices(self):
        seen = set()
        for e in self.entries:
            key = (e.reference_idx, e.node_index)
            if key in seen:
                raise ValueError(
                    f"Duplicate (reference_idx, node_index): ({e.reference_idx}, {e.node_index})"
                )
            seen.add(key)
        return self


class PhylogenyTipResponse(BaseModel):
    inserted: int


class DoGetTicketRequest(BaseModel):
    table: str = Field(min_length=1, max_length=64)


class DoGetTicketResponse(BaseModel):
    ticket: str  # base64-encoded signed ticket bytes
