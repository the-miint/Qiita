"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, EmailStr, Field, model_validator

# `SystemRole` is re-exported so existing `from qiita_common.models import SystemRole`
# imports keep working after the move to qiita_common.auth_constants.
from qiita_common.auth_constants import (  # noqa: F401
    MAX_NAME_LENGTH,
    MAX_TABLE_NAME_LENGTH,
    MAX_VERSION_LENGTH,
    PAT_MAX_TTL_DAYS,
    SERVICE_TOKEN_MAX_TTL_DAYS,
    SystemRole,
)

# ORCID iD format: four groups of four digits separated by hyphens, with the
# final character optionally being 'X' (the ISO 7064 mod-11-2 checksum).
# See https://orcid.org/.
ORCID_PATTERN = r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$"


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
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    kind: ReferenceKind


class ReferenceResponse(BaseModel):
    reference_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    kind: ReferenceKind
    status: ReferenceStatus
    # Phase H.c: the legacy `created_by` UUID column has been dropped.
    # `created_by_idx` is the canonical owner reference, FK to qiita.principal.
    created_by_idx: Annotated[int, Field(gt=0)]
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


class RegisterFilesRequest(BaseModel):
    staging_dir: str = Field(min_length=1)
    files: dict[str, str]  # {filename: ducklake_table_name}


class RegisterFilesResponse(BaseModel):
    registered: list[str]  # permanent paths of registered files


class DoGetTicketRequest(BaseModel):
    table: str = Field(min_length=1, max_length=MAX_TABLE_NAME_LENGTH)


class DoGetTicketResponse(BaseModel):
    ticket: str  # base64-encoded signed ticket bytes


# ============================================================================
# Auth: user-management models (Phase B)
# ============================================================================


class UserCreate(BaseModel):
    """Body for POST /api/v1/users — admin creates a user.

    Phase B uses the existing mock auth dep; the route flips to real auth
    in Phase H.b.
    """

    display_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    email: EmailStr
    affiliation: str = ""
    address: str = ""
    phone: str = ""
    orcid: Annotated[str | None, Field(pattern=ORCID_PATTERN)] = None
    receive_processing_emails: bool = True


class UserUpdate(BaseModel):
    """Body for PATCH /api/v1/users/me. Excludes email and status — those are
    immutable through the self-service path. Email-change requires a separate
    flow (re-verify via OIDC); status changes are admin-only."""

    affiliation: str | None = None
    address: str | None = None
    phone: str | None = None
    orcid: Annotated[str | None, Field(pattern=ORCID_PATTERN)] = None
    receive_processing_emails: bool | None = None


class UserResponse(BaseModel):
    """Returned by user-management endpoints."""

    principal_idx: Annotated[int, Field(gt=0)]
    display_name: str
    email: EmailStr
    affiliation: str
    address: str
    phone: str
    orcid: str | None
    receive_processing_emails: bool
    profile_complete: bool
    created_at: AwareDatetime
    updated_at: AwareDatetime


# ============================================================================
# Auth: API token mint / list models (Phase F)
# ============================================================================


class ApiTokenMintRequest(BaseModel):
    """Body for POST /api/v1/auth/pat (humans) and POST /api/v1/admin/service-accounts (workers).

    `scopes=None` means "default to the principal's full role ceiling" (humans
    only — service accounts must always specify scopes explicitly).
    `ttl_days=None` means "default to QIITA_TOKEN_DEFAULT_TTL_DAYS"; max 365.
    """

    label: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    scopes: list[str] | None = None
    ttl_days: Annotated[int, Field(gt=0, le=PAT_MAX_TTL_DAYS)] | None = None


class ApiTokenMintResponse(BaseModel):
    """Returned exactly once at mint time. The `token` field is the plaintext;
    capture it now and never log it. Subsequent requests retrieve only metadata
    via ApiTokenSummary."""

    token: str  # plaintext qk_... — shown once, never persisted past this response
    token_idx: Annotated[int, Field(gt=0)]
    label: str
    scopes: list[str]
    expires_at: AwareDatetime | None
    created_at: AwareDatetime


class ApiTokenSummary(BaseModel):
    """Returned by GET /api/v1/auth/tokens — metadata only, no plaintext or hash."""

    token_idx: Annotated[int, Field(gt=0)]
    label: str
    scopes: list[str]
    expires_at: AwareDatetime | None
    revoked_at: AwareDatetime | None
    last_used_at: AwareDatetime | None
    created_at: AwareDatetime


# ============================================================================
# Auth: admin-surface models (Phase G)
# ============================================================================


class ServiceAccountCreate(BaseModel):
    """Body for POST /api/v1/admin/service-accounts.

    Scopes are required (no implicit ceiling for service kind) — admins
    must explicitly state what the worker is allowed to do. ttl_days=None
    means no expiry; service tokens are typically long-lived and rotated
    by an out-of-band runbook.
    """

    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    description: str | None = None
    scopes: list[str] = Field(min_length=1)
    ttl_days: Annotated[int, Field(gt=0, le=SERVICE_TOKEN_MAX_TTL_DAYS)] | None = None
    label: str = Field(min_length=1, max_length=MAX_NAME_LENGTH, default="initial")


class ServiceAccountCreateResponse(BaseModel):
    """Returned exactly once at service-account creation. Captures both the
    new principal/service identity and the freshly-minted token."""

    principal_idx: Annotated[int, Field(gt=0)]
    name: str
    description: str | None
    token: str  # plaintext qk_... — shown once
    token_idx: Annotated[int, Field(gt=0)]
    scopes: list[str]
    expires_at: AwareDatetime | None
    created_at: AwareDatetime


class PrincipalDisabledUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principals/{idx}/disabled.

    `disabled=true` requires `reason` (audit-trail). `disabled=false` is
    the round-trip back to active and leaves the audit columns NULL via
    the principal_disabled_consistent CHECK.
    """

    disabled: bool
    reason: str | None = None


class PrincipalRetiredUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principals/{idx}/retired.

    Retirement is terminal (CHECK forbids active → retired → active);
    `reason` is required for the audit trail.
    """

    reason: str = Field(min_length=1)


class PrincipalSystemRoleUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principals/{idx}/system-role.

    `use_enum_values=True` so `model_dump()` returns the lowercase string
    (e.g. `"user"`) rather than the `SystemRole` member — preserves the
    JSON-serialised contract that pre-dated the StrEnum migration.
    """

    model_config = ConfigDict(use_enum_values=True)

    system_role: SystemRole
    reason: str | None = None


class AuthEventResponse(BaseModel):
    """One row from GET /api/v1/admin/audit."""

    event_idx: Annotated[int, Field(gt=0)]
    event_type: str
    principal_idx: int | None
    actor_principal_idx: int | None
    detail: dict
    occurred_at: AwareDatetime


class RevokeAllTokensResponse(BaseModel):
    """Returned by POST /api/v1/admin/principals/{idx}/revoke-all-tokens."""

    revoked_token_idxs: list[int]
    already_revoked_count: int
