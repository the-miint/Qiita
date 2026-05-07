"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
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


class FieldDataType(StrEnum):
    """Closed set of value kinds a biosample/sequenced_sample field may carry.

    Mirrors the Postgres `qiita.field_data_type` enum. Members map 1:1 to the
    value_* columns on the EAV metadata tables: a field with this data_type
    must have its value written into the matching value_* column. The match
    is enforced at write time by the biosample_metadata_apply_field_contract
    trigger (and its sequenced-sample twin).
    """

    TEXT = "text"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    DATE = "date"
    TERMINOLOGY = "terminology"


class Tier(StrEnum):
    """Closed set of access-tier values used for user-to-study access levels
    and for data-visibility requirements.

    Mirrors the Postgres `qiita.tier` enum. Members are listed in ascending
    privilege order; a higher tier implies all lower tiers' privileges.
    `study_access` rows cannot carry `'public'` — a principal with no
    `study_access` row has effective tier `'public'` by absence.
    """

    PUBLIC = "public"
    VIEWER = "viewer"
    MEMBER = "member"
    ADMIN = "admin"


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
    # `created_by_idx` is the canonical owner reference, FK to qiita.principal.
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime


# `genome_source` / `genome_source_id` and the `genome_fields_consistent`
# validator predate the Parquet refactor (commit 3cac813); under the
# path-based contract genome metadata flows through `genome_map.parquet`
# and the half-set check is enforced at the qiita.genome NOT NULL
# constraint instead (covered by
# test_library_mint_features_genome_map_with_null_source_id_fails). The
# fields and validator are kept so any caller that builds the model with
# genome data still gets the validator's protection.
class FeatureHashEntry(BaseModel):
    sequence_hash: UUID
    genome_source: str | None = None
    genome_source_id: str | None = None

    @model_validator(mode="after")
    def genome_fields_consistent(self):
        if (self.genome_source is None) != (self.genome_source_id is None):
            raise ValueError("genome_source and genome_source_id must both be set or both be null")
        return self


class StepRunRequest(BaseModel):
    """Body for POST /api/v1/step/run on the orchestrator.

    Issued by the control-plane runner for every workflow `step:` entry.
    The orchestrator dispatches to its configured ComputeBackend's
    `run_step`. Paths are absolute and live on the workspace shared
    between control plane and orchestrator.
    """

    step_name: str = Field(min_length=1)
    inputs: dict[str, str] = Field(default_factory=dict)
    workspace: str = Field(min_length=1)
    reference_idx: Annotated[int, Field(gt=0)]


class StepRunResponse(BaseModel):
    """Returned by POST /api/v1/step/run.

    `outputs` is the backend's name → path mapping, matching the YAML's
    declared step `outputs:`.
    """

    outputs: dict[str, str]


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


class DoGetTicketRequest(BaseModel):
    table: str = Field(min_length=1, max_length=MAX_TABLE_NAME_LENGTH)


class DoGetTicketResponse(BaseModel):
    ticket: str  # base64-encoded signed ticket bytes


# ============================================================================
# Auth: user-management models
# ============================================================================


class UserCreate(BaseModel):
    """Body for POST /api/v1/user — admin creates a user."""

    display_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    email: EmailStr
    affiliation: str = ""
    address: str = ""
    phone: str = ""
    orcid: Annotated[str | None, Field(pattern=ORCID_PATTERN)] = None
    receive_processing_emails: bool = True


class UserUpdate(BaseModel):
    """Body for PATCH /api/v1/user/me. Excludes email and status — those are
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
# Biosample import models
# ============================================================================


class BiosampleImportRequest(BaseModel):
    """Body for POST /api/v1/study/{study_idx}/biosample.

    The route gates on wet_lab_admin or higher; owner_idx names the user the
    biosample is being created for and must be supplied explicitly. The
    metadata dict carries text values keyed on biosample_global_field
    display_name; the route parses each value into the global field's data
    type before insert. An empty dict is allowed.
    """

    owner_idx: Annotated[int, Field(gt=0)]
    owner_biosample_id_field_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    owner_biosample_id_value: str = Field(min_length=1)
    metadata: dict[str, str] = Field(default_factory=dict)
    metadata_checklist_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: str | None = None
    ena_sample_accession: str | None = None


class BiosampleImportResponse(BaseModel):
    """Returned by POST /api/v1/study/{study_idx}/biosample on success."""

    biosample_idx: Annotated[int, Field(gt=0)]
    biosample_study_field_idx: Annotated[int, Field(gt=0)]
    biosample_study_field_created: bool


class BiosampleGlobalMetadataEntry(BaseModel):
    """One globally-linked metadata value for a biosample, with cosmetic context.

    Returned as a value inside BiosampleResponse.global_metadata, keyed on
    the field's `internal_name`. display_name and description are taken
    from biosample_global_field (the canonical labels), not from any
    per-study biosample_study_field override, because biosample reads
    are not study-scoped. data_type identifies which Python type carries
    the value: TEXT -> str, NUMERIC -> Decimal, DATE -> date.
    """

    display_name: str
    description: str | None
    data_type: FieldDataType
    value: str | Decimal | date


class BiosampleResponse(BaseModel):
    """Returned by GET /api/v1/biosample/{biosample_idx}.

    Mirrors qiita.biosample's caller-visible columns and embeds a dict
    of every globally-linked metadata value the biosample carries,
    keyed on biosample_global_field.internal_name. Purely-local
    metadata (including the owner-biosample-id row) and metadata whose
    biosample_to_study link has been retired are excluded -- both
    surface as biosample_metadata.global_field_idx IS NULL via the
    existing schema triggers and are filtered out by the read.
    `caller_system_role` carries the caller's principal.system_role
    verbatim from the database.
    """

    biosample_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    metadata_checklist_idx: int | None
    biosample_accession: str | None
    ena_sample_accession: str | None
    last_submission_at: AwareDatetime | None
    submission_error: str | None
    last_metadata_change_at: AwareDatetime | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    updated_at: AwareDatetime
    retired: bool
    retired_by_idx: int | None
    retired_at: AwareDatetime | None
    retire_reason: str | None
    global_metadata: dict[str, BiosampleGlobalMetadataEntry]
    caller_system_role: SystemRole


class BiosamplePatchRequest(BaseModel):
    """Body for PATCH /api/v1/biosample/{biosample_idx}.

    Every editable field is optional; the route distinguishes "absent"
    (do not write) from "explicit null" (set the column to NULL) by
    inspecting `model_fields_set`. extra="forbid" rejects bodies that
    name immutable or retirement-managed columns (idx, retired,
    retired_at, retired_by_idx, retire_reason, created_by_idx,
    created_at, updated_at, last_metadata_change_at) with 422. The
    model-level validator enforces the "at least one field" rule and
    the NOT-NULL invariant on owner_idx.
    """

    model_config = ConfigDict(extra="forbid")

    metadata_checklist_idx: Annotated[int, Field(gt=0)] | None = None
    owner_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: str | None = None
    ena_sample_accession: str | None = None
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None

    @model_validator(mode="after")
    def at_least_one_field_and_owner_not_null(self):
        # Empty bodies are rejected here so the route does not have to
        # special-case the "no-op PATCH" path.
        if not self.model_fields_set:
            raise ValueError("at least one editable field is required")

        # owner_idx maps to a NOT NULL column; explicit null is invalid
        # input even though the field is typed Optional for the
        # "absent vs null" distinguishing pattern shared with the
        # other fields.
        if "owner_idx" in self.model_fields_set and self.owner_idx is None:
            raise ValueError("owner_idx may not be null")

        return self


class BiosampleIdxsListResponse(BaseModel):
    """Returned by GET /api/v1/study/{study_idx}/biosample/list-idxs.

    Single-shot bulk-id envelope: every biosample_idx linked to the
    study, subject to retirement filtering, up to a hard cap.
    `truncated` is true when the underlying set exceeded the cap;
    clients seeing it should narrow their scope.
    `caller_system_role` carries the caller's principal.system_role
    verbatim from the database.
    """

    biosample_idxs: list[int]
    count: Annotated[int, Field(ge=0)]
    truncated: bool
    caller_system_role: SystemRole


# ============================================================================
# Study create models
# ============================================================================


# Column-length budgets mirror the qiita.study schema; keeping the limits
# here lets Pydantic reject oversized inputs before they hit Postgres.
_STUDY_TITLE_MAX = 500
_STUDY_ALIAS_MAX = 255
_STUDY_FUNDING_MAX = 500
_STUDY_ACCESSION_MAX = 50


class StudyCreate(BaseModel):
    """Body for POST /api/v1/study — create a study.

    `owner_idx=None` means "default to the calling principal_idx" (caller-
    creates-own-study). When supplied as a different principal, the route
    enforces wet_lab_admin or higher (the lab-tech-on-behalf rule). The
    study row's `created_by_idx` is always the caller; only `owner_idx` is
    transferred. `default_tier=None` lets the DB default ('member') apply.
    """

    title: str = Field(min_length=1, max_length=_STUDY_TITLE_MAX)
    owner_idx: Annotated[int, Field(gt=0)] | None = None
    principal_investigator_idx: Annotated[int, Field(gt=0)] | None = None
    alias: str | None = Field(default=None, max_length=_STUDY_ALIAS_MAX)
    description: str | None = None
    abstract: str | None = None
    funding: str | None = Field(default=None, max_length=_STUDY_FUNDING_MAX)
    ebi_study_accession: str | None = Field(default=None, max_length=_STUDY_ACCESSION_MAX)
    vamps_id: str | None = Field(default=None, max_length=_STUDY_ACCESSION_MAX)
    notes: str | None = None
    extra_metadata: dict[str, object] | None = None
    default_tier: Tier | None = None


class StudyResponse(BaseModel):
    """Returned by POST /api/v1/study on success.

    Mirrors the qiita.study row's caller-visible columns, with the
    generated search_vector and parent_study_idx (not exposed in v1)
    omitted.
    """

    study_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    principal_investigator_idx: int | None
    title: str
    alias: str | None
    description: str | None
    abstract: str | None
    funding: str | None
    ebi_study_accession: str | None
    vamps_id: str | None
    notes: str | None
    extra_metadata: dict[str, object] | None
    default_tier: Tier
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    updated_at: AwareDatetime


# ============================================================================
# Auth: API token mint / list models
# ============================================================================


class ApiTokenMintRequest(BaseModel):
    """Body for POST /api/v1/auth/pat (humans) and POST /api/v1/admin/service-account (workers).

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
    """Returned by GET /api/v1/auth/token — metadata only, no plaintext or hash."""

    token_idx: Annotated[int, Field(gt=0)]
    label: str
    scopes: list[str]
    expires_at: AwareDatetime | None
    revoked_at: AwareDatetime | None
    last_used_at: AwareDatetime | None
    created_at: AwareDatetime


class CliLoginExchangeRequest(BaseModel):
    """Body for POST /api/v1/auth/cli-exchange.

    The CLI redeems a one-time `ot_code` it captured from the AuthRocket
    handoff redirect. Server consumes the row atomically and returns the
    PAT plaintext exactly once via ApiTokenMintResponse.
    """

    ot_code: str = Field(min_length=16, max_length=128)


# ============================================================================
# Auth: admin-surface models
# ============================================================================


class ServiceAccountCreate(BaseModel):
    """Body for POST /api/v1/admin/service-account.

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


# ---------------------------------------------------------------------------
# /auth/whoami — discriminated union over principal kind
# ---------------------------------------------------------------------------


class WhoAmIHumanResponse(BaseModel):
    """`/auth/whoami` response when a HumanUser is authenticated."""

    kind: Literal["human"]
    principal_idx: Annotated[int, Field(gt=0)]
    email: str
    system_role: str
    scopes: list[str]
    profile_complete: bool


class WhoAmIServiceResponse(BaseModel):
    """`/auth/whoami` response when a ServiceAccount is authenticated."""

    kind: Literal["service"]
    principal_idx: Annotated[int, Field(gt=0)]
    name: str
    scopes: list[str]


class WhoAmIAnonymousResponse(BaseModel):
    """`/auth/whoami` response for an unauthenticated caller."""

    kind: Literal["anonymous"]


# Discriminated union — Pydantic / OpenAPI dispatch on the `kind` field.
WhoAmIResponse = Annotated[
    WhoAmIHumanResponse | WhoAmIServiceResponse | WhoAmIAnonymousResponse,
    Field(discriminator="kind"),
]


class PrincipalDisabledUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/disabled.

    `disabled=true` requires `reason` (audit-trail). `disabled=false` is
    the round-trip back to active and leaves the audit columns NULL via
    the principal_disabled_consistent CHECK.
    """

    disabled: bool
    reason: str | None = None


class PrincipalRetiredUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/retired.

    Retirement is terminal (CHECK forbids active → retired → active);
    `reason` is required for the audit trail.
    """

    reason: str = Field(min_length=1)


class PrincipalSystemRoleUpdate(BaseModel):
    """Body for PATCH /api/v1/admin/principal/{idx}/system-role.

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
    """Returned by POST /api/v1/admin/principal/{idx}/revoke-all-tokens."""

    revoked_token_idxs: list[int]
    already_revoked_count: int


# ============================================================================
# Work tickets / actions
# ============================================================================
#
# A WorkTicket is the control-plane's record of an action invocation: who
# requested it, which resource it targets, what action-specific context it
# carries, and what lifecycle state it's in. The orchestrator pulls tickets
# off the queue, dispatches the action's step pipeline (one or more `step`
# entries plus zero or more control-plane `action` entries), and reports
# completion back via state transitions.
#
# `originator_principal_idx` is the submitter; resource profile and SLURM
# priority resolve from the originator, not the executor.


class StepType(StrEnum):
    """Workflow step types.

    `map` runs per-sample (N independent jobs across N samples).
    `reduce` runs once over the union of map outputs.
    `singleton` runs once per workflow invocation — used for system-internal
    one-shots like reference loading.

    `action` (control-plane Postgres-transaction primitive) is *not* a step
    type; it appears as a peer entry in workflow YAML and runs in-process
    in the control plane.
    """

    MAP = "map"
    REDUCE = "reduce"
    SINGLETON = "singleton"


class ScopeTargetKind(StrEnum):
    """Closed set of work-ticket scope-target kinds. Mirrored DB-side by
    the qiita.scope_target_kind ENUM; both work_ticket.scope_target_kind
    and action.target_kind reference it."""

    STUDY_PREP = "study_prep"
    REFERENCE = "reference"


class WorkTicketState(StrEnum):
    """Work-ticket lifecycle. Mirrored DB-side by qiita.work_ticket_state.

    Submission gates: PENDING / QUEUED / PROCESSING block resubmission of
    the same `(scope_target, action_id, action_version)` triple entirely.
    COMPLETED requires explicit DELETE before resubmission. FAILED is the
    permanent-failure terminal state; recovery is operator-driven.
    """

    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class StudyPrepScopeTarget(BaseModel):
    """Work ticket targets a (study, prep) tuple — used for sample-processing
    actions (e.g. deblur, woltka)."""

    kind: Literal[ScopeTargetKind.STUDY_PREP]
    study_idx: Annotated[int, Field(gt=0)]
    prep_idx: Annotated[int, Field(gt=0)]


class ReferenceScopeTarget(BaseModel):
    """Work ticket targets a single reference — used for reference-add and
    any future reference-mutation action."""

    kind: Literal[ScopeTargetKind.REFERENCE]
    reference_idx: Annotated[int, Field(gt=0)]


# Discriminated union — Pydantic and OpenAPI dispatch on the `kind` field.
# DB-side, the same shape is encoded as a tagged union of typed columns
# (`scope_target_kind` plus the subset-relevant `study_idx` / `prep_idx` /
# `reference_idx`) guarded by a CHECK constraint; the `kind` here is the
# discriminator that maps to that column.
ScopeTarget = Annotated[
    StudyPrepScopeTarget | ReferenceScopeTarget,
    Field(discriminator="kind"),
]


class WorkTicket(BaseModel):
    """Control-plane record of an action invocation.

    `(action_id, action_version)` FK into `qiita.action` and pin the exact
    action definition this ticket was submitted against.

    `scope_target` answers "which resource is this work about?" — the
    resource-ACL gate keys off it. `action_context` carries action-defined
    free-form state, validated at submission against the action's declared
    `context_schema`.
    """

    work_ticket_idx: Annotated[int, Field(gt=0)]
    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    action_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    originator_principal_idx: Annotated[int, Field(gt=0)]
    scope_target: ScopeTarget
    action_context: dict[str, Any] = Field(default_factory=dict)
    state: WorkTicketState
    created_at: AwareDatetime
    updated_at: AwareDatetime
