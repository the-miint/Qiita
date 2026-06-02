"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    field_validator,
    model_validator,
)
from pydantic.types import Base64Bytes

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

# matrix_tube_id values are digit-only (per local convention) and may carry
# leading zeros; the {8,10} quantifier also rejects the empty string and
# bounds the length range.
#
# Deliberately duplicated with the column-level CHECK on
# qiita.biosample.matrix_tube_id: the Pydantic side fails at the wire
# boundary with a per-field 422; the DB side is the last line of defense.
# Change one and you must change the other in the same PR.
MATRIX_TUBE_ID_PATTERN = r"^[0-9]{8,10}$"  # same-pattern-ok: DB CHECK parity (see above)


class HealthStatus(StrEnum):
    """Health states used in `HealthResponse.status` and the per-service
    entries inside `HealthResponse.services`.

    Closed set — both the CP aggregator and the landing-page JS pin
    against these literal values, so adding or renaming a member is a
    wire contract change.

    - `OK`: probe succeeded.
    - `DEGRADED`: probe succeeded but the responding service self-
      reported a non-ok state (200 with `status != "ok"`, gRPC
      `Health.Check` returning a state other than `SERVING`, etc.).
    - `UNREACHABLE`: probe failed at the transport layer (timeout,
      connection refused, non-2xx response, parse error). The
      service may be alive but we can't tell.
    - `UNCONFIGURED`: no URL is configured for this service (e.g. a
      CP-only dev instance). Informational — does NOT demote the
      overall aggregate.
    """

    OK = "ok"
    DEGRADED = "degraded"
    UNREACHABLE = "unreachable"
    UNCONFIGURED = "unconfigured"


class HealthResponse(BaseModel):
    """Health-check response shared across the three services.

    `status` and `service` are the original v1 surface — a binary
    `ok` / `degraded` summary and the responding service's name.
    Every existing consumer (the `make verify-health` Makefile target,
    the landing-page JS, monitoring scrapes) reads only these two
    fields and stays compatible.

    `services` is an optional per-component breakdown the control
    plane populates when its `/health` aggregates its own DB probe
    with downstream probes against the orchestrator and the data
    plane. The orchestrator's `/health` leaves it `None` — its
    aggregate is the single `status` field. Keys are component slugs
    (`cp` / `co` / `dp`); values are per-service status strings drawn
    from `HealthStatus`. We intentionally keep this as `dict[str,
    str]` rather than a typed Pydantic submodel so adding a new
    service slug doesn't force a wire-shape revision — both the JS
    and the CP have to know keys anyway, so a typed submodel would
    add code surface without preventing the lockstep change.
    """

    status: str
    service: str
    services: dict[str, str] | None = None


class ReferenceStatus(StrEnum):
    """Lifecycle states of a reference database during staging.

    Mirrored DB-side by the `status` column on `qiita.reference`, which is a
    plain `TEXT` + `CHECK` column (not a Postgres `CREATE TYPE` ENUM) — so this
    enum is intentionally not covered by the parity tests. Keep this set and
    the matching `CHECK` list in sync by hand.
    """

    PENDING = "pending"
    HASHING = "hashing"
    MINTING = "minting"
    LOADING = "loading"
    # `indexing` is entered only by the host-reference-add workflow, after
    # `loading`, while the rype index is built. Regular references skip it
    # (loading → active directly), so `loading` keeps both outgoing edges.
    INDEXING = "indexing"
    ACTIVE = "active"
    FAILED = "failed"


class TerminologyStatus(StrEnum):
    """Lifecycle states of a terminology row.

    Mirrors the Postgres `qiita.terminology_status` enum. `loading` while a
    load is in flight; `active` when the load is complete and the row
    reflects a consistent terminology version; `failed` when a load aborted
    and the row's contents may be inconsistent with the source.
    """

    LOADING = "loading"
    ACTIVE = "active"
    FAILED = "failed"


class TerminologyTermObsoletionKind(StrEnum):
    """Reason a terminology_term row was marked obsolete on the most
    recent load.

    Mirrors the Postgres `qiita.terminology_term_obsoletion_kind` enum.
    `source_deprecated` when the source vocabulary deprecates the term;
    `source_merged` when the source merges this term into another;
    `silently_dropped` when the term disappears from a reload without a
    recorded replacement.
    """

    SOURCE_DEPRECATED = "source_deprecated"
    SOURCE_MERGED = "source_merged"
    SILENTLY_DROPPED = "silently_dropped"


class FieldDataType(StrEnum):
    """Closed set of value kinds a biosample/prep_sample field may carry.

    Mirrors the Postgres `qiita.field_data_type` enum. Members map 1:1 to the
    value_* columns on the EAV metadata tables: a field with this data_type
    must have its value written into the matching value_* column. The match
    is enforced at write time by the biosample_metadata_apply_field_contract
    trigger (and its prep-sample twin).
    """

    TEXT = "text"
    NUMERIC = "numeric"
    BOOLEAN = "boolean"
    DATE = "date"
    TERMINOLOGY = "terminology"


class Platform(StrEnum):
    """Closed set of sequencing platforms recognized by the system.

    Mirrors the Postgres `qiita.platform` enum. Values are the canonical
    platform names from ENA's SRA XSD, lowercased for Postgres convention,
    so downstream submission paths can map 1:1 without a translation
    table. New values may be added as additional platforms come online;
    existing values cannot be removed once any row references them.
    """

    ILLUMINA = "illumina"
    PACBIO_SMRT = "pacbio_smrt"
    OXFORD_NANOPORE = "oxford_nanopore"
    DNBSEQ = "dnbseq"
    LS454 = "ls454"
    ION_TORRENT = "ion_torrent"
    COMPLETE_GENOMICS = "complete_genomics"


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
    # Orthogonal to `kind`: a host reference is still a sequence_reference,
    # but is used as a negative filter (reads matching it are removed). The
    # rype index built for it is wired in as rype's `negative_index`.
    is_host: bool = False


class ReferenceResponse(BaseModel):
    reference_idx: Annotated[int, Field(gt=0)]
    name: str
    version: str
    kind: ReferenceKind
    status: ReferenceStatus
    is_host: bool
    # `created_by_idx` is the canonical owner reference, FK to qiita.principal.
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime


class ReferenceIndex(BaseModel):
    """A built search index for a reference (e.g. a rype `.ryxdi` directory).

    The control plane tracks *where* the index lives and *how* it was built;
    the authoritative manifest (buckets, minimizer params, etc.) lives inside
    the index artifact itself. Mirrors the `qiita.reference_index` row. There
    may be more than one index per reference (different `index_type`, or — once
    references can grow — newer generations of the same type)."""

    reference_index_idx: Annotated[int, Field(gt=0)]
    reference_idx: Annotated[int, Field(gt=0)]
    index_type: str
    fs_path: str
    params: dict[str, Any]
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


class StepBaselineResources(BaseModel):
    """Resource ask for one workflow step. Mirrors qiita_common.actions.
    BaselineResources but lives here so the over-the-wire StepRunRequest
    can include it without a circular import (actions.py imports models)."""

    cpu: Annotated[int, Field(gt=0)]
    mem_gb: Annotated[int, Field(gt=0)]
    walltime_seconds: Annotated[int, Field(gt=0)]
    gpu: Annotated[int, Field(ge=0)] = 0


def check_exactly_one_runtime(
    *,
    container: str | None,
    module: str | None,
    entrypoint: str | None,
    owner: str,
) -> None:
    """Shared runtime-selection check for WorkflowStep (YAML side) and
    StepRunRequest (wire side). Raises ValueError when the shape is wrong.
    Kept in one place so the rule can't drift between the two layers."""
    if (container is None) == (module is None):
        raise ValueError(f"{owner} must declare exactly one of 'container' or 'module'")
    if entrypoint is not None and container is None:
        raise ValueError("'entrypoint' requires 'container'")


class StepRunRequest(BaseModel):
    """Body for POST /api/v1/step/run on the orchestrator.

    Issued by the control-plane runner for every workflow `step:` entry.
    The orchestrator dispatches to its configured ComputeBackend's
    `run_step`. Paths are absolute and live on the workspace shared
    between control plane and orchestrator.

    Runtime selection (`container` vs `module`) follows the same rules
    as `qiita_common.actions.WorkflowStep` — exactly one must be set,
    enforced by the same `check_exactly_one_runtime` helper. See that
    class's docstring for the container-vs-native semantics.

    `work_ticket_idx` flows through so SlurmBackend can stamp the SLURM
    job name with the originating ticket id — making scheduler dumps
    cross-referenceable back to the work_ticket row.

    `scope_target` carries the work ticket's discriminated-union scope
    target (matches `qiita_common.models.ScopeTarget`). The container
    path inspects `scope_target["kind"]` and extracts the scalar(s) it
    needs (e.g. `reference_idx` for reference-add); the native path
    routes the dict through `flatten_native_inputs`, which merges the
    scope's idx scalars into the job's `Inputs` model. Typed as a dict
    (not the ScopeTarget union directly) to avoid a forward-reference /
    model_rebuild dance — the field validator below runs the same
    discriminated-union validation as `WorkTicket.scope_target` AND
    normalizes the dict to JSON shape (`mode="json"`), so callers that
    pass enum objects (e.g. `{"kind": ScopeTargetKind.REFERENCE}`) get
    string values out the back. Downstream code can rely on
    `scope_target["kind"] == ScopeTargetKind.X.value` without worrying
    about which input shape produced the dict.
    """

    step_name: str = Field(min_length=1)
    inputs: dict[str, str] = Field(default_factory=dict)
    workspace: str = Field(min_length=1)
    scope_target: dict[str, Any]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    container: str | None = Field(default=None, min_length=1, max_length=512)
    module: str | None = Field(default=None, min_length=1, max_length=512)
    entrypoint: str | None = None
    baseline_resources: StepBaselineResources | None = None

    @field_validator("scope_target", mode="after")
    @classmethod
    def _validate_scope_target(cls, v: dict[str, Any]) -> dict[str, Any]:
        # Delegate to the ScopeTarget discriminated union (defined later
        # in this module) so the wire-side validation rule lives in one
        # place. Returns a JSON-shape dict so enum inputs (e.g.
        # `kind=ScopeTargetKind.REFERENCE`) come back as plain strings —
        # callers compare against `.value` without caring how the dict
        # was constructed.
        from pydantic import TypeAdapter

        return TypeAdapter(ScopeTarget).validate_python(v).model_dump(mode="json")

    @model_validator(mode="after")
    def _exactly_one_runtime(self) -> StepRunRequest:
        # Mirrors WorkflowStep's exactly-one rule at the wire boundary.
        # Pydantic raises a 422 at FastAPI deserialization, before any
        # backend code runs — single enforcement point, no per-backend
        # drift risk.
        check_exactly_one_runtime(
            container=self.container,
            module=self.module,
            entrypoint=self.entrypoint,
            owner="StepRunRequest",
        )
        return self


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
    # `loading` keeps its direct `→ active` edge for regular references (which
    # never build an index); the host-reference-add workflow instead routes
    # `loading → indexing → active` while it builds the rype index.
    ReferenceStatus.LOADING: {
        ReferenceStatus.INDEXING,
        ReferenceStatus.ACTIVE,
        ReferenceStatus.FAILED,
    },
    ReferenceStatus.INDEXING: {ReferenceStatus.ACTIVE, ReferenceStatus.FAILED},
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
# Upload: generic Arrow-data staging slots
# ============================================================================
# The upload domain is content-agnostic on purpose — no reference_idx, no
# role enum. A `qiita.upload` row is a handle on staged bytes; the workflow
# that references the handle in its `action_context` is what knows what
# the upload IS.


class UploadStatus(StrEnum):
    """Mirrored by the `upload.status` CHECK constraint in
    db/migrations/20260521000000_upload.sql. Stored as TEXT/CHECK, not a
    Postgres ENUM — same carve-out as ReferenceStatus and AuthEventType;
    see CLAUDE.md "Enum parity". Keep both sides in sync by hand."""

    PENDING = "pending"
    READY = "ready"
    CONSUMED = "consumed"
    FAILED = "failed"


class UploadCreateRequest(BaseModel):
    """Body for POST /api/v1/upload.

    `description` is free-form audit text — optional. The slot itself has
    no consumer-specific fields; binding to a reference / study / etc.
    happens later via the work_ticket that references the upload_idx.
    """

    description: str | None = Field(default=None, max_length=MAX_NAME_LENGTH)


# sha256 wire shape: 64 lowercase hex characters. Pinned at the model
# layer so a misbehaving client surfaces as a 422 before the DB write.
_SHA256_HEX_PATTERN = r"^[0-9a-f]{64}$"


class UploadCreateResponse(BaseModel):
    """Returned by POST /api/v1/upload with HTTP 201.

    `doput_ticket` is the base64-encoded HMAC-signed Flight ticket the
    client passes to the data plane on DoPut. The ticket's payload carries
    only `upload_idx`; the data plane resolves the staging path itself.
    The client never names server-side paths.
    """

    upload_idx: Annotated[int, Field(gt=0)]
    doput_ticket: str


class UploadDoneRequest(BaseModel):
    """Body for POST /api/v1/upload/{idx}/done.

    The client forwards the sha256 + row_count + bytes_received the data
    plane returned in its PutResult body. These are recorded descriptively;
    a future authenticated DP→CP channel can replace the client-forwarded
    claim with a server-verified signature.
    """

    sha256: Annotated[str, Field(pattern=_SHA256_HEX_PATTERN)]
    row_count: Annotated[int, Field(ge=0)]
    bytes_received: Annotated[int, Field(ge=0)]


class UploadResponse(BaseModel):
    """Returned by GET /api/v1/upload/{idx} and POST /api/v1/upload/{idx}/done."""

    upload_idx: Annotated[int, Field(gt=0)]
    status: UploadStatus
    description: str | None = None
    sha256: str | None = None
    row_count: int | None = None
    bytes_received: int | None = None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    completed_at: AwareDatetime | None = None


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

    The route gates on `Tier.ADMIN` access to the path's study
    (study owner, an ADMIN study_access row, or wet_lab_admin+ via the
    role bypass). owner_idx names the user the biosample is being
    created for and must be supplied explicitly. The metadata dict
    carries text values keyed on biosample_global_field display_name;
    the route parses each value into the global field's data type
    before insert. An empty dict is allowed.
    """

    owner_idx: Annotated[int, Field(gt=0)]
    owner_biosample_id_field_name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    owner_biosample_id_value: str = Field(min_length=1)
    metadata: dict[str, str] = Field(default_factory=dict)
    metadata_checklist_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: str | None = None
    ena_sample_accession: str | None = None
    matrix_tube_id: Annotated[
        str | None,
        Field(pattern=MATRIX_TUBE_ID_PATTERN),
    ] = None


class BiosampleImportResponse(BaseModel):
    """Returned by POST /api/v1/study/{study_idx}/biosample on success.

    `owner_id_biosample_study_field_*` name the biosample_study_field row
    that holds the owner-biosample-id for this study — the purely-local,
    PII-tier-pinned field flagged is_owner_biosample_id=True on the
    associated biosample_metadata row.
    """

    biosample_idx: Annotated[int, Field(gt=0)]
    owner_id_biosample_study_field_idx: Annotated[int, Field(gt=0)]
    owner_id_biosample_study_field_created: bool


# SQL column name on biosample_metadata / prep_sample_metadata that holds
# an intentionally-missing entry's qiita.missing_value_reason FK. Exposed
# here so MissingReasonRef.value_column has one source of truth and the
# repository-side write dispatch can import it from one place.
MISSING_REASON_VALUE_COLUMN = "value_missing_reason_idx"

# SQL column name on biosample_metadata / prep_sample_metadata that holds
# a terminology-term entry's qiita.terminology_term FK. Mirrors
# MISSING_REASON_VALUE_COLUMN for the terminology variant of the resolved
# value sentinels.
TERMINOLOGY_TERM_VALUE_COLUMN = "value_terminology_term_idx"


class MissingReasonRef(BaseModel):
    """Resolved-once shape for a metadata text value recognised as a marker
    for an intentionally-missing entry. Carries the qiita.missing_value_reason
    row's idx (the FK target on *_metadata.value_missing_reason_idx) and
    the matched reason name. `kind` discriminates this variant from other
    dict-shaped value variants on GlobalMetadataEntry.value. value_column
    is the target value_* column for a missing-reason write.
    """

    kind: Literal["missing_reason"] = "missing_reason"
    idx: Annotated[int, Field(gt=0)]
    name: Annotated[str, Field(min_length=1)]

    @property
    def value_column(self) -> str:
        return MISSING_REASON_VALUE_COLUMN


class TerminologyTermRef(BaseModel):
    """Resolved-once shape for a metadata text value matched against a
    qiita.terminology_term row scoped to the field's terminology_idx.
    Carries the term's idx (the FK target on
    *_metadata.value_terminology_term_idx), its term_id (the CURIE the
    caller passed) and its label (the human-readable term name).
    `kind` discriminates this variant from other dict-shaped value
    variants on GlobalMetadataEntry.value. value_column is the target
    value_* column for a terminology-term write.
    """

    kind: Literal["terminology_term"] = "terminology_term"
    idx: Annotated[int, Field(gt=0)]
    term_id: Annotated[str, Field(min_length=1)]
    label: Annotated[str, Field(min_length=1)]

    @property
    def value_column(self) -> str:
        return TERMINOLOGY_TERM_VALUE_COLUMN


class GlobalMetadataEntry(BaseModel):
    """One globally-linked metadata value for a biosample or prep_sample,
    with cosmetic context.

    Returned as a value inside *Response.global_metadata, keyed on the
    field's `internal_name`. display_name and description are taken from
    the canonical *_global_field row, not from any per-study *_study_field
    override, because these reads are not study-scoped. data_type
    identifies which Python type carries the value: TEXT -> str,
    NUMERIC -> Decimal, DATE -> date; a MissingReasonRef carries an
    intentionally-missing entry's reason idx + name; a TerminologyTermRef
    carries a terminology-term entry's idx + term_id + label. Both Ref
    variants supersede data_type-driven decoding.
    """

    display_name: str
    description: str | None
    data_type: FieldDataType
    value: (
        str
        | Decimal
        | date
        | Annotated[MissingReasonRef | TerminologyTermRef, Field(discriminator="kind")]
    )


class BiosampleResponse(BaseModel):
    """Returned by GET /api/v1/biosample/{biosample_idx}.

    Mirrors qiita.biosample's caller-visible columns and embeds a dict
    of every globally-linked metadata value the biosample carries,
    keyed on biosample_global_field.internal_name. Purely-local
    metadata (including the owner-biosample-id row) and metadata whose
    biosample_to_study link has been retired are excluded -- both
    surface as biosample_metadata.global_field_idx IS NULL via the
    existing schema triggers and are filtered out by the read.
    Intentionally-missing entries (value_missing_reason_idx populated)
    surface via a MissingReasonRef in the entry's `value` field;
    terminology-term entries (value_terminology_term_idx populated)
    surface via a TerminologyTermRef. `caller_system_role` carries the
    caller's principal.system_role verbatim from the database.
    """

    biosample_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    metadata_checklist_idx: int | None
    biosample_accession: str | None
    ena_sample_accession: str | None
    matrix_tube_id: str | None
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
    global_metadata: dict[str, GlobalMetadataEntry]
    caller_system_role: SystemRole


class BiosampleLookupByAccessionRequest(BaseModel):
    """Body for POST /api/v1/biosample/lookup-by-accession.

    Resolves a list of NCBI BioSample accession strings (or any
    biosample_accession value the deploy stores) to their qiita.biosample
    idxs in one round trip. Used by qiita submit-bcl-convert to translate
    the preflight rows' biosample_accession values into the biosample_idx
    the sequenced-sample composer route requires.

    The request body is the natural place for the list because a typical
    bcl-convert pool carries 384 accessions, which exceeds nginx's
    default URL-line cap when threaded through repeated query
    parameters; the body has no such cap.
    """

    model_config = ConfigDict(extra="forbid")

    accessions: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1, max_length=10_000)


class BiosampleLookupByAccessionResponse(BaseModel):
    """Returned by POST /api/v1/biosample/lookup-by-accession.

    `resolved` maps each found accession to its biosample_idx. `missing`
    lists accessions that did not resolve, in input order (deduped). The
    CLI surfaces `missing` to the operator with no side effects when it
    is non-empty so a missing biosample can be imported before re-running.
    """

    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, Annotated[int, Field(gt=0)]]
    missing: list[str]


# same-pattern-ok: per-key wire shape; parallels BiosampleLookupByAccessionRequest
class BiosampleLookupByMatrixTubeIdRequest(BaseModel):
    """Body for POST /api/v1/biosample/lookup-by-matrix-tube-id.

    Bulk-resolves a list of matrix_tube_id values to biosample_idx. Same
    body-vs-querystring rationale as the accession variant.
    """

    model_config = ConfigDict(extra="forbid")

    matrix_tube_ids: list[Annotated[str, Field(pattern=MATRIX_TUBE_ID_PATTERN)]] = Field(
        min_length=1, max_length=10_000
    )


# same-pattern-ok: per-key wire shape; parallels BiosampleLookupByAccessionResponse
class BiosampleLookupByMatrixTubeIdResponse(BaseModel):
    """Returned by POST /api/v1/biosample/lookup-by-matrix-tube-id.

    `resolved` maps each found matrix_tube_id to its biosample_idx.
    `missing` lists matrix_tube_id values that did not resolve, in input
    order (deduped).
    """

    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, Annotated[int, Field(gt=0)]]
    missing: list[str]


class PatchRequestModel(BaseModel):
    """Base class for every PATCH-body Pydantic model in the API.

    Pins extra="forbid" so requests that name immutable or retirement-
    managed columns trip the model-level rejection rather than reaching
    the repo, and enforces the "at least one editable field" rule that
    every PATCH surface shares — derived classes inherit the validator
    automatically. Each subtype declares its own column-typed Optional
    fields; the route layer distinguishes "absent" (do not write) from
    "explicit null" (set the column to NULL) by inspecting
    `model_fields_set`.
    """

    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def at_least_one_field(self):
        # Empty bodies are rejected here so every PATCH route gets the
        # 422 shape for free without per-route special-casing.
        if not self.model_fields_set:
            raise ValueError("at least one editable field is required")
        return self


class BiosamplePatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/biosample/{biosample_idx}.

    Inherits extra="forbid" and the at_least_one_field rule from
    PatchRequestModel; adds the NOT-NULL invariant on owner_idx.
    """

    metadata_checklist_idx: Annotated[int, Field(gt=0)] | None = None
    owner_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: str | None = None
    ena_sample_accession: str | None = None
    matrix_tube_id: Annotated[
        str | None,
        Field(pattern=MATRIX_TUBE_ID_PATTERN),
    ] = None
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None

    @model_validator(mode="after")
    def owner_not_null(self):
        # owner_idx maps to a NOT NULL column; explicit null is invalid
        # input even though the field is typed Optional for the
        # "absent vs null" distinguishing pattern shared with the
        # other fields.
        if "owner_idx" in self.model_fields_set and self.owner_idx is None:
            raise ValueError("owner_idx may not be null")
        return self


class IdxsListResponse(BaseModel):
    """Returned by every bulk-id GET that emits a hard-capped list of idxs.

    `truncated` is true when the underlying set exceeded the route's cap;
    clients seeing it should narrow their scope. `caller_system_role`
    carries the caller's principal.system_role verbatim from the database.
    The generic `idxs` field name lets the same envelope serve every
    resource family without a per-resource class.
    """

    idxs: list[int]
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
    PREP_SAMPLE = "prep_sample"
    SEQUENCED_POOL = "sequenced_pool"


class ProcessingKind(StrEnum):
    """Closed set of downstream-measurement specializations a prep_sample
    may flow into. Mirrors DB-side qiita.processing_kind, defined in
    migrations/20260501000011_prep_sample.sql. Today only 'sequenced'
    exists; future values (e.g., 'mass_specd') would land here as the
    DB ENUM gains them. Used by `qiita.action.target_processing_kinds`
    to declare which kinds an action accepts (kind-specific actions
    list one value; cross-kind admin actions leave the list empty).

    When extending the enum: each workflow YAML's `target_processing_kinds:`
    is an explicit allowlist. New kinds do NOT auto-enroll into existing
    workflows — the submission check (qiita_control_plane/routes/work_ticket.py)
    rejects any prep_sample whose kind is not in the action's list. Adding
    a new kind means landing the DB enum value, the subtype table (see
    qiita-control-plane/tests/test_prep_sample_subtype_invariants.py for
    the structural guardrail), and any new kind-specific workflows; it
    does not require auditing existing YAMLs unless you want the new kind
    to flow through them."""

    SEQUENCED = "sequenced"


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


class FailureType(StrEnum):
    """Discriminates retriable from permanent work-ticket failures.
    Mirrored DB-side by qiita.failure_type.

    `retriable` failures are transient infra issues — NODE_FAIL, OOM,
    transient FS errors, slurmrestd unreachability — that the runner
    bounces back to QUEUED for another attempt while retry_count is
    below max_retries. `permanent` failures (bad input, container
    contract violations, exit codes from a known-terminal workflow) skip
    the retry loop and go straight to FAILED.
    """

    RETRIABLE = "retriable"
    PERMANENT = "permanent"


class WorkTicketFailureStage(StrEnum):
    """Coarse "where in the lifecycle did it fail" enum, mirrored DB-side
    by qiita.work_ticket_failure_stage.

    `STEP_RUN` is paired with a non-NULL `failure_step_name` carrying the
    YAML entry's `.name`; `SUBMISSION` and `FINALIZE` cover everything
    outside the step loop.
    """

    SUBMISSION = "submission"
    STEP_RUN = "step_run"
    FINALIZE = "finalize"


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


class PrepSampleScopeTarget(BaseModel):
    """Work ticket targets one prep_sample (the supertype introduced by
    #35) — used for actions that naturally operate on a single sample at
    a time (e.g. fastq-to-parquet, one FASTQ → one Parquet). Distinct
    from a study_prep-scoped ticket that fans out per sample inside a
    map step: this form is the singleton path, one ticket per sample.

    Kind-specific actions (e.g., fastq-to-parquet only makes sense for
    processing_kind='sequenced') express their constraint through
    `qiita.action.target_processing_kinds`, checked at submission. The
    scope target itself stays kind-agnostic so cross-kind actions
    (future admin/audit operations) can use the same shape."""

    kind: Literal[ScopeTargetKind.PREP_SAMPLE]
    prep_sample_idx: Annotated[int, Field(gt=0)]


class SequencedPoolScopeTarget(BaseModel):
    """Work ticket targets one sequenced_pool (one (run, lane) pair) —
    used for the bcl-convert workflow that demultiplexes the pool's BCL
    run folder into per-biosample FASTQs.

    Carries both the pool idx and its parent run idx. The denormalization
    lets the SA-only preflight read route stay nested under sequencing-run
    and lets the orchestrator's `SCOPE_SCALARS_BY_KIND` flow both scalars
    into the prep step's `Inputs` without an extra DB lookup."""

    kind: Literal[ScopeTargetKind.SEQUENCED_POOL]
    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]


# Discriminated union — Pydantic and OpenAPI dispatch on the `kind` field.
# DB-side, the same shape is encoded as a tagged union of typed columns
# (`scope_target_kind` plus the subset-relevant `study_idx` / `prep_idx` /
# `reference_idx` / `prep_sample_idx` / `sequenced_pool_idx`) guarded by a
# CHECK constraint; the `kind` here is the discriminator that maps to that
# column.
ScopeTarget = Annotated[
    StudyPrepScopeTarget | ReferenceScopeTarget | PrepSampleScopeTarget | SequencedPoolScopeTarget,
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
    # Retry accounting. retry_count starts at 0 and increments on each
    # retriable failure (PROCESSING → QUEUED transition). When a step
    # raises a retriable BackendFailure and retry_count >= max_retries,
    # the runner transitions the ticket to FAILED with the captured
    # failure_*. Tickets inherit the DB default (3) on submission; the
    # column is per-row so an admin can bump max_retries on a specific
    # stuck ticket without redeploying.
    retry_count: Annotated[int, Field(ge=0)] = 0
    max_retries: Annotated[int, Field(ge=0, le=100)] = 3
    # Failure surface. All fields are NULL on non-FAILED tickets and all
    # non-NULL on FAILED tickets (DB CHECK enforces). failure_step_name
    # is non-NULL only when failure_stage is STEP_RUN.
    failure_type: FailureType | None = None
    failure_stage: WorkTicketFailureStage | None = None
    failure_step_name: str | None = Field(default=None, min_length=1, max_length=255)
    failure_reason: str | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class WorkTicketCreateRequest(BaseModel):
    """Body for `POST /api/v1/work-ticket`.

    `originator_principal_idx` is set server-side from the authenticated
    caller — clients cannot submit on behalf of another principal."""

    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    action_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    scope_target: ScopeTarget
    action_context: dict[str, Any] = Field(default_factory=dict)


class WorkTicketResponse(BaseModel):
    """Returned by `POST /api/v1/work-ticket` (with HTTP 202) and by
    `POST /api/v1/work-ticket/{idx}/run`. Carries the ticket id and its
    *post-call* state — typically PENDING for a freshly-created ticket
    or after a FAILED→PENDING reset, but check the field for what the
    server saw rather than assuming."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    state: WorkTicketState


# ============================================================================
# Sequencing-run / sequenced-pool / sequenced-sample import models
# ============================================================================
#
# Bodies and responses for the sequencing-ingestion surface: a sequencing_run
# row, one sequenced_pool per lane, and one sequenced_sample (atomically with
# its parent prep_sample, prep_sample_to_study links, and prep_sample_metadata
# rows) per pool item.


class SequencingRunCreateRequest(BaseModel):
    """Body for POST /api/v1/sequencing-run.

    `instrument_run_id` is the instrument-assigned identifier and must be
    unique across the system; collision surfaces as 409. `extra_metadata`
    is a free-form JSON object (stored as JSONB).
    """

    model_config = ConfigDict(extra="forbid")

    instrument_run_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    platform: Platform
    instrument_model: str | None = None
    instrument_serial: str | None = None
    run_performed_at: AwareDatetime | None = None
    extra_metadata: dict[str, Any] | None = None


class SequencingRunCreateResponse(BaseModel):
    """Returned by POST /api/v1/sequencing-run on success."""

    sequencing_run_idx: Annotated[int, Field(gt=0)]


class SequencedPoolCreateRequest(BaseModel):
    """Body for POST /api/v1/sequencing-run/{sequencing_run_idx}/sequenced-pool.

    `run_preflight_blob` is the run preflight (typically a SQLite file)
    after post-sequencing info has been doped into it.
    Pydantic's Base64Bytes decodes the JSON string field as
    base64 on receive — a plain `bytes` field would otherwise treat the
    incoming string as UTF-8 and the encoded payload would land in BYTEA
    instead of the decoded blob. `run_preflight_filename` is the
    originating file name on disk.

    The preflight is an optional, co-populated pair: send both
    `run_preflight_blob` and `run_preflight_filename` or neither. A
    half-populated pair is rejected (422). When present, each must be
    non-empty (`min_length=1`).
    """

    model_config = ConfigDict(extra="forbid")

    run_preflight_blob: Base64Bytes | None = Field(default=None, min_length=1)
    run_preflight_filename: str | None = Field(default=None, min_length=1)
    extra_metadata: dict[str, Any] | None = None

    @model_validator(mode="after")
    def run_preflight_pair_consistent(self):
        if (self.run_preflight_blob is None) != (self.run_preflight_filename is None):
            raise ValueError(
                "run_preflight_blob and run_preflight_filename must both be"
                " provided or both be omitted"
            )
        return self


class SequencedPoolCreateResponse(BaseModel):
    """Returned by POST /api/v1/sequencing-run/{idx}/sequenced-pool on success."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]


class SequencedPoolPreflightResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/preflight.

    The SA-only read route the bcl-convert prep step calls to materialize
    the sample sheet from the pool's run preflight blob. `Base64Bytes`
    handles the wire-format base64 encoding of the BYTEA column; the
    attribute is raw bytes after deserialization.

    The route 404s if the pool has no preflight (both blob and filename
    NULL on the pool row), so this response model treats both as
    non-nullable.
    """

    model_config = ConfigDict(extra="forbid")

    run_preflight_blob: Base64Bytes
    run_preflight_filename: str = Field(min_length=1)


class SequencedSampleCreateRequest(BaseModel):
    """Body for the sequenced-sample composer POST.

    Atomically creates a prep_sample row (with processing_kind='sequenced'),
    its 1:1 sequenced_sample subtype row, one prep_sample_to_study link
    for `primary_study_idx` plus one per entry in `secondary_study_idxs`,
    and one prep_sample_metadata row per metadata entry (resolved against
    prep_sample_global_field by display_name).

    `primary_study_idx` owns the per-display_name prep_sample_study_field
    rows the composer writes for `metadata`; secondary studies see those
    values through the global field slot but do not own the field row.
    The asymmetry is forced by the schema: a prep_sample has at most one
    prep_sample_study_field per global_field_idx, so exactly one of the
    linked studies must be designated. `secondary_study_idxs` must not
    contain `primary_study_idx`; duplicate entries within it are
    collapsed (order-preserving) rather than rejected.

    `metadata` keys must match seeded prep_sample_global_field display_name
    values; unknown names surface as a single 422 listing every bad key.
    The two ENA accession fields are nullable because they are populated
    later by the submission subsystem.
    """

    model_config = ConfigDict(extra="forbid")

    biosample_idx: Annotated[int, Field(gt=0)]
    prep_protocol_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_item_id: str = Field(min_length=1)
    primary_study_idx: Annotated[int, Field(gt=0)]
    secondary_study_idxs: list[Annotated[int, Field(gt=0)]] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    metadata_checklist_idx: Annotated[int, Field(gt=0)] | None = None
    ena_experiment_accession: str | None = Field(default=None, max_length=50)
    ena_run_accession: str | None = Field(default=None, max_length=50)

    @model_validator(mode="after")
    def dedupe_secondary_study_idxs(self):
        # Collapse duplicate secondary studies (order-preserving). A study
        # repeated in secondary_study_idxs is a benign caller convenience,
        # not a conflict, so normalize rather than reject; primary appearing
        # in secondary remains the genuine error, caught next.
        self.secondary_study_idxs = list(dict.fromkeys(self.secondary_study_idxs))
        return self

    @model_validator(mode="after")
    def primary_not_in_secondary(self):
        if self.primary_study_idx in self.secondary_study_idxs:
            raise ValueError(
                f"primary_study_idx ({self.primary_study_idx}) must not appear"
                " in secondary_study_idxs"
            )
        return self


class SequencedSampleCreateResponse(BaseModel):
    """Returned by the sequenced-sample composer POST on success."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequenced_sample_idx: Annotated[int, Field(gt=0)]


class SequencedSampleResponse(BaseModel):
    """Returned by GET /api/v1/sequenced-sample/{sequenced_sample_idx}.

    Carries every caller-visible column from the sequenced_sample subtype
    row plus the controlling supertype prep_sample row, and embeds a dict
    of every globally-linked metadata value the prep_sample carries,
    keyed on prep_sample_global_field.internal_name. Purely-local
    metadata and metadata whose prep_sample_to_study link has been
    retired are excluded -- both surface as
    prep_sample_metadata.global_field_idx IS NULL via the existing
    schema triggers and are filtered out by the read.

    `effective_updated_at` = GREATEST(prep_sample.updated_at,
    sequenced_sample.updated_at) — a single timestamp that bumps on a
    write to either table, used as the source for the ETag header on
    the GET and the If-Match contract on a future PATCH.
    `caller_system_role` carries the caller's principal.system_role
    verbatim from the database.
    """

    sequenced_sample_idx: Annotated[int, Field(gt=0)]
    prep_sample_idx: Annotated[int, Field(gt=0)]
    biosample_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    prep_protocol_idx: Annotated[int, Field(gt=0)]
    metadata_checklist_idx: int | None
    sequenced_pool_idx: int | None
    sequenced_pool_item_id: str | None
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    last_submission_at: AwareDatetime | None
    submission_error: str | None
    last_metadata_change_at: AwareDatetime | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    effective_updated_at: AwareDatetime
    retired: bool
    retired_by_idx: int | None
    retired_at: AwareDatetime | None
    retire_reason: str | None
    global_metadata: dict[str, GlobalMetadataEntry]
    caller_system_role: SystemRole


class SequencedSamplePatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/sequenced-sample/{sequenced_sample_idx}.

    Carries only the four subtype-table columns that the submission
    surface mutates after ingestion: the two ENA accessions and the
    submission-tracking pair. Supertype prep_sample fields
    (owner_idx, metadata_checklist_idx) and identity-level columns
    (sequenced_pool_idx, sequenced_pool_item_id) are intentionally
    out of scope; the former will land via a future
    PATCH /prep-sample/{idx} endpoint, the latter are not editable.
    Inherits extra="forbid" and the at_least_one_field rule from
    PatchRequestModel.
    """

    ena_experiment_accession: str | None = Field(default=None, max_length=50)
    ena_run_accession: str | None = Field(default=None, max_length=50)
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None


# ---------------------------------------------------------------------------
# Sequence-range allocator
# ---------------------------------------------------------------------------


class SequenceRangeMintRequest(BaseModel):
    """Body for POST /api/v1/sequence-range.

    Allocates `count` contiguous sequence_idx values for `prep_sample_idx`.
    Both fields are positive integers; the route layer additionally
    enforces `count <= Settings.max_sequence_mint_count`. Service-account
    callers with `sequence_range:mint` only — humans never mint.
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[int, Field(gt=0)]
    count: Annotated[int, Field(gt=0)]


class SequenceRange(BaseModel):
    """Returned by POST /api/v1/sequence-range (201) and
    GET /api/v1/sequence-range/{prep_sample_idx} (200).

    The pair (sequence_idx_start, sequence_idx_stop) is inclusive on
    both ends — `stop - start + 1` is the count of sequence_idx values
    reserved for raw reads belonging to this prep_sample.
    """

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequence_idx_start: Annotated[int, Field(gt=0)]
    sequence_idx_stop: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
