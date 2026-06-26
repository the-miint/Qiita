"""Shared Pydantic models: work ticket states, API schemas, identifier types."""

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    computed_field,
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
# leading zeros; the {10} quantifier fixes the length at exactly ten digits
# and rejects the empty string.
#
# Deliberately duplicated with the column-level CHECK on
# qiita.biosample.matrix_tube_id: the Pydantic side fails at the wire
# boundary with a per-field 422; the DB side is the last line of defense.
# Change one and you must change the other in the same PR.
MATRIX_TUBE_ID_PATTERN = r"^[0-9]{10}$"  # same-pattern-ok: DB CHECK parity (see above)


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


class ReadMaskReason(StrEnum):
    """Why a read is kept or dropped by a read mask.

    One value per row of the DuckLake `read_mask` table (the `reason` column).
    `pass` survives the mask (its recorded trims are applied by the `read_masked`
    view); every other value excludes the read from `read_masked`. The `qc_*`
    values come from the `qc` step's `filter_read` fail reasons; the `host_*`
    values come from the `host_filter` step's rype / minimap2 hits.

    Reason precedence (privacy-critical): a read that both fails QC and hits the
    host filter records the `host_*` hit, so a host/human read can never leak
    through a code path that only inspects `qc_*`. Host classification runs only
    on the QC-pass subset, so `host_*` only ever overrides `pass`.

    Backs a DuckLake VARCHAR column, NOT a Postgres `CREATE TYPE ... AS ENUM`.
    Per the enum-parity carve-out in CLAUDE.md (a StrEnum backed by a
    non-Postgres column is a valid, deliberate choice), it has no `ENUM_PAIRS`
    entry and is out of scope for the parity test.
    """

    PASS = "pass"
    QC_TOO_SHORT = "qc_too_short"
    QC_TOO_LONG = "qc_too_long"
    QC_LOW_QUALITY = "qc_low_quality"
    QC_TOO_MANY_N = "qc_too_many_n"
    HOST_RYPE = "host_rype"
    HOST_MINIMAP2 = "host_minimap2"


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


ReferenceKind = Literal["sequence_reference", "taxonomy_authority", "artifact_sequence_set"]
"""Kinds of reference, mirroring the `qiita.reference.kind` TEXT/CHECK column
(NOT a Postgres ENUM — see the reference migrations). `artifact_sequence_set` is
an indexless set of artifact sequences (e.g. the canonical adapter set the QC
step trims against): ingested through the same kind-agnostic reference-add flow,
but carries no taxonomy and builds no rype/minimap2 index."""


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


# The two search-index types a host reference must carry to host-filter: a rype
# `.ryxdi` minimizer index (first pass) and a minimap2 `.mmi` (second pass). Shared
# so the runner's "must carry both" gate (_resolve_host_filter_indexes) and the
# CLI's submit-host-filter-pool pre-check resolve the same pair instead of pinning
# the literals independently and drifting.
HOST_FILTER_INDEX_TYPE_RYPE = "rype"
HOST_FILTER_INDEX_TYPE_MINIMAP2 = "minimap2"
HOST_FILTER_REQUIRED_INDEX_TYPES = frozenset(
    {HOST_FILTER_INDEX_TYPE_RYPE, HOST_FILTER_INDEX_TYPE_MINIMAP2}
)


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


class ReferenceArtifactPurgeResponse(BaseModel):
    """Result of the orchestrator's on-disk reference-artifact cleanup.

    `removed` is True when a `{path_derived}/references/{idx}` directory was
    found and deleted, False when nothing was there (idempotent no-op). `path`
    echoes the directory the orchestrator targeted so the control plane can log
    exactly what was removed."""

    reference_idx: Annotated[int, Field(gt=0)]
    path: str
    removed: bool


class ReferenceDeleteResponse(BaseModel):
    """Summary of a full reference purge across Postgres, DuckLake, and disk.

    Counts are the Postgres rows removed by the cascade; `orphan_feature_count`
    is the subset of this reference's features that no other reference still
    claimed (and so were deleted from `qiita.feature` and the DuckLake
    sequence tables). `artifacts_removed` reflects the orchestrator cleanup."""

    reference_idx: Annotated[int, Field(gt=0)]
    membership_deleted: int
    index_deleted: int
    work_ticket_deleted: int
    orphan_feature_count: int
    artifacts_removed: bool


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
    BaselineResources but lives here so the over-the-wire StepSubmitRequest
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
    StepSubmitRequest (wire side). Raises ValueError when the shape is wrong.
    Kept in one place so the rule can't drift between the two layers."""
    if (container is None) == (module is None):
        raise ValueError(f"{owner} must declare exactly one of 'container' or 'module'")
    if entrypoint is not None and container is None:
        raise ValueError("'entrypoint' requires 'container'")


def _normalize_scope_target(v: dict[str, Any]) -> dict[str, Any]:
    """Validate a wire-side `scope_target` against the ScopeTarget
    discriminated union and normalize it to JSON shape (enum `kind` →
    plain string). Used by StepSubmitRequest's scope_target validator.
    ScopeTarget is defined later in this module; it resolves at call time,
    not definition time."""
    from pydantic import TypeAdapter

    return TypeAdapter(ScopeTarget).validate_python(v).model_dump(mode="json")


# ---------------------------------------------------------------------------
# Decoupled step wire contract: submit / status / result.
#
# The control-plane runner drives these three so it never holds a connection
# open for the duration of a SLURM job: submit returns immediately with a
# handle, the runner polls status until terminal, then asks for the result.
# The orchestrator is stateless across the three calls, so the handle (the
# serialized `StepHandle`) carries everything status/result need and the CP
# persists those fields to re-attach after a restart.
# ---------------------------------------------------------------------------


class StepSubmitRequest(BaseModel):
    """Body for POST /api/v1/step/submit, issued by the control-plane runner
    for every workflow `step:` entry. The orchestrator dispatches to its
    configured ComputeBackend's `submit_step` and returns a handle without
    blocking on completion.

    Runtime selection (`container` vs `module`) follows the same rules as
    `qiita_common.actions.WorkflowStep` — exactly one must be set, enforced by
    the shared `check_exactly_one_runtime` helper. `work_ticket_idx` + `attempt`
    stamp the deterministic SLURM job name `qiita-wt{idx}-{step}-a{attempt}`, so
    a job submitted but not yet recorded can be re-found by name. `scope_target`
    carries the work ticket's discriminated-union scope target (matches
    `qiita_common.models.ScopeTarget`); the field validator below runs the same
    discriminated-union validation as `WorkTicket.scope_target` AND normalizes
    the dict to JSON shape (`mode="json"`), so `scope_target["kind"]` is always
    a plain string downstream. Paths are absolute and live on the workspace
    shared between control plane and orchestrator."""

    step_name: str = Field(min_length=1)
    inputs: dict[str, str] = Field(default_factory=dict)
    workspace: str = Field(min_length=1)
    scope_target: dict[str, Any]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    attempt: Annotated[int, Field(ge=0)] = 0
    container: str | None = Field(default=None, min_length=1, max_length=512)
    module: str | None = Field(default=None, min_length=1, max_length=512)
    entrypoint: str | None = None
    baseline_resources: StepBaselineResources | None = None

    @field_validator("scope_target", mode="after")
    @classmethod
    def _validate_scope_target(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _normalize_scope_target(v)

    @model_validator(mode="after")
    def _exactly_one_runtime(self) -> StepSubmitRequest:
        check_exactly_one_runtime(
            container=self.container,
            module=self.module,
            entrypoint=self.entrypoint,
            owner="StepSubmitRequest",
        )
        return self


class StepHandleWire(BaseModel):
    """Serialized `StepHandle` — POST /step/submit returns one, and POST
    /step/status / /step/result take one back. Paths are strings on the
    wire.

    `terminal_outputs` is the "synchronous backend already finished at
    submit time" sentinel: non-None means the step completed during submit
    (LocalBackend runs the module in-process) and the dict holds its
    outputs — the caller skips polling and uses it directly. For SLURM it
    is None and the caller polls status. **Invariant: non-None implies
    non-empty** — the runner keys off `is not None`, so an empty-but-set
    dict would falsely signal completion."""

    compute_target: ComputeTarget
    step_name: str
    slurm_job_id: int | None = None
    job_name: str | None = None
    output_path: str | None = None
    logs_path: str | None = None
    terminal_outputs: dict[str, str] | None = None


class StepStatusWire(BaseModel):
    """Serialized `StepStatusInfo` — returned by POST /step/status and fed
    back into POST /step/result so the orchestrator (stateless) can finalize
    a terminal step without re-reading slurmrestd."""

    status: StepStatus
    raw_state: str | None = None
    exit_code: int | None = None
    reason: str | None = None


class StepStatusRequest(BaseModel):
    """Body for POST /api/v1/step/status."""

    handle: StepHandleWire


class StepResultRequest(BaseModel):
    """Body for POST /api/v1/step/result."""

    handle: StepHandleWire
    status: StepStatusWire


class StepResultResponse(BaseModel):
    """Returned by POST /api/v1/step/result — the backend's name → path
    output map, matching the YAML's declared step `outputs:`."""

    outputs: dict[str, str]


class StepFindByNameRequest(BaseModel):
    """Body for POST /api/v1/step/find-by-name.

    `job_name` is the deterministic SLURM job name
    `qiita-wt{idx}-{step}-a{attempt}`. The control-plane runner queries this
    during restart recovery to adopt a job it submitted but whose id it never
    persisted (the write-ahead `submitting`-without-id gap) — closing the
    duplicate-job window without re-submitting."""

    job_name: str = Field(min_length=1, max_length=512)


class FoundJobWire(BaseModel):
    """One live SLURM job matched by find-by-name: its id and a status
    snapshot (reusing StepStatusWire). The control plane adopts a found job
    by reconstructing a StepHandle from `slurm_job_id` (workspace paths are
    deterministic from the per-attempt workspace)."""

    slurm_job_id: int
    job_name: str
    status: StepStatusWire


class StepFindByNameResponse(BaseModel):
    """Returned by POST /api/v1/step/find-by-name — the live jobs whose name
    matched. Empty when none match: slurmrestd has purged the job, or the
    backend is in-process (LocalBackend never submits to SLURM)."""

    jobs: list[FoundJobWire]


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
    metadata_checklist_name: str | None = Field(default=None, min_length=1)
    biosample_accession: str | None = Field(default=None, min_length=1)
    ena_sample_accession: str | None = Field(default=None, min_length=1)
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


class OwnerBiosampleIdRow(BaseModel):
    """One row of the owner-id re-identification export.

    Pairs a biosample's stable minted idx and public accession with the
    owner-submitted original sample name — biosample_metadata.value_text on
    the row flagged is_owner_biosample_id=True. That value is PII-pinned and
    masked on the normal biosample read path; this export is the only way to
    recover it, hence system_admin + admin:biosample_owner_id_read.

    `biosample_accession` is None until the biosample is submitted to NCBI.
    `owner_biosample_id` is None only when the biosample has no owner-id
    metadata row at all — surfaced rather than silently dropped.

    The sequencing-pathway fields (prep_sample_idx, ena_experiment_accession,
    ena_run_accession) are populated only when the export was filtered to a
    sequenced_pool; they stay None in the study-wide export.
    """

    biosample_idx: Annotated[int, Field(gt=0)]
    biosample_accession: str | None
    owner_biosample_id: str | None
    prep_sample_idx: Annotated[int | None, Field(gt=0)] = None
    ena_experiment_accession: str | None = None
    ena_run_accession: str | None = None


class OwnerBiosampleIdExportResponse(BaseModel):
    """Returned by GET /admin/study/{study_idx}/owner-biosample-id.

    Re-identification export mapping each biosample's idx + public accession
    back to the owner-submitted original name. When `sequenced_pool_idx` is
    set, rows are restricted to that pool's sequenced_samples that belong to
    the study (active prep_sample_to_study links) and carry the prep_sample_idx
    + ENA experiment/run accessions; otherwise rows cover the study's active
    biosamples. system_admin + admin:biosample_owner_id_read only.
    """

    study_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_idx: Annotated[int | None, Field(gt=0)]
    row_count: Annotated[int, Field(ge=0)]
    rows: list[OwnerBiosampleIdRow]


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


class MetadataChecklistRef(BaseModel):
    """The metadata_checklist a biosample/sequenced_sample claims
    conformance to, carrying both the idx and the name (= the ENA
    checklist accession). Mirrors MissingReasonRef's idx+name shape.
    """

    idx: Annotated[int, Field(gt=0)]
    name: Annotated[str, Field(min_length=1)]

    @classmethod
    def from_row(cls, idx: int | None, name: str | None) -> MetadataChecklistRef | None:
        """Build the ref from a read row's nullable (idx, name); None idx
        (no checklist on the row) yields None."""
        if idx is None:
            return None
        return cls(idx=idx, name=name)


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
    metadata_checklist: MetadataChecklistRef | None
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


# The two qiita.biosample accession columns a lookup may key on; each value is
# the literal Postgres column name it selects.
BiosampleAccessionField = Literal["biosample_accession", "ena_sample_accession"]


class BiosampleLookupByAccessionRequest(BaseModel):
    """Body for POST /api/v1/biosample/lookup-by-accession.

    Resolves a list of biosample accession strings to their qiita.biosample
    idxs in one round trip, keyed on the column named by `accession_field`
    (default biosample_accession). Used by qiita submit-bcl-convert to
    translate the preflight rows' biosample_accession values into the
    biosample_idx the sequenced-sample composer route requires.

    The request body is the natural place for the list because a typical
    bcl-convert pool carries 384 accessions, which exceeds nginx's
    default URL-line cap when threaded through repeated query
    parameters; the body has no such cap.
    """

    model_config = ConfigDict(extra="forbid")

    accessions: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1, max_length=10_000)
    accession_field: BiosampleAccessionField = "biosample_accession"


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


# The two qiita.study accession columns a lookup may key on; each value is the
# literal Postgres column name it selects.
StudyAccessionField = Literal["ena_study_accession", "bioproject_accession"]


# same-pattern-ok: per-key wire shape; parallels BiosampleLookupByAccessionRequest
class StudyLookupByAccessionRequest(BaseModel):
    """Resolves a list of study accession values to study_idxs in one round
    trip, keyed on the column named by `accession_field` (default
    bioproject_accession). Body-shaped (not query-params) so a long accession
    list cannot exceed nginx's default URL-line cap.
    """

    model_config = ConfigDict(extra="forbid")

    accessions: list[Annotated[str, Field(min_length=1)]] = Field(min_length=1, max_length=10_000)
    accession_field: StudyAccessionField = "bioproject_accession"


# same-pattern-ok: per-key wire shape; parallels BiosampleLookupByAccessionResponse
class StudyLookupByAccessionResponse(BaseModel):
    """`resolved` maps each found accession to its study_idx. `missing`
    lists accessions that did not resolve, in input order (deduped).
    """

    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, Annotated[int, Field(gt=0)]]
    missing: list[str]


class PatchRequestModel(BaseModel):
    """Base class for every PATCH-body Pydantic model in the API.

    Pins extra="forbid" so requests that name immutable or retirement-
    managed columns trip the model-level rejection rather than reaching
    the repo, enforces the "at least one editable field" rule that
    every PATCH surface shares, and enforces "explicit null is not
    valid input on a NOT NULL column" via the NOT_NULL_FIELDS hook.
    Derived classes inherit both validators automatically; each subtype
    declares its own column-typed Optional fields, and the ones whose
    column is NOT NULL list those field names in NOT_NULL_FIELDS. The
    route layer distinguishes "absent" (do not write) from "explicit
    null" (set the column to NULL) by inspecting `model_fields_set`.
    """

    model_config = ConfigDict(extra="forbid")

    # Field names whose backing column is NOT NULL. Subclasses override
    # to declare their own; the empty default means "every field is
    # nullable" (no validator-side rejection).
    NOT_NULL_FIELDS: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="after")
    def at_least_one_field(self):
        # Empty bodies are rejected here so every PATCH route gets the
        # 422 shape for free without per-route special-casing.
        if not self.model_fields_set:
            raise ValueError("at least one editable field is required")
        return self

    @model_validator(mode="after")
    def reject_explicit_null_on_not_null_fields(self):
        # Every field in NOT_NULL_FIELDS maps to a NOT NULL column;
        # explicit null is invalid input even though the field is
        # typed Optional for the "absent vs null" distinguishing
        # pattern shared with the nullable fields.
        for field_name in self.NOT_NULL_FIELDS:
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} may not be null")
        return self


class BiosamplePatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/biosample/{biosample_idx}.

    Inherits extra="forbid", the at_least_one_field rule, and the
    NOT_NULL_FIELDS explicit-null guard from PatchRequestModel; lists
    owner_idx as the not-null field.
    """

    NOT_NULL_FIELDS: ClassVar[frozenset[str]] = frozenset({"owner_idx"})

    metadata_checklist_name: str | None = Field(default=None, min_length=1)
    owner_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: str | None = Field(default=None, min_length=1)
    ena_sample_accession: str | None = Field(default=None, min_length=1)
    matrix_tube_id: Annotated[
        str | None,
        Field(pattern=MATRIX_TUBE_ID_PATTERN),
    ] = None
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None


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


class SequencedSampleListItem(BaseModel):
    """One active sequenced_sample in a pool- or run-scoped sample list.

    Carries the subtype idx, its supertype prep_sample_idx and biosample_idx,
    the sequenced_pool_item_id (which equals the bcl-convert per-sample FASTQ
    basename prefix), and the ENA experiment/run plus biosample/ena-sample
    accessions. Lets a caller fan out per-sample work — the pool host-filter
    fan-out matches each sample's FASTQs by sequenced_pool_item_id, and ENA
    experiment submission needs the biosample's BioSample accession as the
    sample_descriptor — without an N+1 of per-idx GETs. The accession columns
    are nullable until their submissions succeed. Host references are not a
    sample property: they parameterize the read mask and are supplied at
    human-filter submission, not carried here.
    """

    sequenced_sample_idx: int
    prep_sample_idx: int
    biosample_idx: int
    sequenced_pool_item_id: str
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    biosample_accession: str | None
    ena_sample_accession: str | None


class SequencedSampleListResponse(BaseModel):
    """Returned by the pool- and run-scoped sequenced-sample list routes
    (GET /sequencing-run/{run}/sequenced-pool/{pool}/sequenced-sample/list
    and GET /sequencing-run/{run}/sequenced-sample/list).

    Unlike IdxsListResponse this carries richer per-sample rows
    (prep_sample_idx + sequenced_pool_item_id), so the segment is `list`
    rather than `list-idxs`. `truncated` mirrors IdxsListResponse semantics:
    true when the underlying set exceeded the route's hard cap.
    """

    samples: list[SequencedSampleListItem]
    count: Annotated[int, Field(ge=0)]
    truncated: bool
    caller_system_role: SystemRole


class StudyListItem(BaseModel):
    """One study a prep_sample is actively linked to, with its accessions.

    Carries the study_idx plus both study accessions so an ENA-submission
    caller can read a prep_sample's studies — and the BioProject accession
    that becomes the experiment study_ref — without a per-study GET. Both
    accession columns are nullable until their submissions succeed.
    """

    study_idx: int
    bioproject_accession: str | None
    ena_study_accession: str | None


class StudyListResponse(BaseModel):
    """Returned by the prep-sample study list (GET
    /prep-sample/{prep_sample_idx}/study/list).

    Carries richer per-study rows (study_idx + both accessions), so the
    segment is `list` rather than `list-idxs`. `truncated` mirrors
    IdxsListResponse semantics: true when the underlying set exceeded the
    route's hard cap.
    """

    studies: list[StudyListItem]
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
    ena_study_accession: str | None = Field(
        default=None, min_length=1, max_length=_STUDY_ACCESSION_MAX
    )
    bioproject_accession: str | None = Field(
        default=None, min_length=1, max_length=_STUDY_ACCESSION_MAX
    )
    notes: str | None = None
    extra_metadata: dict[str, object] | None = None
    default_tier: Tier | None = None


class StudyPatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/study/{study_idx}.

    Carries the editable post-create columns. Field constraints follow
    StudyCreate so a PATCH cannot smuggle in a value that POST would
    reject. owner_idx is intentionally not patchable (ownership transfer
    is a separate surface) and default_tier is intentionally not
    patchable (its policy-shape needs its own design). The
    submission-tracking columns (last_submission_at, submission_error)
    are likewise omitted: this route is owner-accessible, and those
    columns are written by the submission subsystem, not by humans
    editing a study. Inherits extra="forbid", the at_least_one_field
    rule, and the NOT_NULL_FIELDS explicit-null guard from
    PatchRequestModel.
    """

    NOT_NULL_FIELDS: ClassVar[frozenset[str]] = frozenset({"title"})

    title: str | None = Field(default=None, min_length=1, max_length=_STUDY_TITLE_MAX)
    principal_investigator_idx: Annotated[int, Field(gt=0)] | None = None
    alias: str | None = Field(default=None, max_length=_STUDY_ALIAS_MAX)
    description: str | None = None
    abstract: str | None = None
    funding: str | None = Field(default=None, max_length=_STUDY_FUNDING_MAX)
    ena_study_accession: str | None = Field(
        default=None, min_length=1, max_length=_STUDY_ACCESSION_MAX
    )
    bioproject_accession: str | None = Field(
        default=None, min_length=1, max_length=_STUDY_ACCESSION_MAX
    )
    notes: str | None = None
    extra_metadata: dict[str, object] | None = None


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
    ena_study_accession: str | None
    bioproject_accession: str | None
    notes: str | None
    last_submission_at: AwareDatetime | None
    submission_error: str | None
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


class PrepSampleRetiredUpdate(BaseModel):
    """Body for PATCH /api/v1/prep-sample/{idx}/retired.

    Reversible operator disposition (unlike the terminal principal retire): set
    `retired=true` to drop an empty / failed-yield well out of a pool's active
    set, or `retired=false` to un-retire a misclassified one. `reason` is the
    optional retire_reason (only meaningful when retiring; the DB CHECK requires
    retired_by_idx/retired_at when retired=true and forbids them otherwise, both
    populated/cleared by the route).
    """

    retired: bool
    reason: str | None = None


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
    COMPLETED / NO_DATA / FAILED are the three terminal states, with
    different resubmission semantics: COMPLETED is DELETE-gated (a result
    exists, so the prior result must be deleted before a fresh submit);
    FAILED is restarted in place via /run (operator-driven recovery);
    NO_DATA mints no result, so it is freely resubmittable (only an
    in-place /run redrive is refused).

    NO_DATA is the terminal outcome for a step that legitimately produced
    no data — an empty FASTQ well (a blank, a no-template control, or a
    failed-yield well). It is distinct from FAILED: a no_data ticket
    carries NULL failure_* columns and is tallied in its own pool-
    completion bucket so a plate full of empty wells can still reach a
    "done" signal rather than being stuck behind permanent failures.
    """

    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    NO_DATA = "no_data"
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


class ComputeTarget(StrEnum):
    """Where one workflow step entry actually executes.

    `slurm` — a real SLURM job (carries a `slurm_job_id`). `local` — a
    native module run in-process on the orchestrator (LocalBackend; dev /
    test). `control_plane` — an `action:` entry run in-process on the
    control plane (no backend hop, no job id). Only `slurm` is "on
    compute"; the other two are in-process. Mirrored DB-side by the
    `compute_target` TEXT+CHECK column on `qiita.work_ticket_step` — a
    plain TEXT/CHECK, not a Postgres ENUM (see CLAUDE.md "Enum parity");
    keep both sides in sync by hand.
    """

    SLURM = "slurm"
    LOCAL = "local"
    CONTROL_PLANE = "control_plane"


class StepStatus(StrEnum):
    """Live status of a submitted step, as reported by a backend's
    `status_step`. Coarser than SLURM's own state vocabulary — the runner
    and the ticket-summary read only care about queued-vs-running-vs-done.

    `pending` = accepted/queued but not yet on a node; `running` = actively
    executing; `completed` / `failed` are terminal. `completed` means the
    job exited cleanly — the caller still runs `result_step` to verify the
    output contract, which can itself fail.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class StepProgressState(StrEnum):
    """Control-plane-side write-ahead lifecycle of one work-ticket step
    entry, persisted per `(work_ticket_idx, step_index, attempt)` in
    `qiita.work_ticket_step`.

    Distinct from `StepStatus` (a backend's live report of a submitted
    job): this is the CP runner's *own* progress record, the spine of
    restart recovery. `submitting` is the write-ahead intent written
    *before* the backend submit fires; `submitted` records a returned
    `slurm_job_id`; `running` mirrors a status poll; `completed` /
    `failed` are terminal. Mirrored DB-side by the `state` TEXT+CHECK
    column on `qiita.work_ticket_step` — a plain TEXT/CHECK, not a
    Postgres ENUM (same carve-out as `upload.status` / `reference.status`;
    out of scope for `ENUM_PAIRS`). Keep both sides in sync by hand.
    """

    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    RUNNING = "running"
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


class PrepSampleScopeTarget(BaseModel):
    """Work ticket targets one prep_sample (the supertype) — used for
    actions that naturally operate on a single sample at a time (e.g.
    fastq-to-parquet, one FASTQ → one Parquet). Distinct
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
    # In-place-retry visibility (set while the runner is stuck retrying an
    # unreachable orchestrator/slurmrestd for this ticket; NULL otherwise).
    # Advisory and orthogonal to the failure_* surface — the ticket is still
    # `processing`, not `failed`. Mirrors the qiita.work_ticket columns of the
    # same name; the status routes surface them so a wedged-looking ticket is
    # explainable instead of silent.
    transient_reason: str | None = None
    transient_since: AwareDatetime | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ResourceOverride(BaseModel):
    """Per-run resource override for a work ticket's SLURM steps.

    A privileged caller (wet_lab_admin / system_admin) raises the per-step
    memory *floor* for one run — e.g. staging a human genome that OOMs the
    workflow's conservative default — without editing the workflow YAML. The
    runner applies `max(step baseline_resources.mem_gb, mem_gb)` at dispatch,
    still clamped to the action's mem ceiling (an override above the ceiling is
    rejected at submission). `mem_gb=None` (the default) leaves every step's
    YAML baseline untouched. Carried on `qiita.work_ticket` so a control-plane
    restart re-attaches in-flight work with the same override.

    INVARIANT — enforcement is NOT on this model: any route that accepts a
    `resource_override` MUST itself gate it to wet_lab_admin+ (else a regular
    caller could inflate their job's footprint) and clamp it to the action
    ceiling. Today only `POST /work-ticket` accepts one — see the gate in
    `routes/work_ticket.py::submit_work_ticket`. A new route accepting it
    without that gate is a privilege-escalation bug."""

    mem_gb: Annotated[int | None, Field(default=None, gt=0)] = None


class WorkTicketCreateRequest(BaseModel):
    """Body for `POST /api/v1/work-ticket`.

    `originator_principal_idx` is set server-side from the authenticated
    caller — clients cannot submit on behalf of another principal.

    `resource_override` is an optional per-run resource bump, gated server-side
    to wet_lab_admin / system_admin and bounded by the action's ceiling."""

    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    action_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    scope_target: ScopeTarget
    action_context: dict[str, Any] = Field(default_factory=dict)
    resource_override: ResourceOverride | None = None


class WorkTicketResponse(BaseModel):
    """Returned by `POST /api/v1/work-ticket` (with HTTP 202) and by
    `POST /api/v1/work-ticket/{idx}/run`. Carries the ticket id and its
    *post-call* state — typically PENDING for a freshly-created ticket
    or after a FAILED→PENDING reset, but check the field for what the
    server saw rather than assuming."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    state: WorkTicketState


class WorkTicketSummary(WorkTicket):
    """A WorkTicket plus a snapshot of its *current* step entry's compute
    placement. Returned by `GET /api/v1/work-ticket` (the list view) so a
    caller can see, in one round trip, not just a ticket's lifecycle state
    but *where* its in-flight work is running and on which SLURM job.

    The "current entry" is the highest `(step_index, attempt)` row in
    `qiita.work_ticket_step` for the ticket — the entry the runner is on,
    or the last one it finished. The five fields below are all NULL for a
    ticket with no progress rows yet (a PENDING / QUEUED ticket whose first
    write-ahead hasn't fired); for an in-process `action:` entry the
    `slurm_*` fields stay NULL while `compute_target='control_plane'`.

    This read is DB-backed and therefore at most one poll-interval stale
    (the runner persists `running` on a status poll, default ~10s); the
    `slurm_job_id` is exact. A live SLURM hop to refresh `step_state` is a
    separate single-ticket concern, deliberately not done for the list.
    """

    # 0-based index into the action's `steps:` list, plus the entry name.
    current_step_index: int | None = None
    current_step_name: str | None = None
    # Where the current entry runs (`slurm` / `local` / `control_plane`).
    compute_target: ComputeTarget | None = None
    # The SLURM job id — non-NULL only for a `slurm` current entry past
    # write-ahead.
    slurm_job_id: int | None = None
    # The control-plane-side write-ahead lifecycle state of the current
    # entry (the spine's StepProgressState, NOT a live SLURM-native state —
    # see the class docstring on staleness).
    step_state: StepProgressState | None = None


class WorkTicketStepLogs(BaseModel):
    """Returned by `GET /api/v1/work-ticket/{idx}/step/{step_index}/logs`.

    A bounded tail of a single step attempt's stdout/stderr, read by the
    control plane straight off shared scratch (`PATH_SCRATCH/ticket/...`) and
    served over HTTP so an operator can diagnose a failure — an OOM, a bad
    input, a contract violation — without a host shell or sudo. Each stream is
    independently truncated; `*_truncated` is True when older content was
    dropped from the front. A stream the job never wrote comes back as an
    empty string (not an error)."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    step_index: Annotated[int, Field(ge=0)]
    # The attempt actually read — resolved to the latest recorded attempt when
    # the caller didn't pin one, so the response is self-describing.
    attempt: Annotated[int, Field(ge=0)]
    step_name: str
    stdout: str = ""
    stderr: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False


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


class SequencingRunResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{sequencing_run_idx}.

    The caller-visible view of a `qiita.sequencing_run` row (the column set
    `repositories.sequencing_run.fetch_sequencing_run` selects, with the row's
    `idx` surfaced as `sequencing_run_idx`). `instrument_model` is the field the
    `submit-host-filter-pool` fan-out reads to forward QC's polyG gate per sample;
    it is nullable (non-bcl runs may not record it).
    """

    sequencing_run_idx: Annotated[int, Field(gt=0)]
    instrument_run_id: str
    platform: Platform
    instrument_model: str | None = None
    instrument_serial: str | None = None
    run_performed_at: AwareDatetime | None = None
    extra_metadata: dict[str, Any] | None = None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    retired: bool
    retired_by_idx: Annotated[int, Field(gt=0)] | None = None
    retired_at: AwareDatetime | None = None
    retire_reason: str | None = None


# same-pattern-ok: per-key wire shape; parallels StudyLookupByAccessionRequest
class SequencingRunLookupByInstrumentRunIdRequest(BaseModel):
    """Resolves a list of instrument_run_id values to sequencing_run idxs in
    one round trip. Body-shaped (not query-params) so a long id list cannot
    exceed nginx's default URL-line cap.
    """

    model_config = ConfigDict(extra="forbid")

    instrument_run_ids: list[Annotated[str, Field(min_length=1)]] = Field(
        min_length=1, max_length=10_000
    )


# same-pattern-ok: per-key wire shape; parallels StudyLookupByAccessionResponse
class SequencingRunLookupByInstrumentRunIdResponse(BaseModel):
    """`resolved` maps each found instrument_run_id to its sequencing_run_idx.
    `missing` lists ids that did not resolve, in input order (deduped).
    """

    model_config = ConfigDict(extra="forbid")

    resolved: dict[str, Annotated[int, Field(gt=0)]]
    missing: list[str]


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


class PoolReadMetrics(BaseModel):
    """Compute-on-read read-metric rollup for a sequenced_pool.

    The three counts are SUMS over the pool's NON-retired sequenced_samples
    (each NULL until at least one sample in the pool has been processed);
    `fraction_passing_quality_filter` is recomputed from the summed counts via
    `_fraction_passing_quality_filter` — NOT a mean of per-sample fractions — and
    is None when raw is absent or 0. `sample_count` is the pool's non-retired
    sequenced_sample total; `samples_with_metrics` is how many of those carry
    read counts, so a partial rollup (some samples still unprocessed) is
    interpretable rather than looking complete."""

    raw_read_count_r1r2: int | None
    biological_read_count_r1r2: int | None
    quality_filtered_read_count_r1r2: int | None
    sample_count: int
    samples_with_metrics: int

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fraction_passing_quality_filter(self) -> float | None:
        """Pool quality_filtered / raw, recomputed from the SUMMED counts (see
        `_fraction_passing_quality_filter`)."""
        return _fraction_passing_quality_filter(
            self.raw_read_count_r1r2, self.quality_filtered_read_count_r1r2
        )


class SequencedPoolResponse(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}.

    The pool's caller-visible metadata (the BYTEA `run_preflight_blob` is
    omitted — only `run_preflight_filename` is surfaced) plus the compute-on-read
    read-metric rollup. There is no stored pool-level metric: `read_metrics`
    is aggregated from the constituent sequenced_samples at request time, so it
    never drifts when a sample is re-processed or deleted."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    run_preflight_filename: str | None
    extra_metadata: dict[str, Any] | None
    created_by_idx: Annotated[int, Field(gt=0)]
    created_at: AwareDatetime
    read_metrics: PoolReadMetrics


class SampleQCReport(BaseModel):
    """One pool member's persisted QC reports, as carried in PoolQCReport.samples.

    `raw_qc_report` / `filtered_qc_report` are the verbatim qc_report.json
    documents the native `qc_report` job emitted at each point (the
    `{point, layout, read_pairs, mates: {r1, r2}}` shape), or None for a sample
    not yet processed by fastq-to-parquet/1.2.0. `sequenced_pool_item_id` is the
    sample's per-pool item id (lane+barcode); `prep_sample_idx` identifies the
    sample. The blobs are surfaced as-is — the pool report does not re-derive
    them, only merges copies into `merged`."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_item_id: str | None
    raw_qc_report: dict[str, Any] | None
    filtered_qc_report: dict[str, Any] | None


class MateQCAggregate(BaseModel):
    """One mate's (r1 or r2) QC summary pooled across a pool's samples.

    Counts (`reads`, `total_bases`) are plain sums. The means
    (`mean_quality`, `gc_content`, `n_content`, `mean_length`) are
    base- or read-weighted pools of the per-sample means — each per-sample mean
    is multiplied back out by its weight (total_bases for the content/quality
    means, reads for the mean length) to recover the underlying sum, those are
    summed, then divided by the pooled weight. Recovering the sum from a stored
    ratio carries negligible float error, acceptable for a human-facing report.
    A mean is None when no contributing sample carried it. The three histograms
    are per-bucket sums of the per-sample histograms (string bucket keys, the
    same keying the per-sample qc_report uses)."""

    reads: int
    total_bases: int
    mean_quality: float | None
    gc_content: float | None
    n_content: float | None
    min_length: int | None
    max_length: int | None
    mean_length: float | None
    quality_histogram: dict[str, int]
    gc_histogram: dict[str, int]
    length_histogram: dict[str, int]


class PointQCAggregate(BaseModel):
    """One report point (raw or filtered) pooled across a pool's samples.

    `samples` is how many of the pool's samples carried a report at this point;
    `read_pairs` sums their per-sample read_pairs. `mates.r1` is present whenever
    any contributing sample had r1; `mates.r2` is None for an all-single-end
    pool (no sample carried an r2 block)."""

    samples: int
    read_pairs: int
    mates: dict[str, MateQCAggregate | None]


class MergedQCAggregate(BaseModel):
    """Run-level (pool-wide) merge of the per-sample QC reports — the
    multiqc-equivalent summary. `raw` / `filtered` is None when no sample in the
    pool carried a report at that point."""

    raw: PointQCAggregate | None
    filtered: PointQCAggregate | None


def _merge_qc_point(reports: list[dict[str, Any]]) -> PointQCAggregate | None:
    """Pool a list of per-sample qc_report.json documents for ONE point into a
    single PointQCAggregate, or None when the list is empty (no sample carried a
    report at this point).

    Each `report` is one sample's `{point, layout, read_pairs, mates: {r1, r2}}`
    document. The two mates merge independently; an absent mate (None) is
    skipped, so r2 is only present when at least one sample had it."""
    if not reports:
        return None
    mates: dict[str, MateQCAggregate | None] = {}
    for mate in ("r1", "r2"):
        per_sample = [r["mates"][mate] for r in reports if r["mates"].get(mate) is not None]
        mates[mate] = _merge_mate(per_sample) if per_sample else None
    return PointQCAggregate(
        samples=len(reports),
        read_pairs=sum(r["read_pairs"] for r in reports),
        mates=mates,
    )


def _weighted_mean(
    per_sample: list[dict[str, Any]], value_key: str, weight_key: str
) -> float | None:
    """Pool a stored per-sample ratio (`value_key`) weighted by `weight_key`.

    Recovers each sample's underlying sum as ratio * weight, sums those, and
    divides by the pooled weight. None when no sample carries the ratio or the
    pooled weight is 0."""
    num = 0.0
    denom = 0
    for s in per_sample:
        v = s.get(value_key)
        w = s.get(weight_key)
        if v is None or not w:
            continue
        num += v * w
        denom += w
    return num / denom if denom else None


def _merge_histograms(per_sample: list[dict[str, Any]], key: str) -> dict[str, int]:
    """Per-bucket sum of the per-sample `key` histograms (string bucket keys),
    returned ordered by numeric bucket value for a stable response."""
    merged: dict[str, int] = {}
    for s in per_sample:
        for bucket, count in (s.get(key) or {}).items():
            merged[bucket] = merged.get(bucket, 0) + count
    return {k: merged[k] for k in sorted(merged, key=int)}


def _merge_mate(per_sample: list[dict[str, Any]]) -> MateQCAggregate:
    """Pool a list of per-sample mate summaries (each the r1/r2 block of a
    qc_report) into one MateQCAggregate. See MateQCAggregate for the weighting."""
    min_lengths = [s["min_length"] for s in per_sample if s.get("min_length") is not None]
    max_lengths = [s["max_length"] for s in per_sample if s.get("max_length") is not None]
    return MateQCAggregate(
        reads=sum(s["reads"] for s in per_sample),
        total_bases=sum(s["total_bases"] for s in per_sample),
        mean_quality=_weighted_mean(per_sample, "mean_quality", "total_bases"),
        gc_content=_weighted_mean(per_sample, "gc_content", "total_bases"),
        n_content=_weighted_mean(per_sample, "n_content", "total_bases"),
        min_length=min(min_lengths) if min_lengths else None,
        max_length=max(max_lengths) if max_lengths else None,
        mean_length=_weighted_mean(per_sample, "mean_length", "reads"),
        quality_histogram=_merge_histograms(per_sample, "quality_histogram"),
        gc_histogram=_merge_histograms(per_sample, "gc_histogram"),
        length_histogram=_merge_histograms(per_sample, "length_histogram"),
    )


def merge_qc_reports(samples: list[SampleQCReport]) -> MergedQCAggregate:
    """Merge a pool's per-sample QC reports into the run-level MergedQCAggregate.

    Raw and filtered points merge independently over the samples that carry that
    point; a point with no reports yields None. Pure (no DB) so it is unit-tested
    directly from constructed SampleQCReport lists."""
    return MergedQCAggregate(
        raw=_merge_qc_point([s.raw_qc_report for s in samples if s.raw_qc_report is not None]),
        filtered=_merge_qc_point(
            [s.filtered_qc_report for s in samples if s.filtered_qc_report is not None]
        ),
    )


class PoolQCReport(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/qc-report.

    The pool's merged (multiqc-equivalent) QC report: the read-metric rollup
    (reused from the pool metadata endpoint), the run-level `merged` aggregate of
    every per-sample QC report, and the per-sample `samples` detail. Everything
    is compute-on-read — `merged` is aggregated from the constituent
    sequenced_samples' persisted reports at request time, so it never drifts when
    a sample is re-processed or deleted. `samples_with_qc_report` counts how many
    of the pool's non-retired samples carry at least a raw report, so a partial
    pool (some samples still unprocessed) is interpretable."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    sample_count: int
    samples_with_qc_report: int
    read_metrics: PoolReadMetrics
    merged: MergedQCAggregate
    samples: list[SampleQCReport]


class PoolCompletionStatus(BaseModel):
    """Returned by GET /api/v1/sequencing-run/{R}/sequenced-pool/{P}/completion.

    The pool's prep-generation completion rollup: each non-retired
    sequenced_sample is classified by the state of its fastq-to-parquet work
    tickets (any version), and the per-sample states are tallied into the five
    mutually-exclusive buckets below. This is the SPP GenPrepFileJob end-state
    equivalent — it answers "has the pool's per-sample fastq→parquet fan-out
    finished?" — surfaced alongside the read-metric and QC rollups.

    Per-sample classification (precedence, highest first), so a sample appears in
    exactly one bucket:
      completed     — has at least one COMPLETED fastq-to-parquet ticket.
      in_flight     — no COMPLETED ticket but at least one PENDING/QUEUED/
                      PROCESSING (e.g. a re-submitted retry); work is ongoing.
      no_data       — no COMPLETED and nothing in flight, but at least one
                      NO_DATA (an empty/blank well — a terminal outcome that is
                      NOT a failure). Outranks failed so a sample carrying both a
                      no_data and a stale failed ticket counts as no_data.
      failed        — no COMPLETED, nothing in flight, no NO_DATA, but at least
                      one FAILED.
      not_submitted — no fastq-to-parquet ticket at all.

    `complete` is the pool-level done flag: the pool is non-empty and every
    sample is in a terminal-accounted state — COMPLETED or NO_DATA (so a plate
    of real data with empty wells still reaches `complete=True`, and a
    zero-sample pool reads `complete=False`, not vacuously true). Everything is
    compute-on-read over the work_ticket table, so it never drifts when a sample
    is re-processed, re-submitted, or deleted."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]
    sample_count: Annotated[int, Field(ge=0)]
    samples_completed: Annotated[int, Field(ge=0)]
    samples_in_flight: Annotated[int, Field(ge=0)]
    samples_no_data: Annotated[int, Field(ge=0)]
    samples_failed: Annotated[int, Field(ge=0)]
    samples_not_submitted: Annotated[int, Field(ge=0)]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def complete(self) -> bool:
        """True when the pool has samples and every one is in a terminal-
        accounted state: a COMPLETED fastq-to-parquet ticket or a NO_DATA
        (empty-well) outcome."""
        return (
            self.sample_count > 0
            and (self.samples_completed + self.samples_no_data) == self.sample_count
        )


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


class SequencedPoolPreflightUpdateLaneRequest(BaseModel):
    """Body for POST /sequencing-run/{R}/sequenced-pool/{P}/preflight/update-lane.

    Bulk-reassigns the `lane` column inside the pool's run-preflight SQLite blob,
    delegating to `run_preflight.update_lane`: every platform-sample row whose
    current lane equals `from_lane` (NULL is a value) is moved to `to_lane`.

    `platform` selects the platform-specific sample table update_lane targets —
    `"illumina"` (illumina_sample) or `"tellseq"` (tellseq_sample). This is the
    run_preflight platform-table key, NOT qiita's `Platform` enum: TellSeq is an
    Illumina library-prep protocol rather than a sequencing platform, so the two
    value sets deliberately differ — do not substitute `Platform` here.

    `from_lane` / `to_lane` are the source and target lane numbers; either may be
    None to match (or clear to) a NULL lane, but they must differ — an identical
    pair is a no-op and is rejected so the SQLite change_log never gains spurious
    entries. A non-null lane must be >= 1, since update_lane reserves -1 as the
    NULL sentinel in its `COALESCE(lane, -1)` comparisons.

    `reason` is the required audit string; update_lane records one change_log row
    per reassigned sample carrying it.
    """

    model_config = ConfigDict(extra="forbid")

    platform: Literal["illumina", "tellseq"]
    from_lane: Annotated[int, Field(ge=1)] | None = None
    to_lane: Annotated[int, Field(ge=1)] | None = None
    reason: str = Field(min_length=1)

    @field_validator("reason")
    @classmethod
    def reason_not_blank(cls, v: str) -> str:
        # min_length=1 alone admits a whitespace-only string, which update_lane
        # would write verbatim into the immutable change_log audit trail — a
        # meaningless record that defeats the point of requiring a reason. Reject
        # blank/whitespace-only (the value is otherwise stored unchanged).
        if not v.strip():
            raise ValueError("reason must not be blank or whitespace-only")
        return v

    @model_validator(mode="after")
    def lanes_must_differ(self):
        if self.from_lane == self.to_lane:
            raise ValueError("from_lane and to_lane are identical; no lane change requested")
        return self


class SequencedPoolPreflightUpdateLaneResponse(BaseModel):
    """Returned by POST .../preflight/update-lane on success.

    `rows_updated` is the number of platform-sample rows whose lane was
    reassigned — 0 when no row currently sits at `from_lane`."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    rows_updated: Annotated[int, Field(ge=0)]


class SequencedPoolDeleteResponse(BaseModel):
    """Summary of a full sequenced_pool purge across Postgres.

    Counts are the rows removed by the FK-ordered cascade. The parent
    `sequencing_run` is intentionally retained (a run may hold other pools);
    biosample rows are also retained — a biosample is a physical sample
    independent of any single prep and is not pool-owned. `prep_sample_deleted`
    therefore never drives biosample GC. See the route docstring (DELETE
    /sequencing-run/{R}/sequenced-pool/{P}) and the delete-cascade action."""

    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequenced_sample_deleted: int
    prep_sample_deleted: int
    metadata_deleted: int
    field_exception_deleted: int
    study_link_deleted: int
    work_ticket_deleted: int


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
    The two ENA accession fields are nullable: a sample may already carry
    ENA accessions when it is created (e.g. ingesting already-submitted
    data), or have them written back later after an ENA submission.
    """

    model_config = ConfigDict(extra="forbid")

    biosample_idx: Annotated[int, Field(gt=0)]
    prep_protocol_idx: Annotated[int, Field(gt=0)]
    owner_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_item_id: str = Field(min_length=1)
    primary_study_idx: Annotated[int, Field(gt=0)]
    secondary_study_idxs: list[Annotated[int, Field(gt=0)]] = Field(default_factory=list)
    metadata: dict[str, str] = Field(default_factory=dict)
    metadata_checklist_name: str | None = Field(default=None, min_length=1)
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


def _fraction_passing_quality_filter(
    raw_read_count_r1r2: int | None, quality_filtered_read_count_r1r2: int | None
) -> float | None:
    """quality_filtered / raw — the share of raw reads surviving the full QC +
    host-filter pipeline. Computed on read so it can never drift from the counts.
    None when either bound is absent or raw is 0 (no division). Shared
    by the per-sample (SequencedSampleResponse) and pool-rollup (PoolReadMetrics)
    surfaces; the pool passes its SUMMED counts here, so the pool fraction is
    recomputed from the sums, never a mean of per-sample fractions."""
    if raw_read_count_r1r2 is None or quality_filtered_read_count_r1r2 is None:
        return None
    if raw_read_count_r1r2 == 0:
        return None
    return quality_filtered_read_count_r1r2 / raw_read_count_r1r2


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
    metadata_checklist: MetadataChecklistRef | None
    sequenced_pool_idx: int | None
    sequenced_pool_item_id: str | None
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    last_submission_at: AwareDatetime | None
    submission_error: str | None
    # Per-stage read counts, both-mates (R1+R2) totals. NULL until the
    # sample is processed by fastq-to-parquet/1.2.0 (the persist-read-metrics
    # action writes them). raw >= biological >= quality_filtered by the DB CHECK.
    raw_read_count_r1r2: int | None
    biological_read_count_r1r2: int | None
    quality_filtered_read_count_r1r2: int | None
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

    @computed_field  # type: ignore[prop-decorator]
    @property
    def fraction_passing_quality_filter(self) -> float | None:
        """quality_filtered / raw for this sample — see
        `_fraction_passing_quality_filter`. Computed on read, never stored, so it
        can't drift from the counts."""
        return _fraction_passing_quality_filter(
            self.raw_read_count_r1r2, self.quality_filtered_read_count_r1r2
        )


class SequencedSamplePatchRequest(PatchRequestModel):
    """Body for PATCH /api/v1/sequenced-sample/{sequenced_sample_idx}.

    Carries the four subtype-table columns editable after creation: the
    two ENA accessions (which may also be set at create time) and the
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


# ---------------------------------------------------------------------------
# Mask definition (read-filtering config identity)
# ---------------------------------------------------------------------------


class MaskDefinitionMintRequest(BaseModel):
    """Body for POST /api/v1/mask-definition.

    Mints (or returns the existing) mask_idx for a read-filtering config.
    `params` is the full config blob — host references, QC settings, etc.;
    its canonical-JSON SHA-256 is the dedup key, so the same config always
    resolves to the same mask_idx fleet-wide (idempotent mint). Service-account
    callers with `read_masked:doget` only — humans never mint masks.
    """

    model_config = ConfigDict(extra="forbid")

    filter_workflow: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    filter_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    # The config blob the mask_idx is deduplicated on. Must be a JSON object so
    # the hash is over a stable, named-key structure (not a bare scalar/array).
    params: dict[str, Any]


class MaskDefinition(BaseModel):
    """Returned by POST /api/v1/mask-definition (200/201).

    `mask_idx` is the filtering-config discriminator that tags the data plane's
    read_mask / read_masked rows. The same `params` (canonically hashed) always
    yields the same `mask_idx`.
    """

    mask_idx: Annotated[int, Field(gt=0)]
    filter_workflow: str
    filter_version: str
    params: dict[str, Any]
    created_at: AwareDatetime


class MaskDefinitionDeleteResponse(BaseModel):
    """Returned by DELETE /api/v1/mask-definition/{mask_idx}.

    `rows_deleted` is the DuckLake `read_mask` row count the data plane removed
    (0 on an idempotent re-run); the Postgres `mask_definition` row is purged
    last. Referencing `work_ticket` rows detach automatically via the
    `ON DELETE SET NULL` FK."""

    mask_idx: Annotated[int, Field(gt=0)]
    rows_deleted: int


class ReadMaskedDoGetTicketRequest(BaseModel):
    """Body for POST /api/v1/read-masked/ticket/doget.

    Signs a Flight DoGet ticket scoped to a single (prep_sample_idx, mask_idx)
    on the data plane's `read_masked` view. Both identifiers are mandatory: the
    data plane's empty-filter path would dump every sample's pass reads across
    every mask, so the route never signs an unfiltered read_masked ticket
    (the mandatory-filter invariant).
    """

    model_config = ConfigDict(extra="forbid")

    prep_sample_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
