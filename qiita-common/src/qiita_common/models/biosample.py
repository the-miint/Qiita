"""Biosample import models, metadata value shapes, accession/matrix-tube
lookups, the shared bulk-idx / list response envelopes, and the biosample
PATCH body."""

from datetime import date
from decimal import Decimal
from typing import Annotated, ClassVar, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from qiita_common.auth_constants import MAX_NAME_LENGTH, SystemRole
from qiita_common.models._base import PatchRequestModel
from qiita_common.models.reference import FieldDataType

# matrix_tube_id values are digit-only (per local convention) and may carry
# leading zeros; the {10} quantifier fixes the length at exactly ten digits
# and rejects the empty string.
#
# Deliberately duplicated with the column-level CHECK on
# qiita.biosample.matrix_tube_id: the Pydantic side fails at the wire
# boundary with a per-field 422; the DB side is the last line of defense.
# Change one and you must change the other in the same PR.
MATRIX_TUBE_ID_PATTERN = r"^[0-9]{10}$"  # same-pattern-ok: DB CHECK parity (see above)


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

    `human_filtering` is the sample's intake host-filter intent, derived at
    request time from the pool's stored run-preflight blob (the single source
    of truth — it is not a stored sample column) and keyed by
    sequenced_pool_item_id. Only the pool-scoped list route populates it (the
    pool-wide host-filter guard in `submit-host-filter-pool` reads it); the
    run-scoped list leaves it None. It is also None when the pool has no
    preflight populated or the blob carries no row for this item.

    `has_read_mask_ticket` is True when at least one `read-mask` work ticket
    (any state) already exists for the sample's prep_sample_idx. Both list
    routes populate it. It lets `submit-host-filter-pool --only-missing` skip
    samples a prior (possibly interrupted) fan-out already submitted, and gives
    operators per-sample visibility into host-processing coverage without an
    N+1 of work-ticket lookups.
    """

    sequenced_sample_idx: int
    prep_sample_idx: int
    biosample_idx: int
    sequenced_pool_item_id: str
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    biosample_accession: str | None
    ena_sample_accession: str | None
    human_filtering: bool | None = None
    has_read_mask_ticket: bool = False


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
