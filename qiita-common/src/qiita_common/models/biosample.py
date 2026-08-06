"""Biosample import models, metadata value shapes, accession/matrix-tube
lookups, the shared bulk-idx / list response envelopes, and the biosample
PATCH body."""

from datetime import date
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, ClassVar, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    model_validator,
)

from qiita_common.auth_constants import SystemRole
from qiita_common.models._base import (
    AccessionText,
    MetadataRequestModel,
    NonBlankName,
    NonBlankText,
    PatchRequestModel,
)
from qiita_common.models.host_filter_profile import HostFilterResolution
from qiita_common.models.reference import FieldDataType
from qiita_common.models.sample_field import (
    SampleStudyFieldCreateRequest,
    SampleStudyFieldResponse,
)

# matrix_tube_id values are digit-only (per local convention) and may carry
# leading zeros; the {10} quantifier fixes the length at exactly ten digits
# and rejects the empty string.
#
# Deliberately duplicated with the column-level CHECK on
# qiita.biosample.matrix_tube_id: the Pydantic side fails at the wire
# boundary with a per-field 422; the DB side is the last line of defense.
# Change one and you must change the other in the same PR.
MATRIX_TUBE_ID_PATTERN = r"^[0-9]{10}$"  # same-pattern-ok: DB CHECK parity (see above)

# The wire spellings of the two idx fields the biosample study-field shapes
# re-declare with an entity-qualified alias. Named so a caller writing or
# reading one of those payload keys resolves it here instead of retyping it.
BIOSAMPLE_STUDY_FIELD_IDX_WIRE = "biosample_study_field_idx"
BIOSAMPLE_GLOBAL_FIELD_IDX_WIRE = "biosample_global_field_idx"

# Defined here so the wire models and the repository layer share one definition.
type MetadataFieldScope = Literal["global", "local"]


def derive_metadata_field_scope(global_link: object) -> MetadataFieldScope:
    """Map a nullable global-linkage marker to the scope it implies.

    A global-linkage marker is a value that is populated when the field is
    globally linked and null when it is purely local: a row's FK to a global
    field, or the linked global field's internal_name carried alongside it.
    Callers pass whichever one their own type holds. A global field's own idx is
    not such a marker -- it is never null, so it distinguishes nothing.
    """
    return "global" if global_link is not None else "local"


class FieldWriteOutcome(StrEnum):
    """What a single metadata upsert did to one field's slot: created a new
    row, overwrote an existing value in place, or left an already-identical
    value untouched. Reported per field by the metadata-write surface. Not a
    stored value, so it has no Postgres twin.
    """

    INSERTED = "inserted"
    UPDATED = "updated"
    UNCHANGED = "unchanged"


class BiosampleImportRequest(MetadataRequestModel):
    """Body for POST /api/v1/study/{study_idx}/biosample.

    The route gates on `Tier.ADMIN` access to the path's study
    (study owner, an ADMIN study_access row, or wet_lab_admin+ via the
    role bypass). owner_idx names the user the biosample is being
    created for and must be supplied explicitly. The metadata dict
    carries text values keyed on a biosample field's display_name — or,
    when global_internal_names is set, on a biosample_global_field's
    internal_name for global fields (local fields stay display-name-keyed);
    the route parses each value into the field's data type before insert.
    An empty dict is allowed. Every key and value strips on the way in and
    must still carry content, so the field a key names and the value stored
    under it are both the stripped form.
    """

    owner_idx: Annotated[int, Field(gt=0)]
    owner_biosample_id_field_name: NonBlankName
    owner_biosample_id_value: NonBlankText
    metadata: dict[NonBlankText, NonBlankText] = Field(default_factory=dict)
    global_internal_names: bool = False
    metadata_checklist_name: NonBlankText | None = None
    biosample_accession: AccessionText | None = None
    ena_sample_accession: AccessionText | None = None
    matrix_tube_id: Annotated[
        str | None,
        Field(pattern=MATRIX_TUBE_ID_PATTERN),
    ] = None


class BiosampleImportResponse(BaseModel):
    """Returned by POST /api/v1/study/{study_idx}/biosample on success.

    `owner_id_biosample_study_field_*` name the biosample_study_field row
    that holds the owner-biosample-id for this study — the purely-local,
    member-tier-restricted field flagged is_owner_biosample_id=True on the
    associated biosample_metadata row.
    """

    biosample_idx: Annotated[int, Field(gt=0)]
    owner_id_biosample_study_field_idx: Annotated[int, Field(gt=0)]
    owner_id_biosample_study_field_created: bool


class OwnerBiosampleIdRow(BaseModel):
    """One row of the owner-id re-identification export.

    Pairs a biosample's stable minted idx and public accession with the
    owner-submitted original sample name — biosample_metadata.value_text on
    the row flagged is_owner_biosample_id=True. That value is the owner's
    own name for their sample; the owner legitimately needs to recover it.
    Its visibility is restricted to authorized study members rather than
    exposed on the general read path, because submitters sometimes
    incautiously place PII in sample names; this export is the
    system_admin-gated path for recovering it across studies.

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

# `biosample_global_field.internal_name` values the host-filter arc reads. Both
# are terminology-typed against NCBI Taxonomy, and they are NOT the same fact:
#
#   TAXON_ID      — the sample's OWN organism. For a metagenome, the environment
#                   it came from ('human gut metagenome', 'seawater metagenome').
#   HOST_TAXON_ID — the organism the sample was taken FROM. Absent for an
#                   environmental sample; a missing-reason for a control.
#
# Conflating them is the bug this arc exists to prevent, so they are named side
# by side. Shared because the resolver reads host_taxon_id and the backfill reads
# taxon_id to derive it — two consumers, one spelling.
BIOSAMPLE_FIELD_TAXON_ID = "taxon_id"
BIOSAMPLE_FIELD_HOST_TAXON_ID = "host_taxon_id"

# The two `qiita.missing_value_reason` names the host-filter resolver RECOGNISES
# — i.e. the two that say something definite about whether a host exists.
#
# Deliberately not an exhaustive enum of the INSDC vocabulary. The other reasons
# ('not collected', 'not provided', 'restricted access', …) exist in the DB and
# have no constant here ON PURPOSE: recognising a reason is an explicit act that
# promotes it from "abort" to "proceed", and an enum listing every reason would
# invite exactly the mechanical widening the fail-closed rule is there to stop.
MISSING_REASON_NOT_APPLICABLE = "not applicable"
MISSING_REASON_CONTROL_SAMPLE = "missing: control sample"

# The terminology `taxon_id` / `host_taxon_id` name their terms in, and the
# term_id of the human host. Shared so production code and the test seeds spell
# them the same way — production must not reach into `testing/` for them.
#
# `NCBI_TAXONOMY_HUMAN_TERM_ID` is a STRING even though an NCBI taxon id is a number,
# and that is not an oversight. It is a `qiita.terminology_term.term_id`, whose column
# is `VARCHAR(255)` because that table is generic across terminologies — the same column
# holds ENVO ids like 'ENVO:01000249', which are never going to be integers. Typing it
# as an int here would mean casting back to text at every lookup.
#
# Nothing stores this string against a biosample. What a biosample carries is
# `value_terminology_term_idx BIGINT` — the surrogate `terminology_term.idx` — so every
# join and every comparison downstream (`host_filter_resolver.host_term_idx`) is already
# a BIGINT. The string is the natural key, used once, at the boundary, to resolve TO
# that BIGINT.
NCBI_TAXONOMY_NAME = "NCBI Taxonomy"
NCBI_TAXONOMY_HUMAN_TERM_ID = "9606"


class MissingReasonRef(BaseModel):
    """Resolved-once shape for a metadata text value recognised as a marker
    for an intentionally-missing entry. Carries the qiita.missing_value_reason
    row's idx (the FK target on *_metadata.value_missing_reason_idx) and
    the matched reason name. `kind` discriminates this variant from other
    dict-shaped value variants on MetadataEntry.value. value_column
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
    variants on MetadataEntry.value. value_column is the target
    value_* column for a terminology-term write.
    """

    kind: Literal["terminology_term"] = "terminology_term"
    idx: Annotated[int, Field(gt=0)]
    term_id: Annotated[str, Field(min_length=1)]
    label: Annotated[str, Field(min_length=1)]

    @property
    def value_column(self) -> str:
        return TERMINOLOGY_TERM_VALUE_COLUMN


# One resolved metadata value: a scalar, an intentionally-missing marker, or a
# terminology term; the two Ref variants discriminate on `kind`. bool sits after
# Decimal so an int-shaped value still resolves to Decimal; a genuine bool wins
# on exact type either way.
SampleMetadataValue = (
    str
    | Decimal
    | date
    | bool
    | Annotated[MissingReasonRef | TerminologyTermRef, Field(discriminator="kind")]
)


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


class MetadataEntry(BaseModel):
    """One resolved metadata value for a biosample or prep_sample, with
    cosmetic context.

    Returned as a value inside a response's metadata dict. On the
    globally-linked read the dict is keyed on the field's `internal_name` and
    display_name / description are the canonical *_global_field values; on a
    study-scoped local read the dict is keyed on `display_name` and both come
    from the study-local field. data_type identifies which Python type carries
    the value: TEXT -> str, NUMERIC -> Decimal, DATE -> date,
    BOOLEAN -> bool; a MissingReasonRef
    carries an intentionally-missing entry's reason idx + name; a
    TerminologyTermRef carries a terminology-term entry's idx + term_id + label.
    Both Ref variants supersede data_type-driven decoding.
    """

    display_name: str
    description: str | None
    data_type: FieldDataType
    value: SampleMetadataValue


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
    global_metadata: dict[str, MetadataEntry]
    caller_system_role: SystemRole


class StudyScopedBiosampleResponse(BiosampleResponse):
    """Returned by GET /api/v1/study/{study_idx}/biosample/{biosample_idx}.

    A study-scoped view: every field of the biosample-level BiosampleResponse
    (core columns, caller_system_role, and the globally-linked global_metadata
    keyed by internal_name) plus this study's purely-local metadata, keyed by
    display_name. local_metadata is returned only on a study-scoped response,
    where the caller is already authorized on the study.
    """

    local_metadata: dict[str, MetadataEntry]


class MetadataFieldWriteResult(BaseModel):
    """What a metadata write did to one field's slot: the key the value reads
    back under, the write outcome, and the value that now occupies the slot.

    internal_name is the field's globally unique identifier when the write
    resolved to a globally-linked field, and None when it resolved to a
    purely-local one. It is what a subsequent read is keyed on: a global value
    appears in a response's global_metadata under internal_name, a local value
    in local_metadata under the key the caller sent. The two differ for a
    global field, so a caller verifying its own write needs this rather than
    the key it supplied. A numeric value comes back in the form it is stored
    as, which keeps the caller's scale but resolves exponent notation into
    plain digits.
    """

    model_config = ConfigDict(extra="forbid")

    internal_name: str | None
    outcome: FieldWriteOutcome
    value: SampleMetadataValue

    @model_validator(mode="before")
    @classmethod
    def _reject_contradicting_scope(cls, data: object) -> object:
        """Accept a supplied scope only when it matches the derived value.

        Permitting the matching case keeps a serialized result parseable by this
        model; a contradiction raises rather than letting either side win
        silently. The key is dropped from the returned copy so extra="forbid"
        does not then reject the derived field as an unknown input.
        """
        if not isinstance(data, dict) or "scope" not in data:
            return data
        # An absent internal_name is its own error. Deriving from the missing
        # key would report a contradiction against a value never supplied,
        # naming the wrong field as the one to fix.
        if "internal_name" in data:
            derived = derive_metadata_field_scope(data["internal_name"])
            if data["scope"] != derived:
                raise ValueError(
                    f"scope {data['scope']!r} contradicts internal_name; scope is derived"
                    f" and must be omitted or {derived!r}"
                )
        return {key: value for key, value in data.items() if key != "scope"}

    @computed_field  # type: ignore[prop-decorator]
    @property
    def scope(self) -> MetadataFieldScope:
        """Whether the write went through a globally-linked or a purely-local
        field. Computed on read from internal_name, never stored, so the two
        can't drift."""
        return derive_metadata_field_scope(self.internal_name)


class SampleMetadataWriteRequest(MetadataRequestModel):
    """Body for a study-scoped sample-family metadata write (the
    PATCH .../{entity}/metadata routes, e.g. biosample and sequenced-sample).

    metadata carries text values keyed on field display_name — or, when
    global_internal_names is set, on a global field's internal_name (study-local
    fields stay display-name-keyed either way). The route resolves each key
    against the study's existing global or study-local fields and upserts it. At
    least one entry is required — an empty write is almost certainly a client
    error — and every key and value strips on the way in and must still carry
    content, so a name written with stray padding resolves to the same field as
    its unpadded spelling. Unknown field names are rejected by the route rather
    than created.

    Under global_internal_names, a key matching a global field's internal_name
    reads back under that same key, because the globally-linked read is
    internal-name-keyed. A key matching no global internal_name is still resolved
    against the study's local fields by display_name; if that field is an alias
    of a global one, the value goes to the global slot and reads back under the
    global's internal_name rather than the key sent. So the flag aligns the two
    keys for a direct global match, not for every key. Each result's
    internal_name reports the read key in both cases.
    """

    model_config = ConfigDict(extra="forbid")

    metadata: dict[NonBlankText, NonBlankText] = Field(min_length=1)
    global_internal_names: bool = False


class SampleMetadataWriteResponse(BaseModel):
    """Returned by a study-scoped sample-family metadata write route.

    results maps each key the caller sent to what the write did to that field,
    in the caller's input order. The keys are the caller's own in their stripped
    form, since keys strip at the wire — a key sent with surrounding whitespace
    comes back without it. For a globally-linked field the caller's key is also
    not the key the value reads back under; each result's internal_name carries
    that.
    """

    results: dict[str, MetadataFieldWriteResult]


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

    accessions: list[NonBlankText] = Field(min_length=1, max_length=10_000)
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

    accessions: list[NonBlankText] = Field(min_length=1, max_length=10_000)
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

    metadata_checklist_name: NonBlankText | None = None
    owner_idx: Annotated[int, Field(gt=0)] | None = None
    biosample_accession: AccessionText | None = None
    ena_sample_accession: AccessionText | None = None
    matrix_tube_id: Annotated[
        str | None,
        Field(pattern=MATRIX_TUBE_ID_PATTERN),
    ] = None
    last_submission_at: AwareDatetime | None = None
    submission_error: str | None = None


class BiosampleStudyFieldCreateRequest(SampleStudyFieldCreateRequest):
    """Body for POST /api/v1/study/{study_idx}/biosample-field."""

    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        default=None, alias=BIOSAMPLE_GLOBAL_FIELD_IDX_WIRE
    )


class BiosampleStudyFieldResponse(SampleStudyFieldResponse):
    """Returned by POST /api/v1/study/{study_idx}/biosample-field (201 body is
    the created resource), carrying every qiita.biosample_study_field column.
    """

    study_field_idx: Annotated[int, Field(gt=0)] = Field(alias=BIOSAMPLE_STUDY_FIELD_IDX_WIRE)
    global_field_idx: Annotated[int, Field(gt=0)] | None = Field(
        alias=BIOSAMPLE_GLOBAL_FIELD_IDX_WIRE
    )


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


    `has_read_mask_ticket` is True when at least one `read-mask` work ticket
    (any state) already exists for the sample's prep_sample_idx. Both list
    routes populate it. It lets `submit-host-filter-pool --only-missing` skip
    samples a prior (possibly interrupted) fan-out already submitted, and gives
    operators per-sample visibility into host-processing coverage without an
    N+1 of work-ticket lookups.

    `host_filter` is what host filtering the sample WOULD get, resolved at
    request time from its `host_taxon_id` metadata plus the run's platform. It
    is the read-only preview of a submission's plan — nothing acts on it yet.
    Only the pool-scoped list route populates it (it needs the run's platform,
    and the resolution is per-pool work); the run-scoped list leaves it None.

    A sample's host is a property of the SAMPLE, not of the project it was booked
    under, so the decision comes from its own metadata.
    """

    sequenced_sample_idx: int
    prep_sample_idx: int
    biosample_idx: int
    sequenced_pool_item_id: str
    ena_experiment_accession: str | None
    ena_run_accession: str | None
    biosample_accession: str | None
    ena_sample_accession: str | None
    host_filter: HostFilterResolution | None = None
    # PacBio protocol facts, None for an Illumina pool (or when the blob omits the
    # row). Derived at request time from the pool's stored run-preflight blob, the
    # same single-source-of-truth path `human_filtering` uses — none of these is a
    # stored sequenced_sample column. The read-mask submit derives its per-sample
    # gates from them:
    #     syndna_enabled = sheet_type == 'pacbio_absquant'
    #     lima_enabled   = twist_adaptor_id filled AND NOT syndna_is_twisted
    sheet_type: str | None = None
    twist_adaptor_id: str | None = None
    syndna_is_twisted: bool | None = None
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
