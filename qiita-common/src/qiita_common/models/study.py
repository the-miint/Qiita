"""Study create / patch / response models."""

from enum import StrEnum
from typing import Annotated, ClassVar, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from qiita_common.models._base import LookupEmail, PatchRequestModel
from qiita_common.models.reference import STORABLE_ACCESS_TIERS, Tier

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


class StudyRecordView(StrEnum):
    """How much of a study's record a caller may read (see
    docs/architecture/data-model.md, "What a tier reads of a study's own
    record"). No Postgres twin: derived per request."""

    FULL = "full"
    SUMMARY = "summary"


class StudyResponse(BaseModel):
    """Returned by POST /api/v1/study on success, and by GET and PATCH
    /api/v1/study/{study_idx} to a caller who may read the full record
    (`view` = full; see `StudyRecordSummary` for the other view).

    Mirrors the qiita.study row's caller-visible columns, with the
    generated search_vector and parent_study_idx (not exposed in v1)
    omitted.
    """

    view: Literal[StudyRecordView.FULL] = StudyRecordView.FULL
    study_idx: Annotated[int, Field(gt=0)]
    export_entity_id: str
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


class StudyAccessVia(StrEnum):
    """Why a caller can read a study, as GET /api/v1/study reports it.

    Strongest reason first: `owner` (they own it), `grant` (they hold a
    qiita.study_access row), `public` (its default_tier is public), `role`
    (wet_lab_admin+ reads every study). No Postgres twin: derived per request.
    """

    OWNER = "owner"
    GRANT = "grant"
    PUBLIC = "public"
    ROLE = "role"


class StudySummary(BaseModel):
    """One row of GET /api/v1/study — a study the caller may read.

    `caller_tier` is the caller's own tier on the study: `admin` for its
    owner, else their qiita.study_access row, or `public` when they hold none.
    `access_via`
    says why they can read it, which `caller_tier` alone cannot: a
    wet_lab_admin with no grant and a stranger on a public study both have
    `caller_tier` public.
    """

    study_idx: Annotated[int, Field(gt=0)]
    export_entity_id: str
    title: str
    alias: str | None
    bioproject_accession: str | None
    ena_study_accession: str | None
    default_tier: Tier
    caller_tier: Tier
    access_via: StudyAccessVia
    # What GET /study/{study_idx} would return this caller.
    record_view: StudyRecordView
    updated_at: AwareDatetime


class StudySummaryListResponse(BaseModel):
    """GET /api/v1/study — the studies the caller may read, newest first.

    `next_after_study_idx` is the cursor for the next page (pass it back as
    `after_study_idx`), or None on the last page. A page can hold fewer than
    `limit` rows and still not be the last: the cursor advances over every
    study the page considered, including ones the `min_tier` filter dropped.
    """

    studies: list[StudySummary]
    next_after_study_idx: int | None


class StudyRecordSummary(BaseModel):
    """GET /api/v1/study/{study_idx} for a caller who may read only the
    study's summary: a grant below the study's default_tier. The full record
    is `StudyResponse`; `view` tells the two apart."""

    view: Literal[StudyRecordView.SUMMARY] = StudyRecordView.SUMMARY
    study_idx: Annotated[int, Field(gt=0)]
    export_entity_id: str
    title: str
    alias: str | None
    bioproject_accession: str | None
    ena_study_accession: str | None
    default_tier: Tier
    updated_at: AwareDatetime


def _reject_public_tier(tier: Tier) -> Tier:
    """`public` is the implicit tier of a caller with no row, never a stored
    value; reject it before the DB."""
    if tier not in STORABLE_ACCESS_TIERS:
        raise ValueError("access_tier cannot be 'public'; revoke the row instead")
    return tier


class StudyAccessGrant(BaseModel):
    """Body for POST /api/v1/study/{study_idx}/access — grant a tier.

    The grantee is named by the email on their qiita.user row; they must
    have logged in once so that row exists.
    """

    model_config = ConfigDict(extra="forbid")

    email: LookupEmail
    access_tier: Tier

    _no_public = field_validator("access_tier")(_reject_public_tier)


class StudyAccessTierUpdate(BaseModel):
    """Body for PATCH /api/v1/study/{study_idx}/access/{principal_idx}."""

    model_config = ConfigDict(extra="forbid")

    access_tier: Tier

    _no_public = field_validator("access_tier")(_reject_public_tier)


class StudyAccessResponse(BaseModel):
    """One qiita.study_access row, with the grantee's email (None when the
    grantee is not a user-kind principal)."""

    study_idx: Annotated[int, Field(gt=0)]
    principal_idx: Annotated[int, Field(gt=0)]
    email: str | None
    access_tier: Tier
    granted_by_idx: int | None
    granted_at: AwareDatetime
