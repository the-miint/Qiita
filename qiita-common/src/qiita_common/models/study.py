"""Study create / patch / response models."""

from typing import Annotated, ClassVar

from pydantic import AwareDatetime, BaseModel, Field

from qiita_common.models._base import PatchRequestModel
from qiita_common.models.reference import Tier

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
