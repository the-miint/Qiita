"""Per-sample resolution across a definition group, and its wire models.

A group is the masks, alignments or processings that pair decisions (and, for
alignments and processings, derivation) join; each prep_sample is resolved to one
member of it, or to none. The rules are in
`qiita_control_plane.definition_resolver`.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from qiita_common.models._base import PrepSampleCohort
from qiita_common.models.definition_pair import DefinitionKind


class SampleResolutionState(StrEnum):
    """What a prep_sample's results under a group are. Not stored."""

    PROCESSED = "processed"
    INCOMPLETE = "incomplete"
    NOT_PROCESSED = "not_processed"


class SampleResolutionReason(StrEnum):
    """Why a prep_sample is `incomplete` or `not_processed`. Each code is set at one
    site in `qiita_control_plane.definition_resolver`, whose docstring says when."""

    # incomplete
    IN_PROGRESS = "in_progress"
    FAILED = "failed"
    UPSTREAM_MOVED = "upstream_moved"
    UPSTREAM_INCOMPLETE = "upstream_incomplete"
    # not_processed
    NO_RUN = "no_run"
    WITHDRAWN = "withdrawn"
    DEPRECATED = "deprecated"
    UPSTREAM_DEPRECATED = "upstream_deprecated"
    UPSTREAM_WITHDRAWN = "upstream_withdrawn"
    UPSTREAM_NO_RUN = "upstream_no_run"


class DefinitionGroupRefusal(StrEnum):
    """The `reason` code of a 409 refusing a cohort because a sample does not resolve
    to the definition the caller named."""

    RESOLVED_ELSEWHERE = "resolved_elsewhere"
    INCOMPLETE = "incomplete"
    NOT_PROCESSED = "not_processed"


class DefinitionGroupResolveRequest(BaseModel):
    """Body for POST /api/v1/definition-group/resolve: a definition of the group to
    resolve (any member names the whole group) and the prep_samples to resolve,
    which may span pools."""

    model_config = ConfigDict(extra="forbid")

    definition_kind: DefinitionKind
    definition_idx: Annotated[int, Field(gt=0)]
    prep_sample_idx: PrepSampleCohort


class SampleResolution(BaseModel):
    """One prep_sample's resolution.

    `definition_idx` is the group member the state is about: the one whose results
    are used when `processed`, the one with the unfinished or unusable run
    otherwise, and None when no member has a run for the sample (`no_run`).
    `mask_idx` is that member's mask, for an alignment or a processing.
    `work_ticket_idx` is the ticket an `incomplete` sample waits on, when one is
    known. `upstream_kind` / `upstream_definition_idx` name the upstream definition
    an `upstream_*` reason is about: for `upstream_moved`, the one the sample now
    resolves to."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    state: SampleResolutionState
    definition_kind: DefinitionKind
    definition_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    mask_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    reason: SampleResolutionReason | None = None
    work_ticket_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    upstream_kind: DefinitionKind | None = None
    upstream_definition_idx: Annotated[int | None, Field(default=None, gt=0)] = None


class DefinitionGroupResolveResponse(BaseModel):
    """Returned by POST /api/v1/definition-group/resolve: the named definition's
    group (ascending) and one resolution per distinct requested prep_sample,
    ascending by prep_sample_idx."""

    definition_kind: DefinitionKind
    definition_idx: Annotated[int, Field(gt=0)]
    group: list[int]
    samples: list[SampleResolution]
