"""Per-sample resolution across a definition group, and its wire models.

A group is the masks, alignments or processings that pair decisions (and, for
alignments and processings, derivation) join; each prep_sample is resolved to one
member of it, or to none. The rules are in
`qiita_control_plane.definition_resolver`.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from qiita_common.models._base import PrepSampleCohort
from qiita_common.models.definition_pair import DefinitionKind


class SampleResolutionState(StrEnum):
    """What a prep_sample's results under a group are. Not stored."""

    PROCESSED = "processed"
    INCOMPLETE = "incomplete"
    NOT_PROCESSED = "not_processed"


class SampleResolutionReason(StrEnum):
    """Why a prep_sample is `incomplete` or `not_processed`. Each code but
    `no_syndna_counts` is set at one site in `qiita_control_plane.definition_resolver`,
    whose docstring says when. `no_syndna_counts` is set by the SynDNA count export
    (`routes/read_masked.py`, `get_syndna_read_count_route`), whose docstring says
    when."""

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
    NO_DATA = "no_data"
    NO_SYNDNA_COUNTS = "no_syndna_counts"


class DefinitionGroupRefusal(StrEnum):
    """The `reason` code of a 409 refusing a cohort because a prep_sample does not
    resolve to the definition that the caller named, or because that definition is
    deprecated and the caller did not set `include_deprecated` (`DEPRECATED`), or
    because an upstream of it is deprecated and the caller did not set
    `include_deprecated_upstream` (`DEPRECATED_UPSTREAM`)."""

    RESOLVED_ELSEWHERE = "resolved_elsewhere"
    INCOMPLETE = "incomplete"
    NOT_PROCESSED = "not_processed"
    DEPRECATED = "deprecated"
    DEPRECATED_UPSTREAM = "deprecated_upstream"


# The CLI spelling of the flag every explicit read of a deprecated definition takes;
# the API field and query parameter are `include_deprecated`.
INCLUDE_DEPRECATED_FLAG = "--include-deprecated"


def deprecated_read_refusal(kind: str, idx: int, deprecation_reason: str | None) -> str:
    """The message refusing a deprecated definition named without `include_deprecated`,
    shared by the routes' 409 and the CLI's own refusal so both name the same flag."""
    return (
        f"{kind} {idx} is deprecated: {deprecation_reason}. To read its own results, pass"
        f" {INCLUDE_DEPRECATED_FLAG} (API: include_deprecated=true)"
    )


# The CLI spelling of the flag that reads an active definition whose upstream (a mask,
# or a de novo alignment's assembly) is deprecated; the API field and query parameter
# are `include_deprecated_upstream`.
INCLUDE_DEPRECATED_UPSTREAM_FLAG = "--include-deprecated-upstream"


class DeprecatedUpstream(BaseModel):
    """A deprecated upstream of a definition that a read names: its kind, id,
    `params_hash` (hex; what a published manifest cites in place of the id) and
    deprecation reason."""

    definition_kind: DefinitionKind
    definition_idx: Annotated[int, Field(gt=0)]
    params_hash: str
    deprecation_reason: str | None = None

    def describe(self) -> str:
        return f"{self.definition_kind.value} {self.definition_idx} ({self.deprecation_reason})"


def deprecated_upstream_read_refusal(
    kind: str, idx: int, upstreams: list[DeprecatedUpstream]
) -> str:
    """The message refusing an active definition whose upstream is deprecated, named
    without `include_deprecated_upstream`; shared by the routes' 409 and the CLI's own
    refusal so both name the same flag."""
    named = "; ".join(u.describe() for u in upstreams)
    return (
        f"{kind} {idx} reads deprecated upstream definition(s): {named}. To read its"
        f" results, pass {INCLUDE_DEPRECATED_UPSTREAM_FLAG}"
        f" (API: include_deprecated_upstream=true)"
    )


# A decision-set token: `sha256:` and 64 lowercase hex digits. The canonical form it
# hashes is stated in `qiita_control_plane.decision_set`.
DecisionSetToken = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]

# What `as_of_event` rewinds, stated on every response and manifest that carries it.
AS_OF_SCOPE = "pair decisions only; runs, gates, tickets and deprecation are current"


class DecisionProvenance(BaseModel):
    """The pair decisions a resolution was computed under, without internal ids.

    `decision_set` is the token of every decided pair's verdict as of the resolution
    (`qiita_control_plane.decision_set` states the canonical form); passing it back as
    `as_of_event` rebuilds the groups from those decisions. `newest_event_at` is when the
    newest decision event in that state was recorded (None before any). `as_of` is true
    when the caller asked for an earlier decision set. `pairs_used` / `pairs_used_set`
    count and hash, in the same canonical form, the decided pairs that name a definition
    the resolution read (none when it read one definition exactly, without a group)."""

    model_config = ConfigDict(extra="forbid")

    decision_set: DecisionSetToken
    newest_event_at: datetime | None
    as_of: bool
    as_of_scope: Literal[AS_OF_SCOPE] = AS_OF_SCOPE  # type: ignore[valid-type]
    pairs_used: Annotated[int, Field(ge=0)]
    pairs_used_set: DecisionSetToken


class DefinitionGroupResolveRequest(BaseModel):
    """Body for POST /api/v1/definition-group/resolve: a definition of the group to
    resolve (any member names the whole group) and the prep_samples to resolve,
    which may span pools."""

    model_config = ConfigDict(extra="forbid")

    definition_kind: DefinitionKind
    definition_idx: Annotated[int, Field(gt=0)]
    prep_sample_idx: PrepSampleCohort
    as_of_event: DecisionSetToken | None = None
    # Resolve as if no upstream were deprecated (`definition_resolver`, rule 4).
    include_deprecated_upstream: bool = False


class SampleResolution(BaseModel):
    """One prep_sample's resolution.

    `definition_idx` is the group member that the state is about: the one whose
    results are used when `processed`, the one with the unfinished or unusable run
    otherwise, and None when no member has a run for the prep_sample (`no_run`).
    `mask_idx` is that member's mask, for an alignment or a processing.
    `work_ticket_idx` is the ticket that an `incomplete` prep_sample waits on, when
    one is known; the resolve route returns it to a system_admin only.
    `upstream_kind` / `upstream_definition_idx` name the upstream definition that an
    `upstream_*` reason is about: for `upstream_moved`, the one that the prep_sample
    now resolves to. A `processed` resolution names its member
    and carries no reason; any other carries a reason."""

    prep_sample_idx: Annotated[int, Field(gt=0)]
    state: SampleResolutionState
    definition_kind: DefinitionKind
    definition_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    mask_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    reason: SampleResolutionReason | None = None
    work_ticket_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    upstream_kind: DefinitionKind | None = None
    upstream_definition_idx: Annotated[int | None, Field(default=None, gt=0)] = None

    @model_validator(mode="after")
    def _state_fields_agree(self) -> SampleResolution:
        processed = self.state is SampleResolutionState.PROCESSED
        if processed and (self.definition_idx is None or self.reason is not None):
            raise ValueError("a processed prep_sample names its member and carries no reason")
        if not processed and self.reason is None:
            raise ValueError(f"a {self.state.value} prep_sample carries a reason")
        return self

    def describe(self, *, ticket: bool = True) -> str:
        """One line for a person: the prep_sample, its state and reason, the member
        they are about, the ticket it waits on (unless `ticket` is false) and the
        upstream a reason names, each when known."""
        line = f"prep_sample {self.prep_sample_idx}: {self.state.value}"
        if self.reason is not None:
            line += f" ({self.reason.value})"
        if self.definition_idx is not None:
            line += f", {self.definition_kind.value} {self.definition_idx}"
        if ticket and self.work_ticket_idx is not None:
            line += f", work_ticket {self.work_ticket_idx}"
        if self.upstream_kind is not None:
            line += f", upstream {self.upstream_kind.value} {self.upstream_definition_idx}"
        return line


class DefinitionGroupResolveResponse(BaseModel):
    """Returned by POST /api/v1/definition-group/resolve: the named definition's
    group (ascending) and one resolution per distinct requested prep_sample,
    ascending by prep_sample_idx."""

    definition_kind: DefinitionKind
    definition_idx: Annotated[int, Field(gt=0)]
    group: list[int]
    samples: list[SampleResolution]
    # Set by the route; None only on a plan that a client built without resolving (an
    # exact read of a deprecated definition).
    decisions: DecisionProvenance | None = None
