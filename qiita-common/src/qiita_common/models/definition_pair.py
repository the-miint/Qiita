"""Pair decisions between two definitions of one kind, and the admin wire models
around them.

A pair names two masks, two alignments or two processings. A system_admin decides
it `similar` (the two belong to one group) or `not_similar` (they stay apart), and
can revoke the decision, which returns the pair to `pending`. The rules that turn
decisions into groups, and the refusals, are in
`qiita_control_plane.definition_similarity`.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from qiita_common.auth_constants import MAX_REASON_LENGTH


class DefinitionKind(StrEnum):
    """Which definition table a pair names (`qiita.definition_pair.definition_kind`).

    Twin of that column's TEXT + CHECK, not a Postgres ENUM.
    """

    MASK = "mask"
    ALIGNMENT = "alignment"
    PROCESSING = "processing"


class DefinitionPairVerdict(StrEnum):
    """A pair's decision (`qiita.definition_pair.verdict` and
    `qiita.definition_pair_event.verdict`).

    Twin of the TEXT + CHECK on those two columns, not a Postgres ENUM.
    """

    PENDING = "pending"
    SIMILAR = "similar"
    NOT_SIMILAR = "not_similar"


class DefinitionPairEventType(StrEnum):
    """What a `qiita.definition_pair_event` row records.

    Twin of the TEXT + CHECK on `qiita.definition_pair_event.event`, not a Postgres
    ENUM.
    """

    OPENED = "opened"
    DECIDED = "decided"
    REVOKED = "revoked"


class DefinitionPairRefusal(StrEnum):
    """The `reason` code of a 409 refusing a pair operation or a deprecation.

    Each code is raised at one site in `qiita_control_plane.definition_similarity`,
    whose docstring says what it guards.
    """

    DETACHED = "detached"
    CROSS_SCOPE = "cross_scope"
    NO_DIFFERENCE = "no_difference"
    DEPRECATED_SIDE = "deprecated_side"
    SHARD_IDS_CHANGE = "shard_ids_change"
    SYNDNA_REFERENCE_CHANGE = "syndna_reference_change"
    DERIVED_ONLY = "derived_only"
    UPSTREAM_NOT_GROUPED = "upstream_not_grouped"
    CONTRADICTION = "contradiction"
    RELIED_ON = "relied_on"
    NOT_DECIDED = "not_decided"
    SIMILAR_PAIRS = "similar_pairs"


class DeprecationRefusal(StrEnum):
    """The `reason` code of a deprecation `PATCH .../status` 409 that is not a pair
    rule's. A deprecation's 409 carries `detail` (prose) and `reason` (a code): this
    one, or `DefinitionPairRefusal.SIMILAR_PAIRS`."""

    # Unfinished work tickets under the definition, named in `work_ticket_idx`.
    LIVE_WORK = "live_work"


def _nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("reason must not be blank")
    return value


_Reason = Annotated[
    str, Field(min_length=1, max_length=MAX_REASON_LENGTH), AfterValidator(_nonblank)
]


class DefinitionPairOpenRequest(BaseModel):
    """Body for POST /api/v1/admin/definition-pair: the two definitions of one kind.

    Order does not matter; the pair is stored low id first."""

    model_config = ConfigDict(extra="forbid")

    definition_kind: DefinitionKind
    definition_idx: Annotated[list[Annotated[int, Field(gt=0)]], Field(min_length=2, max_length=2)]

    @field_validator("definition_idx")
    @classmethod
    def _two_distinct(cls, value: list[int]) -> list[int]:
        if value[0] == value[1]:
            raise ValueError("definition_idx must name two different definitions")
        return value


class DefinitionPairDecisionRequest(BaseModel):
    """Body for POST /api/v1/admin/definition-pair/{definition_pair_idx}/decision."""

    model_config = ConfigDict(extra="forbid")

    verdict: DefinitionPairVerdict
    reason: _Reason

    @field_validator("verdict")
    @classmethod
    def _not_pending(cls, value: DefinitionPairVerdict) -> DefinitionPairVerdict:
        if value is DefinitionPairVerdict.PENDING:
            raise ValueError(
                "verdict must be 'similar' or 'not_similar'; revoke to return to pending"
            )
        return value


class DefinitionPairRevokeRequest(BaseModel):
    """Body for POST /api/v1/admin/definition-pair/{definition_pair_idx}/revoke."""

    model_config = ConfigDict(extra="forbid")

    reason: _Reason


class DefinitionPair(BaseModel):
    """One pair, as the admin routes return it.

    `low_definition_idx` / `high_definition_idx` are the two definition ids, kept
    when a definition is deleted by hand (`detached_at` is then set). `change` maps
    each differing param path, a JSON Pointer (`/qc/min_length`), to
    `[low value, high value]`. The decision
    fields are set exactly when `verdict` is not `pending`."""

    definition_pair_idx: Annotated[int, Field(gt=0)]
    definition_kind: DefinitionKind
    low_definition_idx: Annotated[int, Field(gt=0)]
    high_definition_idx: Annotated[int, Field(gt=0)]
    detached_at: AwareDatetime | None = None
    change: dict[str, list[Any]]
    derived_only: bool
    verdict: DefinitionPairVerdict
    decided_by_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    decided_at: AwareDatetime | None = None
    decision_reason: str | None = None
    opened_by_idx: Annotated[int, Field(gt=0)]
    opened_at: AwareDatetime


class DefinitionPairEvent(BaseModel):
    """One history row of a pair. `verdict` is the pair's verdict after the event."""

    definition_pair_event_idx: Annotated[int, Field(gt=0)]
    event: DefinitionPairEventType
    verdict: DefinitionPairVerdict
    actor_principal_idx: Annotated[int, Field(gt=0)]
    reason: str | None = None
    occurred_at: AwareDatetime


class DefinitionPairDetail(DefinitionPair):
    """Returned by GET /api/v1/admin/definition-pair/{definition_pair_idx}: the pair
    plus its history, oldest first."""

    events: list[DefinitionPairEvent]


class DefinitionPairListResponse(BaseModel):
    """Returned by GET /api/v1/admin/definition-pair: one page of pairs, newest first.
    `next_after_definition_pair_idx` is the cursor for the next page (pass it back as
    `after_definition_pair_idx`), or None on the last page."""

    pairs: list[DefinitionPair]
    count: int
    next_after_definition_pair_idx: Annotated[int | None, Field(default=None, gt=0)] = None


class DefinitionSettingsRefusal(StrEnum):
    """The `reason` code of a 409 refusing an edit of a definition's settings that is
    not a pair rule's (those carry a `DefinitionPairRefusal`). Raised in
    `qiita_control_plane.definition_settings`."""

    # Another definition of the kind already has the edited params (its params_hash).
    PARAMS_TAKEN = "params_taken"
    # The edit changes no locked column.
    NO_CHANGE = "no_change"


class DefinitionSettingsEditRequest(BaseModel):
    """Body for PATCH /api/v1/admin/definition-settings/{definition_kind}/{definition_idx}:
    the definition's new `params` (stored whole, params_hash recomputed from it), and for
    a mask or a processing its workflow and version columns when they change (unchanged
    when omitted; an alignment has none). `reason` is recorded with the edit."""

    model_config = ConfigDict(extra="forbid")

    params: dict[str, Any]
    workflow: Annotated[str, Field(min_length=1)] | None = None
    version: Annotated[str, Field(min_length=1)] | None = None
    reason: _Reason


class DefinitionSettingsEdit(BaseModel):
    """One recorded edit of a definition's locked settings (`qiita.definition_settings_edit`).

    `old_settings` / `new_settings` map each locked column to its value, params_hash as
    `\\x<hex>`. `repaired_pair_idx` names the pending and not_similar pairs whose
    `change` and `derived_only` were recomputed from the new params."""

    definition_settings_edit_idx: int
    definition_kind: DefinitionKind
    definition_idx: int
    actor_principal_idx: int
    old_settings: dict[str, Any]
    new_settings: dict[str, Any]
    reason: str
    occurred_at: AwareDatetime
    repaired_pair_idx: list[int]
