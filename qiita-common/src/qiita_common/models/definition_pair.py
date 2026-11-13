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

    Twin of a TEXT + CHECK column (`20261113000000_definition_pair.sql`), not a
    Postgres ENUM.
    """

    MASK = "mask"
    ALIGNMENT = "alignment"
    PROCESSING = "processing"


class DefinitionPairVerdict(StrEnum):
    """A pair's decision (`qiita.definition_pair.verdict` and
    `qiita.definition_pair_event.verdict`).

    Twin of a TEXT + CHECK column (`20261113000000_definition_pair.sql`), not a
    Postgres ENUM.
    """

    PENDING = "pending"
    SIMILAR = "similar"
    NOT_SIMILAR = "not_similar"


class DefinitionPairEventType(StrEnum):
    """What a `qiita.definition_pair_event` row records.

    Twin of a TEXT + CHECK column (`20261113000000_definition_pair.sql`), not a
    Postgres ENUM.
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
    DERIVED_ONLY = "derived_only"
    UPSTREAM_NOT_GROUPED = "upstream_not_grouped"
    CONTRADICTION = "contradiction"
    RELIED_ON = "relied_on"
    NOT_DECIDED = "not_decided"
    SIMILAR_PAIRS = "similar_pairs"


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
    each differing dotted param path to `[low value, high value]`. The decision
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
    """Returned by GET /api/v1/admin/definition-pair. `truncated` is true when the
    list hit the route's cap; narrow with `verdict` or `definition_kind`."""

    pairs: list[DefinitionPair]
    count: int
    truncated: bool
