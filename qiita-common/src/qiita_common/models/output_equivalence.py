"""Output equivalences between two definitions of one kind, and the admin wire models
around them.

An output equivalence is one recorded verdict between two masks, two alignments or two
processings about whether their outputs are equivalent. A system_admin opens it
(`pending`), decides it, and can revoke the decision, which returns it to `pending`.
`JOINING_VERDICTS` and `NON_JOINING_VERDICTS` say what each decided verdict does. The
rules that turn decisions into groups, and the refusals, are in
`qiita_control_plane.definition_similarity`.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from qiita_common.auth_constants import MAX_REASON_LENGTH


class DefinitionKind(StrEnum):
    """Which definition table an output equivalence names
    (`qiita.output_equivalence.definition_kind`).

    Twin of that column's TEXT + CHECK, not a Postgres ENUM.
    """

    MASK = "mask"
    ALIGNMENT = "alignment"
    PROCESSING = "processing"


class OutputEquivalenceVerdict(StrEnum):
    """An output equivalence's verdict (`qiita.output_equivalence.verdict` and
    `qiita.output_equivalence_event.verdict`).

    Twin of the TEXT + CHECK on those two columns, not a Postgres ENUM.
    """

    # No decision yet. Set on open and on revoke.
    PENDING = "pending"
    # The outputs are the same. Joins the group.
    SAME = "same"
    # The outputs are similar. Joins the group, exactly as `same` does.
    SIMILAR = "similar"
    # The outputs are not equivalent. Does not join.
    DIFFERENT = "different"
    # Treated as `different`: the two were not benchmarked because reprocessing is
    # easier, and the older one is outdated. Does not join, and has no other effect.
    NOT_BENCHMARKED = "not_benchmarked"


# The decided verdicts that put the two definitions in one group. Every rule that joins,
# and every guard that protects a join, reads this set. Its SQL twin is the function
# `qiita.output_equivalence_verdict_joins` (migration 20261113000000_output_equivalence),
# and `tests/routes/test_output_equivalence.py` pins the two to each other.
JOINING_VERDICTS: frozenset[OutputEquivalenceVerdict] = frozenset(
    {OutputEquivalenceVerdict.SAME, OutputEquivalenceVerdict.SIMILAR}
)
# The decided verdicts that keep the two apart: a group holding both sides of one is a
# contradiction.
NON_JOINING_VERDICTS: frozenset[OutputEquivalenceVerdict] = frozenset(
    {OutputEquivalenceVerdict.DIFFERENT, OutputEquivalenceVerdict.NOT_BENCHMARKED}
)


class OutputEquivalenceEventType(StrEnum):
    """What a `qiita.output_equivalence_event` row records.

    Twin of the TEXT + CHECK on `qiita.output_equivalence_event.event`, not a Postgres
    ENUM.
    """

    OPENED = "opened"
    DECIDED = "decided"
    REVOKED = "revoked"


class OutputEquivalenceRefusal(StrEnum):
    """The `reason` code of a 409 refusing an output-equivalence operation or a
    deprecation.

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
    JOINING_EQUIVALENCES = "joining_equivalences"


class DeprecationRefusal(StrEnum):
    """The `reason` code of a deprecation `PATCH .../status` 409 that is not an
    output-equivalence rule's. A deprecation's 409 carries `detail` (prose) and `reason`
    (a code): this one, or `OutputEquivalenceRefusal.JOINING_EQUIVALENCES`."""

    # Unfinished work tickets under the definition, named in `work_ticket_idx`.
    LIVE_WORK = "live_work"


def _nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("reason must not be blank")
    return value


_Reason = Annotated[
    str, Field(min_length=1, max_length=MAX_REASON_LENGTH), AfterValidator(_nonblank)
]


class OutputEquivalenceOpenRequest(BaseModel):
    """Body for POST /api/v1/admin/output-equivalence: the two definitions of one kind.

    Order does not matter; the output equivalence is stored low id first."""

    model_config = ConfigDict(extra="forbid")

    definition_kind: DefinitionKind
    definition_idx: Annotated[list[Annotated[int, Field(gt=0)]], Field(min_length=2, max_length=2)]

    @field_validator("definition_idx")
    @classmethod
    def _two_distinct(cls, value: list[int]) -> list[int]:
        if value[0] == value[1]:
            raise ValueError("definition_idx must name two different definitions")
        return value


class OutputEquivalenceDecisionRequest(BaseModel):
    """Body for POST /api/v1/admin/output-equivalence/{output_equivalence_idx}/decision."""

    model_config = ConfigDict(extra="forbid")

    verdict: OutputEquivalenceVerdict
    reason: _Reason

    @field_validator("verdict")
    @classmethod
    def _not_pending(cls, value: OutputEquivalenceVerdict) -> OutputEquivalenceVerdict:
        if value is OutputEquivalenceVerdict.PENDING:
            decided = ", ".join(sorted(v.value for v in JOINING_VERDICTS | NON_JOINING_VERDICTS))
            raise ValueError(f"verdict must be one of {decided}; revoke to return to pending")
        return value


class OutputEquivalenceRevokeRequest(BaseModel):
    """Body for POST /api/v1/admin/output-equivalence/{output_equivalence_idx}/revoke."""

    model_config = ConfigDict(extra="forbid")

    reason: _Reason


class OutputEquivalence(BaseModel):
    """One output equivalence, as the admin routes return it.

    `low_definition_idx` / `high_definition_idx` are the two definition ids, kept
    when a definition is deleted by hand (`detached_at` is then set). `change` maps
    each differing param path, a JSON Pointer (`/qc/min_length`), to
    `[low value, high value]`. The decision
    fields are set exactly when `verdict` is not `pending`."""

    output_equivalence_idx: Annotated[int, Field(gt=0)]
    definition_kind: DefinitionKind
    low_definition_idx: Annotated[int, Field(gt=0)]
    high_definition_idx: Annotated[int, Field(gt=0)]
    detached_at: AwareDatetime | None = None
    change: dict[str, list[Any]]
    derived_only: bool
    verdict: OutputEquivalenceVerdict
    decided_by_idx: Annotated[int | None, Field(default=None, gt=0)] = None
    decided_at: AwareDatetime | None = None
    decision_reason: str | None = None
    opened_by_idx: Annotated[int, Field(gt=0)]
    opened_at: AwareDatetime


class OutputEquivalenceEvent(BaseModel):
    """One history row of an output equivalence. `verdict` is its verdict after the
    event."""

    output_equivalence_event_idx: Annotated[int, Field(gt=0)]
    event: OutputEquivalenceEventType
    verdict: OutputEquivalenceVerdict
    actor_principal_idx: Annotated[int, Field(gt=0)]
    reason: str | None = None
    occurred_at: AwareDatetime


class OutputEquivalenceDetail(OutputEquivalence):
    """Returned by GET /api/v1/admin/output-equivalence/{output_equivalence_idx}: the
    output equivalence plus its history, oldest first."""

    events: list[OutputEquivalenceEvent]


class OutputEquivalenceListResponse(BaseModel):
    """Returned by GET /api/v1/admin/output-equivalence: one page of output equivalences,
    newest first. `next_after_output_equivalence_idx` is the cursor for the next page
    (pass it back as `after_output_equivalence_idx`), or None on the last page."""

    output_equivalences: list[OutputEquivalence]
    count: int
    next_after_output_equivalence_idx: Annotated[int | None, Field(default=None, gt=0)] = None


class DefinitionSettingsEditKind(StrEnum):
    """`qiita.definition_settings_edit.edit_kind`. Twin of that column's TEXT + CHECK,
    not a Postgres ENUM."""

    # Written by `qiita.edit_definition_settings`, the system_admin's audited edit.
    ADMIN_EDIT = "admin_edit"
    # Written by the lock trigger for a mask adapter-hash re-key, with no actor.
    ADAPTER_HASH_REKEY = "adapter_hash_rekey"


class DefinitionSettingsRefusal(StrEnum):
    """The `reason` code of a 409 refusing an edit of a definition's settings that is
    not an output-equivalence rule's (those carry an `OutputEquivalenceRefusal`). Raised
    in `qiita_control_plane.definition_settings`."""

    # Another definition of the kind already has the edited params (its params_hash).
    PARAMS_TAKEN = "params_taken"
    # The edit changes no locked column.
    NO_CHANGE = "no_change"
    # Unfinished work tickets (pending, queued, processing, held) run under it.
    LIVE_WORK = "live_work"
    # A public handle not retired cites it: an exported_processing (`QP<n>`) row
    # naming it, or an exported_identifier (`QM<n>`) minted under it.
    PUBLISHED = "published"


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
    `\\x<hex>`. `repaired_output_equivalence_idx` names the output equivalences that do
    not join (pending, or a `NON_JOINING_VERDICTS` verdict) whose `change` and
    `derived_only` were recomputed from the new params."""

    definition_settings_edit_idx: int
    definition_kind: DefinitionKind
    definition_idx: int
    actor_principal_idx: int
    old_settings: dict[str, Any]
    new_settings: dict[str, Any]
    reason: str
    occurred_at: AwareDatetime
    repaired_output_equivalence_idx: list[int]
