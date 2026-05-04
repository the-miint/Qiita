"""Action-registry Pydantic models — YAML format and DB-row reconstruction.

Source of truth for an action definition is `workflows/<action_id>/<version>.yaml`.
The control-plane sync routine loads each YAML, validates it via
`ActionDefinition`, and upserts the YAML-authoritative columns into
`qiita.action`. Both control-plane and compute-orchestrator reconstruct
`ActionDefinition` from DB rows for runtime use; YAML parsing itself lives
only in the control plane.

The YAML entry shape for the `steps` list uses a singular `step:` or
`action:` key whose value is the entry's name:

    steps:
      - step: hash
        step_type: singleton
        container: qiita/reference-hash:1.0.0
        baseline_resources: {cpu: 4, mem_gb: 8, walltime: PT1H}
      - action: mint-features
        inputs: [hash.manifest]
        outputs: [feature_map.ndjson]

A `model_validator(mode="before")` rewrites that shape into a discriminated
union keyed on `kind` for the WorkflowStep / WorkflowAction Pydantic arms.
"""

from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from qiita_common.auth_constants import (
    MAX_NAME_LENGTH,
    MAX_VERSION_LENGTH,
    Scope,
    SystemRole,
)
from qiita_common.models import ScopeTargetKind, StepType


class Audience(BaseModel):
    """Who may invoke this action — answers "may invoke", not "may execute".

    `service=true` means service-account principals may invoke.
    `human_roles` is the set of SystemRole values whose humans may invoke;
    leaving it empty means no human can.

    Execution-side privileges (queue, account, priority) are a separate
    concern handled by the SLURM dispatch profile.
    """

    service: bool
    human_roles: list[SystemRole] = Field(default_factory=list)


class BaselineResources(BaseModel):
    """Per-step resource declaration. At submit time the orchestrator
    multiplies these by the originator's profile and clamps the result by
    the action ceiling.
    """

    cpu: Annotated[int, Field(gt=0)]
    mem_gb: Annotated[int, Field(gt=0)]
    walltime: timedelta
    gpu: Annotated[int, Field(ge=0)] = 0

    @field_validator("walltime")
    @classmethod
    def walltime_positive(cls, v: timedelta) -> timedelta:
        if v.total_seconds() <= 0:
            raise ValueError("walltime must be positive")
        return v


class ActionCeiling(BaselineResources):
    """Action-wide resource caps. Same shape as BaselineResources; the
    distinct type makes call-site intent obvious — `step.baseline_resources`
    is the per-step ask, `action.action_ceiling` is the action-wide hard cap.
    """


class WorkflowStep(BaseModel):
    """SLURM-dispatched containerized step.

    `step_type` ∈ {map, reduce, singleton}: map runs once per sample, reduce
    runs once over the union of map outputs, singleton runs once per workflow
    invocation.
    """

    kind: Literal["step"]
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    step_type: StepType
    container: str = Field(min_length=1, max_length=512)
    entrypoint: str | None = None
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    baseline_resources: BaselineResources


class WorkflowAction(BaseModel):
    """Control-plane primitive referenced by name from a workflow.

    Library primitives are not user-invokable; they execute in-process in
    the control plane during workflow orchestration. A user-invokable
    action's `scopes` list covers the primitives its workflow composes.
    """

    kind: Literal["action"]
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)


# Discriminated union — the model_validator on ActionDefinition rewrites
# `{step: <name>, ...}` / `{action: <name>, ...}` into the discriminator
# form before this union sees the entry.
WorkflowEntry = Annotated[
    WorkflowStep | WorkflowAction,
    Field(discriminator="kind"),
]


class ActionDefinition(BaseModel):
    """Top-level action definition. YAML is source-of-truth; the sync routine
    upserts the YAML-authoritative columns into qiita.action.

    `target_kind` constrains what scope_target.kind a work_ticket invoking
    this action may carry — the route handler 422s on mismatch.

    `scopes` is AND-composed at auth time (every scope must be present on
    the caller's token). String values are validated against
    `qiita_common.auth_constants.Scope` so a typo in YAML becomes a
    deploy-time error, not a runtime auth bypass.
    """

    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    target_kind: ScopeTargetKind
    description: str | None = None

    scopes: list[str] = Field(default_factory=list)
    audience: Audience

    # Per-action JSON Schema fragment. Validated against work_ticket.action_context
    # at submission; default `{}` means accept any object. This Pydantic model
    # does not validate the schema's well-formedness — that lives in the route
    # handler that consumes it (jsonschema lib).
    context_schema: dict[str, Any] = Field(default_factory=dict)

    steps: list[WorkflowEntry] = Field(min_length=1)

    action_ceiling: ActionCeiling

    @model_validator(mode="before")
    @classmethod
    def _normalize_step_entries(cls, data: Any) -> Any:
        """Rewrite `{step: <name>, ...}` and `{action: <name>, ...}` shorthand
        into the discriminator form `{kind: step|action, name: <name>, ...}`
        before the WorkflowEntry union dispatches. An entry that sets both
        keys is rejected; one that sets neither falls through to Pydantic's
        discriminator-missing error.
        """
        if not isinstance(data, dict):
            return data
        steps = data.get("steps")
        if not isinstance(steps, list):
            return data
        rewritten = []
        for entry in steps:
            if not isinstance(entry, dict):
                rewritten.append(entry)
                continue
            has_step = "step" in entry
            has_action = "action" in entry
            if has_step and has_action:
                raise ValueError(
                    "step entry must use exactly one of 'step:' or 'action:', not both"
                )
            if has_step:
                entry = {"kind": "step", "name": entry.pop("step"), **entry}
            elif has_action:
                entry = {"kind": "action", "name": entry.pop("action"), **entry}
            rewritten.append(entry)
        data = {**data, "steps": rewritten}
        return data

    @field_validator("scopes")
    @classmethod
    def _scopes_known_and_unique(cls, v: list[str]) -> list[str]:
        if len(v) != len(set(v)):
            raise ValueError("scopes must not contain duplicates")
        valid = {s.value for s in Scope}
        unknown = sorted(s for s in v if s not in valid)
        if unknown:
            raise ValueError(f"unknown scope(s): {unknown}. Valid scopes: {sorted(valid)}")
        return v
