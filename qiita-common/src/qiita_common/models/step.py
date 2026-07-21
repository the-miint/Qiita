"""Decoupled step wire contract (submit / plan / status / result / find-by-name)
plus the DoGet ticket request/response the data plane's Flight surface uses."""

from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from qiita_common.auth_constants import MAX_TABLE_NAME_LENGTH
from qiita_common.models._base import (
    ComputeTarget,
    StepStatus,
    _normalize_scope_target,
    check_derived_inputs,
    check_exactly_one_runtime,
)


class StepBaselineResources(BaseModel):
    """Resource ask for one workflow step. Mirrors qiita_common.actions.
    BaselineResources but lives here so the over-the-wire StepSubmitRequest
    can include it without a circular import (actions.py imports models)."""

    cpu: Annotated[int, Field(gt=0)]
    mem_gb: Annotated[int, Field(gt=0)]
    walltime_seconds: Annotated[int, Field(gt=0)]
    gpu: Annotated[int, Field(ge=0)] = 0


# ---------------------------------------------------------------------------
# Decoupled step wire contract: submit / status / result.
#
# The control-plane runner drives these three so it never holds a connection
# open for the duration of a SLURM job: submit returns immediately with a
# handle, the runner polls status until terminal, then asks for the result.
# The orchestrator is stateless across the three calls, so the handle (the
# serialized `StepHandle`) carries everything status/result need and the CP
# persists those fields to re-attach after a restart.
# ---------------------------------------------------------------------------


class StepSubmitRequest(BaseModel):
    """Body for POST /api/v1/step/submit, issued by the control-plane runner
    for every workflow `step:` entry. The orchestrator dispatches to its
    configured ComputeBackend's `submit_step` and returns a handle without
    blocking on completion.

    Runtime selection (`container` vs `module`) follows the same rules as
    `qiita_common.actions.WorkflowStep` — exactly one must be set, enforced by
    the shared `check_exactly_one_runtime` helper. `work_ticket_idx` + `attempt`
    stamp the deterministic SLURM job name `qiita-wt{idx}-{step}-a{attempt}`, so
    a job submitted but not yet recorded can be re-found by name. `scope_target`
    carries the work ticket's discriminated-union scope target (matches
    `qiita_common.models.ScopeTarget`); the field validator below runs the same
    discriminated-union validation as `WorkTicket.scope_target` AND normalizes
    the dict to JSON shape (`mode="json"`), so `scope_target["kind"]` is always
    a plain string downstream. Paths are absolute and live on the workspace
    shared between control plane and orchestrator."""

    step_name: str = Field(min_length=1)
    inputs: dict[str, str] = Field(default_factory=dict)
    workspace: str = Field(min_length=1)
    scope_target: dict[str, Any]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    attempt: Annotated[int, Field(ge=0)] = 0
    container: str | None = Field(default=None, min_length=1, max_length=512)
    module: str | None = Field(default=None, min_length=1, max_length=512)
    entrypoint: str | None = None
    baseline_resources: StepBaselineResources | None = None
    # Mirrors WorkflowStep.derived_inputs (see there for the contract).
    derived_inputs: dict[str, str] = Field(default_factory=dict)

    @field_validator("scope_target", mode="after")
    @classmethod
    def _validate_scope_target(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _normalize_scope_target(v)

    @model_validator(mode="after")
    def _exactly_one_runtime(self) -> StepSubmitRequest:
        check_exactly_one_runtime(
            container=self.container,
            module=self.module,
            entrypoint=self.entrypoint,
            owner="StepSubmitRequest",
        )
        check_derived_inputs(
            self.derived_inputs,
            container=self.container,
            owner="StepSubmitRequest",
        )
        return self


class StepPlanRequest(BaseModel):
    """Body for POST /api/v1/step/plan, issued by the control-plane runner
    ONCE per native `step:` entry before its retry loop. The orchestrator
    imports the module, validates these inputs against its `Inputs`, and runs
    the module's optional `plan(inputs)` to return a resource-sizing hint.

    Native (`module`) steps only — a container step has no `plan()`, so the CP
    never issues this for one. The fields mirror the submit request's native
    subset: `inputs` are the same name→(path|scalar) strings, `scope_target` +
    `work_ticket_idx` let the orchestrator run the same `flatten_native_inputs`
    merge submit does, so `plan()` sees identical `Inputs`. No `workspace` /
    `attempt`: `plan()` reads its declared input paths and is attempt-agnostic
    (called once, before any attempt)."""

    step_name: str = Field(min_length=1)
    inputs: dict[str, str] = Field(default_factory=dict)
    scope_target: dict[str, Any]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    module: str = Field(min_length=1, max_length=512)

    @field_validator("scope_target", mode="after")
    @classmethod
    def _validate_scope_target(cls, v: dict[str, Any]) -> dict[str, Any]:
        return _normalize_scope_target(v)


class StepPlanResponse(BaseModel):
    """Returned by POST /api/v1/step/plan — a job's optional resource hint.

    Every field is optional: a field left None means "no opinion, use the
    workflow baseline" for that axis. The control plane composes a non-None
    value into resource resolution by LOWERING the step below its YAML baseline
    (down-sizing); escalation remains the only up-sizing path. An empty
    response (all None) is the no-op a job with no `plan()`, or a `plan()` that
    declined to size, produces — the CP then uses the baseline unchanged.

    Note `cpu` has no escalation backstop (only `mem_gb` grows after OOM and
    `walltime_seconds` after TIMEOUT), so a down-sized `cpu` is never raised
    back on retry — recovering only indirectly via walltime escalation absorbing
    the slowness, up to the walltime ceiling. See `JobResourcePlan` (the CO-side
    twin) for the full rationale."""

    cpu: Annotated[int, Field(gt=0)] | None = None
    mem_gb: Annotated[int, Field(gt=0)] | None = None
    walltime_seconds: Annotated[int, Field(gt=0)] | None = None


class StepHandleWire(BaseModel):
    """Serialized `StepHandle` — POST /step/submit returns one, and POST
    /step/status / /step/result take one back. Paths are strings on the
    wire.

    `terminal_outputs` is the "synchronous backend already finished at
    submit time" sentinel: non-None means the step completed during submit
    (LocalBackend runs the module in-process) and the dict holds its
    outputs — the caller skips polling and uses it directly. For SLURM it
    is None and the caller polls status. **Invariant: non-None implies
    non-empty** — the runner keys off `is not None`, so an empty-but-set
    dict would falsely signal completion."""

    compute_target: ComputeTarget
    step_name: str
    slurm_job_id: int | None = None
    job_name: str | None = None
    output_path: str | None = None
    logs_path: str | None = None
    terminal_outputs: dict[str, str] | None = None


class StepStatusWire(BaseModel):
    """Serialized `StepStatusInfo` — returned by POST /step/status and fed
    back into POST /step/result so the orchestrator (stateless) can finalize
    a terminal step without re-reading slurmrestd."""

    status: StepStatus
    raw_state: str | None = None
    exit_code: int | None = None
    reason: str | None = None


class StepStatusRequest(BaseModel):
    """Body for POST /api/v1/step/status."""

    handle: StepHandleWire


class StepResultRequest(BaseModel):
    """Body for POST /api/v1/step/result."""

    handle: StepHandleWire
    status: StepStatusWire


class StepResultResponse(BaseModel):
    """Returned by POST /api/v1/step/result — the backend's name → path
    output map, matching the YAML's declared step `outputs:`."""

    outputs: dict[str, str]


class StepFindByNameRequest(BaseModel):
    """Body for POST /api/v1/step/find-by-name.

    `job_name` is the deterministic SLURM job name
    `qiita-wt{idx}-{step}-a{attempt}`. The control-plane runner queries this
    during restart recovery to adopt a job it submitted but whose id it never
    persisted (the write-ahead `submitting`-without-id gap) — closing the
    duplicate-job window without re-submitting."""

    job_name: str = Field(min_length=1, max_length=512)


class FoundJobWire(BaseModel):
    """One live SLURM job matched by find-by-name: its id and a status
    snapshot (reusing StepStatusWire). The control plane adopts a found job
    by reconstructing a StepHandle from `slurm_job_id` (workspace paths are
    deterministic from the per-attempt workspace)."""

    slurm_job_id: int
    job_name: str
    status: StepStatusWire


class StepFindByNameResponse(BaseModel):
    """Returned by POST /api/v1/step/find-by-name — the live jobs whose name
    matched. Empty when none match: slurmrestd has purged the job, or the
    backend is in-process (LocalBackend never submits to SLURM)."""

    jobs: list[FoundJobWire]


class StepCancelRequest(BaseModel):
    """Body for POST /api/v1/step/cancel. `work_ticket_idx` selects EVERY live
    SLURM job of the ticket (all attempts) by the deterministic name prefix
    `qiita-wt{idx}-`. The CP calls this only AFTER flipping the ticket terminal, so
    no new attempt can spawn between the find and the scancel."""

    work_ticket_idx: int = Field(gt=0)


class StepCancelResponse(BaseModel):
    """Returned by POST /api/v1/step/cancel — the SLURM job ids actually cancelled
    (empty when none were live: already finished/purged, or an in-process backend).
    Idempotent."""

    cancelled_job_ids: list[int]


# Upper bound on a feature_idx-scoped DoGet ticket's subset list. A reference
# shard is ~hundreds/thousands of features; the cap guards ticket/query size
# (the list rides the signed ticket payload and becomes a `feature_idx IN (...)`
# on the data plane) without constraining any realistic shard roster.
_MAX_DOGET_FEATURE_IDX = 100_000


class DoGetTicketRequest(BaseModel):
    table: str = Field(min_length=1, max_length=MAX_TABLE_NAME_LENGTH)
    # Optional feature_idx subset. Omitted ⇒ a whole-reference ticket
    # (filter={"reference_idx":[idx]}), byte-identical to the historical shape.
    # Present ⇒ the ticket additionally scopes to these features
    # (filter gains "feature_idx":[...]) so a shard builder streams only its
    # own roster's sequences from reference_sequences / reference_sequence_chunks.
    # `min_length=1` rejects an explicit empty list (422) rather than silently
    # widening it to the whole reference — an empty roster is a caller bug we
    # surface loudly, and it also keeps an empty value list from reaching
    # sign_ticket (which rejects one). Whole-reference is expressed by *omitting*
    # the field, never by an empty list.
    feature_idx: list[Annotated[int, Field(gt=0)]] | None = Field(
        default=None, min_length=1, max_length=_MAX_DOGET_FEATURE_IDX
    )
    model_config = ConfigDict(extra="forbid")


class DoGetTicketResponse(BaseModel):
    ticket: str  # base64-encoded signed ticket bytes


class AlignmentDoGetTicketRequest(BaseModel):
    """Body for POST /api/v1/alignment/ticket/doget.

    Signs a Flight DoGet ticket scoped to a single alignment run + its explicit
    prep_sample_idx cohort on the data plane's ``alignment`` table, for the
    feature-table (OGU) compute job. The body carries only ``work_ticket_idx``;
    the route reads ``alignment_idx`` and the ``prep_sample_idx`` cohort from
    that ticket's ``action_context`` (set at plan time), so the potentially large
    sample list never rides the request body or a ``params:`` scalar.
    """

    model_config = ConfigDict(extra="forbid")

    work_ticket_idx: Annotated[int, Field(gt=0)]
