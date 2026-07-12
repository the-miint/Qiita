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

from __future__ import annotations

from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from qiita_common.auth_constants import (
    MAX_NAME_LENGTH,
    MAX_VERSION_LENGTH,
    Scope,
    SystemRole,
)
from qiita_common.models import (
    ProcessingKind,
    ScopeTargetKind,
    StepType,
    check_derived_inputs,
    check_exactly_one_runtime,
)

# Native job modules must live under qiita_compute_orchestrator.jobs.
# Defined here so every layer that checks the prefix (CP sync, CO boot
# scan, CO /step/submit route, the framework dispatcher) imports a single
# value rather than re-typing the string. The wire validator on
# StepSubmitRequest deliberately stays shape-only — the prefix check
# belongs at the layers that actually import / dispatch.
NATIVE_MODULE_PREFIX = "qiita_compute_orchestrator.jobs."


# action_context property keys that name a fastq file path. The
# fastq-to-parquet action declares them in its context_schema (see
# workflows/fastq-to-parquet/1.0.0.yaml) and the orchestrator's
# fastq_to_parquet job binds them as Inputs fields. Defined here, beside
# the action contract, so the control plane's work_ticket submit gate
# tests action_context against one canonical set instead of re-typing
# the strings — a key renamed in the YAML then lights up its importers
# rather than silently drifting. The gate enforces that each such path's
# basename is prefixed by the prep_sample's sequenced_pool_item_id (see
# docs/runbooks/user-cli-quickstart.md).
FASTQ_PATH_CONTEXT_KEYS: tuple[str, str] = ("fastq_path", "reverse_fastq_path")


# The per-sample read-mask action's bare id (its YAML lives at
# workflows/read-mask/<version>.yaml). A sample's reads are stored once by the
# bcl-convert workflow's ingest_reads step; processing a sample then means
# creating a read mask over those reads. Defined here, beside the action
# contract, so the submitter that mints these tickets (the qiita-user
# submit-host-filter-pool gesture) and any reader that keys off them (e.g. the
# pool completion rollup query) import one canonical value instead of re-typing
# the literal — a rename then lights up its importers rather than silently
# zeroing a rollup. Deliberately the bare id, NOT a (id, version) pair: the
# completion rollup is version-agnostic ("did this sample get masked?"
# regardless of version), while the submitter pins its own version separately.
READ_MASK_ACTION_ID = "read-mask"

# The pool-scoped demux action's bare id (its YAML lives at
# workflows/bcl-convert/<version>.yaml). One sequenced_pool-scoped work ticket
# per pool demultiplexes the run and stores each sample's reads once. Defined
# here so readers that key off it (e.g. the pool completion rollup's demux-state
# query) import one canonical value instead of re-typing the literal. Bare id,
# NOT a (id, version) pair: "did this pool's demux finish?" holds regardless of
# which bcl-convert version produced it.
BCL_CONVERT_ACTION_ID = "bcl-convert"


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


class FlatBaselineResources(BaseModel):
    """Flat resource declaration — cpu/mem_gb/walltime/gpu, all required.

    Used as the value type in `BaselineResources.profiles` (one profile per
    instrument family, picked at dispatch by the runner's A4 resolution
    branch) and as the shape `ActionCeiling` carries (the ceiling is always
    a single upper bound, regardless of which baseline-resource population
    the step uses).
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


class BaselineResources(BaseModel):
    """Per-step resource declaration. Two valid populations, never both:

    * **Flat** (every existing workflow): declare ``cpu``, ``mem_gb``,
      ``walltime`` (and optionally ``gpu``) directly. The runner uses
      these values verbatim at dispatch.
    * **Lookup** (introduced for bcl-convert): declare ``from_step_output``
      (the name of an upstream step's output file) and ``profiles``
      (a map from the file's stripped contents to a flat
      ``FlatBaselineResources``). At dispatch the runner reads the file,
      looks up the matching profile, and uses that as the resolved baseline.

    The ``@model_validator`` enforces exactly-one-population. The runner
    clamps the resolved values against the action ceiling.
    """

    # Flat path. gpu defaults to 0 (no GPU) for back-compat with the
    # original BaselineResources shape — every existing YAML omits `gpu:`
    # and expects 0. The exactly-one-population check only inspects
    # cpu/mem_gb/walltime, so gpu=0 doesn't cause the lookup case to
    # spuriously trigger the "mixed populations" error.
    cpu: Annotated[int | None, Field(default=None, gt=0)] = None
    mem_gb: Annotated[int | None, Field(default=None, gt=0)] = None
    walltime: timedelta | None = None
    gpu: Annotated[int, Field(default=0, ge=0)] = 0
    # Lookup path
    from_step_output: str | None = Field(default=None, min_length=1)
    profiles: dict[str, FlatBaselineResources] | None = None

    @field_validator("walltime")
    @classmethod
    def walltime_positive(cls, v: timedelta | None) -> timedelta | None:
        if v is not None and v.total_seconds() <= 0:
            raise ValueError("walltime must be positive")
        return v

    @model_validator(mode="after")
    def _exactly_one_population(self) -> BaselineResources:
        flat_set = self.cpu is not None or self.mem_gb is not None or self.walltime is not None
        lookup_set = self.from_step_output is not None or self.profiles is not None
        if flat_set and lookup_set:
            raise ValueError(
                "baseline_resources: cannot mix flat fields (cpu/mem_gb/walltime/gpu)"
                " with lookup fields (from_step_output/profiles)"
            )
        if not flat_set and not lookup_set:
            raise ValueError(
                "baseline_resources: must populate either flat fields"
                " (cpu/mem_gb/walltime) or lookup fields (from_step_output + profiles)"
            )
        if flat_set and (self.cpu is None or self.mem_gb is None or self.walltime is None):
            raise ValueError("baseline_resources: flat shape requires cpu, mem_gb, and walltime")
        if lookup_set and (self.from_step_output is None or self.profiles is None):
            raise ValueError(
                "baseline_resources: lookup shape requires both from_step_output and profiles"
            )
        if lookup_set and not self.profiles:
            raise ValueError("baseline_resources: profiles must be non-empty")
        return self


class ActionCeiling(FlatBaselineResources):
    """Action-wide resource caps. Always flat — a single upper bound the
    runner clamps the resolved per-step values against, regardless of
    whether the step's `baseline_resources` declares a flat or lookup
    population.
    """


class WorkflowStep(BaseModel):
    """Workflow step. Runs in one of two runtimes, selected by which
    field is set — exactly one of `container` or `module` must be
    populated. Whether a particular backend implements each runtime is
    a backend concern; the schema describes what's expressible.

    - `container` form: the step's image is executed via apptainer (or
      in-process via LocalBackend in dev/test). `entrypoint` may override
      the container's default ENTRYPOINT.
    - `module` form (native step): the named Python module lives under
      `qiita_compute_orchestrator.jobs.*` and runs in the orchestrator's
      Python environment — either in-process via LocalBackend or under
      SLURM via `srun python -m qiita_compute_orchestrator.jobs --job <name>`.
      Use this only when the job's dependencies are already in
      `qiita-compute-orchestrator`'s `pyproject.toml`; anything heavier
      (extra bioinformatics deps, system packages) belongs in a container.

    `step_type` ∈ {map, reduce, singleton}: map runs once per sample, reduce
    runs once over the union of map outputs, singleton runs once per workflow
    invocation.

    `target_status` (optional) is the status the runner ensures the work
    ticket's scope_target carries before this entry runs. Status values are
    target-kind-specific strings (e.g. 'hashing' / 'minting' / 'loading' for
    a reference); the runner PATCHes only when a transition is needed.
    """

    kind: Literal["step"]
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    step_type: StepType
    container: str | None = Field(default=None, min_length=1, max_length=512)
    module: str | None = Field(default=None, min_length=1, max_length=512)
    entrypoint: str | None = None
    inputs: list[str] = Field(default_factory=list)
    # Names that flow through from action_context if present, but do not
    # error when missing. Used for inputs whose presence is workflow-time
    # data (e.g. a taxonomy file accompanies some references but not all).
    optional_inputs: list[str] = Field(default_factory=list)
    # Scalar build parameters pulled from action_context, keyed
    # action_context_key -> native `Inputs` field name. Unlike `inputs` /
    # `optional_inputs` (which the runner resolves to host Paths), these flow
    # through verbatim — the runner merges them into the step's inputs
    # *without* Path-coercion, and the job's Pydantic `Inputs` model re-coerces
    # the string to its declared type (e.g. {"rype_w": "w"} sets the rype
    # build's `w: int`). The key/value split lets a namespaced context key
    # (`rype_w`) map onto the model's bare field (`w`) so two steps can expose
    # same-named knobs without colliding in action_context. Native steps only —
    # container inputs are bind-mount paths, never scalars.
    params: dict[str, str] = Field(default_factory=dict)
    # Read-only host artifacts under PATH_DERIVED that a container step needs
    # at run time, keyed env_var_name -> path relative to PATH_DERIVED (e.g.
    # {"QIITA_CHECKM_DB": "checkm_data"}). The orchestrator joins each value
    # against its own PATH_DERIVED, bind-mounts the result into the container,
    # and forwards the absolute path under the env var name. This is the
    # container-side mirror of `params` (native-only scalars): a container gets
    # only what is explicitly bound + forwarded (apptainer runs `--containall`,
    # so the host env does NOT cross the boundary), and PATH_DERIVED is the
    # orchestrator's to resolve — the control plane never learns compute-node
    # paths. Use it for operator-provisioned reference data that is too large to
    # bake into the SIF (CheckM's DB); ticket-scoped data flows through `inputs`.
    # Container steps only — a native step reads PATH_DERIVED from its own
    # settings and needs no bind.
    derived_inputs: dict[str, str] = Field(default_factory=dict)
    outputs: list[str] = Field(default_factory=list)
    baseline_resources: BaselineResources
    target_status: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)
    # Optional conditional gate: the name of an action_context boolean key.
    # The runner runs this entry only when `bool(action_context.get(when,
    # True))` — i.e. default-ON, skipped only when the key is present and
    # falsy. An omitted `when` always runs. Lets one workflow opt a step (and
    # its downstream register-* action, which carries the same `when`) out of a
    # run from the submission's action_context.
    when: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)

    @model_validator(mode="after")
    def _exactly_one_runtime(self) -> WorkflowStep:
        # Shape-only validator. Prefix validation on `module` is enforced
        # separately (see the NATIVE_MODULE_PREFIX comment above) — keeps
        # the schema decoupled from the orchestrator package path.
        check_exactly_one_runtime(
            container=self.container,
            module=self.module,
            entrypoint=self.entrypoint,
            owner="WorkflowStep",
        )
        check_derived_inputs(
            self.derived_inputs,
            container=self.container,
            owner="WorkflowStep",
        )
        return self


class WorkflowAction(BaseModel):
    """Control-plane primitive referenced by name from a workflow.

    Library primitives are not user-invokable; they execute in-process in
    the control plane during workflow orchestration. A user-invokable
    action's `scopes` list covers the primitives its workflow composes.

    `target_status` mirrors the same field on WorkflowStep — the runner
    drives status transitions, primitives still defend their pre-conditions
    inside the dispatch handler.
    """

    kind: Literal["action"]
    name: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    inputs: list[str] = Field(default_factory=list)
    outputs: list[str] = Field(default_factory=list)
    target_status: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)
    # Same conditional gate as WorkflowStep.when (default-ON; skipped only when
    # the named action_context key is present and falsy). A gated build step
    # and the register-* action that consumes its output share the same `when`
    # so they are skipped together.
    when: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)


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
    # When target_kind = ScopeTargetKind.PREP_SAMPLE, this list declares
    # which prep_sample processing_kind values the action accepts. The
    # submit route reads the prep_sample's actual processing_kind and
    # 422s on mismatch. Empty (default) = "any kind" (cross-kind admin
    # actions). For non-prep_sample target_kinds, the validator below
    # rejects a nonempty list — the DB CHECK
    # `action_processing_kinds_only_for_prep_sample` enforces the same
    # rule at sync time, but catching it on the Pydantic side surfaces
    # a clean error before any DB round-trip.
    target_processing_kinds: list[ProcessingKind] = Field(default_factory=list)
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

    # Workflow-level status terminals. The runner PATCHes the work ticket's
    # scope_target to `success_status` after every entry has succeeded, and
    # best-effort PATCHes to `failure_status` if any entry raises. Both are
    # optional — a workflow that doesn't track a resource lifecycle (e.g.
    # one that targets study_prep with no per-prep status column) leaves
    # them unset and the runner skips the terminal PATCHes.
    success_status: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)
    failure_status: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)

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

    @model_validator(mode="after")
    def _target_processing_kinds_only_for_prep_sample(self) -> ActionDefinition:
        # Mirrors the DB CHECK action_processing_kinds_only_for_prep_sample
        # at the Pydantic boundary so a YAML mistake (declaring
        # target_processing_kinds against a non-prep_sample target_kind)
        # surfaces at load time, not at sync time.
        if self.target_kind is not ScopeTargetKind.PREP_SAMPLE and self.target_processing_kinds:
            raise ValueError(
                "target_processing_kinds is only meaningful when "
                f"target_kind = 'prep_sample'; got target_kind = "
                f"{self.target_kind.value!r} with target_processing_kinds = "
                f"{[k.value for k in self.target_processing_kinds]!r}"
            )
        if len(self.target_processing_kinds) != len(set(self.target_processing_kinds)):
            raise ValueError("target_processing_kinds must not contain duplicates")
        return self

    @model_validator(mode="after")
    def _step_entry_names_unique(self) -> ActionDefinition:
        # SLURM keys a step's job_name and per-attempt workspace on
        # (work_ticket_idx, entry.name, attempt), and `_find_job_by_name` adopts
        # an in-flight job by that name — all of which assume a `step:` name is
        # unique per ticket. Two `step:` entries sharing a name would collide
        # silently (the wrong job could be adopted), so reject it at load time.
        # `action:` entries run in-process keyed on the step INDEX, not the name,
        # so they may repeat (e.g. two `register-index` actions in one workflow).
        step_names = [e.name for e in self.steps if isinstance(e, WorkflowStep)]
        dupes = sorted({n for n in step_names if step_names.count(n) > 1})
        if dupes:
            raise ValueError(
                f"duplicate step name(s) {dupes}: `step:` entry names must be unique "
                "within an action — SLURM job naming and job adoption key on the "
                "name. (`action:` entries run in-process and may repeat.)"
            )
        return self
