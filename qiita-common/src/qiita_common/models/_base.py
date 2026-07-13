"""Shared base types cross-referenced by multiple model submodules.

Holds the genuinely shared foundation — the runtime-selection check, the
scope-target discriminated union, the two enums the step wire contract and the
work-ticket models both reference (ComputeTarget, StepStatus), and the PATCH
base model — so the domain submodules can import FROM here without forming an
import cycle. No submodule imports back from `models/__init__`.
"""

import re
from enum import StrEnum
from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# A `derived_inputs` key becomes an env var forwarded into the container, so it
# must look like one. Anchored: a stray `=` or space would corrupt the
# `--env K=V` apptainer argument it is interpolated into.
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def check_exactly_one_runtime(
    *,
    container: str | None,
    module: str | None,
    entrypoint: str | None,
    owner: str,
) -> None:
    """Shared runtime-selection check for WorkflowStep (YAML side) and
    StepSubmitRequest (wire side). Raises ValueError when the shape is wrong.
    Kept in one place so the rule can't drift between the two layers."""
    if (container is None) == (module is None):
        raise ValueError(f"{owner} must declare exactly one of 'container' or 'module'")
    if entrypoint is not None and container is None:
        raise ValueError("'entrypoint' requires 'container'")


def check_derived_inputs(
    derived_inputs: dict[str, str],
    *,
    container: str | None,
    owner: str,
) -> None:
    """Shared `derived_inputs` shape check for WorkflowStep (YAML side) and
    StepSubmitRequest (wire side). Raises ValueError when the shape is wrong.
    Kept beside check_exactly_one_runtime so the rule can't drift between the
    two layers.

    Each value is a path RELATIVE to the orchestrator's PATH_DERIVED. It is
    rejected here if absolute or if it escapes upward, so the joined path can
    never land outside the derived root — the control plane must not be able to
    name an arbitrary host path for the orchestrator to bind into a container.
    """
    if not derived_inputs:
        return
    if container is None:
        raise ValueError(
            f"{owner} declares 'derived_inputs' on a native step; native jobs read"
            " PATH_DERIVED from their own settings and need no bind"
        )
    for name, rel in derived_inputs.items():
        if not _ENV_NAME_RE.match(name):
            raise ValueError(
                f"{owner} derived_inputs key {name!r} is not a valid env var name"
                " (^[A-Z][A-Z0-9_]*$)"
            )
        if not rel:
            raise ValueError(f"{owner} derived_inputs[{name!r}] is empty")
        if rel.startswith("/"):
            raise ValueError(
                f"{owner} derived_inputs[{name!r}]={rel!r} must be relative to"
                " PATH_DERIVED, not an absolute host path"
            )
        if ".." in rel.split("/"):
            raise ValueError(
                f"{owner} derived_inputs[{name!r}]={rel!r} must not traverse above PATH_DERIVED"
            )


def _normalize_scope_target(v: dict[str, Any]) -> dict[str, Any]:
    """Validate a wire-side `scope_target` against the ScopeTarget
    discriminated union and normalize it to JSON shape (enum `kind` →
    plain string). Used by StepSubmitRequest's scope_target validator.
    ScopeTarget is defined later in this module; it resolves at call time,
    not definition time."""
    from pydantic import TypeAdapter

    return TypeAdapter(ScopeTarget).validate_python(v).model_dump(mode="json")


class ScopeTargetKind(StrEnum):
    """Closed set of work-ticket scope-target kinds. Mirrored DB-side by
    the qiita.scope_target_kind ENUM; both work_ticket.scope_target_kind
    and action.target_kind reference it."""

    STUDY_PREP = "study_prep"
    REFERENCE = "reference"
    PREP_SAMPLE = "prep_sample"
    SEQUENCED_POOL = "sequenced_pool"
    BLOCK = "block"


class StudyPrepScopeTarget(BaseModel):
    """Work ticket targets a (study, prep) tuple — used for sample-processing
    actions (e.g. deblur, woltka)."""

    kind: Literal[ScopeTargetKind.STUDY_PREP]
    study_idx: Annotated[int, Field(gt=0)]
    prep_idx: Annotated[int, Field(gt=0)]


class ReferenceScopeTarget(BaseModel):
    """Work ticket targets a single reference — used for reference-add and
    any future reference-mutation action."""

    kind: Literal[ScopeTargetKind.REFERENCE]
    reference_idx: Annotated[int, Field(gt=0)]


class PrepSampleScopeTarget(BaseModel):
    """Work ticket targets one prep_sample (the supertype) — used for
    actions that naturally operate on a single sample at a time (e.g.
    fastq-to-parquet, one FASTQ → one Parquet). Distinct
    from a study_prep-scoped ticket that fans out per sample inside a
    map step: this form is the singleton path, one ticket per sample.

    Kind-specific actions (e.g., fastq-to-parquet only makes sense for
    processing_kind='sequenced') express their constraint through
    `qiita.action.target_processing_kinds`, checked at submission. The
    scope target itself stays kind-agnostic so cross-kind actions
    (future admin/audit operations) can use the same shape."""

    kind: Literal[ScopeTargetKind.PREP_SAMPLE]
    prep_sample_idx: Annotated[int, Field(gt=0)]


class SequencedPoolScopeTarget(BaseModel):
    """Work ticket targets one sequenced_pool (one (run, lane) pair) —
    used for the bcl-convert workflow that demultiplexes the pool's BCL
    run folder into per-biosample FASTQs.

    Carries both the pool idx and its parent run idx. The denormalization
    lets the SA-only preflight read route stay nested under sequencing-run
    and lets the orchestrator's `SCOPE_SCALARS_BY_KIND` flow both scalars
    into the prep step's `Inputs` without an extra DB lookup."""

    kind: Literal[ScopeTargetKind.SEQUENCED_POOL]
    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    sequencing_run_idx: Annotated[int, Field(gt=0)]


class BlockScopeTarget(BaseModel):
    """Work ticket targets one block — a fixed ~10M-read compute slice drawn
    from prep_samples that share one filtering identity (mask_idx), used by the
    bulk-block read-mask workflow.

    Carries only the block idx; the filtering identity (mask_idx) rides on the
    work_ticket's own mask_idx column, and the block↔sample cover-map lives in
    qiita.block_member. The block is minted before the ticket (the ticket's
    scope target references block_idx), so this arm never needs a companion idx
    the way SequencedPoolScopeTarget does."""

    kind: Literal[ScopeTargetKind.BLOCK]
    block_idx: Annotated[int, Field(gt=0)]


# Discriminated union — Pydantic and OpenAPI dispatch on the `kind` field.
# DB-side, the same shape is encoded as a tagged union of typed columns
# (`scope_target_kind` plus the subset-relevant `study_idx` / `prep_idx` /
# `reference_idx` / `prep_sample_idx` / `sequenced_pool_idx` / `block_idx`)
# guarded by a CHECK constraint; the `kind` here is the discriminator that
# maps to that column.
ScopeTarget = Annotated[
    StudyPrepScopeTarget
    | ReferenceScopeTarget
    | PrepSampleScopeTarget
    | SequencedPoolScopeTarget
    | BlockScopeTarget,
    Field(discriminator="kind"),
]


class ComputeTarget(StrEnum):
    """Where one workflow step entry actually executes.

    `slurm` — a real SLURM job (carries a `slurm_job_id`). `local` — a
    native module run in-process on the orchestrator (LocalBackend; dev /
    test). `control_plane` — an `action:` entry run in-process on the
    control plane (no backend hop, no job id). Only `slurm` is "on
    compute"; the other two are in-process. Mirrored DB-side by the
    `compute_target` TEXT+CHECK column on `qiita.work_ticket_step` — a
    plain TEXT/CHECK, not a Postgres ENUM (see CLAUDE.md "Enum parity");
    keep both sides in sync by hand.
    """

    SLURM = "slurm"
    LOCAL = "local"
    CONTROL_PLANE = "control_plane"


class StepStatus(StrEnum):
    """Live status of a submitted step, as reported by a backend's
    `status_step`. Coarser than SLURM's own state vocabulary — the runner
    and the ticket-summary read only care about queued-vs-running-vs-done.

    `pending` = accepted/queued but not yet on a node; `running` = actively
    executing; `completed` / `failed` are terminal. `completed` means the
    job exited cleanly — the caller still runs `result_step` to verify the
    output contract, which can itself fail.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class PatchRequestModel(BaseModel):
    """Base class for every PATCH-body Pydantic model in the API.

    Pins extra="forbid" so requests that name immutable or retirement-
    managed columns trip the model-level rejection rather than reaching
    the repo, enforces the "at least one editable field" rule that
    every PATCH surface shares, and enforces "explicit null is not
    valid input on a NOT NULL column" via the NOT_NULL_FIELDS hook.
    Derived classes inherit both validators automatically; each subtype
    declares its own column-typed Optional fields, and the ones whose
    column is NOT NULL list those field names in NOT_NULL_FIELDS. The
    route layer distinguishes "absent" (do not write) from "explicit
    null" (set the column to NULL) by inspecting `model_fields_set`.
    """

    model_config = ConfigDict(extra="forbid")

    # Field names whose backing column is NOT NULL. Subclasses override
    # to declare their own; the empty default means "every field is
    # nullable" (no validator-side rejection).
    NOT_NULL_FIELDS: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="after")
    def at_least_one_field(self):
        # Empty bodies are rejected here so every PATCH route gets the
        # 422 shape for free without per-route special-casing.
        if not self.model_fields_set:
            raise ValueError("at least one editable field is required")
        return self

    @model_validator(mode="after")
    def reject_explicit_null_on_not_null_fields(self):
        # Every field in NOT_NULL_FIELDS maps to a NOT NULL column;
        # explicit null is invalid input even though the field is
        # typed Optional for the "absent vs null" distinguishing
        # pattern shared with the nullable fields.
        for field_name in self.NOT_NULL_FIELDS:
            if field_name in self.model_fields_set and getattr(self, field_name) is None:
                raise ValueError(f"{field_name} may not be null")
        return self
