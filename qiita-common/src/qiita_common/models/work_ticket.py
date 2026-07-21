"""Work tickets / actions.

A WorkTicket is the control-plane's record of an action invocation: who
requested it, which resource it targets, what action-specific context it
carries, and what lifecycle state it's in. The orchestrator pulls tickets
off the queue, dispatches the action's step pipeline (one or more `step`
entries plus zero or more control-plane `action` entries), and reports
completion back via state transitions.

`originator_principal_idx` is the submitter; resource profile and SLURM
priority resolve from the originator, not the executor.
"""

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from qiita_common.auth_constants import MAX_NAME_LENGTH, MAX_VERSION_LENGTH
from qiita_common.models._base import ComputeTarget, ScopeTarget


class StepType(StrEnum):
    """Workflow step types.

    `map` runs per-sample (N independent jobs across N samples).
    `reduce` runs once over the union of map outputs.
    `singleton` runs once per workflow invocation — used for system-internal
    one-shots like reference loading.

    `action` (control-plane Postgres-transaction primitive) is *not* a step
    type; it appears as a peer entry in workflow YAML and runs in-process
    in the control plane.
    """

    MAP = "map"
    REDUCE = "reduce"
    SINGLETON = "singleton"


class ProcessingKind(StrEnum):
    """Closed set of downstream-measurement specializations a prep_sample
    may flow into. Mirrors DB-side qiita.processing_kind, defined in
    migrations/20260501000011_prep_sample.sql. Today only 'sequenced'
    exists; future values (e.g., 'mass_specd') would land here as the
    DB ENUM gains them. Used by `qiita.action.target_processing_kinds`
    to declare which kinds an action accepts (kind-specific actions
    list one value; cross-kind admin actions leave the list empty).

    When extending the enum: each workflow YAML's `target_processing_kinds:`
    is an explicit allowlist. New kinds do NOT auto-enroll into existing
    workflows — the submission check (qiita_control_plane/routes/work_ticket.py)
    rejects any prep_sample whose kind is not in the action's list. Adding
    a new kind means landing the DB enum value, the subtype table (see
    qiita-control-plane/tests/test_prep_sample_subtype_invariants.py for
    the structural guardrail), and any new kind-specific workflows; it
    does not require auditing existing YAMLs unless you want the new kind
    to flow through them."""

    SEQUENCED = "sequenced"


class WorkTicketState(StrEnum):
    """Work-ticket lifecycle. Mirrored DB-side by qiita.work_ticket_state.

    Submission gates: PENDING / QUEUED / PROCESSING block resubmission of
    the same `(scope_target, action_id, action_version)` triple entirely.
    COMPLETED / NO_DATA / FAILED are the three terminal states, with
    different resubmission semantics: COMPLETED is DELETE-gated (a result
    exists, so the prior result must be deleted before a fresh submit);
    FAILED is restarted in place via /run (operator-driven recovery);
    NO_DATA mints no result, so it is freely resubmittable (only an
    in-place /run redrive is refused).

    NO_DATA is the terminal outcome for a step that legitimately produced
    no data — an empty FASTQ well (a blank, a no-template control, or a
    failed-yield well). It is distinct from FAILED: a no_data ticket
    carries NULL failure_* columns and is tallied in its own pool-
    completion bucket so a plate full of empty wells can still reach a
    "done" signal rather than being stuck behind permanent failures.

    CANCELLED is the terminal outcome for an OPERATOR-stopped ticket (the
    `qiita-admin ticket cancel` path): the CP flips it terminal so the poll
    loop aborts and no new attempt is submitted, then reaps its SLURM job(s).
    It carries NULL failure_* columns — distinct from FAILED so a deliberate
    stop is legible (in `ticket list`, the pool rollups, the notify digest)
    rather than masquerading as a genuine failure. Like FAILED it is redrivable
    in place via /run once the blocker is fixed.

    Mirrored DB-side by qiita.work_ticket_state; the two value sets are kept in
    lockstep by tests (test_enum_parity + test_work_ticket_state_parity) — change
    both in the same PR.
    """

    PENDING = "pending"
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    NO_DATA = "no_data"
    FAILED = "failed"
    CANCELLED = "cancelled"


# The terminal/non-terminal split of WorkTicketState, defined once and imported
# everywhere. A ticket is terminal once it has an OUTCOME: it succeeded
# (COMPLETED), it legitimately produced nothing (NO_DATA), or it failed (FAILED).
#
# Only the terminal side is named; the non-terminal side is DERIVED as its
# complement. That direction is deliberate and load-bearing:
#
#   * The two can never disagree, and a new state added to the enum lands in
#     exactly one of them by construction rather than by someone remembering.
#   * Unknown-state-defaults-to-non-terminal is the fail-SAFE direction for every
#     consumer: a new state blocks deletes, keeps a watch loop polling, doesn't
#     abort a running workflow, and isn't emailed as an outcome.
#
# Both are tuples in the enum's own lifecycle order, so a caller can bind them
# straight to a `qiita.work_ticket_state[]` array or render them to a user
# without re-shaping. NOTE: the DB does NOT inherit the derivation — the
# `work_ticket_one_in_flight_per_*` partial indexes spell the non-terminal set out
# in SQL, and a new enum value must be added to each by migration. A parity test
# (test_work_ticket_state_parity.py) fails loudly until that happens.
TERMINAL_WORK_TICKET_STATES: tuple[str, ...] = (
    WorkTicketState.COMPLETED.value,
    WorkTicketState.NO_DATA.value,
    WorkTicketState.FAILED.value,
    WorkTicketState.CANCELLED.value,
)

NON_TERMINAL_WORK_TICKET_STATES: tuple[str, ...] = tuple(
    state.value for state in WorkTicketState if state.value not in TERMINAL_WORK_TICKET_STATES
)


class FailureType(StrEnum):
    """Discriminates retriable from permanent work-ticket failures.
    Mirrored DB-side by qiita.failure_type.

    `retriable` failures are transient infra issues — NODE_FAIL, OOM,
    transient FS errors, slurmrestd unreachability — that the runner
    bounces back to QUEUED for another attempt while retry_count is
    below max_retries. `permanent` failures (bad input, container
    contract violations, exit codes from a known-terminal workflow) skip
    the retry loop and go straight to FAILED.
    """

    RETRIABLE = "retriable"
    PERMANENT = "permanent"


class WorkTicketFailureStage(StrEnum):
    """Coarse "where in the lifecycle did it fail" enum, mirrored DB-side
    by qiita.work_ticket_failure_stage.

    `STEP_RUN` is paired with a non-NULL `failure_step_name` carrying the
    YAML entry's `.name`; `SUBMISSION` and `FINALIZE` cover everything
    outside the step loop.
    """

    SUBMISSION = "submission"
    STEP_RUN = "step_run"
    FINALIZE = "finalize"


class StepProgressState(StrEnum):
    """Control-plane-side write-ahead lifecycle of one work-ticket step
    entry, persisted per `(work_ticket_idx, step_index, attempt)` in
    `qiita.work_ticket_step`.

    Distinct from `StepStatus` (a backend's live report of a submitted
    job): this is the CP runner's *own* progress record, the spine of
    restart recovery. `submitting` is the write-ahead intent written
    *before* the backend submit fires; `submitted` records a returned
    `slurm_job_id`; `running` mirrors a status poll; `completed` /
    `failed` are terminal. Mirrored DB-side by the `state` TEXT+CHECK
    column on `qiita.work_ticket_step` — a plain TEXT/CHECK, not a
    Postgres ENUM (same carve-out as `upload.status` / `reference.status`;
    out of scope for `ENUM_PAIRS`). Keep both sides in sync by hand.
    """

    SUBMITTING = "submitting"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class WorkTicket(BaseModel):
    """Control-plane record of an action invocation.

    `(action_id, action_version)` FK into `qiita.action` and pin the exact
    action definition this ticket was submitted against.

    `scope_target` answers "which resource is this work about?" — the
    resource-ACL gate keys off it. `action_context` carries action-defined
    free-form state, validated at submission against the action's declared
    `context_schema`.
    """

    work_ticket_idx: Annotated[int, Field(gt=0)]
    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    action_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    originator_principal_idx: Annotated[int, Field(gt=0)]
    scope_target: ScopeTarget
    # Analysis-index shard ordinal (0..N-1) for a sharded reference build
    # ticket; None for every non-sharded ticket. Mirrors qiita.work_ticket.
    # shard_id, whose CHECK ties a non-NULL value to reference scope. Lets N
    # concurrent same-action build tickets fan out over one reference without
    # colliding on work_ticket_one_in_flight_per_reference.
    shard_id: int | None = None
    action_context: dict[str, Any] = Field(default_factory=dict)
    state: WorkTicketState
    # Retry accounting. retry_count starts at 0 and increments on each
    # retriable failure (PROCESSING → QUEUED transition). When a step
    # raises a retriable BackendFailure and retry_count >= max_retries,
    # the runner transitions the ticket to FAILED with the captured
    # failure_*. Tickets inherit the DB default (3) on submission; the
    # column is per-row so an admin can bump max_retries on a specific
    # stuck ticket without redeploying.
    retry_count: Annotated[int, Field(ge=0)] = 0
    max_retries: Annotated[int, Field(ge=0, le=100)] = 3
    # Failure surface. All fields are NULL on non-FAILED tickets and all
    # non-NULL on FAILED tickets (DB CHECK enforces). failure_step_name
    # is non-NULL only when failure_stage is STEP_RUN.
    failure_type: FailureType | None = None
    failure_stage: WorkTicketFailureStage | None = None
    failure_step_name: str | None = Field(default=None, min_length=1, max_length=255)
    failure_reason: str | None = None
    # In-place-retry visibility (set while the runner is stuck retrying an
    # unreachable orchestrator/slurmrestd for this ticket; NULL otherwise).
    # Advisory and orthogonal to the failure_* surface — the ticket is still
    # `processing`, not `failed`. Mirrors the qiita.work_ticket columns of the
    # same name; the status routes surface them so a wedged-looking ticket is
    # explainable instead of silent.
    transient_reason: str | None = None
    transient_since: AwareDatetime | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime


class ResourceOverride(BaseModel):
    """Per-run resource override for a work ticket's SLURM steps.

    A privileged caller (wet_lab_admin / system_admin) raises the per-step
    memory *floor* for one run — e.g. staging a human genome that OOMs the
    workflow's conservative default — without editing the workflow YAML. The
    runner applies `max(step baseline_resources.mem_gb, mem_gb)` at dispatch,
    still clamped to the action's mem ceiling (an override above the ceiling is
    rejected at submission). `mem_gb=None` (the default) leaves every step's
    YAML baseline untouched. Carried on `qiita.work_ticket` so a control-plane
    restart re-attaches in-flight work with the same override.

    INVARIANT — enforcement is NOT on this model: any route that accepts a
    `resource_override` MUST itself gate it to wet_lab_admin+ (else a regular
    caller could inflate their job's footprint) and clamp it to the action
    ceiling. Today only `POST /work-ticket` accepts one — see the gate in
    `routes/work_ticket.py::submit_work_ticket`. A new route accepting it
    without that gate is a privilege-escalation bug."""

    mem_gb: Annotated[int | None, Field(default=None, gt=0)] = None


class WorkTicketCreateRequest(BaseModel):
    """Body for `POST /api/v1/work-ticket`.

    `originator_principal_idx` is set server-side from the authenticated
    caller — clients cannot submit on behalf of another principal.

    `resource_override` is an optional per-run resource bump, gated server-side
    to wet_lab_admin / system_admin and bounded by the action's ceiling.

    `force` re-submits a sequenced_pool action even when a COMPLETED ticket
    already exists for the same `(pool, action, version)`. Default-refused
    because a re-run re-registers the pool's reads into the lake (DuckLake has
    no uniqueness — duplicate rows result); the intended recovery for a stored
    result is `delete-sequenced-pool` then resubmit. It is privileged regardless
    of scope: setting `force=true` requires wet_lab_admin / system_admin (403
    otherwise) for ANY action. It only *changes submission behavior* for the
    sequenced_pool COMPLETED gate, though — for other scopes, or when no
    COMPLETED ticket exists, an authorized `force=true` is simply a no-op. It
    never relaxes the in-flight gate (a PENDING/QUEUED/PROCESSING ticket still
    blocks)."""

    action_id: str = Field(min_length=1, max_length=MAX_NAME_LENGTH)
    action_version: str = Field(min_length=1, max_length=MAX_VERSION_LENGTH)
    scope_target: ScopeTarget
    action_context: dict[str, Any] = Field(default_factory=dict)
    resource_override: ResourceOverride | None = None
    force: bool = False


class WorkTicketResponse(BaseModel):
    """Returned by `POST /api/v1/work-ticket` (with HTTP 202) and by
    `POST /api/v1/work-ticket/{idx}/run`. Carries the ticket id and its
    *post-call* state — typically PENDING for a freshly-created ticket
    or after a FAILED→PENDING reset, but check the field for what the
    server saw rather than assuming."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    state: WorkTicketState


def _check_host_ref_override(
    *, host_rype_reference_idx: int | None, host_minimap2_reference_idx: int | None, force: bool
) -> None:
    """Shared host-ref coherence for the block/align plan requests.

    Host filtering is resolved per sample server-side, so the request's host
    references are a `force`-only override. Enforce the same rule the CLI does:
    minimap2 is the optional second stage (needs rype), and a host reference set
    WITHOUT `force` is an error rather than a silent no-op. Kept here so both plan
    requests validate identically and the server surface cannot disagree with the
    CLI guard (`_validate_host_ref_override_args`)."""
    if host_minimap2_reference_idx is not None and host_rype_reference_idx is None:
        raise ValueError(
            "host_minimap2_reference_idx requires host_rype_reference_idx"
            " (minimap2 is the optional second host-filter stage)"
        )
    if host_rype_reference_idx is not None and not force:
        raise ValueError(
            "host_rype_reference_idx / host_minimap2_reference_idx are a force-only"
            " override: host filtering is resolved per sample from each sample's"
            " host_taxon_id metadata. Set force=true to apply the given reference(s)"
            " pool-wide instead, bypassing resolution."
        )


class BlockMaskPlanRequest(BaseModel):
    """Request body for `POST .../sequenced-pool/{P}/block-mask-plan` — the
    bulk-block read-masking entrypoint (the block-compute analog of the
    per-sample submit-host-filter-pool fan-out).

    Host filtering is resolved PER SAMPLE, server-side, from each sample's own
    `host_taxon_id` metadata + the run's platform — not chosen on the request.
    Samples that resolve to different hosts get different masks and tile into
    different blocks. So the normal request carries NO host reference at all.

    `host_rype_reference_idx` (with the optional second-stage
    `host_minimap2_reference_idx`) is a `--force` OVERRIDE only: it applies the
    given reference(s) pool-wide, blanks included, BYPASSING resolution. Supplying
    a host reference without `force=True` is rejected — an override that silently
    did nothing is the worst outcome. minimap2 is the optional second stage and
    never runs without rype.

    `only_missing` drops samples already carrying a completion gate for their
    resolved mask, so an interrupted plan re-runs only the gap; off by default so
    a deliberate re-plan still tiles pool-wide.
    """

    host_rype_reference_idx: Annotated[int, Field(gt=0)] | None = None
    host_minimap2_reference_idx: Annotated[int, Field(gt=0)] | None = None
    force: bool = False
    only_missing: bool = False

    @model_validator(mode="after")
    def _validate_host_ref_override(self) -> BlockMaskPlanRequest:
        _check_host_ref_override(
            host_rype_reference_idx=self.host_rype_reference_idx,
            host_minimap2_reference_idx=self.host_minimap2_reference_idx,
            force=self.force,
        )
        return self


class BlockPlanPartition(BaseModel):
    """One mask-partition of a block plan: the samples sharing a resolved
    `mask_idx`, the host filtering THIS partition resolved to (its own refs, not a
    pool-wide flag — a heterogeneous pool yields several partitions differing by
    host), and how many blocks they tiled into."""

    mask_idx: Annotated[int, Field(gt=0)]
    sample_count: Annotated[int, Field(ge=0)]
    block_count: Annotated[int, Field(ge=0)]
    host_filter_enabled: bool
    host_rype_reference_idx: int | None
    host_minimap2_reference_idx: int | None


class BlockPlanBlock(BaseModel):
    """One planned block: its idx, the block work_ticket dispatched for it, the
    partition mask it carries, and its size (members + reads)."""

    block_idx: Annotated[int, Field(gt=0)]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
    member_count: Annotated[int, Field(gt=0)]
    read_count: Annotated[int, Field(gt=0)]


class BlockMaskPlanResponse(BaseModel):
    """Returned (HTTP 202) by `POST .../block-mask-plan`: the plan the server
    persisted + dispatched. `blocks` lists every created block with its
    dispatched work_ticket; `partitions` summarizes the per-mask tiling and the
    host filtering EACH partition resolved to (there is no single pool-wide host
    answer any more — it is per sample); the `samples_*` counts reconcile the
    pool's samples (planned + skipped-existing + skipped-no-reads). A pool with
    nothing to do returns 202 with zero counts."""

    sequencing_run_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    instrument_model: str | None
    samples_planned: Annotated[int, Field(ge=0)]
    samples_skipped_existing: Annotated[int, Field(ge=0)]
    samples_skipped_no_reads: Annotated[int, Field(ge=0)]
    blocks_created: Annotated[int, Field(ge=0)]
    partitions: list[BlockPlanPartition]
    blocks: list[BlockPlanBlock]


class AlignPlanRequest(BaseModel):
    """Request body for `POST .../sequenced-pool/{P}/align-plan` — the bulk-block
    alignment entrypoint (the align analog of `block-mask-plan`).

    Aligns the pool's HOST-DEPLETED, QC-passed reads (the completed read-mask the
    block-mask-plan produced) against a sharded `reference_idx`. The aligner is NOT
    a caller choice — the server derives it from the run's sequencing platform
    (short-read Illumina → bowtie2, long-read PacBio HiFi / Nanopore → minimap2) and
    reports it in the response.

    Which mask each sample's reads were depleted under is resolved PER SAMPLE,
    server-side — the SAME resolution the block-mask-plan minted under — so the
    planner looks up each sample's already-minted mask_idx (it never mints a mask)
    for that sample's own decision. So the normal request carries NO host reference.
    `host_rype_reference_idx` (+ optional `host_minimap2_reference_idx`) is a
    `--force` OVERRIDE only, mirroring block-mask-plan: it looks the mask up under
    the given reference(s) pool-wide, bypassing resolution (use it to align a pool
    that was block-masked with `force`). A host reference without `force=True` is
    rejected. minimap2 is the optional second host stage and never rides without rype.

    `only_missing` drops samples already carrying a completion gate for their
    resolved alignment, so an interrupted plan re-runs only the gap; off by default
    so a deliberate re-plan still tiles pool-wide (and is refused, 409, if any
    sample is already gated — DELETE the alignment first or pass only_missing)."""

    reference_idx: Annotated[int, Field(gt=0)]
    host_rype_reference_idx: Annotated[int, Field(gt=0)] | None = None
    host_minimap2_reference_idx: Annotated[int, Field(gt=0)] | None = None
    force: bool = False
    only_missing: bool = False

    @model_validator(mode="after")
    def _validate_host_ref_override(self) -> AlignPlanRequest:
        _check_host_ref_override(
            host_rype_reference_idx=self.host_rype_reference_idx,
            host_minimap2_reference_idx=self.host_minimap2_reference_idx,
            force=self.force,
        )
        return self


class AlignPlanPartition(BaseModel):
    """One mask-partition of an align plan: the samples sharing a resolved
    `mask_idx` (and the `alignment_idx` minted over it) and how many blocks they
    tiled into."""

    alignment_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
    sample_count: Annotated[int, Field(ge=0)]
    block_count: Annotated[int, Field(ge=0)]


class AlignPlanBlock(BaseModel):
    """One planned align block: its idx, the block work_ticket dispatched for it,
    the alignment + partition-mask it carries, and its size (members + reads)."""

    block_idx: Annotated[int, Field(gt=0)]
    work_ticket_idx: Annotated[int, Field(gt=0)]
    alignment_idx: Annotated[int, Field(gt=0)]
    mask_idx: Annotated[int, Field(gt=0)]
    member_count: Annotated[int, Field(gt=0)]
    read_count: Annotated[int, Field(gt=0)]


class AlignPlanResponse(BaseModel):
    """Returned (HTTP 202) by `POST .../align-plan`: the plan the server persisted
    + dispatched. `blocks` lists every created block with its dispatched
    work_ticket; `partitions` summarizes the per-mask tiling and the alignment_idx
    minted over each; the `samples_*` counts reconcile the pool's samples (planned +
    the several skip reasons). A pool with nothing to align returns 202 with zero
    counts."""

    sequencing_run_idx: Annotated[int, Field(gt=0)]
    sequenced_pool_idx: Annotated[int, Field(gt=0)]
    reference_idx: Annotated[int, Field(gt=0)]
    aligner: Literal["minimap2", "bowtie2"]
    samples_planned: Annotated[int, Field(ge=0)]
    # Dropped by only_missing (already carry an alignment_sample gate).
    samples_skipped_existing: Annotated[int, Field(ge=0)]
    # No read-mask minted for the sample's resolved filtering config (it was never
    # block-masked under this host config).
    samples_skipped_no_mask: Annotated[int, Field(ge=0)]
    # A mask exists but is not `completed` for the sample (its masking is still
    # in-flight / failed) — align only fully-masked samples.
    samples_skipped_mask_incomplete: Annotated[int, Field(ge=0)]
    # ACTIVE pool sample with no stored reads (no sequence_range) — nothing to tile.
    samples_skipped_no_reads: Annotated[int, Field(ge=0)]
    blocks_created: Annotated[int, Field(ge=0)]
    partitions: list[AlignPlanPartition]
    blocks: list[AlignPlanBlock]


class WorkTicketSummary(WorkTicket):
    """A WorkTicket plus a snapshot of its *current* step entry's compute
    placement. Returned by `GET /api/v1/work-ticket` (the list view) so a
    caller can see, in one round trip, not just a ticket's lifecycle state
    but *where* its in-flight work is running and on which SLURM job.

    The "current entry" is the highest `(step_index, attempt)` row in
    `qiita.work_ticket_step` for the ticket — the entry the runner is on,
    or the last one it finished. The five fields below are all NULL for a
    ticket with no progress rows yet (a PENDING / QUEUED ticket whose first
    write-ahead hasn't fired); for an in-process `action:` entry the
    `slurm_*` fields stay NULL while `compute_target='control_plane'`.

    This read is DB-backed and therefore at most one poll-interval stale
    (the runner persists `running` on a status poll, default ~10s); the
    `slurm_job_id` is exact. A live SLURM hop to refresh `step_state` is a
    separate single-ticket concern, deliberately not done for the list.
    """

    # 0-based index into the action's `steps:` list, plus the entry name.
    current_step_index: int | None = None
    current_step_name: str | None = None
    # Where the current entry runs (`slurm` / `local` / `control_plane`).
    compute_target: ComputeTarget | None = None
    # The SLURM job id — non-NULL only for a `slurm` current entry past
    # write-ahead.
    slurm_job_id: int | None = None
    # The control-plane-side write-ahead lifecycle state of the current
    # entry (the spine's StepProgressState, NOT a live SLURM-native state —
    # see the class docstring on staleness).
    step_state: StepProgressState | None = None


class WorkTicketStepLogs(BaseModel):
    """Returned by `GET /api/v1/work-ticket/{idx}/step/{step_index}/logs`.

    A bounded tail of a single step attempt's stdout/stderr, read by the
    control plane straight off shared scratch (`PATH_SCRATCH/ticket/...`) and
    served over HTTP so an operator can diagnose a failure — an OOM, a bad
    input, a contract violation — without a host shell or sudo. Each stream is
    independently truncated; `*_truncated` is True when older content was
    dropped from the front. A stream the job never wrote comes back as an
    empty string (not an error)."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    step_index: Annotated[int, Field(ge=0)]
    # The attempt actually read — resolved to the latest recorded attempt when
    # the caller didn't pin one, so the response is self-describing.
    attempt: Annotated[int, Field(ge=0)]
    step_name: str
    stdout: str = ""
    stderr: str = ""
    stdout_truncated: bool = False
    stderr_truncated: bool = False


# Upper bound on an explicit cancel idx list, so one request can't fan a cancel
# across an unbounded number of tickets (a fan-out is dozens, not thousands).
_MAX_CANCEL_IDXS = 1000


class WorkTicketCancelRequest(BaseModel):
    """Body for POST /api/v1/work-ticket/cancel — operator-cancel of in-flight
    compute (system_admin, `work_ticket:cancel`).

    Selects the tickets by an explicit idx list AND/OR a filter. The filter is
    `action_id` (required to filter) plus an optional `sequencing_run_idx` /
    `sequenced_pool_idx` narrowing, and matches only NON-terminal tickets
    (cancelling a terminal ticket is a no-op). Explicit `work_ticket_idxs` are
    reaped regardless of state (defensive orphan-reap). At least one selector must
    be present; run/pool narrowing requires `action_id`."""

    model_config = ConfigDict(extra="forbid")

    work_ticket_idxs: list[Annotated[int, Field(gt=0)]] = Field(
        default_factory=list, max_length=_MAX_CANCEL_IDXS
    )
    action_id: str | None = Field(default=None, min_length=1, max_length=MAX_NAME_LENGTH)
    sequencing_run_idx: Annotated[int, Field(gt=0)] | None = None
    sequenced_pool_idx: Annotated[int, Field(gt=0)] | None = None

    @model_validator(mode="after")
    def at_least_one_selector(self):
        if not self.work_ticket_idxs and self.action_id is None:
            raise ValueError(
                "provide work_ticket_idxs and/or an action_id filter to select tickets to cancel"
            )
        if (
            self.sequencing_run_idx is not None or self.sequenced_pool_idx is not None
        ) and self.action_id is None:
            raise ValueError(
                "sequencing_run_idx / sequenced_pool_idx narrow the action_id filter;"
                " set action_id too"
            )
        return self


class WorkTicketCancelResult(BaseModel):
    """The outcome for ONE ticket in a cancel request. `cancelled` is True iff this
    call flipped it terminal (False = it was already terminal, a no-op); the scancel
    reap runs either way. `not_found` marks an explicit idx that does not exist.
    `reap_error` is set when the terminal flip landed but the scancel failed (the
    flip stands — re-run cancel to retry the reap)."""

    work_ticket_idx: Annotated[int, Field(gt=0)]
    previous_state: str | None = None
    state: str | None = None
    cancelled: bool = False
    cancelled_job_ids: list[int] = Field(default_factory=list)
    reap_error: str | None = None
    not_found: bool = False


class WorkTicketCancelResponse(BaseModel):
    """Returned by POST /api/v1/work-ticket/cancel — one result per selected ticket,
    plus the count actually flipped this call. Compute-on-read over the selection."""

    requested: int
    cancelled: int
    results: list[WorkTicketCancelResult]
