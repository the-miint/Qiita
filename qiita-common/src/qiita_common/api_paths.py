"""Centralized REST API constants — paths, primitive names, network hosts.

Shared by routes, tests, and clients so a deploy-time URL change or a
new library primitive lands in one place.

Two flavours per path:

- ``PATH_*`` — sub-path relative to the router prefix (and the router
  prefix itself, ``PATH_<TAG>_PREFIX``). Used by FastAPI route decorators
  via ``@router.post(PATH_MEMBERSHIP)`` so the handler doesn't repeat
  the prefix.

- ``URL_*`` — full path under :data:`API_PREFIX`, with ``{placeholder}``
  segments where the route is parameterized. Used by tests and clients
  via ``client.post(URL_MEMBERSHIP.format(reference_idx=42), ...)`` or
  by f-string composition for unparameterized paths.

Adding a route requires both flavours so the router and its callers
stay in lockstep; removing one without the other will surface as a name
error at import time rather than a silent route mismatch at runtime.

All control-plane routes are covered here. When a new route is added,
its PATH_/URL_ pair MUST land in this file in the same change — the
parity test in ``qiita-common/tests/test_api_paths.py`` checks that
``URL_X == API_PREFIX + PATH_X_PREFIX + PATH_X`` for every triple.

A few routers share a prefix (``/study`` is reused by biosample and
sequenced-sample; ``/sequencing-run`` is reused by sequenced-sample);
in those cases the router declares ``prefix=PATH_STUDY_PREFIX`` and the
URL_ for the foreign route composes against that same constant so a
prefix change moves every router at once.
"""

from enum import StrEnum
from pathlib import Path

from qiita_common.auth_constants import API_PREFIX

# =============================================================================
# Network constants
# =============================================================================

# IPv4 loopback. Used for test-fixture binds, CLI loopback servers (OAuth
# return URLs), and dev-mode service URLs. A future "switch to ::1" or
# "bind to 0.0.0.0 in container" change becomes a one-line edit here
# rather than a cross-cutting find/replace.
LOOPBACK_HOST = "127.0.0.1"

# =============================================================================
# /reference/*
# =============================================================================

PATH_REFERENCE_PREFIX = "/reference"
PATH_REFERENCE_ROOT = ""  # POST/list against the prefix itself
PATH_REFERENCE_BY_IDX = "/{reference_idx}"
PATH_REFERENCE_STATUS = "/{reference_idx}/status"
PATH_REFERENCE_INDEX = "/{reference_idx}/index"
PATH_REFERENCE_DOGET = "/{reference_idx}/ticket/doget"

URL_REFERENCE_PREFIX = f"{API_PREFIX}{PATH_REFERENCE_PREFIX}"
URL_REFERENCE_BY_IDX = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_BY_IDX}"
URL_REFERENCE_STATUS = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_STATUS}"
URL_REFERENCE_INDEX = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_INDEX}"
URL_REFERENCE_DOGET = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_DOGET}"

# =============================================================================
# /prep-protocol/*
# =============================================================================

PATH_PREP_PROTOCOL_PREFIX = "/prep-protocol"
PATH_PREP_PROTOCOL_ROOT = ""  # list against the prefix itself

URL_PREP_PROTOCOL_PREFIX = f"{API_PREFIX}{PATH_PREP_PROTOCOL_PREFIX}"


# =============================================================================
# Library primitive names
# =============================================================================
# The runner dispatches workflow `action:` entries to LIBRARY[name] in
# qiita_control_plane.actions.library — direct in-process call, no HTTP.
# This enum is the single declaration point so YAML and dispatch stay in
# lockstep.


class LibraryPrimitive(StrEnum):
    """Closed set of library-primitive names referenced by workflow YAML.

    StrEnum members compare equal to their string value, so dict keys built
    around bare strings (e.g. JSONB-decoded `WorkflowAction.name`) keep
    working while new code gets the typo-catching benefit of an enum.
    """

    MINT_FEATURES = "mint-features"
    WRITE_MEMBERSHIP = "write-membership"
    REGISTER_FILES = "register-files"
    REGISTER_INDEX = "register-index"
    # Reference sharding: assign a reference's genome-bearing features to N
    # lineage-sorted shards (reference_membership.shard_id). A CP-side primitive
    # (its inputs — feature_genome, reference_membership, taxonomy — are
    # Postgres/DuckLake, and it writes shard_id back), not a native compute job.
    # See qiita_control_plane.actions.library.plan_shards.
    PLAN_SHARDS = "plan-shards"
    # Reference sharding: the terminal step of each shard's build ticket.
    # Count-based, fail-closed completion — when every expected index_type has a
    # registered row for all N shards, does the guarded `indexing -> active`.
    # See qiita_control_plane.actions.library.finalize_shard.
    FINALIZE_SHARD = "finalize-shard"
    PERSIST_READ_METRICS = "persist-read-metrics"
    PERSIST_QC_REPORT = "persist-qc-report"
    # Block-compute: idempotent block replace. Runs immediately BEFORE
    # register-files in the bulk-block read-mask workflow — deletes this block's
    # exact read_mask footprint (its member sub-ranges under the ticket's
    # mask_idx) so a re-run deletes-then-re-registers without double-counting. On
    # a fresh block it deletes 0 rows. See
    # qiita_control_plane.actions.library.delete_read_mask_block_data.
    DELETE_READ_MASK_BLOCK = "delete-block-mask"
    # Block-compute: the bulk-block read-mask workflow's terminal step. Marks the
    # block completed, then finalizes each covered sample once ALL its covering
    # blocks are done (the per-sample rollup the per-sample path did via
    # persist-read-metrics, gated on block completion so a partially-masked sample
    # never finalizes). See qiita_control_plane.actions.library.reconcile_block.
    RECONCILE_BLOCK = "reconcile-block"


# =============================================================================
# /step/* — orchestrator HTTP API
# =============================================================================
# The control-plane runner dispatches each workflow `step:` entry to the
# orchestrator's ComputeBackend via the decoupled submit / status / result
# trio: submit returns a handle immediately, the CP runner polls status until
# terminal, then asks for the verified result — so the runner can drive a long
# SLURM job without holding a connection open. find-by-name closes the
# write-ahead idempotency gap. See docs/architecture.md "Compute Orchestrator".

PATH_STEP_PREFIX = "/step"
PATH_STEP_SUBMIT = "/submit"
PATH_STEP_STATUS = "/status"
PATH_STEP_RESULT = "/result"
# Resource planning: a native step's optional plan() sizing hint, read by the CP
# runner BEFORE submit so it can lower a step below its YAML baseline for a
# small input (down-sizing). Advisory — a missing/failed plan degrades to the
# baseline; escalation remains the up-sizing path. Native (`module`) steps only.
PATH_STEP_PLAN = "/plan"
# Recovery / idempotency: look up live SLURM jobs by their deterministic
# name so the CP can adopt a job it submitted but never recorded the id for.
PATH_STEP_FIND_BY_NAME = "/find-by-name"

URL_STEP_PREFIX = f"{API_PREFIX}{PATH_STEP_PREFIX}"
URL_STEP_SUBMIT = f"{URL_STEP_PREFIX}{PATH_STEP_SUBMIT}"
URL_STEP_STATUS = f"{URL_STEP_PREFIX}{PATH_STEP_STATUS}"
URL_STEP_RESULT = f"{URL_STEP_PREFIX}{PATH_STEP_RESULT}"
URL_STEP_PLAN = f"{URL_STEP_PREFIX}{PATH_STEP_PLAN}"
URL_STEP_FIND_BY_NAME = f"{URL_STEP_PREFIX}{PATH_STEP_FIND_BY_NAME}"


# =============================================================================
# /reference-artifact/* — orchestrator on-disk reference-artifact cleanup
# =============================================================================
# The control-plane DELETE /reference/{idx} flow calls this so the orchestrator
# (the only side with access to PATH_DERIVED on the compute host) can remove a
# reference's persistent on-disk index artifacts —
# `{path_derived}/references/{idx}/{rype,minimap2}/...`. A direct, synchronous
# filesystem op, not a SLURM step: there is no job to schedule, just a
# best-effort idempotent rmtree. Shares the CP↔CO bearer with /step/*.

PATH_REFERENCE_ARTIFACT_PREFIX = "/reference-artifact"
PATH_REFERENCE_ARTIFACT_BY_IDX = "/{reference_idx}"

URL_REFERENCE_ARTIFACT_PREFIX = f"{API_PREFIX}{PATH_REFERENCE_ARTIFACT_PREFIX}"
URL_REFERENCE_ARTIFACT_BY_IDX = f"{URL_REFERENCE_ARTIFACT_PREFIX}{PATH_REFERENCE_ARTIFACT_BY_IDX}"


# =============================================================================
# /work-ticket/* — control-plane work-ticket lifecycle
# =============================================================================
# Submission (POST root) creates a ticket and fires an in-process
# `asyncio.Task` calling `runner.run_workflow` (option C, in-process
# dispatch — no polling worker). The /run endpoint is the human-override
# path that resets a FAILED ticket and re-dispatches; auto-retry is not
# implemented, so /run is the only retry mechanism.

PATH_WORK_TICKET_PREFIX = "/work-ticket"
PATH_WORK_TICKET_ROOT = ""  # POST (submit) and GET (list) against the prefix itself
PATH_WORK_TICKET_BY_IDX = "/{work_ticket_idx}"
PATH_WORK_TICKET_RUN = "/{work_ticket_idx}/run"
# Read a single step attempt's stdout/stderr tail (operator diagnosis without
# a host shell — the logs live under PATH_SCRATCH/ticket, served by the CP).
PATH_WORK_TICKET_STEP_LOGS = "/{work_ticket_idx}/step/{step_index}/logs"

URL_WORK_TICKET_PREFIX = f"{API_PREFIX}{PATH_WORK_TICKET_PREFIX}"
# GET-list URL — same path as the POST root, named distinctly so clients
# and tests read intent (and so it carries its own parity triple).
URL_WORK_TICKET_LIST = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_ROOT}"
URL_WORK_TICKET_BY_IDX = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_BY_IDX}"
URL_WORK_TICKET_RUN = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_RUN}"
URL_WORK_TICKET_STEP_LOGS = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_STEP_LOGS}"


# =============================================================================
# /upload/* — generic Arrow-data staging slots
# =============================================================================
# POST /upload mints a row in `qiita.upload` and returns a signed DoPut
# Flight ticket. POST /upload/{idx}/done records the client's completion
# claim and transitions pending → ready. GET /upload/{idx} reads status.
# The domain is content-agnostic by design; no reference / role / consumer
# fields cross the wire.

PATH_UPLOAD_PREFIX = "/upload"
PATH_UPLOAD_ROOT = ""  # POST/list against the prefix itself
PATH_UPLOAD_BY_IDX = "/{upload_idx}"
PATH_UPLOAD_DONE = "/{upload_idx}/done"

URL_UPLOAD_PREFIX = f"{API_PREFIX}{PATH_UPLOAD_PREFIX}"
URL_UPLOAD_BY_IDX = f"{URL_UPLOAD_PREFIX}{PATH_UPLOAD_BY_IDX}"
URL_UPLOAD_DONE = f"{URL_UPLOAD_PREFIX}{PATH_UPLOAD_DONE}"


def compute_upload_staging_path(staging_root: Path, upload_idx: int) -> Path:
    """Canonical filesystem path for a staged DoPut upload.

    Mirrors the Rust ``staging_path_for(root, idx)`` in
    ``qiita-data-plane/src/flight_service.rs``: a single source of truth
    so the data plane (writes here on DoPut) and the control-plane
    runner (reads here on workflow start) agree byte-for-byte. The
    layout — ``{root}/uploads/{idx}/upload.parquet`` — is locked by
    the Rust unit test ``staging_path_for_layout``; this Python
    function MUST stay in lockstep with that test.

    Lives here, not on the data-plane side, because the path layout is
    a cross-service contract — both sides need it, and qiita-common is
    the only place both already depend on. Not in ``qiita_common.upload``
    because there is no such module; ``api_paths`` already owns
    deploy-shape constants for the upload domain (PATH_UPLOAD_*).
    """
    return staging_root / "uploads" / str(upload_idx) / "upload.parquet"


def compute_reads_staging_path(staging_root: Path, prep_sample_idx: int) -> Path:
    """Canonical filesystem path for a prep_sample's durable staged reads.

    The bcl-convert ``ingest_reads`` step writes each sample's full
    ``read.parquet`` here once (in addition to registering it into the
    DuckLake ``read`` table). It is the input the repeatable read-mask
    workflow binds as ``reads`` — masks read the stored sequences from
    this stable, prep_sample-addressable copy rather than re-deriving
    them from FASTQ, so a second host reference is a new mask over the
    same reads, never a re-run of ingest.

    Layout — ``{root}/reads/{prep_sample_idx}/read.parquet`` — is
    deterministic in ``prep_sample_idx`` (NOT ticket-scoped): the
    ingest step (in the pool's bcl-convert ticket) writes it; a later
    read-mask ticket for the same sample reads it back. Mirrors
    ``compute_upload_staging_path``'s deterministic-by-idx shape so the
    writer (ingest) and reader (``_resolve_staged_reads`` in the
    runner) agree byte-for-byte.
    """
    return staging_root / "reads" / str(prep_sample_idx) / "read.parquet"


# =============================================================================
# /sequence-range/* — control-plane sequence_idx allocator
# =============================================================================
# Mints contiguous bigint ranges (`sequence_idx_start..stop`) the data
# plane uses to key raw sequencing reads. POST is service-account-only
# (Scope.SEQUENCE_RANGE_MINT); GET accepts Scope.PREP_SAMPLE_READ OR
# Scope.SEQUENCE_RANGE_MINT (the latter lets the minter read back its own
# range on the ingest_reads retry/reuse path).

PATH_SEQUENCE_RANGE_PREFIX = "/sequence-range"
PATH_SEQUENCE_RANGE_ROOT = ""  # POST against the prefix itself
PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE = "/{prep_sample_idx}"

URL_SEQUENCE_RANGE_PREFIX = f"{API_PREFIX}{PATH_SEQUENCE_RANGE_PREFIX}"
URL_SEQUENCE_RANGE_BY_PREP_SAMPLE = (
    f"{URL_SEQUENCE_RANGE_PREFIX}{PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE}"
)

# =============================================================================
# /mask-definition/* — control-plane read-filtering config identity
# =============================================================================
# Mints (idempotently, deduped on a canonical-config hash) the mask_idx that
# tags the data plane's read_mask / read_masked rows. POST is service-account-
# only (Scope.READ_MASKED_DOGET).

PATH_MASK_DEFINITION_PREFIX = "/mask-definition"
PATH_MASK_DEFINITION_ROOT = ""  # POST against the prefix itself
PATH_MASK_DEFINITION_BY_IDX = "/{mask_idx}"  # DELETE a mask (lake rows + Postgres row)

URL_MASK_DEFINITION_PREFIX = f"{API_PREFIX}{PATH_MASK_DEFINITION_PREFIX}"
URL_MASK_DEFINITION_BY_IDX = f"{URL_MASK_DEFINITION_PREFIX}{PATH_MASK_DEFINITION_BY_IDX}"

# =============================================================================
# /read-masked/* — Flight DoGet ticket for the masked-read surface
# =============================================================================
# Signs an HMAC DoGet ticket scoped to a single (prep_sample_idx, mask_idx) on
# the data plane's `read_masked` view. POST is service-account-only
# (Scope.READ_MASKED_DOGET). The route enforces the mandatory-filter invariant:
# both identifiers are required, so an unfiltered read_masked ticket is never
# signed.

PATH_READ_MASKED_PREFIX = "/read-masked"
PATH_READ_MASKED_DOGET = "/ticket/doget"

URL_READ_MASKED_PREFIX = f"{API_PREFIX}{PATH_READ_MASKED_PREFIX}"
URL_READ_MASKED_DOGET = f"{URL_READ_MASKED_PREFIX}{PATH_READ_MASKED_DOGET}"


# =============================================================================
# /auth/* — OIDC handoff, PAT mint/list/revoke, CLI device flow
# =============================================================================

PATH_AUTH_PREFIX = "/auth"
PATH_AUTH_WHOAMI = "/whoami"
PATH_AUTH_PAT = "/pat"
PATH_AUTH_TOKEN = "/token"
PATH_AUTH_TOKEN_BY_IDX = "/token/{token_idx}"
PATH_AUTH_LOGIN = "/login"
PATH_AUTH_HANDOFF = "/handoff"
PATH_AUTH_CLI_EXCHANGE = "/cli-exchange"

URL_AUTH_PREFIX = f"{API_PREFIX}{PATH_AUTH_PREFIX}"
URL_AUTH_WHOAMI = f"{URL_AUTH_PREFIX}{PATH_AUTH_WHOAMI}"
URL_AUTH_PAT = f"{URL_AUTH_PREFIX}{PATH_AUTH_PAT}"
URL_AUTH_TOKEN = f"{URL_AUTH_PREFIX}{PATH_AUTH_TOKEN}"
URL_AUTH_TOKEN_BY_IDX = f"{URL_AUTH_PREFIX}{PATH_AUTH_TOKEN_BY_IDX}"
URL_AUTH_LOGIN = f"{URL_AUTH_PREFIX}{PATH_AUTH_LOGIN}"
URL_AUTH_HANDOFF = f"{URL_AUTH_PREFIX}{PATH_AUTH_HANDOFF}"
URL_AUTH_CLI_EXCHANGE = f"{URL_AUTH_PREFIX}{PATH_AUTH_CLI_EXCHANGE}"


# =============================================================================
# /admin/* — service-account mint, principal lifecycle, audit feed
# =============================================================================

PATH_ADMIN_PREFIX = "/admin"
PATH_ADMIN_SERVICE_ACCOUNT = "/service-account"
PATH_ADMIN_PRINCIPAL_DISABLED = "/principal/{principal_idx}/disabled"
PATH_ADMIN_PRINCIPAL_RETIRED = "/principal/{principal_idx}/retired"
PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE = "/principal/{principal_idx}/system-role"
PATH_ADMIN_AUDIT = "/audit"
PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS = "/principal/{principal_idx}/revoke-all-tokens"
# Re-identification export: owner-submitted sample names for a study (optionally
# filtered to one sequenced_pool via ?sequenced_pool_idx=). system_admin only.
PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID = "/study/{study_idx}/owner-biosample-id"
# Masked-read export (system_admin only): the manifest GET lists a
# sequenced_pool's non-retired samples to export under ?mask_idx=; the ticket
# POST mints a per-sample DoGet ticket on the data plane's read_masked view.
PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT = (
    "/sequenced-pool/{sequenced_pool_idx}/masked-read-export"
)
PATH_ADMIN_MASKED_READ_EXPORT_TICKET = "/masked-read-export/ticket"

URL_ADMIN_PREFIX = f"{API_PREFIX}{PATH_ADMIN_PREFIX}"
URL_ADMIN_SERVICE_ACCOUNT = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_SERVICE_ACCOUNT}"
URL_ADMIN_PRINCIPAL_DISABLED = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_DISABLED}"
URL_ADMIN_PRINCIPAL_RETIRED = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_RETIRED}"
URL_ADMIN_PRINCIPAL_SYSTEM_ROLE = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE}"
URL_ADMIN_AUDIT = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_AUDIT}"
URL_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS = (
    f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS}"
)
URL_ADMIN_STUDY_OWNER_BIOSAMPLE_ID = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_STUDY_OWNER_BIOSAMPLE_ID}"
URL_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT = (
    f"{URL_ADMIN_PREFIX}{PATH_ADMIN_SEQUENCED_POOL_MASKED_READ_EXPORT}"
)
URL_ADMIN_MASKED_READ_EXPORT_TICKET = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_MASKED_READ_EXPORT_TICKET}"


# =============================================================================
# /user/* — self-service profile (create, GET /me, PATCH /me)
# =============================================================================

PATH_USER_PREFIX = "/user"
PATH_USER_ROOT = ""  # POST against the prefix itself
PATH_USER_ME = "/me"

URL_USER_PREFIX = f"{API_PREFIX}{PATH_USER_PREFIX}"
URL_USER_ME = f"{URL_USER_PREFIX}{PATH_USER_ME}"


# =============================================================================
# /study/* — study CRUD, plus biosample + sequenced-sample under /study
# =============================================================================
# PATH_STUDY_PREFIX is the shared anchor for three routers: study itself,
# the biosample router whose paths are scoped under /study/{study_idx}/...,
# and the sequenced-sample list-by-study endpoint. URL_BIOSAMPLE_BY_STUDY
# / URL_SEQUENCED_SAMPLE_LIST_BY_STUDY below compose against this prefix.

PATH_STUDY_PREFIX = "/study"
PATH_STUDY_ROOT = ""  # POST against the prefix itself
PATH_STUDY_BY_IDX = "/{study_idx}"
# Bulk lookup of study_idx by a selectable study accession column
# (ena_study_accession or bioproject_accession; default bioproject); same
# body-vs-querystring rationale as the biosample lookup variants.
PATH_STUDY_LOOKUP_BY_ACCESSION = "/lookup-by-accession"

URL_STUDY_PREFIX = f"{API_PREFIX}{PATH_STUDY_PREFIX}"
URL_STUDY_BY_IDX = f"{URL_STUDY_PREFIX}{PATH_STUDY_BY_IDX}"
URL_STUDY_LOOKUP_BY_ACCESSION = f"{URL_STUDY_PREFIX}{PATH_STUDY_LOOKUP_BY_ACCESSION}"


# =============================================================================
# /sequencing-run/* — run CRUD + sequenced-pool POST + sequenced-sample
# =============================================================================
# Like /study, this prefix is shared. The sequenced-sample router with
# prefix="/sequencing-run" composes URL_SEQUENCED_SAMPLE_FROM_RUN /
# URL_SEQUENCED_SAMPLE_LIST_BY_RUN below against PATH_SEQUENCING_RUN_PREFIX.

PATH_SEQUENCING_RUN_PREFIX = "/sequencing-run"
PATH_SEQUENCING_RUN_ROOT = ""  # POST against the prefix itself
# GET one sequencing_run by idx (run metadata incl. instrument_model).
PATH_SEQUENCING_RUN_BY_IDX = "/{sequencing_run_idx}"
# Bulk lookup of sequencing_run_idx by instrument_run_id; same body-vs-
# querystring rationale as the study / biosample accession lookups.
PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID = "/lookup-by-instrument-run-id"
PATH_SEQUENCING_RUN_SEQUENCED_POOL = "/{sequencing_run_idx}/sequenced-pool"
PATH_SEQUENCED_POOL_PREFLIGHT = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/preflight"
)
# POST action: bulk-reassign the lane on the pool's run-preflight SQLite blob
# (wet_lab_admin+). A verb sub-path (not PATCH) because the preflight is not
# human-readable — there is no ETag to mint an If-Match from — and the operation
# is a server-side command (load blob -> run_preflight.update_lane -> store).
PATH_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/preflight/update-lane"
)
# DELETE target: full hard-delete of one sequenced_pool (system_admin only).
PATH_SEQUENCED_POOL_BY_IDX = "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}"
# GET the pool's merged (multiqc-equivalent) QC report: read-metric rollup +
# per-sample reports + the run-level merged aggregate.
PATH_SEQUENCED_POOL_QC_REPORT = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/qc-report"
)
# GET the pool's end-to-end processing rollup: the demux (bcl-convert) stage
# state plus per-sample read-mask (host-masking) state bucketed into completed /
# in-flight / no-data / failed / not-submitted counts, with `complete` (host
# masking) and `fully_processed` (demux + masking) flags.
PATH_SEQUENCED_POOL_COMPLETION = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/completion"
)
# POST action: plan + submit the pool's bulk-block read masking. One call tiles
# the pool's samples into fixed ~10M-read blocks (partitioned by mask identity),
# persists the block cover-map + per-sample completion gate, and dispatches one
# block work ticket per block. A verb sub-path (not a resource) — it is a
# server-side command (resolve masks -> tile -> persist -> dispatch), replacing
# the per-sample fan-out of the CLI's submit-host-filter-pool.
PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/block-mask-plan"
)

URL_SEQUENCING_RUN_PREFIX = f"{API_PREFIX}{PATH_SEQUENCING_RUN_PREFIX}"
URL_SEQUENCING_RUN_BY_IDX = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_BY_IDX}"
URL_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_LOOKUP_BY_INSTRUMENT_RUN_ID}"
)
URL_SEQUENCING_RUN_SEQUENCED_POOL = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_SEQUENCED_POOL}"
)
URL_SEQUENCED_POOL_PREFLIGHT = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_PREFLIGHT}"
URL_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_PREFLIGHT_UPDATE_LANE}"
)
URL_SEQUENCED_POOL_BY_IDX = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_BY_IDX}"
URL_SEQUENCED_POOL_QC_REPORT = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_QC_REPORT}"
URL_SEQUENCED_POOL_COMPLETION = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_COMPLETION}"
URL_SEQUENCED_POOL_BLOCK_MASK_PLAN = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_BLOCK_MASK_PLAN}"
)


# =============================================================================
# /biosample/* — direct biosample GET/PATCH (study-scoped POST is above)
# =============================================================================
# Two routers, one prefix anchor each:
#   • POST/list under /study/{study_idx}/biosample  → composes on PATH_STUDY_PREFIX
#   • GET/PATCH /biosample/{biosample_idx}          → its own prefix

PATH_BIOSAMPLE_BY_STUDY = "/{study_idx}/biosample"
PATH_BIOSAMPLE_LIST_BY_STUDY = "/{study_idx}/biosample/list-idxs"

PATH_BIOSAMPLE_PREFIX = "/biosample"
PATH_BIOSAMPLE_BY_IDX = "/{biosample_idx}"
# Bulk lookup of biosample_idx by a selectable biosample accession column
# (biosample_accession or ena_sample_accession; default biosample_accession).
# POST (not GET) because the accession list lives in the body — a typical
# bcl-convert pool carries ~384 accessions, which exceeds nginx's default
# URL-line cap when threaded through query-params. The response shape carries
# the resolved {accession: idx} map plus a missing[] list so the CLI can
# fail-fast naming every miss without N round trips.
PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION = "/lookup-by-accession"
# Bulk lookup of biosample_idx by matrix_tube_id; same body-vs-querystring
# rationale as the accession variant.
PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID = "/lookup-by-matrix-tube-id"

URL_BIOSAMPLE_BY_STUDY = f"{URL_STUDY_PREFIX}{PATH_BIOSAMPLE_BY_STUDY}"
URL_BIOSAMPLE_LIST_BY_STUDY = f"{URL_STUDY_PREFIX}{PATH_BIOSAMPLE_LIST_BY_STUDY}"
URL_BIOSAMPLE_PREFIX = f"{API_PREFIX}{PATH_BIOSAMPLE_PREFIX}"
URL_BIOSAMPLE_BY_IDX = f"{URL_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_BY_IDX}"
URL_BIOSAMPLE_LOOKUP_BY_ACCESSION = f"{URL_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_ACCESSION}"
URL_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID = (
    f"{URL_BIOSAMPLE_PREFIX}{PATH_BIOSAMPLE_LOOKUP_BY_MATRIX_TUBE_ID}"
)


# =============================================================================
# /sequenced-sample/* — direct GET/PATCH + run/study-scoped list endpoints
# =============================================================================
# Three routers anchored at three different prefixes:
#   • POST /sequencing-run/{run}/sequenced-pool/{pool}/sequenced-sample
#   • GET  /sequencing-run/{run}/sequenced-sample/list-idxs
#   • GET  /study/{study}/sequenced-sample/list-idxs
#   • GET/PATCH /sequenced-sample/{sequenced_sample_idx}

PATH_SEQUENCED_SAMPLE_FROM_RUN = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/sequenced-sample"
)
PATH_SEQUENCED_SAMPLE_LIST_BY_RUN = "/{sequencing_run_idx}/sequenced-sample/list-idxs"
# Run-scoped sibling of LIST_BY_RUN that returns the richer per-sample rows
# (SequencedSampleListResponse), not bare idxs — hence the `list` segment
# rather than `list-idxs`, paralleling LIST_BY_POOL.
PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL = "/{sequencing_run_idx}/sequenced-sample/list"
PATH_SEQUENCED_SAMPLE_LIST_BY_STUDY = "/{study_idx}/sequenced-sample/list-idxs"
# Pool-scoped sibling of LIST_BY_RUN. Returns richer per-sample rows
# (prep_sample_idx + sequenced_pool_item_id), hence the `list` segment rather
# than `list-idxs`. Anchored on /sequencing-run so require_sequenced_pool_in_run
# can check both ids in one round trip.
PATH_SEQUENCED_SAMPLE_LIST_BY_POOL = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/sequenced-sample/list"
)

PATH_SEQUENCED_SAMPLE_PREFIX = "/sequenced-sample"
PATH_SEQUENCED_SAMPLE_BY_IDX = "/{sequenced_sample_idx}"

URL_SEQUENCED_SAMPLE_FROM_RUN = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_FROM_RUN}"
URL_SEQUENCED_SAMPLE_LIST_BY_RUN = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_RUN}"
URL_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_RUN_FULL}"
)
URL_SEQUENCED_SAMPLE_LIST_BY_STUDY = f"{URL_STUDY_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_STUDY}"
URL_SEQUENCED_SAMPLE_LIST_BY_POOL = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_POOL}"
)
URL_SEQUENCED_SAMPLE_PREFIX = f"{API_PREFIX}{PATH_SEQUENCED_SAMPLE_PREFIX}"
URL_SEQUENCED_SAMPLE_BY_IDX = f"{URL_SEQUENCED_SAMPLE_PREFIX}{PATH_SEQUENCED_SAMPLE_BY_IDX}"


# =============================================================================
# /prep-sample/* — prep-sample reads (study membership)
# =============================================================================
# The study list returns richer per-study rows (study_idx + both accessions)
# as StudyListResponse, hence the `list` segment rather than `list-idxs`.

PATH_PREP_SAMPLE_PREFIX = "/prep-sample"
PATH_PREP_SAMPLE_STUDY_LIST = "/{prep_sample_idx}/study/list"
# Reversible operator disposition of a prep_sample: PATCH the `retired` flag so
# an empty / failed-yield well drops out of (or returns to) a pool's active set
# without a raw production UPDATE. Reversible by design (a misclassified well
# must be recoverable), unlike the terminal principal retire.
PATH_PREP_SAMPLE_RETIRED = "/{prep_sample_idx}/retired"

URL_PREP_SAMPLE_PREFIX = f"{API_PREFIX}{PATH_PREP_SAMPLE_PREFIX}"
URL_PREP_SAMPLE_STUDY_LIST = f"{URL_PREP_SAMPLE_PREFIX}{PATH_PREP_SAMPLE_STUDY_LIST}"
URL_PREP_SAMPLE_RETIRED = f"{URL_PREP_SAMPLE_PREFIX}{PATH_PREP_SAMPLE_RETIRED}"
