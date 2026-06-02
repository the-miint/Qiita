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


# =============================================================================
# /step/* — orchestrator HTTP API
# =============================================================================
# Single endpoint the control-plane runner uses to dispatch a workflow
# `step:` entry to the orchestrator's ComputeBackend. Synchronous for now:
# the request blocks for the duration of `backend.run_step`. Async +
# callback model is deferred to when SlurmBackend is wired (see
# docs/architecture.md "Compute Orchestrator").

PATH_STEP_PREFIX = "/step"
PATH_STEP_RUN = "/run"

URL_STEP_PREFIX = f"{API_PREFIX}{PATH_STEP_PREFIX}"
URL_STEP_RUN = f"{URL_STEP_PREFIX}{PATH_STEP_RUN}"


# =============================================================================
# /work-ticket/* — control-plane work-ticket lifecycle
# =============================================================================
# Submission (POST root) creates a ticket and fires an in-process
# `asyncio.Task` calling `runner.run_workflow` (option C, in-process
# dispatch — no polling worker). The /run endpoint is the human-override
# path that resets a FAILED ticket and re-dispatches; auto-retry is not
# implemented, so /run is the only retry mechanism.

PATH_WORK_TICKET_PREFIX = "/work-ticket"
PATH_WORK_TICKET_ROOT = ""  # POST against the prefix itself
PATH_WORK_TICKET_BY_IDX = "/{work_ticket_idx}"
PATH_WORK_TICKET_RUN = "/{work_ticket_idx}/run"

URL_WORK_TICKET_PREFIX = f"{API_PREFIX}{PATH_WORK_TICKET_PREFIX}"
URL_WORK_TICKET_BY_IDX = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_BY_IDX}"
URL_WORK_TICKET_RUN = f"{URL_WORK_TICKET_PREFIX}{PATH_WORK_TICKET_RUN}"


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


# =============================================================================
# /sequence-range/* — control-plane sequence_idx allocator
# =============================================================================
# Mints contiguous bigint ranges (`sequence_idx_start..stop`) the data
# plane uses to key raw sequencing reads. POST is service-account-only
# (Scope.SEQUENCE_RANGE_MINT); GET piggybacks on Scope.PREP_SAMPLE_READ.

PATH_SEQUENCE_RANGE_PREFIX = "/sequence-range"
PATH_SEQUENCE_RANGE_ROOT = ""  # POST against the prefix itself
PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE = "/{prep_sample_idx}"

URL_SEQUENCE_RANGE_PREFIX = f"{API_PREFIX}{PATH_SEQUENCE_RANGE_PREFIX}"
URL_SEQUENCE_RANGE_BY_PREP_SAMPLE = (
    f"{URL_SEQUENCE_RANGE_PREFIX}{PATH_SEQUENCE_RANGE_BY_PREP_SAMPLE}"
)


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

URL_ADMIN_PREFIX = f"{API_PREFIX}{PATH_ADMIN_PREFIX}"
URL_ADMIN_SERVICE_ACCOUNT = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_SERVICE_ACCOUNT}"
URL_ADMIN_PRINCIPAL_DISABLED = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_DISABLED}"
URL_ADMIN_PRINCIPAL_RETIRED = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_RETIRED}"
URL_ADMIN_PRINCIPAL_SYSTEM_ROLE = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_SYSTEM_ROLE}"
URL_ADMIN_AUDIT = f"{URL_ADMIN_PREFIX}{PATH_ADMIN_AUDIT}"
URL_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS = (
    f"{URL_ADMIN_PREFIX}{PATH_ADMIN_PRINCIPAL_REVOKE_ALL_TOKENS}"
)


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
# Bulk lookup of study_idx by ebi_study_accession; same body-vs-querystring
# rationale as the biosample lookup variants.
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
PATH_SEQUENCING_RUN_SEQUENCED_POOL = "/{sequencing_run_idx}/sequenced-pool"
PATH_SEQUENCED_POOL_PREFLIGHT = (
    "/{sequencing_run_idx}/sequenced-pool/{sequenced_pool_idx}/preflight"
)

URL_SEQUENCING_RUN_PREFIX = f"{API_PREFIX}{PATH_SEQUENCING_RUN_PREFIX}"
URL_SEQUENCING_RUN_SEQUENCED_POOL = (
    f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCING_RUN_SEQUENCED_POOL}"
)
URL_SEQUENCED_POOL_PREFLIGHT = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_POOL_PREFLIGHT}"


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
# Bulk lookup of biosample_idx by biosample_accession. POST (not GET)
# because the accession list lives in the body — a typical bcl-convert
# pool carries ~384 accessions, which exceeds nginx's default URL-line
# cap when threaded through query-params. The response shape carries the
# resolved {accession: idx} map plus a missing[] list so the CLI can
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
PATH_SEQUENCED_SAMPLE_LIST_BY_STUDY = "/{study_idx}/sequenced-sample/list-idxs"

PATH_SEQUENCED_SAMPLE_PREFIX = "/sequenced-sample"
PATH_SEQUENCED_SAMPLE_BY_IDX = "/{sequenced_sample_idx}"

URL_SEQUENCED_SAMPLE_FROM_RUN = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_FROM_RUN}"
URL_SEQUENCED_SAMPLE_LIST_BY_RUN = f"{URL_SEQUENCING_RUN_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_RUN}"
URL_SEQUENCED_SAMPLE_LIST_BY_STUDY = f"{URL_STUDY_PREFIX}{PATH_SEQUENCED_SAMPLE_LIST_BY_STUDY}"
URL_SEQUENCED_SAMPLE_PREFIX = f"{API_PREFIX}{PATH_SEQUENCED_SAMPLE_PREFIX}"
URL_SEQUENCED_SAMPLE_BY_IDX = f"{URL_SEQUENCED_SAMPLE_PREFIX}{PATH_SEQUENCED_SAMPLE_BY_IDX}"
