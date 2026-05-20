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

Currently covers /feature/* and /reference/*; other tags
(/auth, /admin, /user) still hardcode their paths and will migrate as
they're touched.
"""

from enum import StrEnum

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
PATH_REFERENCE_DOGET = "/{reference_idx}/ticket/doget"

URL_REFERENCE_PREFIX = f"{API_PREFIX}{PATH_REFERENCE_PREFIX}"
URL_REFERENCE_BY_IDX = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_BY_IDX}"
URL_REFERENCE_STATUS = f"{URL_REFERENCE_PREFIX}{PATH_REFERENCE_STATUS}"
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
