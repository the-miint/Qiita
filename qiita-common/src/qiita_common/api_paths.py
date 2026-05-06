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
