"""Centralized REST path constants — API contract shared by routes, tests,
and clients.

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

from qiita_common.auth_constants import API_PREFIX

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
# /library/*
# =============================================================================
# The single transport between workflow runners (orchestrator) and the
# control-plane library primitives. Per-primitive request shape is
# validated inside the dispatch handler; URLs do not vary by primitive.

PATH_LIBRARY_PREFIX = "/library"
PATH_LIBRARY_NAME = "/{name}"

URL_LIBRARY_PREFIX = f"{API_PREFIX}{PATH_LIBRARY_PREFIX}"
URL_LIBRARY_NAME = f"{URL_LIBRARY_PREFIX}{PATH_LIBRARY_NAME}"
