"""Config-level `EnaResolver` selection — swapping backends is a change here, never
in callers. Mirrors `qiita_compute_orchestrator.main._build_backend`'s
factory-raises-on-unknown pattern for `ComputeBackend`."""

from __future__ import annotations

from .http_resolver import HttpEnaResolver
from .miint_resolver import MiintEnaResolver
from .resolver import EnaResolver

BACKEND_MIINT = "miint"
BACKEND_HTTP = "http"


def get_resolver(backend: str = BACKEND_MIINT) -> EnaResolver:
    """Return the `EnaResolver` for `backend`: `"miint"` (default) drives DuckDB +
    the miint extension; `"http"` is the experimental, off-by-default ENA Portal API
    fallback. Raises `ValueError` on an unknown name — never silently defaults."""
    if backend == BACKEND_MIINT:
        return MiintEnaResolver()
    if backend == BACKEND_HTTP:
        return HttpEnaResolver()
    raise ValueError(
        f"unknown ENA resolver backend={backend!r}; expected {BACKEND_MIINT!r} or {BACKEND_HTTP!r}"
    )
